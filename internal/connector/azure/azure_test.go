package azure

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"opscopilot/internal/connector"
	"opscopilot/pkg/readonly"
)

func testCredential() *readonly.Credential {
	c := readonly.NewBearer("azure-cred", "test-token")
	return &c
}

func testConfig(baseURL string) Config {
	return Config{
		ID:             "azure-test",
		SubscriptionID: "sub-0001",
		Credential:     testCredential(),
		BaseURL:        baseURL,
	}
}

// vmJSON 构造一台虚拟机的 ARM 响应片段。
func vmJSON(id, name, vmID, rg string) string {
	return fmt.Sprintf(`{
		"id": "/subscriptions/sub-0001/resourceGroups/%s/providers/Microsoft.Compute/virtualMachines/%s",
		"name": %q,
		"location": "chinaeast2",
		"tags": {"env": "prod", "team": "ops"},
		"properties": {
			"vmId": %q,
			"provisioningState": "Succeeded",
			"hardwareProfile": {"vmSize": "Standard_D2s_v3"}
		}
	}`, rg, name, name, vmID)
}

// newFakeARM 启动假 ARM 服务：校验 Bearer，返回单一列表响应。
func newFakeARM(t *testing.T, body string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-token" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestNew_Validation(t *testing.T) {
	bad := readonly.Credential{ID: "x", Secret: "s", ReadOnly: false} // 写权限
	cases := []struct {
		name string
		cfg  Config
	}{
		{"no id", Config{SubscriptionID: "s", Credential: testCredential()}},
		{"no subscription", Config{ID: "a", Credential: testCredential()}},
		{"no credential", Config{ID: "a", SubscriptionID: "s"}},
		{"write credential", Config{ID: "a", SubscriptionID: "s", Credential: &bad}},
	}
	for _, tc := range cases {
		if _, err := New(tc.cfg); err == nil {
			t.Errorf("%s: expected error, got nil", tc.name)
		}
	}
}

func TestDiscover_MapsVMs(t *testing.T) {
	body := fmt.Sprintf(`{"value": [%s, %s]}`,
		vmJSON("r1", "vm-web-01", "guid-aaa", "rg-web"),
		vmJSON("r2", "vm-db-01", "guid-bbb", "rg-db"))
	srv := newFakeARM(t, body)

	d, err := New(testConfig(srv.URL))
	if err != nil {
		t.Fatal(err)
	}
	res, err := d.Discover(context.Background())
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(res.Nodes) != 2 {
		t.Fatalf("nodes = %d, want 2", len(res.Nodes))
	}
	n := res.Nodes[0]
	if n.Key != "azure://vm/guid-aaa" {
		t.Errorf("key = %q, want azure://vm/guid-aaa (stable GUID, not mutable name)", n.Key)
	}
	if n.Type != "vm" {
		t.Errorf("type = %q, want vm", n.Type)
	}
	if n.Source != "azure-test" {
		t.Errorf("source = %q, want azure-test", n.Source)
	}
	want := map[string]string{
		"name": "vm-web-01", "resource_group": "rg-web", "location": "chinaeast2",
		"vm_size": "Standard_D2s_v3", "provisioning_state": "Succeeded",
		"tag:env": "prod", "tag:team": "ops", "vm_id": "guid-aaa",
		"subscription_id": "sub-0001",
	}
	for k, v := range want {
		if n.Labels[k] != v {
			t.Errorf("label[%q] = %q, want %q", k, n.Labels[k], v)
		}
	}
	if n.ObservedAt.IsZero() {
		t.Error("ObservedAt must be filled")
	}
	// 归一化结果可直接对接 topology.NodeInput（字段对齐由编译期保证，这里只做形状确认）
	_ = connector.ResourceNode{}
}

func TestDiscover_FollowsNextLink(t *testing.T) {
	page1 := fmt.Sprintf(`{"value": [%s], "nextLink": "PLACEHOLDER"}`,
		vmJSON("r1", "vm-1", "guid-1", "rg"))
	page2 := fmt.Sprintf(`{"value": [%s]}`, vmJSON("r2", "vm-2", "guid-2", "rg"))

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-token" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if strings.Contains(r.URL.RawQuery, "marker=2") {
			_, _ = w.Write([]byte(page2))
			return
		}
		// nextLink 指向第二页
		_, _ = w.Write([]byte(strings.Replace(page1, "PLACEHOLDER", srvURL+"/vms?marker=2", 1)))
	}))
	defer srv.Close()
	srvURL = srv.URL

	d, err := New(testConfig(srv.URL))
	if err != nil {
		t.Fatal(err)
	}
	res, err := d.Discover(context.Background())
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(res.Nodes) != 2 {
		t.Fatalf("nodes = %d, want 2 (across 2 pages)", len(res.Nodes))
	}
	if res.Nodes[0].Key != "azure://vm/guid-1" || res.Nodes[1].Key != "azure://vm/guid-2" {
		t.Errorf("nodes = [%s, %s], want both pages followed", res.Nodes[0].Key, res.Nodes[1].Key)
	}
}

// srvURL 用于在 handler 内引用自身地址（httptest URL 在 Listen 后才确定）。
var srvURL string

func TestDiscover_InfiniteNextLinkGuarded(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 永远返回带 nextLink 的响应——恶意/异常服务端。
		// 自引用地址用包级 srvURL（声明未完成时闭包内不能引用 srv 本身）。
		resp := map[string]string{"nextLink": srvURL + "/vms?marker=loop"}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()
	srvURL = srv.URL

	d, err := New(testConfig(srv.URL))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.Discover(context.Background()); err == nil {
		t.Error("infinite nextLink must surface an error, not loop forever")
	}
}

func TestDiscover_Unauthorized(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()

	d, err := New(testConfig(srv.URL))
	if err != nil {
		t.Fatal(err)
	}
	_, err = d.Discover(context.Background())
	if err == nil || !strings.Contains(err.Error(), "401") {
		t.Errorf("error = %v, want HTTP 401 mention", err)
	}
}

func TestDiscover_MalformedJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"value": [`))
	}))
	defer srv.Close()

	d, _ := New(testConfig(srv.URL))
	if _, err := d.Discover(context.Background()); err == nil {
		t.Error("malformed JSON must be rejected")
	}
}

func TestDiscover_BodyTooLarge(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"value": []` + strings.Repeat(" ", 4096) + `}`))
	}))
	defer srv.Close()

	cfg := testConfig(srv.URL)
	cfg.MaxResponseBytes = 1024
	d, _ := New(cfg)
	if _, err := d.Discover(context.Background()); err == nil {
		t.Error("oversized response must be rejected")
	}
}

func TestCollect_ErrUnsupported(t *testing.T) {
	srv := newFakeARM(t, `{"value": []}`)
	d, _ := New(testConfig(srv.URL))
	_, err := d.Collect(context.Background(), connector.CollectRequest{})
	if !errors.Is(err, connector.ErrUnsupported) {
		t.Errorf("error = %v, want ErrUnsupported", err)
	}
}

func TestHealthCheck(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-token" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if strings.Contains(r.URL.Path, "/subscriptions/sub-0001") && !strings.Contains(r.URL.Path, "virtualMachines") {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	d, _ := New(testConfig(srv.URL))
	h, err := d.HealthCheck(context.Background())
	if err != nil || h.Status != connector.HealthHealthy {
		t.Errorf("health = %+v, %v; want healthy", h, err)
	}

	// 凭证被拒 → degraded（云 API 侧凭据失效是"半死"，不是连接器崩溃）
	deny := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer deny.Close()
	d2, _ := New(testConfig(deny.URL))
	h2, err := d2.HealthCheck(context.Background())
	if err != nil || h2.Status != connector.HealthDegraded {
		t.Errorf("health = %+v, %v; want degraded", h2, err)
	}
}

func TestResourceGroupOf(t *testing.T) {
	cases := map[string]string{
		"/subscriptions/s/resourceGroups/rg-1/providers/Microsoft.Compute/virtualMachines/vm": "rg-1",
		"/subscriptions/s/resourcegroups/rg-2/providers/x":                                    "rg-2", // 大小写不敏感
		"/subscriptions/s/providers/x":                                                        "",
	}
	for in, want := range cases {
		if got := resourceGroupOf(in); got != want {
			t.Errorf("resourceGroupOf(%q) = %q, want %q", in, got, want)
		}
	}
}
