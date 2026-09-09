package prometheus

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"opscopilot/internal/connector"
	"opscopilot/pkg/readonly"
)

// mockProm 启动一个记录所有请求方法/路径的 httptest 服务。
// 按路径返回对应 JSON，并捕获请求方法与 Authorization 头用于只读断言。
func mockProm(t *testing.T, alerts, targets, query string) (*httptest.Server, *[]recordedReq) {
	t.Helper()
	var reqs []recordedReq
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqs = append(reqs, recordedReq{
			method: r.Method,
			path:   r.URL.Path,
			query:  r.URL.RawQuery,
			auth:   r.Header.Get("Authorization"),
		})
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/-/healthy":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("OK"))
		case r.URL.Path == "/api/v1/alerts":
			_, _ = w.Write([]byte(alerts))
		case r.URL.Path == "/api/v1/targets":
			_, _ = w.Write([]byte(targets))
		case r.URL.Path == "/api/v1/query":
			_, _ = w.Write([]byte(query))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	return srv, &reqs
}

type recordedReq struct {
	method string
	path   string
	query  string
	auth   string
}

const sampleAlerts = `{
  "status": "success",
  "data": {
    "alerts": [
      {
        "fingerprint": "fp-1",
        "generatorURL": "http://prom/graph?g0.expr=up",
        "labels": {"severity":"warning","alertname":"HighCpu","instance":"node-1"},
        "annotations": {"summary":"CPU high on node-1"},
        "startsAt": "2026-09-09T10:00:00Z",
        "endsAt": "2026-09-09T10:05:00Z",
        "status": {"state":"firing"}
      }
    ]
  }
}`

const sampleTargets = `{
  "status": "success",
  "data": {
    "activeTargets": [
      {
        "labels": {"job":"node","instance":"10.0.0.1:9100"},
        "scrapePool": "node",
        "scrapeUrl": "http://10.0.0.1:9100/metrics",
        "health": "up"
      }
    ]
  }
}`

const sampleQuery = `{
  "status": "success",
  "data": {
    "resultType": "vector",
    "result": [
      {"metric":{"__name__":"up","job":"node","instance":"10.0.0.1:9100"},"value":[1757400000,"1"]},
      {"metric":{"__name__":"up","job":"node","instance":"10.0.0.2:9100"},"value":[1757400000,"0"]}
    ]
  }
}`

func TestNew_Validation(t *testing.T) {
	if _, err := New(Config{ID: "", BaseURL: "http://x"}); err == nil {
		t.Fatal("empty ID should error")
	}
	if _, err := New(Config{ID: "p1", BaseURL: ""}); err == nil {
		t.Fatal("empty BaseURL should error")
	}
	c, err := New(Config{ID: "p1", BaseURL: "http://localhost:9090/"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if c.cfg.BaseURL != "http://localhost:9090" {
		t.Fatalf("BaseURL not normalized: %q", c.cfg.BaseURL)
	}
	if c.ID() != "p1" || c.Type() != "prometheus" {
		t.Fatalf("ID/Type mismatch: %q/%q", c.ID(), c.Type())
	}
}

// TestNew_ReadOnlyCredentialEnforced 只读闸门（ADR-002）：
// 写权限/过期凭证必须在构造期被拒，不得进入采集链路。
func TestNew_ReadOnlyCredentialEnforced(t *testing.T) {
	// 非只读凭证
	bad := readonly.Credential{ID: "c1", Secret: "tok", ReadOnly: false}
	if _, err := New(Config{ID: "p1", BaseURL: "http://x", Credential: &bad}); !errors.Is(err, readonly.ErrNotReadOnly) {
		t.Fatalf("non-readonly credential must be rejected, got %v", err)
	}
	// 已过期的只读凭证
	expired := readonly.Credential{ID: "c2", Secret: "tok", ReadOnly: true, ExpiresAt: time.Now().Add(-time.Hour)}
	if _, err := New(Config{ID: "p1", BaseURL: "http://x", Credential: &expired}); !errors.Is(err, readonly.ErrExpired) {
		t.Fatalf("expired credential must be rejected, got %v", err)
	}
	// 合法只读凭证通过，且 Secret 自动作为 Bearer Token
	good := readonly.NewBearer("c3", "secret-tok")
	c, err := New(Config{ID: "p1", BaseURL: "http://x", Credential: &good})
	if err != nil {
		t.Fatalf("valid readonly credential should pass, got %v", err)
	}
	if c.cfg.Token != "secret-tok" {
		t.Fatalf("Token = %q, want secret-tok (from credential.Secret)", c.cfg.Token)
	}
}

func TestHealthCheck(t *testing.T) {
	srv, _ := mockProm(t, sampleAlerts, sampleTargets, sampleQuery)
	c, _ := New(Config{ID: "p1", BaseURL: srv.URL})
	h, err := c.HealthCheck(context.Background())
	if err != nil {
		t.Fatalf("HealthCheck err: %v", err)
	}
	if h.Status != connector.HealthHealthy {
		t.Fatalf("health = %q, want healthy", h.Status)
	}

	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(bad.Close)
	c2, _ := New(Config{ID: "p2", BaseURL: bad.URL})
	h2, err := c2.HealthCheck(context.Background())
	if err != nil {
		t.Fatalf("degraded should not return err: %v", err)
	}
	if h2.Status != connector.HealthDegraded {
		t.Fatalf("health = %q, want degraded", h2.Status)
	}
}

// TestCollect_UnsupportedOptions Since/Scope 不得被静默忽略，
// 必须返回 connector.ErrUnsupported 让调用方知晓过滤未生效。
func TestCollect_UnsupportedOptions(t *testing.T) {
	srv, _ := mockProm(t, sampleAlerts, sampleTargets, sampleQuery)
	c, _ := New(Config{ID: "p1", BaseURL: srv.URL})

	if _, err := c.Collect(context.Background(), connector.CollectRequest{Since: time.Now()}); !errors.Is(err, connector.ErrUnsupported) {
		t.Fatalf("Since must return ErrUnsupported, got %v", err)
	}
	if _, err := c.Collect(context.Background(), connector.CollectRequest{Scope: "ns-1"}); !errors.Is(err, connector.ErrUnsupported) {
		t.Fatalf("Scope must return ErrUnsupported, got %v", err)
	}
}

func TestCollect_AlertNormalization(t *testing.T) {
	srv, reqs := mockProm(t, sampleAlerts, sampleTargets, sampleQuery)
	c, _ := New(Config{ID: "prom-prod", BaseURL: srv.URL, TenantID: "t-a"})

	res, err := c.Collect(context.Background(), connector.CollectRequest{})
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if len(res.Alerts) != 1 {
		t.Fatalf("alerts len = %d, want 1", len(res.Alerts))
	}
	a := res.Alerts[0]
	if a.Fingerprint != "fp-1" || a.Severity != "warning" || a.Status != "firing" {
		t.Errorf("alert normalization mismatch: %+v", a)
	}
	if a.Source != "prom-prod" || res.TenantID != "t-a" {
		t.Errorf("source/tenant mismatch: %q/%q", a.Source, res.TenantID)
	}
	if a.StartsAt.IsZero() {
		t.Error("StartsAt should parse to non-zero")
	}
	if len(*reqs) == 0 || !strings.HasSuffix((*reqs)[0].path, "/api/v1/alerts") {
		t.Errorf("unexpected request path: %+v", *reqs)
	}
}

// TestCollect_Metrics 指标采集：配置了 PromQL 才拉取指标，
// 未配置时（默认）不采集，保持向后兼容。
func TestCollect_Metrics(t *testing.T) {
	// 未配置 Queries：不采集指标，且不报错
	srvNoQ, _ := mockProm(t, sampleAlerts, sampleTargets, sampleQuery)
	cNoQ, _ := New(Config{ID: "p1", BaseURL: srvNoQ.URL})
	resNoQ, err := cNoQ.Collect(context.Background(), connector.CollectRequest{})
	if err != nil {
		t.Fatalf("Collect without queries: %v", err)
	}
	if len(resNoQ.Metrics) != 0 {
		t.Fatalf("no queries configured, Metrics len = %d, want 0", len(resNoQ.Metrics))
	}

	// 配置 Queries：拉取并归一化
	srv, _ := mockProm(t, sampleAlerts, sampleTargets, sampleQuery)
	c, _ := New(Config{ID: "prom-prod", BaseURL: srv.URL, Queries: []Query{{Name: "node_up", Expr: `up{job="node"}`}}})
	res, err := c.Collect(context.Background(), connector.CollectRequest{})
	if err != nil {
		t.Fatalf("Collect with queries: %v", err)
	}
	if len(res.Metrics) != 2 {
		t.Fatalf("Metrics len = %d, want 2", len(res.Metrics))
	}
	m := res.Metrics[0]
	if m.Name != "node_up" {
		t.Errorf("Name = %q, want node_up (query name takes precedence)", m.Name)
	}
	if m.Value != 1 {
		t.Errorf("Value = %v, want 1", m.Value)
	}
	if m.Timestamp.IsZero() {
		t.Error("Timestamp should be parsed from sample")
	}
	if m.Source != "prom-prod" {
		t.Errorf("Source = %q", m.Source)
	}
	if m.Labels["instance"] != "10.0.0.1:9100" {
		t.Errorf("Labels[instance] = %q", m.Labels["instance"])
	}
}

// TestCollect_MetricsFallbackName 未指定 Query.Name 时回退到 __name__ 标签。
func TestCollect_MetricsFallbackName(t *testing.T) {
	srv, _ := mockProm(t, sampleAlerts, sampleTargets, sampleQuery)
	c, _ := New(Config{ID: "p1", BaseURL: srv.URL, Queries: []Query{{Expr: "up"}}})
	res, err := c.Collect(context.Background(), connector.CollectRequest{})
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if len(res.Metrics) == 0 {
		t.Fatal("expected metrics")
	}
	if res.Metrics[0].Name != "up" {
		t.Errorf("Name = %q, want up (fallback to __name__)", res.Metrics[0].Name)
	}
}

func TestCollect_OldStatusFormat(t *testing.T) {
	old := `{"status":"success","data":{"alerts":[{"fingerprint":"fp-2","labels":{"severity":"critical"},"status":"firing"}]}}`
	srv, _ := mockProm(t, old, sampleTargets, sampleQuery)
	c, _ := New(Config{ID: "p1", BaseURL: srv.URL})
	res, err := c.Collect(context.Background(), connector.CollectRequest{})
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if len(res.Alerts) != 1 {
		t.Fatalf("alerts len = %d", len(res.Alerts))
	}
	if res.Alerts[0].Status != "firing" {
		t.Errorf("old-format Status = %q, want firing", res.Alerts[0].Status)
	}
}

func TestDiscover_Normalization(t *testing.T) {
	srv, _ := mockProm(t, sampleAlerts, sampleTargets, sampleQuery)
	c, _ := New(Config{ID: "prom-prod", BaseURL: srv.URL})

	res, err := c.Discover(context.Background())
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(res.Nodes) != 1 {
		t.Fatalf("nodes len = %d, want 1", len(res.Nodes))
	}
	n := res.Nodes[0]
	if n.Key != "prometheus://node/10.0.0.1:9100" || n.Type != "node" || n.Source != "prom-prod" {
		t.Errorf("node normalization mismatch: %+v", n)
	}
	// target 健康状态必须保留，不能归一化丢信息
	if n.Labels["health"] != "up" {
		t.Errorf("Labels[health] = %q, want up (health must be preserved)", n.Labels["health"])
	}
}

// TestReadOnlyOnlyGET 只读纪律：所有请求必须是 GET，且按配置透传 Bearer。
func TestReadOnlyOnlyGET(t *testing.T) {
	srv, reqs := mockProm(t, sampleAlerts, sampleTargets, sampleQuery)
	c, _ := New(Config{ID: "p1", BaseURL: srv.URL, Token: "secret-token"})

	if _, err := c.HealthCheck(context.Background()); err != nil {
		t.Fatalf("HealthCheck: %v", err)
	}
	if _, err := c.Collect(context.Background(), connector.CollectRequest{}); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if _, err := c.Discover(context.Background()); err != nil {
		t.Fatalf("Discover: %v", err)
	}

	for _, r := range *reqs {
		if r.method != http.MethodGet {
			t.Errorf("non-GET request issued: %s %s", r.method, r.path)
		}
		if r.auth != "Bearer secret-token" {
			t.Errorf("request %s missing bearer: %q", r.path, r.auth)
		}
	}
	if len(*reqs) < 3 {
		t.Fatalf("expected at least 3 requests, got %d", len(*reqs))
	}
}

// TestResponseSizeLimit 响应体必须有上限，防止异常响应打爆内存。
func TestResponseSizeLimit(t *testing.T) {
	big := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(strings.Repeat("A", 8192)))
	}))
	t.Cleanup(big.Close)

	c, err := New(Config{ID: "p1", BaseURL: big.URL, MaxResponseBytes: 1024})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	_, err = c.Collect(context.Background(), connector.CollectRequest{})
	if err == nil {
		t.Fatal("oversized response should be rejected")
	}
	if !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("err = %v, want size-limit error", err)
	}
}

// TestCollect_MalformedSampleFails 样本格式异常必须显式失败，不得静默跳过——
// 否则降噪/拓扑会基于一份"悄悄缺了样本"的指标继续计算。
func TestCollect_MalformedSampleFails(t *testing.T) {
	badQuery := `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up"},"value":[1757400000]}]}}`
	srv, _ := mockProm(t, sampleAlerts, sampleTargets, badQuery)
	c, _ := New(Config{ID: "p1", BaseURL: srv.URL, Queries: []Query{{Expr: "up"}}})

	if _, err := c.Collect(context.Background(), connector.CollectRequest{}); err == nil {
		t.Fatal("malformed sample should fail loudly, not be skipped silently")
	}
}

// TestNew_EmptyQueryExprRejected 空 PromQL 属配置错误，应在构造期暴露。
func TestNew_EmptyQueryExprRejected(t *testing.T) {
	if _, err := New(Config{ID: "p1", BaseURL: "http://x", Queries: []Query{{Name: "x"}}}); err == nil {
		t.Fatal("empty PromQL expression should be rejected at construction")
	}
}

// TestQueryUsesPOSTForLongExpr 超长 PromQL 放不进 URL，应自动降级为 POST
// （仍属只读查询），保证功能可用。
func TestQueryUsesPOSTForLongExpr(t *testing.T) {
	srv, reqs := mockProm(t, sampleAlerts, sampleTargets, sampleQuery)
	longExpr := `up{job="` + strings.Repeat("a", 2000) + `"}`

	c, err := New(Config{ID: "p1", BaseURL: srv.URL, Queries: []Query{{Name: "x", Expr: longExpr}}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	res, err := c.Collect(context.Background(), connector.CollectRequest{})
	if err != nil {
		t.Fatalf("Collect with long expr: %v", err)
	}
	if len(res.Metrics) != 2 {
		t.Fatalf("Metrics len = %d, want 2", len(res.Metrics))
	}

	var sawQuery bool
	for _, r := range *reqs {
		if r.path == "/api/v1/query" {
			sawQuery = true
			if r.method != http.MethodPost {
				t.Errorf("long PromQL should fall back to POST, got %s", r.method)
			}
		}
	}
	if !sawQuery {
		t.Fatal("query request not observed")
	}
}

// TestCollect_TimestampPrecision 指标时间戳必须保留亚秒精度，
// 不能因 int64 截断退化到整秒。
func TestCollect_TimestampPrecision(t *testing.T) {
	// 1757400000.5 秒 = 整秒 + 500ms
	q := `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up"},"value":[1757400000.5,"1"]}]}}`
	srv, _ := mockProm(t, sampleAlerts, sampleTargets, q)
	c, _ := New(Config{ID: "p1", BaseURL: srv.URL, Queries: []Query{{Expr: "up"}}})

	res, err := c.Collect(context.Background(), connector.CollectRequest{})
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if len(res.Metrics) != 1 {
		t.Fatalf("Metrics len = %d, want 1", len(res.Metrics))
	}
	want := time.Unix(1757400000, int64(500*time.Millisecond))
	got := res.Metrics[0].Timestamp
	// 允许浮点换算带来的纳秒级误差，但必须远小于"整秒截断"（500ms）
	if d := got.Sub(want); d > time.Microsecond || d < -time.Microsecond {
		t.Fatalf("Timestamp = %v, want ~%v (sub-second precision lost? diff=%v)", got, want, d)
	}
}

// TestCollect_PartialSuccessAggregatesErrors 单条查询失败不应吞掉其余已成功的指标，
// 且失败要聚合返回（显式，不是静默忽略）。
func TestCollect_PartialSuccessAggregatesErrors(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/api/v1/alerts":
			_, _ = w.Write([]byte(sampleAlerts))
		case r.URL.Path == "/api/v1/query":
			// 仅让 bad_expr 失败，其余正常返回
			if strings.Contains(r.URL.RawQuery, "bad_expr") {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			_, _ = w.Write([]byte(sampleQuery))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)

	c, err := New(Config{ID: "p1", BaseURL: srv.URL, Queries: []Query{
		{Name: "good", Expr: "up"},
		{Name: "bad", Expr: "bad_expr"},
	}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	res, err := c.Collect(context.Background(), connector.CollectRequest{})
	if err == nil {
		t.Fatal("expected aggregated error for the failing query")
	}
	if res == nil || len(res.Metrics) != 2 {
		t.Fatalf("successful query metrics must be retained, got %v", res)
	}
}
