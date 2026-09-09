package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"opscopilot/internal/topology"
)

// newTestWebhook 构造带严格节点校验的 webhook（与生产装配方式一致：
// 节点查找函数来自拓扑图）。
func newTestWebhook(t *testing.T) (*ChangeWebhook, *topology.ChangeStore) {
	t.Helper()
	b := topology.NewBuilder()
	if err := b.AddNode(topology.NodeInput{Key: "host:demo", Type: "host", Source: "test"}); err != nil {
		t.Fatalf("seed graph: %v", err)
	}
	g := b.Build()
	store := topology.NewChangeStore(func(k string) bool {
		_, ok := g.Nodes[k]
		return ok
	})
	h, err := NewChangeWebhook(store, "manual")
	if err != nil {
		t.Fatalf("NewChangeWebhook: %v", err)
	}
	return h, store
}

func post(t *testing.T, h *ChangeWebhook, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/changes", strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestChangeWebhook_PostOK(t *testing.T) {
	h, store := newTestWebhook(t)
	rec := post(t, h, `{"id":"git-abc123","node_key":"host:demo","type":"deploy",
		"source":"git","author":"thomas","summary":"v1.2.3 发布"}`)

	if rec.Code != http.StatusOK {
		t.Fatalf("code = %d, body = %s, want 200", rec.Code, rec.Body)
	}
	var resp struct {
		Status string               `json:"status"`
		Event  topology.ChangeEvent `json:"event"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("response not JSON: %v", err)
	}
	if resp.Status != "ok" {
		t.Errorf("status = %q, want ok", resp.Status)
	}
	if resp.Event.Confidence != topology.ConfidenceMedium {
		t.Errorf("default confidence = %q, want medium", resp.Event.Confidence)
	}
	if store.Len() != 1 {
		t.Errorf("store len = %d, want 1", store.Len())
	}
	// 事件确实可通过 ByNode 查到（验收：变更事件能关联到节点）
	got := store.ByNode("host:demo")
	if len(got) != 1 || got[0].ID != "git-abc123" {
		t.Errorf("ByNode did not return the recorded event: %+v", got)
	}
}

func TestChangeWebhook_DefaultSourceFilled(t *testing.T) {
	h, _ := newTestWebhook(t)
	rec := post(t, h, `{"id":"m1","node_key":"host:demo","type":"config_change"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("code = %d, body = %s", rec.Code, rec.Body)
	}
	var resp struct {
		Event topology.ChangeEvent `json:"event"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp.Event.Source != "manual" {
		t.Errorf("source = %q, want default %q", resp.Event.Source, "manual")
	}
}

func TestChangeWebhook_MethodNotAllowed(t *testing.T) {
	h, _ := newTestWebhook(t)
	req := httptest.NewRequest(http.MethodGet, "/changes", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("code = %d, want 405", rec.Code)
	}
	if allow := rec.Header().Get("Allow"); allow != http.MethodPost {
		t.Errorf("Allow header = %q, want POST", allow)
	}
}

func TestChangeWebhook_BadJSON(t *testing.T) {
	h, _ := newTestWebhook(t)
	if rec := post(t, h, `{not json`); rec.Code != http.StatusBadRequest {
		t.Errorf("broken json: code = %d, want 400", rec.Code)
	}
}

func TestChangeWebhook_UnknownFieldRejected(t *testing.T) {
	h, _ := newTestWebhook(t)
	// node_ky 是拼写错误：DisallowUnknownFields 必须当场拦下，而不是
	// 静默丢字段导致事件关联不上节点。
	rec := post(t, h, `{"id":"m2","node_ky":"host:demo","type":"deploy"}`)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("unknown field: code = %d, want 400, body = %s", rec.Code, rec.Body)
	}
}

func TestChangeWebhook_MissingFields(t *testing.T) {
	h, _ := newTestWebhook(t)
	cases := map[string]string{
		"no id":          `{"node_key":"host:demo","type":"deploy"}`,
		"no node_key":    `{"id":"m3","type":"deploy"}`,
		"bad type":       `{"id":"m4","node_key":"host:demo","type":"restart"}`,
		"bad confidence": `{"id":"m5","node_key":"host:demo","type":"deploy","confidence":"HIGH"}`,
	}
	for name, body := range cases {
		if rec := post(t, h, body); rec.Code != http.StatusBadRequest {
			t.Errorf("%s: code = %d, want 400 (body=%s)", name, rec.Code, rec.Body)
		}
	}
}

func TestChangeWebhook_DuplicateConflict(t *testing.T) {
	h, _ := newTestWebhook(t)
	body := `{"id":"dup","node_key":"host:demo","type":"deploy"}`
	if rec := post(t, h, body); rec.Code != http.StatusOK {
		t.Fatalf("first post: code = %d, want 200", rec.Code)
	}
	rec := post(t, h, body)
	if rec.Code != http.StatusConflict {
		t.Errorf("duplicate post: code = %d, want 409", rec.Code)
	}
}

func TestChangeWebhook_NodeNotFound422(t *testing.T) {
	h, _ := newTestWebhook(t)
	rec := post(t, h, `{"id":"ghost","node_key":"host:ghost","type":"deploy"}`)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Errorf("code = %d, want 422, body = %s", rec.Code, rec.Body)
	}
}

func TestChangeWebhook_BodyTooLarge(t *testing.T) {
	h, _ := newTestWebhook(t)
	h.MaxBodyBytes = 64
	big := fmt.Sprintf(`{"id":"big","node_key":"host:demo","type":"deploy","summary":"%s"}`,
		strings.Repeat("x", 128))
	rec := post(t, h, big)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("code = %d, want 413", rec.Code)
	}
}

func TestChangeWebhook_NilStoreRejected(t *testing.T) {
	if _, err := NewChangeWebhook(nil, "manual"); err == nil {
		t.Error("nil store must be rejected at construction")
	}
}

// TestChangeWebhook_AssociationWindow 端到端验证核心用例：
// "告警发生前 30 分钟内该节点有没有变更"——变更先入库，再用时间窗查询命中。
func TestChangeWebhook_AssociationWindow(t *testing.T) {
	h, store := newTestWebhook(t)
	alertAt := time.Now()

	// 模拟 40 分钟前的部署与 10 分钟前的配置修改
	body1 := fmt.Sprintf(`{"id":"dep1","node_key":"host:demo","type":"deploy","occurred_at":%q}`,
		alertAt.Add(-40*time.Minute).Format(time.RFC3339Nano))
	body2 := fmt.Sprintf(`{"id":"cfg1","node_key":"host:demo","type":"config_change","occurred_at":%q}`,
		alertAt.Add(-10*time.Minute).Format(time.RFC3339Nano))
	if rec := post(t, h, body1); rec.Code != http.StatusOK {
		t.Fatalf("post dep1: code = %d, body = %s", rec.Code, rec.Body)
	}
	if rec := post(t, h, body2); rec.Code != http.StatusOK {
		t.Fatalf("post cfg1: code = %d, body = %s", rec.Code, rec.Body)
	}

	hits := store.ByNodeWithin("host:demo", alertAt.Add(-30*time.Minute), alertAt)
	if len(hits) != 1 || hits[0].ID != "cfg1" {
		t.Errorf("30min window hits = %+v, want only cfg1", hits)
	}
	hits = store.ByNodeWithin("host:demo", alertAt.Add(-time.Hour), alertAt)
	if len(hits) != 2 {
		t.Errorf("1h window hits = %d, want 2", len(hits))
	}
}
