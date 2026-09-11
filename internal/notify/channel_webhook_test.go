// channel_webhook_test.go W9-2 渠道投递测试：载荷模板、失败语义、校验。
package notify

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// captureSink 记录收到的请求体与请求头（并发安全）。
type captureSink struct {
	mu       sync.Mutex
	bodies   []string
	status   int
	respBody string
}

func (s *captureSink) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		s.mu.Lock()
		s.bodies = append(s.bodies, string(b))
		s.mu.Unlock()
		st := s.status
		if st == 0 {
			st = http.StatusOK
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(st)
		_, _ = io.WriteString(w, s.respBody)
	}
}

func (s *captureSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.bodies)
}

func (s *captureSink) last() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.bodies) == 0 {
		return ""
	}
	return s.bodies[len(s.bodies)-1]
}

func TestWebhookChannelPayloads(t *testing.T) {
	sink := &captureSink{}
	srv := httptest.NewServer(sink.handler())
	defer srv.Close()

	msg := Message{TenantID: "t1", ClusterKey: "c:fp@1", Title: "HighDisk", Severity: "critical", Body: "节点 n1 磁盘告急"}
	cases := []struct {
		kind  string
		check func(t *testing.T, raw string)
	}{
		{KindGeneric, func(t *testing.T, raw string) {
			var got map[string]any
			if err := json.Unmarshal([]byte(raw), &got); err != nil {
				t.Fatalf("generic payload not json: %v", err)
			}
			if got["cluster_key"] != "c:fp@1" || got["severity"] != "critical" || got["tenant_id"] != "t1" {
				t.Fatalf("generic payload fields wrong: %v", got)
			}
			if got["source"] != "opscopilot" {
				t.Fatalf("generic payload missing source marker: %v", got)
			}
		}},
		{KindFeishu, func(t *testing.T, raw string) {
			var got struct {
				MsgType string `json:"msg_type"`
				Content struct {
					Text string `json:"text"`
				} `json:"content"`
			}
			if err := json.Unmarshal([]byte(raw), &got); err != nil {
				t.Fatalf("feishu payload not json: %v", err)
			}
			if got.MsgType != "text" || !strings.Contains(got.Content.Text, "HighDisk") {
				t.Fatalf("feishu payload wrong: %s", raw)
			}
		}},
		{KindWecom, func(t *testing.T, raw string) {
			var got struct {
				MsgType string `json:"msgtype"`
				Text    struct {
					Content string `json:"content"`
				} `json:"text"`
			}
			if err := json.Unmarshal([]byte(raw), &got); err != nil {
				t.Fatalf("wecom payload not json: %v", err)
			}
			if got.MsgType != "text" || !strings.Contains(got.Text.Content, "c:fp@1") {
				t.Fatalf("wecom payload wrong: %s", raw)
			}
		}},
	}
	for _, c := range cases {
		ch, err := NewWebhookChannel(WebhookOptions{Name: "ch-" + c.kind, Kind: c.kind, URL: srv.URL})
		if err != nil {
			t.Fatalf("construct %s: %v", c.kind, err)
		}
		if err := ch.Send(msg); err != nil {
			t.Fatalf("send %s: %v", c.kind, err)
		}
		c.check(t, sink.last())
	}
	if sink.count() != len(cases) {
		t.Fatalf("sink got %d requests, want %d", sink.count(), len(cases))
	}
}

func TestWebhookChannelFailures(t *testing.T) {
	// 非 2xx
	sink := &captureSink{status: http.StatusInternalServerError, respBody: "boom"}
	srv := httptest.NewServer(sink.handler())
	defer srv.Close()
	ch, err := NewWebhookChannel(WebhookOptions{Name: "c1", Kind: KindGeneric, URL: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	if err := ch.Send(Message{Title: "x"}); err == nil {
		t.Fatal("non-2xx must be an error")
	}

	// HTTP 200 + 平台 errcode != 0（IM 平台常见假成功）
	sink2 := &captureSink{respBody: `{"errcode":93000,"errmsg":"invalid webhook"}`}
	srv2 := httptest.NewServer(sink2.handler())
	defer srv2.Close()
	ch2, _ := NewWebhookChannel(WebhookOptions{Name: "c2", Kind: KindWecom, URL: srv2.URL})
	err = ch2.Send(Message{Title: "x"})
	if err == nil || !strings.Contains(err.Error(), "93000") {
		t.Fatalf("platform errcode must fail with code, got %v", err)
	}

	// 连接失败（端口未监听）
	ch3, _ := NewWebhookChannel(WebhookOptions{Name: "c3", Kind: KindGeneric, URL: "http://127.0.0.1:1/none"})
	if err := ch3.Send(Message{Title: "x"}); err == nil {
		t.Fatal("connect failure must be an error")
	}
}

func TestWebhookChannelValidation(t *testing.T) {
	if _, err := NewWebhookChannel(WebhookOptions{Name: "", Kind: KindGeneric, URL: "http://x"}); err == nil {
		t.Fatal("empty name must fail")
	}
	if _, err := NewWebhookChannel(WebhookOptions{Name: "a", Kind: "slack", URL: "http://x"}); err == nil {
		t.Fatal("unknown kind must fail")
	}
	if _, err := NewWebhookChannel(WebhookOptions{Name: "a", Kind: KindGeneric, URL: "ftp://x"}); err == nil {
		t.Fatal("non-http url must fail")
	}
	if ch, err := NewWebhookChannel(WebhookOptions{Name: "a", Kind: KindGeneric, URL: " http://x "}); err != nil || ch.Kind() != KindGeneric {
		t.Fatalf("trimmed url must be accepted: %v", err)
	}
}

// TestGatePolicyOnlyNewIncidentNotifies W9-2 验收口径：只有新事件发通知，
// 窗口重复与故障域并入都不重复通知。
func TestGatePolicyOnlyNewIncidentNotifies(t *testing.T) {
	sink := &captureSink{}
	srv := httptest.NewServer(sink.handler())
	defer srv.Close()
	ch, _ := NewWebhookChannel(WebhookOptions{Name: "sink", Kind: KindGeneric, URL: srv.URL})
	reg := NewRegistry()
	reg.Register(ch)
	gate := NewGate(reg)

	cases := []struct {
		name     string
		d        Decision
		admitted bool
	}{
		{"new-incident", Decision{ClusterKey: "c:1", Title: "HighDisk", Reason: ReasonNewIncident}, true},
		{"dedup-window", Decision{ClusterKey: "c:1", Title: "HighDisk", WouldSuppress: true, Reason: ReasonDedupWindow}, false},
		{"cluster-merge", Decision{ClusterKey: "c:1", Title: "InodeExhaust", Reason: ReasonClusterMerge}, false},
		{"empty reason (no fingerprint)", Decision{ClusterKey: "c:2", Title: "X"}, true},
	}
	for _, c := range cases {
		got, err := gate.Admit(c.d)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if got != c.admitted {
			t.Fatalf("%s: admitted=%v, want %v", c.name, got, c.admitted)
		}
	}
	if sink.count() != 2 {
		t.Fatalf("sink got %d notifications, want 2 (only new-incident)", sink.count())
	}
	st := gate.Stats()
	if st.Dispatched != 2 || st.Suppressed != 2 {
		t.Fatalf("gate stats = %+v, want dispatched=2 suppressed=2", st)
	}
}

// 收敛原因常量在 noise 包定义；此处用字面量锚定契约（防两边漂移由
// cmd/opscopilot 的装配测试兜底）。
const (
	ReasonNewIncident  = "new-incident"
	ReasonDedupWindow  = "dedup-window"
	ReasonClusterMerge = "cluster-merge"
)
