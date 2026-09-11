package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"opscopilot/internal/connector"
	"opscopilot/internal/topology"
)

func newTestAssembly(t *testing.T) *Assembly {
	t.Helper()
	a, err := NewAssembly(nil, testAssemblyConfig(""))
	if err != nil {
		t.Fatalf("NewAssembly: %v", err)
	}
	return a
}

func TestAssembly_WebhookTokenWired(t *testing.T) {
	// S1：装配层必须把共享密钥接到 webhook（写路径准入）。
	a, err := NewAssembly(nil, testAssemblyConfig("s3cret"))
	if err != nil {
		t.Fatal(err)
	}
	if a.Webhook.Token != "s3cret" {
		t.Fatalf("webhook token = %q, want wired from NewAssembly", a.Webhook.Token)
	}
	srv := httptest.NewServer(a.Handler())
	defer srv.Close()
	resp, err := http.Post(srv.URL+changeWebhookPath, "application/json",
		strings.NewReader(`{"id":"c1","node_key":"host:x","type":"deploy"}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("missing token: code = %d, want 401", resp.StatusCode)
	}
}

func TestAssembly_ChangeRequiresDiscoveredNode(t *testing.T) {
	a := newTestAssembly(t)
	srv := httptest.NewServer(a.Handler())
	defer srv.Close()

	// 1. 图里没有节点：提交关联任意节点的变更 → 422（严格校验闭环生效）
	post := func(body string) int {
		resp, err := http.Post(srv.URL+changeWebhookPath, "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatalf("post: %v", err)
		}
		defer resp.Body.Close()
		return resp.StatusCode
	}
	if code := post(`{"id":"c0","node_key":"host:ghost","type":"deploy"}`); code != http.StatusUnprocessableEntity {
		t.Fatalf("change for undiscovered node: code = %d, want 422", code)
	}

	// 2. 模拟连接器发现：DiscoverResult 经 Sink 入图
	err := a.Sink.IngestDiscover(context.Background(), &connector.DiscoverResult{
		Nodes: []connector.ResourceNode{
			{Key: "host:web-01", Type: "host", Source: "prom", ObservedAt: time.Now()},
		},
	})
	if err != nil {
		t.Fatalf("ingest discover: %v", err)
	}

	// 3. 现在变更可以关联到该节点 → 200
	if code := post(`{"id":"c1","node_key":"host:web-01","type":"deploy","summary":"上线 v1"}`); code != http.StatusOK {
		t.Fatalf("change for discovered node: code = %d, want 200", code)
	}

	// 4. 幽灵节点依旧拒绝
	if code := post(`{"id":"c2","node_key":"host:ghost","type":"deploy"}`); code != http.StatusUnprocessableEntity {
		t.Fatalf("ghost node still rejected: code = %d, want 422", code)
	}

	// 5. 入库的事件可按节点 + 时间窗查到（验收语义闭环）
	hits := a.Changes.ByNodeWithin("host:web-01", time.Now().Add(-time.Minute), time.Now().Add(time.Minute))
	if len(hits) != 1 || hits[0].ID != "c1" {
		t.Fatalf("window query = %+v, want c1", hits)
	}
}

func TestAssembly_Healthz(t *testing.T) {
	a := newTestAssembly(t)
	srv := httptest.NewServer(a.Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("healthz = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Errorf("content-type = %q, want json", ct)
	}
}

func TestAssembly_HasNodeConcurrentWithDiscover(t *testing.T) {
	// 并发写图（模拟 Host 采集轮）+ 并发节点校验（模拟变更提交），锁定无数据竞争。
	a := newTestAssembly(t)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 100; i++ {
			_ = a.Sink.IngestDiscover(context.Background(), &connector.DiscoverResult{
				Nodes: []connector.ResourceNode{{Key: "host:c", Type: "host"}},
			})
		}
	}()
	for i := 0; i < 100; i++ {
		_, _ = a.Changes.Record(topology.ChangeEvent{ID: "x", NodeKey: "host:c", Type: topology.ChangeDeploy})
		_ = a.Sink.HasNode("host:c")
	}
	<-done
}

func TestNewAssembly_NilLoggerOK(t *testing.T) {
	a := newTestAssembly(t)
	if a.Sink == nil || a.Changes == nil || a.Webhook == nil {
		t.Fatal("assembly components must be wired")
	}
	// nil logger 下吞告警不炸
	if err := a.Sink.IngestCollect(context.Background(), &connector.CollectResult{
		Alerts: []connector.Alert{{Fingerprint: "f1"}},
	}); err != nil {
		t.Fatalf("ingest collect with nil logger: %v", err)
	}
	if a.Sink.AlertCount() != 1 {
		t.Errorf("alert count = %d, want 1", a.Sink.AlertCount())
	}
}
