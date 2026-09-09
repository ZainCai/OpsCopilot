package main

import (
	"context"
	"testing"
	"time"

	"opscopilot/internal/connector"
	"opscopilot/internal/topology"
)

// discardLogger 静默日志。
type discardLogger struct{}

func (discardLogger) Printf(string, ...interface{}) {}

// tLogger 把组件日志打到测试输出。
type tLogger struct{ t *testing.T }

func (l tLogger) Printf(format string, args ...interface{}) {
	l.t.Logf("[host] "+format, args...)
}

var obs = time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)

func TestTopologySink_DiscoverToGraph(t *testing.T) {
	b := topology.NewBuilder()
	sink, err := NewTopologySink(b, discardLogger{})
	if err != nil {
		t.Fatalf("NewTopologySink: %v", err)
	}

	disc := &connector.DiscoverResult{
		TenantID: "t-a",
		Nodes: []connector.ResourceNode{
			{Key: "prometheus://node/10.0.0.1:9100", Type: "node",
				Labels: map[string]string{"job": "node", "health": "up"},
				Source: "prom-prod", ObservedAt: obs},
			{Key: "prometheus://caddy/localhost:2019", Type: "caddy",
				Labels: map[string]string{"health": "up"},
				Source: "prom-prod", ObservedAt: obs},
		},
	}
	if err := sink.IngestDiscover(context.Background(), disc); err != nil {
		t.Fatalf("IngestDiscover: %v", err)
	}

	g := sink.Graph()
	if len(g.Nodes) != 2 {
		t.Fatalf("nodes = %d, want 2", len(g.Nodes))
	}
	n := g.Nodes["prometheus://node/10.0.0.1:9100"]
	if n == nil {
		t.Fatal("node missing")
	}
	// 字段对齐断言：搬运不丢信息
	if n.Type != "node" || n.Source != "prom-prod" {
		t.Errorf("type/source mismatch: %+v", n)
	}
	if n.Labels["health"] != "up" || n.Labels["job"] != "node" {
		t.Errorf("labels mismatch: %v", n.Labels)
	}
	if !n.ValidFrom.Equal(obs) {
		t.Errorf("ValidFrom = %v, want %v", n.ValidFrom, obs)
	}
	// 直接发现 → 默认 high（ADR-007 规则 1）
	if n.Confidence != topology.ConfidenceHigh {
		t.Errorf("confidence = %q, want high (直接发现=事实)", n.Confidence)
	}

	// 覆盖率自检：全 high 图应 100% 通过门禁
	st := sink.CoverageReport()
	if st.Nodes != 2 || st.CausalNodes != 2 {
		t.Errorf("stats = %d nodes / %d causal, want 2/2", st.Nodes, st.CausalNodes)
	}
	if st.NodesBySource["prom-prod"] != 2 {
		t.Errorf("source bucket mismatch: %v", st.NodesBySource)
	}
}

func TestTopologySink_CollectCountsAlerts(t *testing.T) {
	b := topology.NewBuilder()
	sink, _ := NewTopologySink(b, discardLogger{})

	col := &connector.CollectResult{
		Alerts: []connector.Alert{
			{Fingerprint: "fp-1"}, {Fingerprint: "fp-2"},
		},
	}
	if err := sink.IngestCollect(context.Background(), col); err != nil {
		t.Fatalf("IngestCollect: %v", err)
	}
	if err := sink.IngestCollect(context.Background(), col); err != nil {
		t.Fatalf("IngestCollect#2: %v", err)
	}
	if got := sink.AlertCount(); got != 4 {
		t.Fatalf("alert count = %d, want 4 (累计计数)", got)
	}
	// 告警不进拓扑图（消费方是 W4 降噪）
	if len(sink.Graph().Nodes) != 0 {
		t.Error("alerts must not become topology nodes in W3")
	}
}

func TestTopologySink_Defensive(t *testing.T) {
	// nil builder 拒绝
	if _, err := NewTopologySink(nil, nil); err == nil {
		t.Fatal("nil builder should be rejected")
	}
	// nil 结果不报错（Host 不会传 nil，但防御到位）
	sink, _ := NewTopologySink(topology.NewBuilder(), nil)
	if err := sink.IngestDiscover(context.Background(), nil); err != nil {
		t.Errorf("nil discover should be no-op, got %v", err)
	}
	if err := sink.IngestCollect(context.Background(), nil); err != nil {
		t.Errorf("nil collect should be no-op, got %v", err)
	}
}
