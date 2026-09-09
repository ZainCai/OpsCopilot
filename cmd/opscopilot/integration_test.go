//go:build integration

package main

import (
	"context"
	"testing"
	"time"

	"opscopilot/internal/connector"
	"opscopilot/internal/connector/prometheus"
	"opscopilot/internal/topology"
)

// TestIntegration_DiscoverFeedsTopology W2→W3 全链路真实验证：
// 真实 Prometheus → Host 调度 → TopologySink → 拓扑图 → 覆盖率自检。
//
// 运行（依赖外网，默认不执行）：
//
//	go test -tags integration ./cmd/opscopilot/ -v -run Integration
func TestIntegration_DiscoverFeedsTopology(t *testing.T) {
	prom, err := prometheus.New(prometheus.Config{
		ID:      "demo",
		BaseURL: "https://prometheus.demo.prometheus.io",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	host := connector.NewHost(connector.WithLogger(tLogger{t}))
	if err := host.Register(prom); err != nil {
		t.Fatalf("Register: %v", err)
	}

	sink, err := NewTopologySink(topology.NewBuilder(), tLogger{t})
	if err != nil {
		t.Fatalf("NewTopologySink: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	// 一轮：健康 → 采集（告警计数）→ 发现（入图）→ 投递
	if err := host.RunOnce(ctx, sink); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	st := sink.CoverageReport()
	t.Logf("[覆盖率自检] nodes=%d edges=%d causal=%d/%d byConfidence=%v bySource=%v",
		st.Nodes, st.Edges, st.CausalNodes, st.CausalEdges, st.NodesByConfidence, st.NodesBySource)

	if st.Nodes == 0 {
		t.Fatal("真实 Prometheus 发现的节点应进入拓扑图")
	}
	// 连接器直接发现的节点应全部为 high，门禁通过率 100%
	if st.NodesByConfidence[topology.ConfidenceHigh] != st.Nodes {
		t.Errorf("直接发现的节点应全部 high: %v", st.NodesByConfidence)
	}
	if st.CausalNodes != st.Nodes {
		t.Errorf("全 high 图门禁通过率应 100%%: %d/%d", st.CausalNodes, st.Nodes)
	}
	if sink.AlertCount() < 0 {
		t.Error("alert count should never be negative")
	}

	// pb 映射冒烟：契约层能承接真实图（W5 API 的前置）
	resp := sink.graphSnapshot().ToProto()
	if len(resp.GetNodes()) != st.Nodes {
		t.Errorf("pb nodes = %d, want %d", len(resp.GetNodes()), st.Nodes)
	}
	for _, n := range resp.GetNodes() {
		if n.GetConfidence() != "high" || n.GetValidFrom() == "" {
			t.Errorf("pb node %s: confidence=%q validFrom=%q", n.GetNodeKey(), n.GetConfidence(), n.GetValidFrom())
		}
	}
	t.Logf("[pb 映射] %d 个节点已序列化到契约层（W5 API 可直接消费）", len(resp.GetNodes()))
}
