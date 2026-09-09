package topology

import (
	"strings"
	"testing"
	"time"
)

var t0 = time.Date(2026, 9, 9, 10, 0, 0, 0, time.UTC)

func TestParseConfidence(t *testing.T) {
	// 合法值
	for _, s := range []string{"high", "medium", "low"} {
		if c, err := ParseConfidence(s); err != nil || c != Confidence(s) {
			t.Errorf("ParseConfidence(%q) = %v, %v", s, c, err)
		}
	}
	// 空串 → 保守按 low
	if c, err := ParseConfidence(""); err != nil || c != ConfidenceLow {
		t.Errorf("empty should map to low, got %v, %v", c, err)
	}
	// 非法值必须拒绝（不得静默降级）
	if _, err := ParseConfidence("bogus"); err == nil {
		t.Error("unknown confidence should be rejected")
	}
}

func TestBuilder_AddDiscoveryMerge(t *testing.T) {
	b := NewBuilder()
	// 第一次观测：high、T1、来源 s1
	if err := b.AddDiscovery([]NodeInput{{
		Key: "n1", Type: "host", Source: "s1",
		ObservedAt: t0.Add(time.Hour), Confidence: ConfidenceHigh,
		Labels: map[string]string{"a": "1"},
	}}); err != nil {
		t.Fatalf("add#1: %v", err)
	}
	// 第二次观测（乱序：时间更早）：medium、T0、来源 s2
	if err := b.AddDiscovery([]NodeInput{{
		Key: "n1", Type: "host", Source: "s2",
		ObservedAt: t0, Confidence: ConfidenceMedium,
		Labels: map[string]string{"b": "2", "a": ""}, // a 空值不应覆盖
	}}); err != nil {
		t.Fatalf("add#2: %v", err)
	}

	g := b.Build()
	n := g.Nodes["n1"]
	if n == nil {
		t.Fatal("node missing")
	}
	if n.Confidence != ConfidenceHigh {
		t.Errorf("merged confidence = %q, want high (取高，有直接观测不降级)", n.Confidence)
	}
	if !n.ValidFrom.Equal(t0) {
		t.Errorf("ValidFrom = %v, want %v (取最早，bi-temporal 不回退)", n.ValidFrom, t0)
	}
	if n.Source != "s2" {
		t.Errorf("Source = %q, want s2 (后加入者覆盖)", n.Source)
	}
	if n.Labels["a"] != "1" || n.Labels["b"] != "2" {
		t.Errorf("Labels merge mismatch: %v", n.Labels)
	}
	if !n.ValidTo.IsZero() {
		t.Errorf("ValidTo should stay zero (仍被观测 → 当前有效), got %v", n.ValidTo)
	}
}

func TestBuilder_DefaultsAndValidation(t *testing.T) {
	b := NewBuilder()

	// 空 Key 拒绝
	if err := b.AddDiscovery([]NodeInput{{Key: ""}}); err == nil {
		t.Error("empty key should be rejected")
	}
	// 非法置信度拒绝
	err := b.AddDiscovery([]NodeInput{{Key: "x", Confidence: Confidence("bogus")}})
	if err == nil || !strings.Contains(err.Error(), "unknown confidence") {
		t.Errorf("bogus confidence should be rejected, got %v", err)
	}

	// 节点默认 high（直接发现 = 事实）
	if err := b.AddNode(NodeInput{Key: "n1", ObservedAt: t0}); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if got := b.Build().Nodes["n1"].Confidence; got != ConfidenceHigh {
		t.Errorf("node default confidence = %q, want high", got)
	}

	// 边默认 low（边几乎都是推断）；端点缺失/空 Relation 拒绝
	if err := b.AddEdge(EdgeInput{SrcKey: "ghost", DstKey: "n1", Relation: "r"}); err == nil {
		t.Error("missing src endpoint should be rejected")
	}
	if err := b.AddEdge(EdgeInput{SrcKey: "n1", DstKey: "ghost", Relation: "r"}); err == nil {
		t.Error("missing dst endpoint should be rejected")
	}
	if err := b.AddEdge(EdgeInput{SrcKey: "n1", DstKey: "n1"}); err == nil {
		t.Error("empty relation should be rejected")
	}
	if err := b.AddEdge(EdgeInput{SrcKey: "n1", DstKey: "n1", Relation: "self", ObservedAt: t0}); err != nil {
		t.Fatalf("AddEdge: %v", err)
	}
	if got := b.Build().Edges[0].Confidence; got != ConfidenceLow {
		t.Errorf("edge default confidence = %q, want low", got)
	}
}

func TestBuilder_EdgeMergeTakesHigher(t *testing.T) {
	b := NewBuilder()
	_ = b.AddNode(NodeInput{Key: "a", ObservedAt: t0})
	_ = b.AddNode(NodeInput{Key: "b", ObservedAt: t0})

	// 先来一条推断边（low），再来同 key 三元组的显式声明（high）→ 取高
	if err := b.AddEdge(EdgeInput{SrcKey: "a", DstKey: "b", Relation: "depends_on", ObservedAt: t0}); err != nil {
		t.Fatalf("edge#1: %v", err)
	}
	if err := b.AddEdge(EdgeInput{SrcKey: "a", DstKey: "b", Relation: "depends_on",
		ObservedAt: t0.Add(time.Hour), Confidence: ConfidenceHigh, Source: "cloud-api"}); err != nil {
		t.Fatalf("edge#2: %v", err)
	}
	g := b.Build()
	if len(g.Edges) != 1 {
		t.Fatalf("edges = %d, want 1 (同三元组合并)", len(g.Edges))
	}
	if g.Edges[0].Confidence != ConfidenceHigh {
		t.Errorf("edge confidence = %q, want high", g.Edges[0].Confidence)
	}
	if !g.Edges[0].ValidFrom.Equal(t0) {
		t.Errorf("edge ValidFrom = %v, want earliest %v", g.Edges[0].ValidFrom, t0)
	}
}

func TestCausalSubgraph(t *testing.T) {
	b := NewBuilder()
	_ = b.AddDiscovery([]NodeInput{
		{Key: "observed", Type: "host", ObservedAt: t0},                                    // high
		{Key: "aggregated", Type: "service", ObservedAt: t0, Confidence: ConfidenceMedium}, // medium
		{Key: "guessed", Type: "host", ObservedAt: t0, Confidence: ConfidenceLow},          // low
	})
	_ = b.AddEdge(EdgeInput{SrcKey: "observed", DstKey: "aggregated", Relation: "depends_on",
		ObservedAt: t0, Confidence: ConfidenceMedium})
	// low 边（即使两端都是合法节点）
	_ = b.AddEdge(EdgeInput{SrcKey: "aggregated", DstKey: "guessed", Relation: "guess",
		ObservedAt: t0, Confidence: ConfidenceLow})
	// high 端点间的边，但 dst 是 low 节点 → 悬空应被剔除
	_ = b.AddEdge(EdgeInput{SrcKey: "observed", DstKey: "guessed", Relation: "points_to",
		ObservedAt: t0, Confidence: ConfidenceHigh})

	g := b.Build()
	c := g.CausalSubgraph()

	if _, ok := c.Nodes["guessed"]; ok {
		t.Error("low node must be excluded from causal graph")
	}
	if _, ok := c.Nodes["observed"]; !ok {
		t.Error("high node must be kept")
	}
	if _, ok := c.Nodes["aggregated"]; !ok {
		t.Error("medium node must be kept (ADR-007：门禁只拦 low)")
	}
	for _, e := range c.Edges {
		if e.Confidence == ConfidenceLow {
			t.Error("low edge must be excluded from causal graph")
		}
		if e.DstKey == "guessed" || e.SrcKey == "guessed" {
			t.Errorf("dangling edge to excluded node must be dropped: %s->%s", e.SrcKey, e.DstKey)
		}
	}
	if len(c.Edges) != 1 {
		t.Errorf("causal edges = %d, want 1 (仅 observed->aggregated)", len(c.Edges))
	}
	// 原图不受影响
	if len(g.Nodes) != 3 || len(g.Edges) != 3 {
		t.Errorf("original graph mutated: %d nodes / %d edges", len(g.Nodes), len(g.Edges))
	}
}

func TestStats_CoverageReport(t *testing.T) {
	b := NewBuilder()
	_ = b.AddDiscovery([]NodeInput{
		{Key: "n1", Source: "prom", ObservedAt: t0},
		{Key: "n2", Source: "prom", ObservedAt: t0},
		{Key: "n3", Source: "inferred", ObservedAt: t0, Confidence: ConfidenceLow},
	})
	_ = b.AddEdge(EdgeInput{SrcKey: "n1", DstKey: "n2", Relation: "r", ObservedAt: t0})

	s := b.Build().Stats()
	if s.Nodes != 3 || s.Edges != 1 {
		t.Fatalf("counts = %d/%d, want 3/1", s.Nodes, s.Edges)
	}
	if s.NodesByConfidence[ConfidenceHigh] != 2 || s.NodesByConfidence[ConfidenceLow] != 1 {
		t.Errorf("node confidence buckets mismatch: %v", s.NodesByConfidence)
	}
	if s.NodesBySource["prom"] != 2 {
		t.Errorf("source buckets mismatch: %v", s.NodesBySource)
	}
	// 覆盖率自检的核心数字：3 节点中 2 个可进因果推理；
	// 唯一的边未声明置信度 → 默认 low → 被门禁拦截（这正是门禁该有的行为）
	if s.CausalNodes != 2 || s.CausalEdges != 0 {
		t.Errorf("causal = %d/%d, want 2/0 (未声明置信度的边应被门禁拦下)", s.CausalNodes, s.CausalEdges)
	}
}

func TestToProto_Mapping(t *testing.T) {
	b := NewBuilder()
	_ = b.AddDiscovery([]NodeInput{
		{Key: "z-node", Type: "host", Source: "prom",
			ObservedAt: t0, Labels: map[string]string{"secret-label": "v"}}, // 排序在后
		{Key: "a-node", Type: "host", Source: "prom", ObservedAt: t0.Add(time.Hour)},
	})
	_ = b.AddEdge(EdgeInput{SrcKey: "a-node", DstKey: "z-node", Relation: "depends_on",
		Source: "cloud-api", ObservedAt: t0, Confidence: ConfidenceMedium})

	resp := b.Build().ToProto()
	if len(resp.Nodes) != 2 || len(resp.Edges) != 1 {
		t.Fatalf("proto nodes/edges = %d/%d", len(resp.Nodes), len(resp.Edges))
	}
	// 节点按 Key 排序 → a-node 在前（输出稳定）
	if resp.Nodes[0].NodeKey != "a-node" {
		t.Errorf("nodes not sorted: first = %q", resp.Nodes[0].NodeKey)
	}
	// ValidFrom 已填、ValidTo 零值 → 空串（当前有效）
	n := resp.Nodes[0]
	if n.ValidFrom == "" {
		t.Error("ValidFrom should be serialized")
	}
	if n.ValidTo != "" {
		t.Errorf("ValidTo should be empty when active, got %q", n.ValidTo)
	}
	if n.Confidence != string(ConfidenceHigh) {
		t.Errorf("Confidence = %q", n.Confidence)
	}
	e := resp.Edges[0]
	if e.SrcKey != "a-node" || e.DstKey != "z-node" || e.Relation != "depends_on" {
		t.Errorf("edge mapping mismatch: %+v", e)
	}
	if e.Confidence != "medium" {
		t.Errorf("edge confidence = %q", e.Confidence)
	}
}

// TestBuilder_NilReceiver 显式 nil 防御（配合零值安全的项目惯例）。
func TestBuilder_NilReceiver(t *testing.T) {
	var b *Builder
	if err := b.AddDiscovery(nil); err == nil {
		t.Fatal("nil builder AddDiscovery should error, not panic")
	}
	if err := b.AddEdge(EdgeInput{SrcKey: "a", DstKey: "b", Relation: "r"}); err == nil {
		t.Fatal("nil builder AddEdge should error, not panic")
	}
}
