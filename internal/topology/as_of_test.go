package topology

import (
	"testing"
	"time"
)

// buildTemporalGraph 构造带时态语义的测试图：
//   - host:old  ValidFrom=T0（一直有效）
//   - host:new  ValidFrom=T2（T2 才被发现）
//   - host:dead ValidFrom=T0, ValidTo=T3（T3 失效）
//   - 边 old->dead（T0）、old->new（T2）
func buildTemporalGraph(t *testing.T) (*Graph, time.Time) {
	t.Helper()
	base := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	g := &Graph{Nodes: map[string]*Node{
		"host:old":  {Key: "host:old", Type: "host", Confidence: ConfidenceHigh, ValidFrom: base},
		"host:new":  {Key: "host:new", Type: "host", Confidence: ConfidenceHigh, ValidFrom: base.Add(2 * time.Hour)},
		"host:dead": {Key: "host:dead", Type: "host", Confidence: ConfidenceHigh, ValidFrom: base, ValidTo: base.Add(3 * time.Hour)},
	}}
	g.Edges = []*Edge{
		{SrcKey: "host:old", DstKey: "host:dead", Relation: "depends_on", Confidence: ConfidenceHigh, ValidFrom: base},
		{SrcKey: "host:old", DstKey: "host:new", Relation: "depends_on", Confidence: ConfidenceHigh, ValidFrom: base.Add(2 * time.Hour)},
	}
	return g, base
}

func TestAsOf_Visibility(t *testing.T) {
	g, base := buildTemporalGraph(t)

	// T1：old 可见；new 未发现；dead 尚有效
	snap := g.AsOf(base.Add(1 * time.Hour))
	if _, ok := snap.Nodes["host:old"]; !ok {
		t.Error("host:old should be visible at T1")
	}
	if _, ok := snap.Nodes["host:new"]; ok {
		t.Error("host:new must NOT be visible at T1 (not yet discovered)")
	}
	if _, ok := snap.Nodes["host:dead"]; !ok {
		t.Error("host:dead should be visible at T1 (before ValidTo)")
	}
	// old->dead 边可见；old->new 边因 dst 不可见被剔除
	if len(snap.Edges) != 1 || snap.Edges[0].DstKey != "host:dead" {
		t.Errorf("edges at T1 = %+v, want only old->dead", snap.Edges)
	}

	// T2.5：全部节点可见
	snap = g.AsOf(base.Add(2*time.Hour + 30*time.Minute))
	if len(snap.Nodes) != 3 {
		t.Errorf("nodes at T2.5 = %d, want 3", len(snap.Nodes))
	}
	if len(snap.Edges) != 2 {
		t.Errorf("edges at T2.5 = %d, want 2", len(snap.Edges))
	}

	// T4：dead 已失效；old->dead 边被剔除（dst 不可见）
	snap = g.AsOf(base.Add(4 * time.Hour))
	if _, ok := snap.Nodes["host:dead"]; ok {
		t.Error("host:dead must NOT be visible at T4 (after ValidTo)")
	}
	if _, ok := snap.Nodes["host:new"]; !ok {
		t.Error("host:new should be visible at T4")
	}
	for _, e := range snap.Edges {
		if e.DstKey == "host:dead" {
			t.Errorf("edge to invalidated node must be dropped at T4: %+v", e)
		}
	}
}

func TestAsOf_ZeroTimeReturnsCurrentView(t *testing.T) {
	g, base := buildTemporalGraph(t)
	// 零 t：全部已发生的证据都可见（当前视图）
	snap := g.AsOf(time.Time{})
	if len(snap.Nodes) != 3 || len(snap.Edges) != 2 {
		t.Errorf("zero as_of = %d nodes / %d edges, want 3/2", len(snap.Nodes), len(snap.Edges))
	}
	_ = base
}

func TestAsOf_MergeKeepsEarliestValidFrom(t *testing.T) {
	// 合并语义验证：节点 T0 首次观测，T2 再次观测（ValidFrom 取最早），
	// as_of(T1) 仍应命中——历史时点不因后续观测而"失忆"。
	b := NewBuilder()
	t0 := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	if err := b.AddNode(NodeInput{Key: "h", Type: "host", ObservedAt: t0}); err != nil {
		t.Fatal(err)
	}
	if err := b.AddNode(NodeInput{Key: "h", Type: "host", ObservedAt: t0.Add(2 * time.Hour)}); err != nil {
		t.Fatal(err)
	}
	snap := b.Build().AsOf(t0.Add(1 * time.Hour))
	if _, ok := snap.Nodes["h"]; !ok {
		t.Error("merged node (earliest ValidFrom) must be visible at T1")
	}
}

func TestAsOf_SnapshotIsIndependent(t *testing.T) {
	g, base := buildTemporalGraph(t)
	snap := g.AsOf(base.Add(1 * time.Hour))
	// 快照增删不影响原图
	delete(snap.Nodes, "host:old")
	snap.Edges = nil
	if len(g.Nodes) != 3 || len(g.Edges) != 2 {
		t.Errorf("original graph mutated by snapshot: %d nodes / %d edges", len(g.Nodes), len(g.Edges))
	}
}

func TestAsOf_EmptyAndNil(t *testing.T) {
	var nilGraph *Graph
	snap := nilGraph.AsOf(time.Now())
	if snap == nil || snap.Nodes == nil {
		t.Error("nil graph should return empty (non-nil, initialized) snapshot")
	}
	empty := &Graph{Nodes: map[string]*Node{}}
	snap = empty.AsOf(time.Now())
	if len(snap.Nodes) != 0 || len(snap.Edges) != 0 {
		t.Error("empty graph should return empty snapshot")
	}
}

func TestAsOf_ThenCausalGate(t *testing.T) {
	// 组合用法：先取历史时点，再过因果门禁——两步可链式组合。
	g, base := buildTemporalGraph(t)
	// 给 new->? 加一条低置信度边，验证门禁在 as_of 结果上同样生效
	g.Edges = append(g.Edges, &Edge{
		SrcKey: "host:old", DstKey: "host:new", Relation: "guessed",
		Confidence: ConfidenceLow, ValidFrom: base.Add(2 * time.Hour),
	})
	snap := g.AsOf(base.Add(2 * time.Hour))
	causal := snap.CausalSubgraph()
	// T2 可见的高置信边有两条（old->dead、old->new depends_on），
	// guessed（low）必须被门禁拦下。
	if len(causal.Edges) != 2 {
		t.Errorf("causal edges after gate = %d, want 2", len(causal.Edges))
	}
	for _, e := range causal.Edges {
		if e.Relation == "guessed" {
			t.Errorf("low-confidence edge must be gated out: %+v", e)
		}
	}
}

func TestParseAsOf(t *testing.T) {
	// 空 = 当前
	if got, err := ParseAsOf(""); err != nil || !got.IsZero() {
		t.Errorf("empty ParseAsOf = %v, %v; want zero, nil", got, err)
	}
	// 合法 RFC3339
	want := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	got, err := ParseAsOf("2026-09-01T12:00:00Z")
	if err != nil || !got.Equal(want) {
		t.Errorf("ParseAsOf = %v, %v; want %v, nil", got, err, want)
	}
	// 非法格式必须报错（不静默当"当前"）
	if _, err := ParseAsOf("yesterday"); err == nil {
		t.Error("invalid as_of must be rejected")
	}
}

func TestToProtoAsOf_ResolvesField(t *testing.T) {
	g, base := buildTemporalGraph(t)
	t1 := base.Add(1 * time.Hour)
	resp := g.AsOf(t1).ToProtoAsOf(t1)
	if resp.ResolvedAsOf != t1.Format(time.RFC3339) {
		t.Errorf("ResolvedAsOf = %q, want %q", resp.ResolvedAsOf, t1.Format(time.RFC3339))
	}
	if len(resp.Nodes) != 2 {
		t.Errorf("nodes = %d, want 2 (T1 view)", len(resp.Nodes))
	}
	// 零 resolved → 空串（"当前"表达）
	if resp := g.ToProtoAsOf(time.Time{}); resp.ResolvedAsOf != "" {
		t.Errorf("zero resolved should map to empty string, got %q", resp.ResolvedAsOf)
	}
}
