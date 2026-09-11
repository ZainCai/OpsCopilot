// builder_memlimit_test.go 优化方案 #6：拓扑图容量护栏。
// 验证触限淘汰按"最久未活跃"、连带删边、计数正确、指标进文本。
package topology

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"opscopilot/pkg/memguard"
	"opscopilot/pkg/metrics"
)

func TestBuilderBoundedEvictsLeastRecentlyActive(t *testing.T) {
	nodeGuard := memguard.New("builder", 3, 0)
	edgeGuard := memguard.New("builder_edges", 100, 0)
	b := NewBuilderWithLimits(nodeGuard, edgeGuard)
	t0 := time.Now()
	add := func(key string, at time.Time) {
		t.Helper()
		if err := b.AddDiscovery([]NodeInput{{Key: key, Type: "node", ObservedAt: at}}); err != nil {
			t.Fatalf("AddDiscovery %s: %v", key, err)
		}
	}
	add("n1", t0)
	add("n2", t0.Add(time.Second))
	add("n3", t0.Add(2*time.Second))
	if err := b.AddEdge(EdgeInput{SrcKey: "n1", DstKey: "n2", Relation: "depends_on", ObservedAt: t0}); err != nil {
		t.Fatalf("AddEdge: %v", err)
	}
	// n1 再观测一次 → n2 成为最久未活跃者（lastSeen 是"最近活跃"不是入图时刻）。
	add("n1", t0.Add(3*time.Second))
	// 第 4 个节点进图（cap=3）→ 淘汰 n2，连带 n1->n2 的边。
	add("n4", t0.Add(4*time.Second))

	g := b.Build()
	if _, ok := g.Nodes["n2"]; ok {
		t.Fatal("n2（最久未活跃）应被淘汰")
	}
	for _, keep := range []string{"n1", "n3", "n4"} {
		if _, ok := g.Nodes[keep]; !ok {
			t.Fatalf("%s 不该被误伤", keep)
		}
	}
	if len(g.Edges) != 0 {
		t.Fatalf("被删节点的关联边必须一并清除，剩 %d 条", len(g.Edges))
	}
	if got := nodeGuard.Evictions(); got != 1 {
		t.Fatalf("node evictions = %d, want 1", got)
	}
	if got := edgeGuard.Evictions(); got != 1 {
		t.Fatalf("edge evictions（节点连带删边也计数）= %d, want 1", got)
	}
	if b.NodeEntries() != 3 || b.EdgeEntries() != 0 {
		t.Fatalf("原子规模镜像失真: nodes=%d edges=%d", b.NodeEntries(), b.EdgeEntries())
	}

	// gauge/counter 出现在 Prometheus 文本输出。
	reg := metrics.New()
	for _, gd := range b.MemGuards() {
		gd.RegisterTo(reg)
	}
	var buf bytes.Buffer
	if err := reg.WritePrometheus(&buf); err != nil {
		t.Fatalf("write: %v", err)
	}
	body := buf.String()
	for _, want := range []string{
		`opscopilot_mem_entries{store="builder"} 3`,
		`opscopilot_mem_entries{store="builder_edges"} 0`,
		`opscopilot_mem_evictions_total{store="builder"} 1`,
		`opscopilot_mem_evictions_total{store="builder_edges"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics 文本缺 %q\n---\n%s", want, body)
		}
	}
}

// TestBuilderEdgeCapEvictsOldestEdges 独立边上限（节点不超、边超）：
// 淘汰最久未观测的边，节点本体不动。
func TestBuilderEdgeCapEvictsOldestEdges(t *testing.T) {
	nodeGuard := memguard.New("builder", 0, 0) // 0 → nil → 不设限
	if nodeGuard != nil {
		t.Fatal("max<=0 必须返回 nil 护栏")
	}
	edgeGuard := memguard.New("builder_edges", 2, 0)
	b := NewBuilderWithLimits(nil, edgeGuard)
	t0 := time.Now()
	if err := b.AddDiscovery([]NodeInput{
		{Key: "a", ObservedAt: t0}, {Key: "b", ObservedAt: t0}, {Key: "c", ObservedAt: t0}, {Key: "d", ObservedAt: t0},
	}); err != nil {
		t.Fatalf("AddDiscovery: %v", err)
	}
	mk := func(src, dst string, at time.Time) {
		t.Helper()
		if err := b.AddEdge(EdgeInput{SrcKey: src, DstKey: dst, Relation: "depends_on", ObservedAt: at}); err != nil {
			t.Fatalf("AddEdge %s->%s: %v", src, dst, err)
		}
	}
	mk("a", "b", t0) // 最旧
	mk("b", "c", t0.Add(time.Second))
	mk("c", "d", t0.Add(2*time.Second)) // cap=2 → 淘汰 a->b
	if len(b.Build().Edges) != 2 {
		t.Fatalf("edges = %d, want 2", len(b.Build().Edges))
	}
	for _, e := range b.Build().Edges {
		if e.SrcKey == "a" {
			t.Fatal("最旧边 a->b 应被淘汰")
		}
	}
	if got := edgeGuard.Evictions(); got != 1 {
		t.Fatalf("edge evictions = %d, want 1", got)
	}
	// 节点全部健在（边上限不牵连节点）。
	if len(b.Build().Nodes) != 4 {
		t.Fatalf("nodes = %d, want 4", len(b.Build().Nodes))
	}
}

// TestBuilderUnboundedKeepsOldBehavior 默认构造（无护栏）不装限、
// lastSeen 记账不发生——行为与 #6 之前逐字一致。
func TestBuilderUnboundedKeepsOldBehavior(t *testing.T) {
	b := NewBuilder()
	for i := 0; i < 50; i++ {
		if err := b.AddDiscovery([]NodeInput{{Key: string(rune('a' + i))}}); err != nil {
			t.Fatalf("add: %v", err)
		}
	}
	if len(b.Build().Nodes) != 50 {
		t.Fatalf("unbounded builder must keep everything, got %d", len(b.Build().Nodes))
	}
	if len(b.MemGuards()) != 0 {
		t.Fatal("unbounded builder exposes no guards")
	}
}
