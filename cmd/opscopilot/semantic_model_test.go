// W5-2.1 SemanticModel 契约测试：经 transport.DialInProcess 走真实
// gRPC 序列化回环，锁定 as_of/邻域裁剪/窗口过滤/字段映射契约。
package main

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"opscopilot/internal/connector"
	pb "opscopilot/internal/contracts/pb"
	"opscopilot/internal/topology"
	"opscopilot/internal/transport"
)

// newSemanticTest 起一个装配并返回回环客户端 + 装配引用。
// 生命周期：t.Cleanup 里 srv.Stop()（R6：DialInProcess 的 Serve goroutine
// 只能靠 Stop 退出）。
func newSemanticTest(t *testing.T) (pb.SemanticModelClient, *Assembly) {
	t.Helper()
	asm, err := NewAssembly(newQuietLogger(), "")
	if err != nil {
		t.Fatalf("assembly: %v", err)
	}
	t.Cleanup(func() { asm.GRPC.Stop() })
	conn, err := transport.DialInProcess(context.Background(), asm.GRPC)
	if err != nil {
		t.Fatalf("dial in-process: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return pb.NewSemanticModelClient(conn), asm
}

// addTestEdge 测试直接经 sink 的 builder 加边（同包访问；锁纪律照守）。
func addTestEdge(asm *Assembly, src, dst string, at time.Time) {
	asm.Sink.mu.Lock()
	defer asm.Sink.mu.Unlock()
	_ = asm.Sink.builder.AddEdge(topology.EdgeInput{
		SrcKey: src, DstKey: dst, Relation: "depends_on",
		ObservedAt: at, Confidence: topology.ConfidenceMedium,
	})
}

func TestGetTopologyAsOfFiltersFutureNodes(t *testing.T) {
	client, asm := newSemanticTest(t)

	// 两个节点：t0 已知、t2 才观测到（ValidFrom = ObservedAt）。
	t0 := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	t2 := t0.Add(48 * time.Hour)
	if err := asm.Sink.IngestDiscover(nil, &connector.DiscoverResult{
		Nodes: []connector.ResourceNode{
			{Key: "n-old", Type: "host", ObservedAt: t0},
			{Key: "n-new", Type: "host", ObservedAt: t2},
		},
	}); err != nil {
		t.Fatalf("seed discover: %v", err)
	}

	// as_of = t1 → 只见 n-old。
	resp, err := client.GetTopology(context.Background(), &pb.GetTopologyRequest{
		AsOf: t0.Add(24 * time.Hour).Format(time.RFC3339),
	})
	if err != nil {
		t.Fatalf("GetTopology(as_of t1): %v", err)
	}
	if len(resp.Nodes) != 1 || resp.Nodes[0].NodeKey != "n-old" {
		t.Fatalf("as_of nodes = %v, want only n-old", resp.Nodes)
	}
	if resp.ResolvedAsOf == "" {
		t.Fatal("ResolvedAsOf must echo the query time")
	}

	// as_of = t3 → 两节点都在。
	resp, err = client.GetTopology(context.Background(), &pb.GetTopologyRequest{
		AsOf: t2.Add(time.Hour).Format(time.RFC3339),
	})
	if err != nil {
		t.Fatalf("GetTopology(as_of t3): %v", err)
	}
	if len(resp.Nodes) != 2 {
		t.Fatalf("as_of t3 nodes = %d, want 2", len(resp.Nodes))
	}

	// 无 as_of = 当前 → 全图，ResolvedAsOf 空串。
	resp, err = client.GetTopology(context.Background(), &pb.GetTopologyRequest{})
	if err != nil {
		t.Fatalf("GetTopology(current): %v", err)
	}
	if len(resp.Nodes) != 2 || resp.ResolvedAsOf != "" {
		t.Fatalf("current topology: nodes=%d resolved=%q, want 2/empty", len(resp.Nodes), resp.ResolvedAsOf)
	}
}

func TestGetTopologyNeighborhoodPrune(t *testing.T) {
	client, asm := newSemanticTest(t)

	now := time.Now()
	if err := asm.Sink.IngestDiscover(nil, &connector.DiscoverResult{
		Nodes: []connector.ResourceNode{
			{Key: "a", Type: "host", ObservedAt: now},
			{Key: "b", Type: "host", ObservedAt: now},
			{Key: "c", Type: "host", ObservedAt: now},
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// 边：a-b（1 跳）、b-c（a 到 c 2 跳）。
	addTestEdge(asm, "a", "b", now)
	addTestEdge(asm, "b", "c", now)

	// depth=1：a + b，不含 c。
	resp, err := client.GetTopology(context.Background(), &pb.GetTopologyRequest{
		NodeKey: "a", Depth: 1,
	})
	if err != nil {
		t.Fatalf("GetTopology(neighborhood): %v", err)
	}
	keys := map[string]bool{}
	for _, n := range resp.Nodes {
		keys[n.NodeKey] = true
	}
	if len(resp.Nodes) != 2 || !keys["a"] || !keys["b"] || keys["c"] {
		t.Fatalf("depth=1 nodes = %v, want {a,b}", keys)
	}
	if len(resp.Edges) != 1 {
		t.Fatalf("depth=1 edges = %d, want 1 (a-b)", len(resp.Edges))
	}

	// depth=2：a/b/c 全部。
	resp, err = client.GetTopology(context.Background(), &pb.GetTopologyRequest{
		NodeKey: "a", Depth: 2,
	})
	if err != nil {
		t.Fatalf("GetTopology(depth2): %v", err)
	}
	if len(resp.Nodes) != 3 || len(resp.Edges) != 2 {
		t.Fatalf("depth=2: nodes=%d edges=%d, want 3/2", len(resp.Nodes), len(resp.Edges))
	}

	// root 不存在 → NotFound（宁可报错不返回空图误导"当时无拓扑"）。
	_, err = client.GetTopology(context.Background(), &pb.GetTopologyRequest{NodeKey: "ghost"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("ghost root: code = %s, want NotFound", status.Code(err))
	}
}

func TestGetTopologyInvalidAsOf(t *testing.T) {
	client, _ := newSemanticTest(t)

	_, err := client.GetTopology(context.Background(), &pb.GetTopologyRequest{AsOf: "not-a-time"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid as_of: code = %s, want InvalidArgument", status.Code(err))
	}
}

func TestGetRecentChangesWindowAndMapping(t *testing.T) {
	client, asm := newSemanticTest(t)

	// 种子：节点 n1 已发现（变更校验需要节点存在）。
	t0 := time.Date(2026, 9, 10, 8, 0, 0, 0, time.UTC)
	if err := asm.Sink.IngestDiscover(nil, &connector.DiscoverResult{
		Nodes: []connector.ResourceNode{{Key: "n1", Type: "host", ObservedAt: t0}},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// 两条变更：窗口内 + 窗口外。
	if _, err := asm.Changes.Record(topology.ChangeEvent{
		ID: "ev-1", NodeKey: "n1", Type: topology.ChangeDeploy, Source: "git",
		Author: "alice", Ref: "main", Revision: "abc123", Summary: "deploy v2",
		OccurredAt: t0.Add(time.Hour), Confidence: topology.ConfidenceHigh,
	}); err != nil {
		t.Fatalf("record in-window: %v", err)
	}
	if _, err := asm.Changes.Record(topology.ChangeEvent{
		ID: "ev-0", NodeKey: "n1", Type: topology.ChangeConfig,
		OccurredAt: t0.Add(-time.Hour),
	}); err != nil {
		t.Fatalf("record out-window: %v", err)
	}

	resp, err := client.GetRecentChanges(context.Background(), &pb.GetRecentChangesRequest{
		NodeKey:     "n1",
		WindowStart: t0.Format(time.RFC3339),
		WindowEnd:   t0.Add(2 * time.Hour).Format(time.RFC3339),
	})
	if err != nil {
		t.Fatalf("GetRecentChanges: %v", err)
	}
	if len(resp.Changes) != 1 {
		t.Fatalf("changes = %d, want 1 (window filter)", len(resp.Changes))
	}
	got := resp.Changes[0]
	// C5 ②：全字段映射完整性（证据字段一个都不能丢）。
	if got.Id != "ev-1" || got.NodeKey != "n1" || got.ChangeType != "deploy" ||
		got.Actor != "alice" || got.Source != "git" || got.Ref != "main" ||
		got.Revision != "abc123" || got.Summary != "deploy v2" || got.Confidence != "high" {
		t.Fatalf("mapping mismatch: %+v", got)
	}
	if got.OccurredAt != t0.Add(time.Hour).Format(time.RFC3339) {
		t.Fatalf("occurred_at = %q", got.OccurredAt)
	}

	// node_key 为空 → 全租户窗口（含 n1 的一条）。
	resp, err = client.GetRecentChanges(context.Background(), &pb.GetRecentChangesRequest{
		WindowStart: t0.Format(time.RFC3339),
		WindowEnd:   t0.Add(2 * time.Hour).Format(time.RFC3339),
	})
	if err != nil {
		t.Fatalf("GetRecentChanges(all nodes): %v", err)
	}
	if len(resp.Changes) != 1 {
		t.Fatalf("all-node window changes = %d, want 1", len(resp.Changes))
	}
}

func TestGetRecentChangesValidation(t *testing.T) {
	client, _ := newSemanticTest(t)
	ctx := context.Background()

	// 无 window_start → 拒绝（证据窗口必须有下界）。
	_, err := client.GetRecentChanges(ctx, &pb.GetRecentChangesRequest{})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("no start: code = %s, want InvalidArgument", status.Code(err))
	}
	// end < start → 拒绝。
	_, err = client.GetRecentChanges(ctx, &pb.GetRecentChangesRequest{
		WindowStart: "2026-09-10T10:00:00Z", WindowEnd: "2026-09-10T09:00:00Z",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("end<start: code = %s, want InvalidArgument", status.Code(err))
	}
	// 非法时间格式 → 拒绝。
	_, err = client.GetRecentChanges(ctx, &pb.GetRecentChangesRequest{WindowStart: "yesterday"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("bad format: code = %s, want InvalidArgument", status.Code(err))
	}
}
