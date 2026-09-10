// W6-0 集成测试：故障注入剧本 → opscopilot 聚类 = 已知答案。
// 锁定 R1 拍板的评估方法：影子判决 vs 注入答案。
package main

import (
	"testing"
	"time"

	"opscopilot/internal/connector"
)

// faultTestEnv 组装 + 静态边 + 发现入图（预热完成后的状态）。
func faultTestEnv(t *testing.T, edges string) *Assembly {
	t.Helper()
	asm, err := NewAssembly(newQuietLogger(), "")
	if err != nil {
		t.Fatalf("assembly: %v", err)
	}
	inputs, err := parseStaticEdges(edges)
	if err != nil {
		t.Fatalf("parse edges: %v", err)
	}
	asm.Sink.AttachStaticEdges(inputs)
	t0 := time.Now()
	nodes := make([]connector.ResourceNode, 0, 5)
	for _, n := range []string{"n1", "n2", "n3", "n4", "n5"} {
		nodes = append(nodes, connector.ResourceNode{
			Key: "prometheus://nodes/" + n, Type: "nodes",
			Labels: map[string]string{"instance": n, "job": "nodes"}, ObservedAt: t0,
		})
	}
	if err := asm.Sink.IngestDiscover(nil, &connector.DiscoverResult{Nodes: nodes, TenantID: "default"}); err != nil {
		t.Fatalf("discover: %v", err)
	}
	return asm
}

func alertAt(fp, instance string, at time.Time) connector.Alert {
	return connector.Alert{
		Fingerprint: fp,
		Labels:      map[string]string{"alertname": fp, "instance": instance, "job": "nodes", "severity": "critical"},
		Severity:    "critical", StartsAt: at,
	}
}

// clusterGrouping 返回活跃簇的节点分组（评估对照形态）。
func clusterGrouping(asm *Assembly) [][]string {
	var out [][]string
	for _, cl := range asm.Noise.shadow.Clusterer().ActiveClusters() {
		group := make([]string, 0, len(cl.NodeKeys))
		for k := range cl.NodeKeys {
			group = append(group, k)
		}
		out = append(out, group)
	}
	return out
}

// TestScenarioA_SameNodeDifferentFingerprintsOneCluster
// 场景 A 已知答案：n1 的两条不同指纹告警应聚成 1 簇（同节点距离 0）。
func TestScenarioA_SameNodeDifferentFingerprintsOneCluster(t *testing.T) {
	asm := faultTestEnv(t, "")
	now := time.Now()
	asm.Noise.ProcessAlerts([]connector.Alert{
		alertAt("fp-a-disk", "n1", now),
		alertAt("fp-a-inode", "n1", now.Add(time.Second)),
	})
	groups := clusterGrouping(asm)
	if len(groups) != 1 || len(groups[0]) != 1 {
		t.Fatalf("scenario A: groups = %v, want 1 cluster on [n1]", groups)
	}
}

// TestScenarioB_FaultDomainAggregationViaStaticEdges
// 场景 B 已知答案：n1/n2/n3 同时 HighLatency，经静态边 n1->n2->n3
// 的故障域聚合应并成 1 簇。
func TestScenarioB_FaultDomainAggregationViaStaticEdges(t *testing.T) {
	asm := faultTestEnv(t, "n1->n2,n2->n3")
	now := time.Now()
	asm.Noise.ProcessAlerts([]connector.Alert{
		alertAt("fp-b-lat-n1", "n1", now),
		alertAt("fp-b-lat-n2", "n2", now.Add(time.Second)),
		alertAt("fp-b-lat-n3", "n3", now.Add(2*time.Second)),
	})
	groups := clusterGrouping(asm)
	if len(groups) != 1 || len(groups[0]) != 3 {
		t.Fatalf("scenario B: groups = %v, want 1 cluster over {n1,n2,n3}", groups)
	}
}

// TestScenarioD_UnrelatedAlertsIndependentClusters
// 场景 D 已知答案：n4/n5 互不相干（无边、不同节点）→ 2 个独立簇。
func TestScenarioD_UnrelatedAlertsIndependentClusters(t *testing.T) {
	asm := faultTestEnv(t, "n1->n2,n2->n3")
	now := time.Now()
	asm.Noise.ProcessAlerts([]connector.Alert{
		alertAt("fp-d-cert", "n4", now),
		alertAt("fp-d-backup", "n5", now.Add(time.Second)),
	})
	groups := clusterGrouping(asm)
	if len(groups) != 2 {
		t.Fatalf("scenario D: groups = %v, want 2 independent clusters", groups)
	}
}

// TestPendingEdgesLandAfterDiscovery 静态边挂起机制：边声明先于发现
// 也能在节点进图后落图（覆盖真实时序——首轮采集告警先于发现）。
func TestPendingEdgesLandAfterDiscovery(t *testing.T) {
	asm, err := NewAssembly(newQuietLogger(), "")
	if err != nil {
		t.Fatalf("assembly: %v", err)
	}
	inputs, err := parseStaticEdges("n1->n2")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	asm.Sink.AttachStaticEdges(inputs)
	if got := asm.Sink.CoverageReport().Edges; got != 0 {
		t.Fatalf("edges before discovery = %d, want 0 (pending)", got)
	}
	t0 := time.Now()
	if err := asm.Sink.IngestDiscover(nil, &connector.DiscoverResult{
		Nodes: []connector.ResourceNode{
			{Key: "prometheus://nodes/n1", Type: "nodes", Labels: map[string]string{"instance": "n1"}, ObservedAt: t0},
			{Key: "prometheus://nodes/n2", Type: "nodes", Labels: map[string]string{"instance": "n2"}, ObservedAt: t0},
		},
	}); err != nil {
		t.Fatalf("discover: %v", err)
	}
	// 发现进图后挂起边应自动落图（下一轮发现时才补——本轮刚入图的
	// 节点在本轮锁内即可落，AddDiscovery 后同一锁区间内执行）。
	if got := asm.Sink.CoverageReport().Edges; got != 1 {
		t.Fatalf("edges after discovery = %d, want 1 (pending edge landed)", got)
	}
}
