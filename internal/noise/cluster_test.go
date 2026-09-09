// W4-1.3 聚类器测试：锁定并入规则、状态机与决定性契约。
package noise

import (
	"errors"
	"testing"
	"time"
)

var base = time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)

// fakeDomain 测试用故障域函数：显式边表。
type fakeDomain map[string][]string

func (f fakeDomain) same(a, b string) bool {
	for _, k := range f[a] {
		if k == b {
			return true
		}
	}
	for _, k := range f[b] {
		if k == a {
			return true
		}
	}
	return false
}

func TestIngestSameFingerprintSameCluster(t *testing.T) {
	c := NewClusterer(10*time.Minute, nil)
	cl1, created := c.Ingest(Event{Fingerprint: "fp1", NodeKey: "n1", Severity: "warning", Summary: "s1", OccurredAt: base})
	if !created {
		t.Fatal("first ingest must create")
	}
	cl2, created := c.Ingest(Event{Fingerprint: "fp1", NodeKey: "n2", Severity: "critical", Summary: "s2", OccurredAt: base.Add(time.Minute)})
	if created {
		t.Fatal("same fingerprint within window must merge, not create")
	}
	if cl1.Key != cl2.Key {
		t.Fatalf("merged into different clusters: %s vs %s", cl1.Key, cl2.Key)
	}
	if cl2.Severity != "critical" {
		t.Fatalf("cluster severity = %s, want max(critical, warning)", cl2.Severity)
	}
	if cl2.AlertCount != 2 {
		t.Fatalf("AlertCount = %d, want 2", cl2.AlertCount)
	}
	if len(cl2.NodeKeys) != 2 {
		t.Fatalf("NodeKeys = %d, want 2 (n1+n2)", len(cl2.NodeKeys))
	}
	if cl2.Summary != "s2" {
		t.Fatalf("summary should be newest, got %q", cl2.Summary)
	}
}

func TestIngestSameNodeDifferentFingerprintMerges(t *testing.T) {
	c := NewClusterer(10*time.Minute, nil) // 无拓扑函数
	cl1, _ := c.Ingest(Event{Fingerprint: "fpA", NodeKey: "n1", OccurredAt: base})
	cl2, created := c.Ingest(Event{Fingerprint: "fpB", NodeKey: "n1", OccurredAt: base.Add(time.Second)})
	if created {
		t.Fatal("same node (distance 0) must merge even without domain func")
	}
	if cl1.Key != cl2.Key {
		t.Fatalf("keys differ: %s vs %s", cl1.Key, cl2.Key)
	}
	if len(cl2.Fingerprints) != 2 {
		t.Fatalf("cluster fingerprints = %d, want 2", len(cl2.Fingerprints))
	}
}

func TestIngestDomainConnectedMerges(t *testing.T) {
	d := fakeDomain{"n1": {"n2"}, "n2": {"n3"}} // n1-n2-n3 连通，n4 孤立
	c := NewClusterer(10*time.Minute, d.same)
	c.Ingest(Event{Fingerprint: "fpA", NodeKey: "n1", OccurredAt: base})
	_, created := c.Ingest(Event{Fingerprint: "fpB", NodeKey: "n2", OccurredAt: base.Add(time.Second)})
	if created {
		t.Fatal("domain-connected nodes must merge")
	}
	_, created = c.Ingest(Event{Fingerprint: "fpC", NodeKey: "n4", OccurredAt: base.Add(2 * time.Second)})
	if !created {
		t.Fatal("isolated node must form a new cluster")
	}
}

func TestIngestUnlocatableAlertOnlyMatchesByFingerprint(t *testing.T) {
	c := NewClusterer(10*time.Minute, nil)
	c.Ingest(Event{Fingerprint: "fpA", NodeKey: "n1", OccurredAt: base})
	// NodeKey 为空的告警：不得被域聚合吸进任何簇。
	_, created := c.Ingest(Event{Fingerprint: "fpB", OccurredAt: base.Add(time.Second)})
	if !created {
		t.Fatal("alert without NodeKey must not merge via domain proximity")
	}
}

func TestIngestEmptyFingerprintNotClustered(t *testing.T) {
	c := NewClusterer(10*time.Minute, nil)
	cl, created := c.Ingest(Event{Fingerprint: "", NodeKey: "n1", OccurredAt: base})
	if cl != nil || created {
		t.Fatalf("empty fingerprint must return (nil, false), got (%v, %v)", cl, created)
	}
	if c.ActiveCount() != 0 {
		t.Fatalf("ActiveCount = %d, want 0", c.ActiveCount())
	}
}

func TestWindowExpiryResolvesAndSplits(t *testing.T) {
	c := NewClusterer(10*time.Minute, nil)
	cl1, _ := c.Ingest(Event{Fingerprint: "fp1", NodeKey: "n1", OccurredAt: base})
	if n := c.Sweep(base.Add(9 * time.Minute)); n != 0 {
		t.Fatalf("sweep inside window resolved %d, want 0", n)
	}
	if n := c.Sweep(base.Add(10 * time.Minute)); n != 1 {
		t.Fatalf("sweep at boundary resolved %d, want 1", n)
	}
	if got := c.Get(cl1.Key); got.State != StateResolved {
		t.Fatalf("state = %s, want resolved", got.State)
	}
	// resolve 后同指纹重新告警 → 新簇（新窗口桶）。
	cl2, created := c.Ingest(Event{Fingerprint: "fp1", NodeKey: "n1", OccurredAt: base.Add(11 * time.Minute)})
	if !created {
		t.Fatal("post-expiry alert must create a new cluster")
	}
	if cl1.Key == cl2.Key {
		t.Fatal("new window must produce a new cluster key")
	}
}

func TestAckStateMachine(t *testing.T) {
	c := NewClusterer(10*time.Minute, nil)
	cl, _ := c.Ingest(Event{Fingerprint: "fp1", NodeKey: "n1", OccurredAt: base})

	st, err := c.Ack(cl.Key)
	if err != nil || st != StateAcked {
		t.Fatalf("Ack open: (%s, %v), want acked", st, err)
	}
	// 幂等：重复 ack 不报错不变形。
	st, err = c.Ack(cl.Key)
	if err != nil || st != StateAcked {
		t.Fatalf("Ack idempotent: (%s, %v)", st, err)
	}
	// 窗口内新告警不改变 acked（有人在处理）。
	cl2, _ := c.Ingest(Event{Fingerprint: "fp2", NodeKey: "n1", OccurredAt: base.Add(time.Minute)})
	if cl2.State != StateAcked {
		t.Fatalf("new alert must not demote acked, got %s", cl2.State)
	}
	// resolved 是终态：Ack 不能拉回。
	c.Sweep(base.Add(20 * time.Minute))
	st, err = c.Ack(cl.Key)
	if err != nil || st != StateResolved {
		t.Fatalf("Ack on resolved must be no-op, got (%s, %v)", st, err)
	}
	// 未知簇。
	if _, err := c.Ack("nope"); !errors.Is(err, ErrUnknownCluster) {
		t.Fatalf("Ack unknown: err = %v, want ErrUnknownCluster", err)
	}
}

func TestCandidateSelectionDeterministic(t *testing.T) {
	// 两个候选簇：同 NodeKey 构造域命中（nil domain 时同键即命中）。
	c := NewClusterer(30*time.Minute, nil)
	// cluster A：更早 LastSeen；cluster B：更晚 LastSeen。
	a, _ := c.Ingest(Event{Fingerprint: "fpA", NodeKey: "na", OccurredAt: base})
	b, _ := c.Ingest(Event{Fingerprint: "fpB", NodeKey: "nb", OccurredAt: base.Add(5 * time.Minute)})
	_ = a
	_ = b
	// nb 与 na 是不同节点、无域函数 → 本应不合并；同键命中只在共享节点时发生。
	// 构造共享节点：C 簇在 na 上。
	ca, _ := c.Ingest(Event{Fingerprint: "fpC", NodeKey: "na", OccurredAt: base.Add(2 * time.Minute)})
	// na 上的 fpA（LastSeen=base）与 fpC（LastSeen=base+2m）：fpD 在 na 上应并入 fpC 簇（LastSeen 更新）。
	cl, created := c.Ingest(Event{Fingerprint: "fpD", NodeKey: "na", OccurredAt: base.Add(3 * time.Minute)})
	if created {
		t.Fatal("shared node must merge")
	}
	if cl.Key != ca.Key {
		t.Fatalf("candidate should be most recent (fpC cluster %s), got %s", ca.Key, cl.Key)
	}
}

func TestReplayDeterminism(t *testing.T) {
	events := []Event{
		{Fingerprint: "fp1", NodeKey: "n1", Severity: "warning", Summary: "a", OccurredAt: base},
		{Fingerprint: "fp2", NodeKey: "n2", Severity: "critical", Summary: "b", OccurredAt: base.Add(time.Second)},
		{Fingerprint: "fp3", NodeKey: "n3", Severity: "info", Summary: "c", OccurredAt: base.Add(2 * time.Second)},
	}
	d := fakeDomain{"n1": {"n2"}}
	run := func() []Cluster {
		c := NewClusterer(10*time.Minute, d.same)
		for _, e := range events {
			c.Ingest(e)
		}
		return c.Clusters()
	}
	r1, r2 := run(), run()
	if len(r1) != len(r2) {
		t.Fatalf("replay cluster count differs: %d vs %d", len(r1), len(r2))
	}
	for i := range r1 {
		if r1[i].Key != r2[i].Key || r1[i].Severity != r2[i].Severity {
			t.Fatalf("replay not deterministic at %d: %+v vs %+v", i, r1[i], r2[i])
		}
	}
	// fp1/fp2 同域合并、fp3 独立 → 2 簇。
	if len(r1) != 2 {
		t.Fatalf("cluster count = %d, want 2", len(r1))
	}
	// Clusters() 按 Key 字典序有序。
	for i := 1; i < len(r1); i++ {
		if r1[i-1].Key >= r1[i].Key {
			t.Fatal("Clusters() must be sorted by key")
		}
	}
}

func TestSeverityRanking(t *testing.T) {
	if !(SeverityRank["critical"] > SeverityRank["warning"] && SeverityRank["warning"] > SeverityRank["info"] && SeverityRank["info"] > SeverityRank[""]) {
		t.Fatal("severity ranking must be critical > warning > info > empty")
	}
	// 未知等级不高于 info。
	if SeverityRank["unknown-thing"] >= SeverityRank["critical"] {
		t.Fatal("unknown severity must rank below critical")
	}
}

func TestZeroWindowEveryAlertOwnCluster(t *testing.T) {
	c := NewClusterer(0, nil)
	c1, _ := c.Ingest(Event{Fingerprint: "fp1", NodeKey: "n1", OccurredAt: base})
	c2, created := c.Ingest(Event{Fingerprint: "fp1", NodeKey: "n1", OccurredAt: base.Add(time.Second)})
	if !created || c1.Key == c2.Key {
		t.Fatal("zero window = no clustering: each alert its own cluster")
	}
}

func TestGetSnapshotIsolation(t *testing.T) {
	c := NewClusterer(10*time.Minute, nil)
	cl, _ := c.Ingest(Event{Fingerprint: "fp1", NodeKey: "n1", OccurredAt: base})
	got := c.Get(cl.Key)
	got.Fingerprints["injected"] = struct{}{}
	got.NodeKeys["injected"] = struct{}{}
	again := c.Get(cl.Key)
	if _, ok := again.Fingerprints["injected"]; ok {
		t.Fatal("Get must return deep copies (fingerprint set leaked)")
	}
	if _, ok := again.NodeKeys["injected"]; ok {
		t.Fatal("Get must return deep copies (node set leaked)")
	}
}
