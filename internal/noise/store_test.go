// W4-1.5 持久化契约单元测试：ToRecord 完整性 + Restore 语义。
package noise

import (
	"testing"
	"time"
)

func TestToRecordComplete(t *testing.T) {
	// n1-n2 域连通：两条异指纹告警并入同一簇，导出记录须完整。
	c := NewClusterer(10*time.Minute, fakeDomain{"n2": {"n1"}}.same)
	cl, _ := c.Ingest(Event{Fingerprint: "fpB", NodeKey: "n2", Severity: "warning", Summary: "s", OccurredAt: base})
	c.Ingest(Event{Fingerprint: "fpA", NodeKey: "n1", Severity: "critical", OccurredAt: base.Add(time.Second)})
	rec := cl.ToRecord("tenant-x")
	if rec.TenantID != "tenant-x" || rec.ClusterKey != cl.Key || rec.Severity != "critical" || rec.AlertCount != 2 {
		t.Fatalf("record fields wrong: %+v", rec)
	}
	// 集合导出必须有序（决定性，JSON/DB 行稳定）。
	if rec.Fingerprints[0] != "fpA" || rec.Fingerprints[1] != "fpB" {
		t.Fatalf("fingerprints not sorted: %v", rec.Fingerprints)
	}
	if rec.NodeKeys[0] != "n1" || rec.NodeKeys[1] != "n2" {
		t.Fatalf("node keys not sorted: %v", rec.NodeKeys)
	}
}

func TestRestoreContinuesIdempotentChain(t *testing.T) {
	// 原始生命周期。
	src := NewClusterer(10*time.Minute, fakeDomain{"n1": {"n2"}}.same)
	src.Ingest(Event{Fingerprint: "fpA", NodeKey: "n1", OccurredAt: base})
	src.Ingest(Event{Fingerprint: "fpB", NodeKey: "n2", OccurredAt: base.Add(time.Minute)})
	var recs []ClusterRecord
	for _, cl := range src.Clusters() {
		recs = append(recs, cl.ToRecord("default"))
	}

	// 重建。
	dst := NewClusterer(10*time.Minute, nil)
	if err := dst.Restore(recs); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	got := dst.Clusters()
	if len(got) != len(recs) {
		t.Fatalf("restored %d clusters, want %d", len(got), len(recs))
	}
	if got[0].Key != recs[0].ClusterKey || got[0].State != recs[0].State {
		t.Fatal("restore must preserve key and state verbatim")
	}

	// 重建后继续 Ingest：同指纹命中原簇（不新建），域函数重建后仍可注入生效。
	dst.SetDomain(fakeDomain{"n1": {"n3"}}.same)
	v, _ := dst.Ingest(Event{Fingerprint: "fpA", NodeKey: "n1", OccurredAt: base.Add(2 * time.Minute)})
	if v.Key != recs[0].ClusterKey {
		t.Fatalf("post-restore ingest hit %s, want %s", v.Key, recs[0].ClusterKey)
	}
	v2, created := dst.Ingest(Event{Fingerprint: "fpC", NodeKey: "n3", OccurredAt: base.Add(3 * time.Minute)})
	if created || v2.Key != recs[0].ClusterKey {
		t.Fatalf("domain proximity broken after restore: created=%v key=%s", created, v2.Key)
	}
}

func TestRestoreResolvedClusterNotIndexed(t *testing.T) {
	c := NewClusterer(10*time.Minute, nil)
	if err := c.Restore([]ClusterRecord{{
		ClusterKey: "c:old@0", State: StateResolved,
		FirstSeen: base, LastSeen: base,
		Fingerprints: []string{"fpOld"}, NodeKeys: []string{"n1"},
	}}); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	// resolved 只留本体：指纹不得再命中（不吸收新告警）。
	cl, created := c.Ingest(Event{Fingerprint: "fpOld", NodeKey: "n1", OccurredAt: base.Add(time.Minute)})
	if !created || cl.Key == "c:old@0" {
		t.Fatalf("resolved record must not absorb new alerts: created=%v key=%s", created, cl.Key)
	}
	// 但本体保留可查。
	if got := c.Get("c:old@0"); got == nil || got.State != StateResolved {
		t.Fatal("resolved cluster body must be queryable after restore")
	}
}

func TestRestoreReplacesState(t *testing.T) {
	c := NewClusterer(10*time.Minute, nil)
	c.Ingest(Event{Fingerprint: "fpStale", OccurredAt: base})
	// 用不含 fpStale 的记录重建：旧状态整体消失（替换语义，不是合并）。
	if err := c.Restore([]ClusterRecord{{
		ClusterKey: "c:fresh@0", State: StateOpen,
		Fingerprints: []string{"fpFresh"},
	}}); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if _, hit := c.Get("c:fresh@0"), true; !hit {
		t.Fatal("restored cluster missing")
	}
	for _, cl := range c.Clusters() {
		if _, ok := cl.Fingerprints["fpStale"]; ok {
			t.Fatal("stale cluster survived restore (must replace, not merge)")
		}
	}
	// fpStale 不再命中任何簇 → 重新建簇。
	_, created := c.Ingest(Event{Fingerprint: "fpStale", OccurredAt: base.Add(time.Minute)})
	if !created {
		t.Fatal("stale fingerprint must be clusterable again after replace-style restore")
	}
}
