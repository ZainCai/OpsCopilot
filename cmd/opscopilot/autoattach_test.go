// autoattach_test.go W10-6 簇→事件生产自动挂簇（OPS_AUTOATTACH）行为锁定：
//   - off（默认）零行为——开关未开 / 出口未挂载 / shadow 引擎误挂载，三种
//     "不该动"的形态都不建单不挂簇（防御在判决出口，不信任配置）；
//   - on + enforce new-incident 内存端到端：建单（幂等键与链路 A 同源）+
//     挂簇 + attach_cluster 审计（AuditAttachCluster 不再是死代码）；
//   - 共簇冲突（一簇一事件唯一索引）跳过并计 conflict，不报错不级联；
//   - 幂等重放不产生副本；labels 缺失算不出幂等键 → skipped。
//
// PG 真库端到端见 autoattach_e2e_pg_test.go（OPS_TEST_PG_DSN 门控）。
package main

import (
	"testing"
	"time"

	"opscopilot/internal/config"
	"opscopilot/internal/connector"
	"opscopilot/internal/incident"
)

// autoAttachEngine enforce 引擎（AutoAttach 开关按需覆写），拓扑用共享夹具。
func autoAttachEngine(t *testing.T, auto bool) *NoiseEngine {
	t.Helper()
	ne := NewNoiseEngine(noiseTestSink(t), newQuietLogger(), testTenant,
		noiseSpec(func(s *config.NoiseSection) {
			s.Mode = ModeEnforce
			s.AutoAttach = auto
		}), config.MemLimitSection{})
	if ne == nil {
		t.Fatal("enforce engine expected (noise disabled?)")
	}
	return ne
}

func autoAttachAlert(fp, inst string, at time.Time) connector.Alert {
	return connector.Alert{
		Fingerprint: fp,
		Labels:      map[string]string{"alertname": "DiskFull", "instance": inst},
		Severity:    "critical",
		StartsAt:    at,
	}
}

// mustZeroBehavior 断言 store/audit 全空（off 三形态共用）。
func mustZeroBehavior(t *testing.T, label string, store *incident.MemStore, audit *MemAuditLog) {
	t.Helper()
	list, err := store.List("")
	if err != nil {
		t.Fatalf("%s: list: %v", label, err)
	}
	if len(list) != 0 {
		t.Fatalf("%s: off 必须零行为，却有事件 %d 条", label, len(list))
	}
	entries, err := audit.List("")
	if err != nil {
		t.Fatalf("%s: audit list: %v", label, err)
	}
	if len(entries) != 0 {
		t.Fatalf("%s: off 不该有任何审计，got %v", label, entries)
	}
}

func TestAutoAttachOffZeroBehavior(t *testing.T) {
	now := time.Now()

	t.Run("switch-off-but-mounted", func(t *testing.T) {
		store := incident.NewMemStore()
		audit := NewMemAuditLog()
		ne := autoAttachEngine(t, false)
		ne.SetAutoAttach(store, audit) // 挂了出口但开关 off（装配层不会这样，防御面）
		ne.ProcessAlerts([]connector.Alert{autoAttachAlert("fp-off-1", "i1", now)})
		mustZeroBehavior(t, "switch-off", store, audit)
	})

	t.Run("switch-on-but-not-mounted", func(t *testing.T) {
		store := incident.NewMemStore()
		audit := NewMemAuditLog()
		ne := autoAttachEngine(t, true) // SetAutoAttach 未调（如嵌入式装配遗漏）
		ne.ProcessAlerts([]connector.Alert{autoAttachAlert("fp-off-2", "i1", now)})
		mustZeroBehavior(t, "not-mounted", store, audit)
	})

	t.Run("shadow-never-fires", func(t *testing.T) {
		store := incident.NewMemStore()
		audit := NewMemAuditLog()
		ne := NewNoiseEngine(noiseTestSink(t), newQuietLogger(), testTenant,
			noiseSpec(func(s *config.NoiseSection) { s.AutoAttach = true }), // shadow（默认）+ 开关误开
			config.MemLimitSection{})
		ne.SetAutoAttach(store, audit)
		ne.ProcessAlerts([]connector.Alert{autoAttachAlert("fp-off-3", "i1", now)})
		mustZeroBehavior(t, "shadow", store, audit)
	})
}

func TestAutoAttachOnEnforceMemoryEndToEnd(t *testing.T) {
	store := incident.NewMemStore()
	audit := NewMemAuditLog()
	m := NewAppMetrics()
	ne := autoAttachEngine(t, true)
	ne.SetMetrics(m)
	ne.SetAutoAttach(store, audit)

	now := time.Now()
	labels := map[string]string{"alertname": "DiskFull", "instance": "i1"}
	ne.ProcessAlerts([]connector.Alert{{
		Fingerprint: "fp-e2e-1", Labels: labels, Severity: "critical", StartsAt: now,
	}})

	list, err := store.List("")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("new-incident 应建 1 单，got %d", len(list))
	}
	inc := list[0]
	if want := promFingerprint(labels); inc.SourceRef != want {
		t.Fatalf("source_ref = %q, want 链路 A 同款幂等键 %q", inc.SourceRef, want)
	}
	if inc.Origin != incident.OriginPrometheus {
		t.Fatalf("origin = %q", inc.Origin)
	}
	if inc.CreatedBy != autoAttachActor {
		t.Fatalf("created_by = %q, want %q", inc.CreatedBy, autoAttachActor)
	}
	if inc.Title != "DiskFull" {
		t.Fatalf("title = %q", inc.Title)
	}
	active := ne.shadow.Clusterer().ActiveClusters()
	if len(active) != 1 || len(inc.ClusterKeys) != 1 || inc.ClusterKeys[0] != active[0].Key {
		t.Fatalf("挂簇结果 = %v, want 首簇 %v", inc.ClusterKeys, active)
	}
	if m.AutoAttach["attached"].Value() != 1 {
		t.Fatalf("attached 计数 = %d, want 1", m.AutoAttach["attached"].Value())
	}

	// attach_cluster 审计已落（本任务后 AuditAttachCluster 不再是死代码）。
	entries, err := audit.List(inc.ID)
	if err != nil {
		t.Fatalf("audit: %v", err)
	}
	if len(entries) != 1 || entries[0].Action != AuditAttachCluster || entries[0].Actor != autoAttachActor {
		t.Fatalf("audit entries = %+v, want 1 条 attach_cluster/%s", entries, autoAttachActor)
	}
	if got := entries[0].Detail["cluster_key"]; got != active[0].Key {
		t.Fatalf("audit detail cluster_key = %v, want %v", got, active[0].Key)
	}

	// 窗口内重复告警：dedup 判决不再触发挂簇（不重复建单/挂簇/审计）。
	ne.ProcessAlerts([]connector.Alert{autoAttachAlert("fp-e2e-1", "i1", now.Add(time.Second))})
	list2, _ := store.List("")
	if len(list2) != 1 {
		t.Fatalf("窗口重复不应新建事件，got %d", len(list2))
	}
	entries2, _ := audit.List(inc.ID)
	if len(entries2) != 1 {
		t.Fatalf("窗口重复不应新增挂簇审计，got %d", len(entries2))
	}
}

// TestAutoAttachConflictSharedClusterSkipsAndCounts 一簇一事件唯一索引：
// 簇已被首单占用时后来的建单联动**跳过并计 conflict**——不报错不级联、
// 不覆盖首挂（多指纹共簇仅首单持故障域的自动侧口径；其余事件归并走人工
// MergeInto，见 docs/M2执行排期 W10-6 注记）。
func TestAutoAttachConflictSharedClusterSkipsAndCounts(t *testing.T) {
	store := incident.NewMemStore()
	audit := NewMemAuditLog()
	m := NewAppMetrics()
	ne := autoAttachEngine(t, true)
	ne.SetMetrics(m)
	ne.SetAutoAttach(store, audit)

	shared := "c:fp-shared@100"
	req1 := autoAttachRequest{labels: map[string]string{"alertname": "A", "instance": "i1"},
		fingerprint: "fp-1", clusterKey: shared, severity: "critical", summary: "A"}
	ne.autoAttachClusters(store, audit, []autoAttachRequest{req1})

	// 另一指纹的联动任务携带同一簇键（竞态/回放/共簇场景）。
	req2 := autoAttachRequest{labels: map[string]string{"alertname": "B", "instance": "i2"},
		fingerprint: "fp-2", clusterKey: shared, severity: "warning", summary: "B"}
	ne.autoAttachClusters(store, audit, []autoAttachRequest{req2}) // 不得 panic / 不得上抛

	if got := m.AutoAttach["attached"].Value(); got != 1 {
		t.Fatalf("attached = %d, want 1", got)
	}
	if got := m.AutoAttach["conflict"].Value(); got != 1 {
		t.Fatalf("conflict = %d, want 1（共簇冲突单独计桶，不混 skipped）", got)
	}
	if got := m.AutoAttach["skipped"].Value(); got != 0 {
		t.Fatalf("conflict 不该进 skipped 桶，got %d", got)
	}
	if ne.attachFailures.Load() != 0 {
		t.Fatalf("conflict 不是故障，attachFailures 不该涨，got %d", ne.attachFailures.Load())
	}
	list, _ := store.List("")
	if len(list) != 2 {
		t.Fatalf("两指纹各成一单（事件照常建，只是不挂簇），got %d", len(list))
	}
	totalKeys := 0
	audited := 0
	for _, inc := range list {
		totalKeys += len(inc.ClusterKeys)
		entries, _ := audit.List(inc.ID)
		audited += len(entries)
	}
	if totalKeys != 1 || audited != 1 {
		t.Fatalf("首挂独占：cluster_keys 合计 = %d, 审计 = %d 条, want 1/1", totalKeys, audited)
	}
}

// TestAutoAttachIdempotentReplay 重放同批联动任务：UpsertExternal 命中未解决
// 代刷新、AttachCluster 重复挂同簇同单幂等——不产生副本、不覆盖首挂。
func TestAutoAttachIdempotentReplay(t *testing.T) {
	store := incident.NewMemStore()
	audit := NewMemAuditLog()
	m := NewAppMetrics()
	ne := autoAttachEngine(t, true)
	ne.SetMetrics(m)
	ne.SetAutoAttach(store, audit)

	req := autoAttachRequest{labels: map[string]string{"alertname": "A", "instance": "i1"},
		fingerprint: "fp-replay", clusterKey: "c:fp-replay@200", severity: "warning", summary: "A"}
	ne.autoAttachClusters(store, audit, []autoAttachRequest{req})
	ne.autoAttachClusters(store, audit, []autoAttachRequest{req})

	list, _ := store.List("")
	if len(list) != 1 {
		t.Fatalf("重放不应新建第二单，got %d", len(list))
	}
	if len(list[0].ClusterKeys) != 1 {
		t.Fatalf("重复挂同簇同单不得产生副本，got %v", list[0].ClusterKeys)
	}
	if got := m.AutoAttach["attached"].Value(); got != 2 {
		t.Fatalf("重放同单幂等挂簇仍记 attached（幂等成功），got %d", got)
	}
}

// TestAutoAttachNoLabelsSkipped labels 缺失算不出链路 A 同款幂等键 →
// skipped 计数 + WARNING，宁可不建也不造第二身份。
func TestAutoAttachNoLabelsSkipped(t *testing.T) {
	store := incident.NewMemStore()
	m := NewAppMetrics()
	ne := autoAttachEngine(t, true)
	ne.SetMetrics(m)
	ne.SetAutoAttach(store, NewMemAuditLog())

	ne.autoAttachClusters(store, NewMemAuditLog(), []autoAttachRequest{{
		fingerprint: "fp-nolabels", clusterKey: "c:fp-nolabels@300", severity: "critical", summary: "X",
	}})
	if got := m.AutoAttach["skipped"].Value(); got != 1 {
		t.Fatalf("skipped = %d, want 1", got)
	}
	list, _ := store.List("")
	if len(list) != 0 {
		t.Fatalf("无幂等键不得建单，got %v", list)
	}
}
