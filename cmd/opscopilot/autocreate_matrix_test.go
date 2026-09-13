// autocreate_matrix_test.go W10-4（ADR-016）配置矩阵的行为侧断言：
// OPS_INCIDENT_AUTOCREATE=on + OPS_AUTOATTACH=on + enforce 两路同开——
// enforce 判决直写建单挂簇（链路 B'）与链路 A 队列消费建单共用同一幂等键
// （origin=prometheus + source_ref=promFingerprint(labels)），"天然收敛一单、
// 无硬依赖"此前只是 assembly.go 注释与排期 W10-6 注记里的口径，这里给它上锁：
//   - 判决先建挂簇单后，worker 消费同一告警只刷新不新建（单数/ID/首挂簇/审计不变）；
//   - 反向不压制：另一告警经 worker 照常新建（收敛靠幂等键，不靠开关互斥）。
//
// 配置层矩阵（合法性 + fail-fast 不放松）见 internal/config load_test 的
// TestValidateAutoCreateAutoAttachMatrix。纯内存 Store，无需 OPS_TEST_PG_DSN。
package main

import (
	"testing"
	"time"

	"opscopilot/internal/connector"
	"opscopilot/internal/incident"
)

func TestAutoCreateAutoAttachConvergeSingleIncident(t *testing.T) {
	store := incident.NewMemStore()
	audit := NewMemAuditLog()
	m := NewAppMetrics()

	// ① 链路 B'：enforce + AUTOATTACH=on，new-incident 判决 → 直写建单 + 挂簇。
	ne := autoAttachEngine(t, true)
	ne.SetMetrics(m)
	ne.SetAutoAttach(store, audit)
	labels := map[string]string{"alertname": "NetDown", "instance": "sw-1"}
	ne.ProcessAlerts([]connector.Alert{{
		Fingerprint: "fp-matrix-1", Labels: labels, Severity: "critical", StartsAt: time.Now(),
	}})
	list, err := store.List("")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("判决直写应建 1 单，got %d", len(list))
	}
	inc := list[0]
	if inc.Origin != incident.OriginPrometheus || inc.SourceRef != promFingerprint(labels) {
		t.Fatalf("首单键位 = %s/%s, want prometheus/%s", inc.Origin, inc.SourceRef, promFingerprint(labels))
	}

	// ② 链路 A：autoCreate=on 的 worker 消费同一告警（pull 形态载荷：origin 与
	//    source_ref 与判决直写同源）。queue 传 nil 合法——process 路径不触碰队列
	//    （领取/确认属 drain 职责，装配接线由 assembly_ingest_test.go 覆盖）。
	w := NewIngestWorker(nil, store, audit, 0, 0, true, 0, 0, nil)
	payload := `{"labels":{"alertname":"NetDown","instance":"sw-1","severity":"critical"},` +
		`"startsAt":"2026-09-13T00:00:00Z","endsAt":"0001-01-01T00:00:00Z",` +
		`"fingerprint":"` + inc.SourceRef + `"}`
	if perr := w.process(Item{ID: 1, Origin: incident.OriginPrometheus,
		SourceRef: inc.SourceRef, Payload: payload}); perr != nil {
		t.Fatalf("worker 消费同键告警: %v", perr)
	}

	list2, _ := store.List("")
	if len(list2) != 1 || list2[0].ID != inc.ID {
		t.Fatalf("两路同开必须收敛一单，got %v", list2)
	}
	if len(list2[0].ClusterKeys) != 1 || list2[0].Title != "NetDown" || list2[0].Severity != "critical" {
		t.Fatalf("刷新不得改动首挂簇/标题/严重级：keys=%v title=%q sev=%q",
			list2[0].ClusterKeys, list2[0].Title, list2[0].Severity)
	}
	if got := m.AutoAttach["attached"].Value(); got != 1 {
		t.Fatalf("worker 路径不产生挂簇动作，attached = %d, want 1", got)
	}
	if entries, _ := audit.List(inc.ID); len(entries) != 1 || entries[0].Action != AuditAttachCluster {
		t.Fatalf("刷新不占建单额度也不新增审计，want 仅 1 条 attach_cluster，got %v", entries)
	}

	// ③ 反例护栏：收敛靠 (origin, source_ref) 幂等键，不是全局压制——
	//    另一告警经同一 worker 照常新建并留 create 审计。
	otherRef := "prom:matrix-other-ref"
	otherPayload := `{"labels":{"alertname":"CpuHot","severity":"warning"},` +
		`"startsAt":"2026-09-13T00:00:00Z","endsAt":"0001-01-01T00:00:00Z",` +
		`"fingerprint":"` + otherRef + `"}`
	if perr := w.process(Item{ID: 2, Origin: incident.OriginPrometheus,
		SourceRef: otherRef, Payload: otherPayload}); perr != nil {
		t.Fatalf("worker 消费另一告警: %v", perr)
	}
	list3, _ := store.List("")
	if len(list3) != 2 {
		t.Fatalf("异键告警应照常新建（不压制），got %d 单", len(list3))
	}
	created, err := store.Get("prometheus:" + otherRef)
	if err != nil || len(created.ClusterKeys) != 0 {
		t.Fatalf("链路 A 所建单无域（挂簇是判决路径的事），got %+v err=%v", created, err)
	}
	if entries, _ := audit.List(created.ID); len(entries) != 1 || entries[0].Action != AuditCreate {
		t.Fatalf("新建应有 1 条 create 审计，got %v", entries)
	}
}
