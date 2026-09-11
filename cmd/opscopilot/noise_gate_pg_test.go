// noise_gate_pg_test.go W9-1：闸门计数真相源读写（OPS_TEST_PG_DSN 门控）。
package main

import (
	"testing"

	"opscopilot/internal/notify"
)

func TestGateStatsPGRoundtrip(t *testing.T) {
	sink := pgSinkForTest(t)
	gs := &pgPoolGateStats{pool: sink.pool}
	tenant := "gate-stats-test"

	// 缺行 = 零值不报错（首次运行常态）。
	st, err := gs.LoadGateStats(tenant)
	if err != nil {
		t.Fatalf("load missing stats: %v", err)
	}
	if st.Suppressed != 0 || st.Dispatched != 0 {
		t.Fatalf("missing stats must be zero, got %+v", st)
	}

	// 写 → 读回。
	if err := gs.SaveGateStats(tenant, notify.Stats{Suppressed: 11, Dispatched: 4}); err != nil {
		t.Fatalf("save: %v", err)
	}
	st, err = gs.LoadGateStats(tenant)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if st.Suppressed != 11 || st.Dispatched != 4 {
		t.Fatalf("roundtrip = %+v, want 11/4", st)
	}

	// 覆盖写幂等（累计值更新）。
	if err := gs.SaveGateStats(tenant, notify.Stats{Suppressed: 12, Dispatched: 5}); err != nil {
		t.Fatalf("save 2: %v", err)
	}
	st, _ = gs.LoadGateStats(tenant)
	if st.Suppressed != 12 || st.Dispatched != 5 {
		t.Fatalf("upsert = %+v, want 12/5", st)
	}

	// 清理测试租户行（不留垃圾数据）。
	pgExec(t, sink, `DELETE FROM notify_gate_stats WHERE tenant_id = $1`, tenant)
}
