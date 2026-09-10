// W6-1 TimescaleDB 真相源集成测试。
// 门控：仅当 OPS_TEST_PG_DSN 设置时运行（本机
// postgres://opscopilot:opsdev@127.0.0.1:5432/opscopilot?sslmode=disable，
// 需 compose 栈的 opscopilot-db 在跑）；CI 无 DB 自动跳过。
package main

import (
	"context"
	"os"
	"testing"
	"time"

	"opscopilot/internal/noise"
)

func pgSinkForTest(t *testing.T) *PGClusterSink {
	t.Helper()
	dsn := os.Getenv("OPS_TEST_PG_DSN")
	if dsn == "" {
		t.Skipf("OPS_TEST_PG_DSN not set — TimescaleDB integration test skipped")
	}
	sink, err := NewPGClusterSink(context.Background(), dsn, "default")
	if err != nil {
		t.Fatalf("pg sink: %v", err)
	}
	t.Cleanup(sink.Close)
	return sink
}

func TestPGClusterUpsertIdempotent(t *testing.T) {
	sink := pgSinkForTest(t)
	rec := noise.ClusterRecord{
		TenantID:     "default",
		ClusterKey:   "c:pgtest@1",
		State:        noise.StateOpen,
		FirstSeen:    time.Now().Add(-time.Hour),
		LastSeen:     time.Now(),
		Severity:     "critical",
		Summary:      "PGTest",
		AlertCount:   2,
		Fingerprints: []string{"fp1", "fp2"},
		NodeKeys:     []string{"prometheus://nodes/n1"},
	}
	// 两次保存（第二次 AlertCount=3）——最终状态=第二次（幂等覆盖写）。
	rec2 := rec
	rec2.AlertCount = 3
	if err := sink.SaveCluster(rec); err != nil {
		t.Fatalf("SaveCluster#1: %v", err)
	}
	if err := sink.SaveCluster(rec2); err != nil {
		t.Fatalf("SaveCluster#2: %v", err)
	}
	var state string
	var alertCount int
	err := sink.pool.QueryRow(context.Background(), `
SELECT state, (evidence->>'alert_count')::int
FROM alert_cluster WHERE tenant_id='default' AND cluster_key=$1`,
		rec.ClusterKey).Scan(&state, &alertCount)
	if err != nil {
		t.Fatalf("query cluster: %v", err)
	}
	if state != "open" || alertCount != 3 {
		t.Fatalf("upsert state=%s alertCount=%d, want open/3", state, alertCount)
	}
	pgExec(t, sink, `DELETE FROM alert_cluster WHERE cluster_key = $1`, rec.ClusterKey)
}

// TestPGVerdictInsertAndRoundtrip 逐告警判决落库（alert_event hypertable）。
func TestPGVerdictInsertAndRoundtrip(t *testing.T) {
	sink := pgSinkForTest(t)
	rec := noise.VerdictRecord{
		TenantID:      "default",
		Fingerprint:   "fp-verdict-test",
		NodeKey:       "prometheus://nodes/n1",
		OccurredAt:    time.Now().UTC(),
		ClusterKey:    "c:fp-verdict-test@1",
		Severity:      "warning",
		Summary:       "HighDiskUsage",
		WouldSuppress: false,
		WouldConverge: true,
		Reason:        "cluster-merge",
	}
	if err := sink.SaveVerdict(rec); err != nil {
		t.Fatalf("SaveVerdict: %v", err)
	}
	var clusterKey, reason string
	var conv bool
	err := sink.pool.QueryRow(context.Background(), `
SELECT cluster_key, payload->>'reason', (payload->>'would_converge')::boolean
FROM alert_event WHERE fingerprint = $1 AND source = 'shadow'
ORDER BY occurred_at DESC LIMIT 1`, rec.Fingerprint).Scan(&clusterKey, &reason, &conv)
	if err != nil {
		t.Fatalf("query verdict: %v", err)
	}
	if clusterKey != rec.ClusterKey || reason != "cluster-merge" || !conv {
		t.Fatalf("verdict roundtrip: key=%s reason=%s conv=%v", clusterKey, reason, conv)
	}
	pgExec(t, sink, `DELETE FROM alert_event WHERE fingerprint = $1`, rec.Fingerprint)
}

func pgExec(t *testing.T, sink *PGClusterSink, sql string, args ...any) {
	t.Helper()
	if _, err := sink.pool.Exec(context.Background(), sql, args...); err != nil {
		t.Fatalf("pg exec: %v", err)
	}
}
