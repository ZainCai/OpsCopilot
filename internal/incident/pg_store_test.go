// PGStore 打库集成测试（W9）：OPS_TEST_PG_DSN 门控，无 DB 自动跳过。
// 前置：migrations/000004 已应用（compose 栈已执行）。
package incident

import (
	"context"
	"os"
	"testing"
	"time"
)

func pgStoreForTest(t *testing.T) *PGStore {
	t.Helper()
	dsn := os.Getenv("OPS_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("OPS_TEST_PG_DSN not set — PG incident integration skipped")
	}
	s, err := NewPGStore(context.Background(), dsn, "default")
	if err != nil {
		t.Fatalf("pg store: %v", err)
	}
	t.Cleanup(s.Close)
	return s
}

// TestPGStoreRoundTrip 全生命周期：创建→转换→关联→反查→列表过滤。
// 清理按 incident_id 前缀，不残留。
func TestPGStoreRoundTrip(t *testing.T) {
	s := pgStoreForTest(t)
	ctx := context.Background()
	id := "INC-R6-" + time.Now().Format("150405")
	cleanup := func() {
		s.pool.Exec(ctx, `DELETE FROM incident_cluster WHERE incident_row_id IN
			(SELECT id FROM incident WHERE incident_id LIKE 'INC-R6-%')`)
		s.pool.Exec(ctx, `DELETE FROM incident WHERE incident_id LIKE 'INC-R6-%'`)
	}
	cleanup()
	t.Cleanup(cleanup)

	// 创建 → 重复 ID 拒绝。
	if _, err := s.Create(id, "pg roundtrip", "critical", "ops"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := s.Create(id, "dup", "critical", "ops"); err == nil {
		t.Fatal("duplicate id accepted")
	}
	// 关联（幂等）。
	if err := s.AttachCluster(id, "c:pgtest@1"); err != nil {
		t.Fatalf("attach: %v", err)
	}
	if err := s.AttachCluster(id, "c:pgtest@1"); err != nil {
		t.Fatalf("attach idempotent: %v", err)
	}
	// Get 回读：状态/关联齐。
	got, err := s.Get(id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.State != StateOpen || len(got.ClusterKeys) != 1 || got.ClusterKeys[0] != "c:pgtest@1" {
		t.Fatalf("get mismatch: %+v", got)
	}
	// 反查。
	if inc, ok := s.IncidentForCluster("c:pgtest@1"); !ok || inc.ID != id {
		t.Fatalf("for-cluster: %+v ok=%v", inc, ok)
	}
	// 转换 → resolved 落戳。
	got, err = s.Transition(id, StateResolved, "ops")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got.ResolvedAt.IsZero() {
		t.Fatal("resolved_at empty")
	}
	// 非法转换拒绝：resolved 后回退 acked。
	if _, err := s.Transition(id, StateAcked, "ops"); err == nil {
		t.Fatal("resolved -> acked accepted")
	}
	// List 过滤。
	if list := s.List(StateResolved); len(list) == 0 {
		t.Fatal("resolved list empty")
	}
	if s.Persistence() != "timescaledb" {
		t.Fatal("persistence label wrong")
	}
}

// TestPGStoreClusterOwnership 一簇被其他事件占用 → 拒绝。
func TestPGStoreClusterOwnership(t *testing.T) {
	s := pgStoreForTest(t)
	ctx := context.Background()
	stamp := time.Now().Format("150405")
	idA, idB := "INC-R6A-"+stamp, "INC-R6B-"+stamp
	ck := "c:r6own@" + stamp
	cleanup := func() {
		s.pool.Exec(ctx, `DELETE FROM incident_cluster WHERE incident_row_id IN
			(SELECT id FROM incident WHERE incident_id LIKE 'INC-R6%')`)
		s.pool.Exec(ctx, `DELETE FROM incident WHERE incident_id LIKE 'INC-R6%'`)
	}
	cleanup()
	t.Cleanup(cleanup)
	if _, err := s.Create(idA, "a", "critical", "ops"); err != nil {
		t.Fatalf("create a: %v", err)
	}
	if _, err := s.Create(idB, "b", "warning", "ops"); err != nil {
		t.Fatalf("create b: %v", err)
	}
	if err := s.AttachCluster(idA, ck); err != nil {
		t.Fatalf("attach a: %v", err)
	}
	err := s.AttachCluster(idB, ck)
	if err == nil || err.Error() != "incident: cluster \""+ck+"\" already attached to \""+idA+"\"" {
		t.Fatalf("ownership: err = %v, want already-attached", err)
	}
}

// TestPGStoreListFillClustersBatched List 的簇关联批量填充必须"分桶正确"：
// 每个事件只带自己的 cluster_keys（批量 ANY 查询 + 按 incident_id 回填的
// 主要风险就是串桶）。回归自：fillClusters 从逐事件一查（N+1）改为单次
// 批量查询。
func TestPGStoreListFillClustersBatched(t *testing.T) {
	s := pgStoreForTest(t)
	ctx := context.Background()
	stamp := time.Now().Format("150405.000000")
	idA, idB := "INC-BATCHA-"+stamp, "INC-BATCHB-"+stamp
	ckA, ckB := "c:batchA@"+stamp, "c:batchB@"+stamp
	cleanup := func() {
		s.pool.Exec(ctx, `DELETE FROM incident_cluster WHERE incident_row_id IN
			(SELECT id FROM incident WHERE incident_id LIKE 'INC-BATCH%')`)
		s.pool.Exec(ctx, `DELETE FROM incident WHERE incident_id LIKE 'INC-BATCH%'`)
	}
	cleanup()
	t.Cleanup(cleanup)

	if _, err := s.Create(idA, "batch a", "critical", "ops"); err != nil {
		t.Fatalf("create a: %v", err)
	}
	if _, err := s.Create(idB, "batch b", "warning", "ops"); err != nil {
		t.Fatalf("create b: %v", err)
	}
	if err := s.AttachCluster(idA, ckA); err != nil {
		t.Fatalf("attach a: %v", err)
	}
	if err := s.AttachCluster(idB, ckB); err != nil {
		t.Fatalf("attach b: %v", err)
	}

	byID := map[string][]string{}
	for _, inc := range s.List("") {
		byID[inc.ID] = inc.ClusterKeys
	}
	if got := byID[idA]; len(got) != 1 || got[0] != ckA {
		t.Fatalf("A clusters = %v, want [%s]", got, ckA)
	}
	if got := byID[idB]; len(got) != 1 || got[0] != ckB {
		t.Fatalf("B clusters = %v, want [%s]", got, ckB)
	}
}
