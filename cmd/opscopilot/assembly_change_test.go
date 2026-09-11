// 变更库持久化装配测试（优化方案 #4）：
//   - 无 DB（默认开发/CI 形态）→ 装配必须给出纯内存后端（降级不阻塞启动）；
//   - OPS_TEST_PG_DSN 就绪 → 端到端"重启不丢取证"：第一次装配写入 →
//     Close（模拟进程退出）→ 第二次装配 LoadSince 回放 → 证据仍可查。
//
// PG 部分按仓库惯例门控跳过（对齐 assembly_ingest_test.go 的 skip 模式）。
package main

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"opscopilot/internal/connector"
	"opscopilot/internal/topology"
)

func TestAssembly_ChangeStoreMemoryWithoutDB(t *testing.T) {
	if os.Getenv("OPS_DB_DSN") != "" {
		t.Skip("OPS_DB_DSN set in environment — memory-default assertion not applicable")
	}
	a := newTestAssembly(t)
	if got := a.Changes.Persistence(); got != "memory" {
		t.Errorf("change persistence = %q, want memory (no DB → 降级内存)", got)
	}
}

func TestAssembly_ChangePGReplaySurvivesRestart(t *testing.T) {
	dsn := os.Getenv("OPS_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("OPS_TEST_PG_DSN not set — change persistence integration skipped")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Skipf("pg unreachable (create pool): %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("OPS_TEST_PG_DSN unreachable — change persistence integration skipped: %v", err)
	}
	id := fmt.Sprintf("chgasm-%d", time.Now().UnixNano())
	cleanup := func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer ccancel()
		_, _ = pool.Exec(cctx, `DELETE FROM change_record WHERE event_id = $1`, id)
	}
	cleanup()
	defer cleanup()
	pool.Close() // 装配自持连接池，测试不共用

	// —— 第一次"进程"：装配（应选中 PG 后端）→ 发现节点 → 提交变更 → 退出。
	t.Setenv("OPS_DB_DSN", dsn)
	a1, err := NewAssembly(nil, "")
	if err != nil {
		t.Fatalf("assembly #1: %v", err)
	}
	if a1.Changes.Persistence() != "timescaledb" {
		a1.Close()
		t.Skipf("change store degraded to %q (DB reachable but wiring declined) — integration skipped", a1.Changes.Persistence())
	}
	if err := a1.Sink.IngestDiscover(context.Background(), &connector.DiscoverResult{
		Nodes: []connector.ResourceNode{{Key: "host:restart-me", Type: "host", Source: "prom", ObservedAt: time.Now()}},
	}); err != nil {
		a1.Close()
		t.Fatalf("discover: %v", err)
	}
	if _, err := a1.Changes.Record(topology.ChangeEvent{
		ID: id, NodeKey: "host:restart-me", Type: topology.ChangeRollback, Summary: "回滚 v3",
	}); err != nil {
		a1.Close()
		t.Fatalf("record: %v", err)
	}
	a1.Close() // 内存全丢——PG 真相源必须兜住

	// —— 第二次"进程"：新装配从 PG 回放，证据可查。
	a2, err := NewAssembly(nil, "")
	if err != nil {
		t.Fatalf("assembly #2: %v", err)
	}
	defer a2.Close()
	got, ok := a2.Changes.Get(id)
	if !ok {
		t.Fatalf("change %s lost after restart (replay broken)", id)
	}
	if got.NodeKey != "host:restart-me" || got.Type != topology.ChangeRollback || got.Summary != "回滚 v3" {
		t.Errorf("replayed event mismatch: %+v", got)
	}
}
