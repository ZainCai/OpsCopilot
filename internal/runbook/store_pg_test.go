// store_pg_test.go W11-4 runbook store 契约（PG 真跑，OPS_TEST_PG_DSN 门控，
// 仓库惯例同 internal/incident/pg_store_test.go；缺库先 bash scripts/reset_test_pg.sh）。
// 每用例独立租户键空间，t.Cleanup 按租户物理清行（测试库共享、互不串扰）。
//
// 锁定口径：
//   - 挂载幂等（重复挂载恰一次，首挂留痕不被后写刷新）；
//   - 解挂只断挂载关系：执行记录不随之消失（append-only 历史）；
//   - 执行记录 append 不可改：store 对该表零 UPDATE/DELETE；seq 由 IDENTITY
//     发号（GENERATED ALWAYS 拒用户供给值——直插带 seq 被 PG 拒绝即证）；
//   - 悬挂防线：挂载不存在的手册 → FK 归一 ErrRunbookNotFound；未挂载点记
//     执行 → ErrNotMounted。
package runbook

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// setupStore 门控建 store：无 DSN skip；连通失败 skip（提示跑 reset 脚本）。
// 返回 store 与清理函数（按租户删三表行）。
func setupStore(t *testing.T) (*Store, func()) {
	t.Helper()
	dsn := os.Getenv("OPS_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("OPS_TEST_PG_DSN not set — runbook store pg integration skipped")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Skipf("pg unreachable (create pool): %v（先跑 bash scripts/reset_test_pg.sh）", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("OPS_TEST_PG_DSN unreachable — skipped: %v（先跑 bash scripts/reset_test_pg.sh）", err)
	}
	tenant := fmt.Sprintf("rb-test-%d", time.Now().UnixNano())
	s := NewStore(pool, tenant)
	cleanup := func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer ccancel()
		for _, tbl := range []string{"runbook_execution_log", "incident_runbook", "runbook"} {
			if _, err := pool.Exec(cctx, "DELETE FROM "+tbl+" WHERE tenant_id = $1", tenant); err != nil {
				t.Errorf("cleanup %s: %v", tbl, err)
			}
		}
		pool.Close()
	}
	return s, cleanup
}

func mustCreate(t *testing.T, s *Store, id string) {
	t.Helper()
	rb := &Runbook{ID: id, Title: "手册 " + id, Content: "# 步骤\n1. 看一眼",
		ScopeSeverity: "warning", ScopeService: "mysql", CreatedBy: "alice"}
	if err := s.Create(context.Background(), rb); err != nil {
		t.Fatalf("create %s: %v", id, err)
	}
	if rb.CreatedAt.IsZero() || rb.UpdatedAt.IsZero() {
		t.Fatalf("create %s: timestamps not backfilled", id)
	}
}

func TestStoreRunbookCRUD(t *testing.T) {
	s, done := setupStore(t)
	defer done()
	ctx := context.Background()

	// 表驱动：建两本 + 读回字段 + 重复 id + 不存在 id。
	mustCreate(t, s, "rb-a")
	mustCreate(t, s, "rb-b")

	cases := []struct {
		name  string
		check func(t *testing.T)
	}{
		{"get_roundtrip", func(t *testing.T) {
			got, err := s.Get(ctx, "rb-a")
			if err != nil {
				t.Fatalf("get: %v", err)
			}
			if got.Title != "手册 rb-a" || got.Content == "" || got.ScopeSeverity != "warning" ||
				got.ScopeService != "mysql" || got.CreatedBy != "alice" {
				t.Fatalf("field roundtrip broken: %+v", got)
			}
		}},
		{"duplicate_id", func(t *testing.T) {
			rb := &Runbook{ID: "rb-a", Title: "dup", CreatedBy: "bob"}
			if err := s.Create(ctx, rb); !errors.Is(err, ErrDuplicateID) {
				t.Fatalf("dup create err = %v, want ErrDuplicateID", err)
			}
		}},
		{"get_missing", func(t *testing.T) {
			if _, err := s.Get(ctx, "rb-nope"); !errors.Is(err, ErrRunbookNotFound) {
				t.Fatalf("missing get err = %v, want ErrRunbookNotFound", err)
			}
		}},
		{"list_newest_first", func(t *testing.T) {
			list, err := s.List(ctx)
			if err != nil {
				t.Fatalf("list: %v", err)
			}
			if len(list) != 2 {
				t.Fatalf("list len = %d, want 2 (%+v)", len(list), list)
			}
			if list[0].ID != "rb-b" { // 后建在前（created_at DESC；同刻按 id 稳定）
				t.Fatalf("list order = %s,%s, want rb-b first", list[0].ID, list[1].ID)
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, tc.check)
	}
}

func TestStoreMountLifecycle(t *testing.T) {
	s, done := setupStore(t)
	defer done()
	ctx := context.Background()
	mustCreate(t, s, "rb-m")

	// 表驱动：幂等挂载 / 未知手册 / 解挂 / 重复解挂 / 解挂后可重挂。
	cases := []struct {
		name  string
		check func(t *testing.T)
	}{
		{"mount_idempotent", func(t *testing.T) {
			if err := s.Mount(ctx, "INC-1", "rb-m", "alice"); err != nil {
				t.Fatalf("mount1: %v", err)
			}
			first, _ := s.ListMounts(ctx, "INC-1")
			if err := s.Mount(ctx, "INC-1", "rb-m", "bob"); err != nil {
				t.Fatalf("mount2: %v", err)
			}
			mounts, err := s.ListMounts(ctx, "INC-1")
			if err != nil {
				t.Fatalf("list mounts: %v", err)
			}
			if len(mounts) != 1 {
				t.Fatalf("re-mount must not duplicate: %d rows", len(mounts))
			}
			if mounts[0].MountedBy != first[0].MountedBy || !mounts[0].MountedAt.Equal(first[0].MountedAt) {
				t.Fatalf("re-mount refreshed first-mount trace: %v -> %v", first[0], mounts[0])
			}
			if mounts[0].Title != "手册 rb-m" || mounts[0].RunbookID != "rb-m" {
				t.Fatalf("join view not flattened from runbook: %+v", mounts[0])
			}
		}},
		{"mount_unknown_runbook", func(t *testing.T) {
			if err := s.Mount(ctx, "INC-1", "rb-ghost", "alice"); !errors.Is(err, ErrRunbookNotFound) {
				t.Fatalf("unknown runbook mount err = %v, want ErrRunbookNotFound", err)
			}
		}},
		{"unmount_keeps_history", func(t *testing.T) {
			if _, err := s.AppendExecution(ctx, "INC-1", "rb-m", "alice", "清盘完成", nil); err != nil {
				t.Fatalf("append: %v", err)
			}
			if err := s.Unmount(ctx, "INC-1", "rb-m"); err != nil {
				t.Fatalf("unmount: %v", err)
			}
			execs, err := s.ListExecutions(ctx, "INC-1", "rb-m")
			if err != nil {
				t.Fatalf("executions after unmount: %v", err)
			}
			if len(execs) != 1 {
				t.Fatalf("execution history must survive unmount, got %d", len(execs))
			}
			if ok, err := s.Mounted(ctx, "INC-1", "rb-m"); err != nil || ok {
				t.Fatalf("mount row must be gone after unmount (ok=%v err=%v)", ok, err)
			}
		}},
		{"unmount_missing", func(t *testing.T) {
			if err := s.Unmount(ctx, "INC-1", "rb-m"); !errors.Is(err, ErrNotMounted) {
				t.Fatalf("double unmount err = %v, want ErrNotMounted", err)
			}
		}},
		{"remount_sees_prior_history", func(t *testing.T) {
			if err := s.Mount(ctx, "INC-1", "rb-m", "carol"); err != nil {
				t.Fatalf("remount: %v", err)
			}
			mounts, _ := s.ListMounts(ctx, "INC-1")
			if len(mounts) != 1 || mounts[0].ExecutionCount != 1 {
				t.Fatalf("remount must surface prior executions in count: %+v", mounts)
			}
			// 重挂后继续 append：seq 全局发号不与历史撞车。
			if _, err := s.AppendExecution(ctx, "INC-1", "rb-m", "carol", "复检 OK", json.RawMessage(`["https://x/2"]`)); err != nil {
				t.Fatalf("append after remount: %v", err)
			}
		}},
		{"append_requires_mount", func(t *testing.T) {
			if err := s.Unmount(ctx, "INC-1", "rb-m"); err != nil {
				t.Fatalf("unmount: %v", err)
			}
			if _, err := s.AppendExecution(ctx, "INC-1", "rb-m", "dave", "x", nil); !errors.Is(err, ErrNotMounted) {
				t.Fatalf("append on unmounted err = %v, want ErrNotMounted", err)
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, tc.check)
	}
}

func TestStoreExecutionAppendOnly(t *testing.T) {
	s, done := setupStore(t)
	defer done()
	ctx := context.Background()
	mustCreate(t, s, "rb-e")
	if err := s.Mount(ctx, "INC-9", "rb-e", "alice"); err != nil {
		t.Fatalf("mount: %v", err)
	}

	var last int64
	for i := 0; i < 3; i++ {
		e, err := s.AppendExecution(ctx, "INC-9", "rb-e", "alice", fmt.Sprintf("第 %d 次处置", i),
			json.RawMessage(fmt.Sprintf(`[{"url":"https://a/%d","label":"截图"}]`, i)))
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		if e.Seq <= last {
			t.Fatalf("seq must strictly increase: %d after %d", e.Seq, last)
		}
		last = e.Seq
		if e.ExecutedAt.IsZero() {
			t.Fatalf("executed_at not returned")
		}
	}
	execs, err := s.ListExecutions(ctx, "INC-9", "rb-e")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(execs) != 3 {
		t.Fatalf("len = %d, want 3", len(execs))
	}
	for i, e := range execs { // 旧→新、refs 原样往返（JSONB 保序内层）
		if e.Result != fmt.Sprintf("第 %d 次处置", i) {
			t.Fatalf("executions not in append order: %+v", execs)
		}
		if want := fmt.Sprintf("https://a/%d", i); !json.Valid(e.Refs) || len(e.Refs) == 0 {
			t.Fatalf("refs not valid json roundtrip: %s (want url %s)", e.Refs, want)
		}
	}
	// append-only 的 schema 级证据：seq 是 GENERATED ALWAYS IDENTITY，
	// 用户供给值的直插被 PG 拒（store 也不暴露任何 UPDATE/DELETE 方法——
	// "不可改"由 API 面 + schema 双重锁定）。
	_, err = s.pool.Exec(ctx, `INSERT INTO runbook_execution_log
		(tenant_id, incident_id, runbook_id, seq, executed_by, result)
		VALUES ($1,'INC-9','rb-e',1,'hacker','篡改')`, s.tenant)
	if err == nil {
		t.Fatal("explicit seq insert must be rejected by GENERATED ALWAYS IDENTITY")
	}
	// 空 refs 归一为 []（GET 侧无需判 null）。
	e, err := s.AppendExecution(ctx, "INC-9", "rb-e", "bob", "无佐证一笔", nil)
	if err != nil {
		t.Fatalf("append empty refs: %v", err)
	}
	if string(e.Refs) != "[]" {
		t.Fatalf("nil refs must normalize to [], got %s", e.Refs)
	}
}
