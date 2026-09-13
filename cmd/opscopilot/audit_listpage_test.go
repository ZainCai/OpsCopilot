// W12 审计解锁包：AuditLog.ListPage 双 store 契约测试（表驱动，落在 cmd——
// AuditLog 就在 package main，与 audit_test.go 同家）。
//
// 断言集合（Mem/PG 逐字一致的语义）：
//   - 排序 (occurred_at DESC, seq/id DESC) 决定性全序（含同刻并列的 tiebreak）；
//   - 游标翻页不重不漏（含"翻页期间来了新行"的 keyset 稳定性）；
//   - actor/action/组合过滤 + [since, until) 半开时间窗（边界行含下排除上）；
//   - 未知 action → ErrBadAuditAction、坏游标 → ErrBadAuditCursor（两实现
//     同口径——REST 的 400 不随部署形态漂移）；
//   - limit 归一化（auditPageLimit 纯函数单测）。
package main

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

// --- 种子数据：两个 store 共用的逻辑行集 ---
//
// base 之后的相对偏移固定；t2a/t2b **同刻**（验证 seq/id DESC tiebreak）。
// 追加序 = 期望 (occurred_at, seq) 升序 = 期望输出 (DESC) 的倒序。

type auditSeedRow struct {
	incident string
	action   AuditAction
	actor    string
	offset   time.Duration // 相对 base 的 occurred_at 偏移
	detail   map[string]any
}

var auditSeedRows = []auditSeedRow{
	{incident: "INC-1", action: AuditCreate, actor: "zhang", offset: 0,
		detail: map[string]any{"title": "磁盘满", "origin": "manual"}},
	{incident: "INC-1", action: AuditTransition, actor: "li", offset: time.Hour,
		detail: map[string]any{"to": "acked"}},
	{incident: "INC-2", action: AuditMerge, actor: "zhang", offset: 2 * time.Hour,
		detail: map[string]any{"merged_into": "INC-1"}},
	// 同刻两行（2h + 一点，彼此相等）：id/seq DESC 定序 → 后追加者（INC-4）在前
	{incident: "INC-3", action: AuditRCA, actor: "auto", offset: 3 * time.Hour,
		detail: map[string]any{"root_causes": float64(1), "findings": float64(4), "duration_ms": float64(1200)}},
	{incident: "INC-4", action: AuditAttachCluster, actor: "system:autoattach", offset: 3 * time.Hour,
		detail: map[string]any{"cluster_key": "ck-9"}},
	{incident: "INC-5", action: AuditRateLimited, actor: "system:prometheus", offset: 4 * time.Hour,
		detail: map[string]any{"source_ref": "sr-1"}},
	{incident: "INC-6", action: AuditIngestFailed, actor: "system:webhook", offset: 5 * time.Hour,
		detail: map[string]any{"error": "boom", "attempts": float64(3)}},
	{incident: "INC-7", action: AuditExternalRecoveryIgnored, actor: "zhang", offset: 6 * time.Hour,
		detail: map[string]any{"source_ref": "sr-7"}},
}

// wantIDs 期望全序（最新优先）：offset 倒序，同刻按追加序倒排
// （INC-4 attach 在 INC-3 rca 之后追加 → INC-4 在前）。
var wantIDs = []string{"INC-7", "INC-6", "INC-5", "INC-4", "INC-3", "INC-2", "INC-1", "INC-1"}

// memLogAt 构造带显式时间戳/发号序的内存审计（同包测试特权：直接装配
// entries，绕开 Append 的 time.Now()，让两 store 拿到逐字节同形的数据）。
func memLogAt(base time.Time) *MemAuditLog {
	l := &MemAuditLog{}
	for i, row := range auditSeedRows {
		l.entries = append(l.entries, AuditEntry{
			IncidentID: row.incident, Action: row.action, Actor: row.actor,
			Detail: row.detail, OccurredAt: base.Add(row.offset), seq: int64(i + 1),
		})
	}
	l.seq = int64(len(auditSeedRows))
	return l
}

// pgLogAt 真库种同形数据（occurred_at 显式给定；id 由 BIGSERIAL 按插入序
// 发号 = 与内存 seq 同角色）。返回 pool 供 keyset 稳定性用例追加新行。
func pgLogAt(t *testing.T, dsn, tenant string, base time.Time) (*PGAuditLog, *pgxpool.Pool) {
	t.Helper()
	pool, err := pgxpool.New(t.Context(), dsn)
	if err != nil {
		t.Fatalf("pgxpool: %v", err)
	}
	t.Cleanup(pool.Close)
	for _, row := range auditSeedRows {
		d := "{}"
		if len(row.detail) > 0 {
			b, err := json.Marshal(row.detail)
			if err != nil {
				t.Fatalf("marshal detail: %v", err)
			}
			d = string(b)
		}
		if _, err := pool.Exec(t.Context(), `
INSERT INTO incident_audit (tenant_id, incident_id, action, actor, detail, occurred_at)
VALUES ($1,$2,$3,$4,$5::jsonb,$6)`,
			tenant, row.incident, row.action, row.actor, d, base.Add(row.offset)); err != nil {
			t.Fatalf("seed audit: %v", err)
		}
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = pool.Exec(ctx, `DELETE FROM incident_audit WHERE tenant_id=$1`, tenant)
	})
	return NewPGAuditLog(pool, tenant, func(string, ...any) {}), pool
}

// runAuditListPageContract 对任意 AuditLog 实现执行同一套断言。
// seqOf 提供"翻页期间追加一行更新的审计"的能力（keyset 稳定性用例）。
func runAuditListPageContract(t *testing.T, l AuditLog, base time.Time, appendNewer func()) {
	t.Helper()

	// 1) 全量单页：顺序 = wantIDs。
	page, err := l.ListPage(AuditQuery{Limit: 100})
	if err != nil {
		t.Fatalf("listpage all: %v", err)
	}
	if got := idsOf(page.Items); !eqStr(got, wantIDs) {
		t.Fatalf("full order = %v, want %v", got, wantIDs)
	}
	if page.NextCursor != "" {
		t.Fatalf("next_cursor should be empty when exhausted, got %q", page.NextCursor)
	}

	// 2) limit=2 逐页翻：不重不漏 = 全量序列。
	walk := []string{}
	cur := ""
	for i := 0; i < 20; i++ { // 防死循环上界
		p, err := l.ListPage(AuditQuery{Limit: 2, Cursor: cur})
		if err != nil {
			t.Fatalf("walk page %d: %v", i, err)
		}
		walk = append(walk, idsOf(p.Items)...)
		if p.NextCursor == "" {
			break
		}
		cur = p.NextCursor
	}
	if !eqStr(walk, wantIDs) {
		t.Fatalf("paged walk = %v, want %v", walk, wantIDs)
	}

	// 3) keyset 稳定性：第一页取回后追加一条**更新**的行，再按游标续翻——
	//    续页不得混入新行（游标锚定旧位置），也不得吞掉任何旧行。
	first, err := l.ListPage(AuditQuery{Limit: 2})
	if err != nil {
		t.Fatalf("stability first page: %v", err)
	}
	appendNewer()
	rest := []string{}
	cur = first.NextCursor
	for i := 0; i < 20; i++ {
		p, err := l.ListPage(AuditQuery{Limit: 2, Cursor: cur})
		if err != nil {
			t.Fatalf("stability walk %d: %v", i, err)
		}
		rest = append(rest, idsOf(p.Items)...)
		if p.NextCursor == "" {
			break
		}
		cur = p.NextCursor
	}
	if !eqStr(append(append([]string{}, idsOf(first.Items)...), rest...), wantIDs) {
		t.Fatalf("paging shifted while a newer row arrived mid-walk: first=%v rest=%v want=%v",
			idsOf(first.Items), rest, wantIDs)
	}

	// 4) actor / action / 组合过滤（顺序保持）。
	page, err = l.ListPage(AuditQuery{Actor: "zhang", Limit: 100})
	if err != nil {
		t.Fatalf("actor filter: %v", err)
	}
	if got := idsOf(page.Items); !eqStr(got, []string{"INC-7", "INC-2", "INC-1"}) {
		t.Fatalf("actor=zhang → %v", got)
	}
	page, err = l.ListPage(AuditQuery{Action: string(AuditRCA), Limit: 100})
	if err != nil {
		t.Fatalf("action filter: %v", err)
	}
	if got := idsOf(page.Items); !eqStr(got, []string{"INC-3"}) {
		t.Fatalf("action=rca → %v", got)
	}
	page, err = l.ListPage(AuditQuery{Actor: "zhang", Action: string(AuditCreate), Limit: 100})
	if err != nil {
		t.Fatalf("combo filter: %v", err)
	}
	if got := idsOf(page.Items); !eqStr(got, []string{"INC-1"}) {
		t.Fatalf("actor=zhang&action=create → %v", got)
	}

	// 5) 半开时间窗 [2h, 5h)：含 offset=2h 下边界行与同刻 3h 两行，
	//    不含 offset=5h 的上边界行（INC-6 出窗）。
	page, err = l.ListPage(AuditQuery{Since: base.Add(2 * time.Hour), Until: base.Add(5 * time.Hour), Limit: 100})
	if err != nil {
		t.Fatalf("window filter: %v", err)
	}
	if got := idsOf(page.Items); !eqStr(got, []string{"INC-5", "INC-4", "INC-3", "INC-2"}) {
		t.Fatalf("window [2h,5h) → %v", got)
	}

	// 6) 参数错口径：未知 action / 坏游标（两实现都必须以同样错误失败）。
	if _, err := l.ListPage(AuditQuery{Action: "delete_everything"}); !errors.Is(err, ErrBadAuditAction) {
		t.Fatalf("unknown action err = %v, want ErrBadAuditAction", err)
	}
	if _, err := l.ListPage(AuditQuery{Cursor: "!!!not-a-cursor"}); !errors.Is(err, ErrBadAuditCursor) {
		t.Fatalf("bad cursor err = %v, want ErrBadAuditCursor", err)
	}
}

func TestAuditListPageContractMem(t *testing.T) {
	base := time.Now().Add(-24 * time.Hour)
	l := memLogAt(base)
	runAuditListPageContract(t, l, base, func() {
		l.Append(AuditEntry{IncidentID: "INC-NEW", Action: AuditCreate, Actor: "ops"})
	})
}

func TestAuditListPageContractPG(t *testing.T) {
	dsn := os.Getenv("OPS_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("OPS_TEST_PG_DSN not set")
	}
	tenant := fmt.Sprintf("audit-listpage-%d", time.Now().UnixNano())
	base := time.Now().Add(-24 * time.Hour)
	l, pool := pgLogAt(t, dsn, tenant, base)
	runAuditListPageContract(t, l, base, func() {
		if _, err := pool.Exec(t.Context(), `
INSERT INTO incident_audit (tenant_id, incident_id, action, actor) VALUES ($1,'INC-NEW','create','ops')`, tenant); err != nil {
			t.Fatalf("append newer: %v", err)
		}
	})
}

// TestPGAuditListPageNoPool 后端缺席必须返回 ErrAuditUnavailable 根因错误
// （D1 同款：故障 ≠ 空列表）。
func TestPGAuditListPageNoPool(t *testing.T) {
	l := &PGAuditLog{}
	if _, err := l.ListPage(AuditQuery{}); !errors.Is(err, ErrAuditUnavailable) {
		t.Fatalf("nil pool err = %v, want ErrAuditUnavailable root", err)
	}
}

// TestAuditPageLimitNormalization limit 默认/上限归一化（对齐 incident
// 分页同源常量）。
func TestAuditPageLimitNormalization(t *testing.T) {
	for _, tc := range []struct{ in, want int }{
		{-5, 200}, {0, 200}, {1, 1}, {200, 200}, {999999, 1000},
	} {
		if got := auditPageLimit(tc.in); got != tc.want {
			t.Fatalf("auditPageLimit(%d) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

// TestAuditCursorRoundTrip 游标编解码决定性往返 + 非法输入归类。
func TestAuditCursorRoundTrip(t *testing.T) {
	at := time.Now().UTC().Truncate(time.Microsecond)
	gotT, gotSeq, err := decodeAuditCursor(encodeAuditCursor(at, 42))
	if err != nil || !gotT.Equal(at) || gotSeq != 42 {
		t.Fatalf("round trip: %v %v %v", gotT, gotSeq, err)
	}
	for _, bad := range []string{"!!!", "e333", encodeAuditCursor(at, 42)[:3] + "\x00junk"} {
		if _, _, err := decodeAuditCursor(bad); !errors.Is(err, ErrBadAuditCursor) {
			t.Fatalf("decode %q err = %v, want ErrBadAuditCursor", bad, err)
		}
	}
}

func idsOf(entries []AuditEntry) []string {
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.IncidentID)
	}
	return out
}

func eqStr(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
