// PGChangeStore 打库集成测试（优化方案 #4）：OPS_TEST_PG_DSN 门控，
// 无 DB 自动跳过（对齐 internal/incident/pg_store_test.go 的 skip 模式）。
// 前置：migrations/000001+000002（change_record 表与列映射）、000015
// （回放/清理索引）已应用（compose 栈执行）。清理按 event_id 前缀，不残留。
package topology

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func pgChangePoolForTest(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("OPS_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("OPS_TEST_PG_DSN not set — PG change store integration skipped")
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("pg change pool: %v", err)
	}
	pctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := pool.Ping(pctx); err != nil {
		pool.Close()
		t.Skipf("OPS_TEST_PG_DSN unreachable — PG change store integration skipped: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// cleanupChangeEvents 按前缀清掉本测试写入的行（含内存重建前的残留）。
func cleanupChangeEvents(t *testing.T, pool *pgxpool.Pool, prefix string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := pool.Exec(ctx, `DELETE FROM change_record WHERE event_id LIKE $1`, prefix+"%"); err != nil {
		t.Fatalf("cleanup change_record: %v", err)
	}
}

func fullChangeEvent(id string, at time.Time) ChangeEvent {
	return ChangeEvent{
		ID: id, NodeKey: "host:pg-" + id, Type: ChangeDeploy,
		Source: "jenkins", Author: "alice", Ref: "release/1.2",
		Revision: "deadbeef", Summary: "上线 v1.2",
		OccurredAt: at, Confidence: ConfidenceHigh,
	}
}

// TestPGChangeStoreRecordReplayRoundTrip 写→（模拟重启：全新内存缓存）→
// 回放→逐字段一致。这是"重启不丢变更取证"的直接验收。
func TestPGChangeStoreRecordReplayRoundTrip(t *testing.T) {
	pool := pgChangePoolForTest(t)
	prefix := fmt.Sprintf("chgpg-rt-%d-", time.Now().Unix())
	cleanupChangeEvents(t, pool, prefix)
	t.Cleanup(func() { cleanupChangeEvents(t, pool, prefix) })

	s := NewPGChangeStore(NewChangeStore(nil), pool, "default", t.Logf)
	if s.Persistence() != "timescaledb" {
		t.Fatalf("Persistence = %q, want timescaledb", s.Persistence())
	}
	at := time.Now().Round(0).Truncate(time.Millisecond).Add(-42 * time.Minute) // Round(0) 去单调钟；截到 ms 防 µs 截断失真
	want := fullChangeEvent(prefix+"one", at)
	got, err := s.Record(want)
	if err != nil {
		t.Fatalf("record: %v", err)
	}
	if got.Confidence != ConfidenceHigh { // 显式 high 不被默认值吞掉
		t.Errorf("record copy confidence = %q, want high", got.Confidence)
	}

	// 模拟重启：新内存缓存 + 回放。
	s2 := NewPGChangeStore(NewChangeStore(nil), pool, "default", t.Logf)
	n, err := s2.LoadSince(at.Add(-time.Hour))
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if n == 0 {
		t.Fatal("replay loaded 0 events, want >=1")
	}
	back, ok := s2.Get(want.ID)
	if !ok {
		t.Fatalf("event %s lost after restart+replay", want.ID)
	}
	if back.ID != want.ID || back.NodeKey != want.NodeKey || back.Type != want.Type ||
		back.Source != want.Source || back.Author != want.Author ||
		back.Ref != want.Ref || back.Revision != want.Revision ||
		back.Summary != want.Summary || back.Confidence != want.Confidence ||
		!back.OccurredAt.Equal(want.OccurredAt) {
		t.Errorf("replayed event mismatch:\n got %+v\nwant %+v", back, want)
	}
}

// TestPGChangeStoreReplayWindow LoadSince 的下界语义：窗口外不回放、
// 零值全量；跨实现幂等（重复 LoadSince 不产生副本）。
func TestPGChangeStoreReplayWindow(t *testing.T) {
	pool := pgChangePoolForTest(t)
	prefix := fmt.Sprintf("chgpg-win-%d-", time.Now().Unix())
	cleanupChangeEvents(t, pool, prefix)
	t.Cleanup(func() { cleanupChangeEvents(t, pool, prefix) })

	old := fullChangeEvent(prefix+"old", time.Now().Truncate(time.Millisecond).Add(-72*time.Hour))
	fresh := fullChangeEvent(prefix+"fresh", time.Now().Truncate(time.Millisecond))
	s := NewPGChangeStore(NewChangeStore(nil), pool, "default", t.Logf)
	for _, ev := range []ChangeEvent{old, fresh} {
		if _, err := s.Record(ev); err != nil {
			t.Fatalf("record %s: %v", ev.ID, err)
		}
	}

	s2 := NewPGChangeStore(NewChangeStore(nil), pool, "default", t.Logf)
	n, err := s2.LoadSince(time.Now().Add(-24 * time.Hour))
	if err != nil {
		t.Fatalf("windowed replay: %v", err)
	}
	if _, ok := s2.Get(old.ID); ok {
		t.Error("event outside retention window was replayed")
	}
	if _, ok := s2.Get(fresh.ID); !ok {
		t.Error("event inside window missing after replay")
	}
	before := n
	if _, err := s2.LoadSince(time.Time{}); err != nil { // 幂等重放不翻倍
		t.Fatalf("full replay: %v", err)
	}
	if s2.Len() < before {
		t.Fatalf("Len dropped on idempotent replay: %d -> %d", before, s2.Len())
	}
	if _, ok := s2.Get(old.ID); !ok {
		t.Error("full replay (zero since) must include old events")
	}
}

// TestPGChangeStoreReplayTenantIsolation 回放恒按装配租户过滤（跨租户/跨 run
// 证据泄漏收口，评测 0129078 实锤）：双租户各播一条同节点变更，A 租户视角
// 重启只回放 A 的行、B 的行不得进读缓存；B 视角对称。全量回放（零值 since）
// 同样限本租户。
func TestPGChangeStoreReplayTenantIsolation(t *testing.T) {
	pool := pgChangePoolForTest(t)
	prefix := fmt.Sprintf("chgpg-iso-%d-", time.Now().Unix())
	tenantA, tenantB := prefix+"tenant-a", prefix+"tenant-b"
	cleanupChangeEvents(t, pool, prefix)
	t.Cleanup(func() {
		cleanupChangeEvents(t, pool, prefix)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for _, id := range []string{tenantA, tenantB} {
			if _, err := pool.Exec(ctx, `DELETE FROM tenant WHERE id = $1`, id); err != nil {
				t.Fatalf("cleanup tenant %s: %v", id, err)
			}
		}
	})

	at := time.Now().Truncate(time.Millisecond)
	evA := ChangeEvent{ID: prefix + "a", NodeKey: "host:iso", Type: ChangeDeploy, OccurredAt: at}
	evB := ChangeEvent{ID: prefix + "b", NodeKey: "host:iso", Type: ChangeConfig, OccurredAt: at}
	for _, rec := range []struct {
		tenant string
		ev     ChangeEvent
	}{{tenantA, evA}, {tenantB, evB}} {
		s := NewPGChangeStore(NewChangeStore(nil), pool, rec.tenant, t.Logf)
		if _, err := s.Record(rec.ev); err != nil {
			t.Fatalf("record %s under tenant %s: %v", rec.ev.ID, rec.tenant, err)
		}
	}

	// A 视角"重启"：全量回放也只该带回 A 自己的证据。
	replayA := NewPGChangeStore(NewChangeStore(nil), pool, tenantA, t.Logf)
	n, err := replayA.LoadSince(time.Time{})
	if err != nil {
		t.Fatalf("tenant-A replay: %v", err)
	}
	if n < 1 {
		t.Fatalf("tenant-A replay loaded %d events, want >=1", n)
	}
	if _, ok := replayA.Get(evA.ID); !ok {
		t.Error("own-tenant event missing after replay")
	}
	if _, ok := replayA.Get(evB.ID); ok {
		t.Error("foreign-tenant event leaked into tenant-A replay (cross-run evidence leak)")
	}

	replayB := NewPGChangeStore(NewChangeStore(nil), pool, tenantB, t.Logf)
	if _, err := replayB.LoadSince(time.Time{}); err != nil {
		t.Fatalf("tenant-B replay: %v", err)
	}
	if _, ok := replayB.Get(evA.ID); ok {
		t.Error("tenant-A event leaked into tenant-B replay")
	}
	if _, ok := replayB.Get(evB.ID); !ok {
		t.Error("tenant-B own event missing after replay")
	}
}

// TestPGChangeStoreDuplicateAndNodeCheck 判重/校验仍由内存层把守：
// 重复 ID → ErrDuplicateChange；nodeCheck 拒绝的节点不落库。
func TestPGChangeStoreDuplicateAndNodeCheck(t *testing.T) {
	pool := pgChangePoolForTest(t)
	prefix := fmt.Sprintf("chgpg-dup-%d-", time.Now().Unix())
	cleanupChangeEvents(t, pool, prefix)
	t.Cleanup(func() { cleanupChangeEvents(t, pool, prefix) })

	s := NewPGChangeStore(NewChangeStore(func(k string) bool { return k == "host:real" }), pool, "default", t.Logf)
	ev := ChangeEvent{ID: prefix + "d1", NodeKey: "host:real", Type: ChangeConfig}
	if _, err := s.Record(ev); err != nil {
		t.Fatalf("record: %v", err)
	}
	if _, err := s.Record(ev); !errors.Is(err, ErrDuplicateChange) {
		t.Fatalf("dup record: err = %v, want ErrDuplicateChange", err)
	}
	if _, err := s.Record(ChangeEvent{ID: prefix + "d2", NodeKey: "host:ghost", Type: ChangeConfig}); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("ghost node: err = %v, want ErrNodeNotFound", err)
	}

	var n int
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM change_record WHERE event_id = $1`, ev.ID).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 1 {
		t.Errorf("rows for %s = %d, want 1 (dup must not double-write)", ev.ID, n)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM change_record WHERE event_id = $1`, prefix+"d2").Scan(&n); err != nil {
		t.Fatalf("count ghost: %v", err)
	}
	if n != 0 {
		t.Errorf("rejected event leaked to db: %d rows, want 0", n)
	}
}

// TestPGChangeStorePruneClearsBoth PruneBefore 双清：内存与 PG 同窗收口。
func TestPGChangeStorePruneClearsBoth(t *testing.T) {
	pool := pgChangePoolForTest(t)
	prefix := fmt.Sprintf("chgpg-prune-%d-", time.Now().Unix())
	cleanupChangeEvents(t, pool, prefix)
	t.Cleanup(func() { cleanupChangeEvents(t, pool, prefix) })

	s := NewPGChangeStore(NewChangeStore(nil), pool, "default", t.Logf)
	old := ChangeEvent{ID: prefix + "old", NodeKey: "host:a", Type: ChangeDeploy,
		OccurredAt: time.Now().Truncate(time.Millisecond).Add(-48 * time.Hour)}
	keep := ChangeEvent{ID: prefix + "keep", NodeKey: "host:a", Type: ChangeRollback,
		OccurredAt: time.Now().Truncate(time.Millisecond)}
	for _, ev := range []ChangeEvent{old, keep} {
		if _, err := s.Record(ev); err != nil {
			t.Fatalf("record %s: %v", ev.ID, err)
		}
	}
	if n := s.PruneBefore(time.Now().Add(-24 * time.Hour)); n != 1 {
		t.Fatalf("pruned = %d, want 1", n)
	}
	if _, ok := s.Get(old.ID); ok {
		t.Error("pruned event still in memory")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM change_record WHERE event_id = $1`, old.ID).Scan(&n); err != nil {
		t.Fatalf("count pruned: %v", err)
	}
	if n != 0 {
		t.Errorf("pg row survived prune: count=%d, want 0", n)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM change_record WHERE event_id = $1`, keep.ID).Scan(&n); err != nil {
		t.Fatalf("count kept: %v", err)
	}
	if n != 1 {
		t.Errorf("kept event deleted from pg: count=%d, want 1", n)
	}
}

// TestPGChangeStoreWriteFailureDegrades DB 不可达时：Record **不报错**
// （采集链路不被阻断），事件留在内存，并打 WARNING 日志。这是"写失败降级"
// 承诺的直接测试——不需要真库，用必然连接失败的 DSN 构造故障。
func TestPGChangeStoreWriteFailureDegrades(t *testing.T) {
	// .invalid 是 RFC 2606 保留 TLD，永不解析；connect_timeout=1 快速失败。
	bad := "postgres://ops:ops@no-such-host.invalid:5432/ops?connect_timeout=1"
	pool, err := pgxpool.New(context.Background(), bad)
	if err != nil {
		t.Fatalf("bad pool: %v", err)
	}
	defer pool.Close()

	var mu sync.Mutex
	var warnings []string
	logf := func(format string, args ...any) {
		mu.Lock()
		defer mu.Unlock()
		warnings = append(warnings, fmt.Sprintf(format, args...))
	}
	s := NewPGChangeStore(NewChangeStore(nil), pool, "default", logf)
	ev := ChangeEvent{ID: "chgpg-down-1", NodeKey: "host:a", Type: ChangeDeploy}
	if _, err := s.Record(ev); err != nil {
		t.Fatalf("record must NOT fail when pg is down, got: %v", err)
	}
	if _, ok := s.Get("chgpg-down-1"); !ok {
		t.Fatal("event lost on pg write failure (memory degradation broken)")
	}
	mu.Lock()
	defer mu.Unlock()
	found := false
	for _, w := range warnings {
		if strings.Contains(w, "persist failed") && strings.Contains(w, "chgpg-down-1") {
			found = true
		}
	}
	if !found {
		t.Errorf("no WARNING log for write failure, got %v", warnings)
	}
}
