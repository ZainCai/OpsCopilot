// escalation_test.go W9-3：值班升级最小版验证。
//
// 覆盖口径：超时才升级 / 只升级一次 / acked 不升级 / 发送失败回滚重试 /
// PG 台账跨实例幂等（门控）。
package main

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"opscopilot/internal/incident"
	"opscopilot/internal/notify"
)

// fakeDispatcher 升级通知出口桩（记录消息；err 非空时发送失败且不记录）。
type fakeDispatcher struct {
	mu   sync.Mutex
	msgs []notify.Message
	err  error
}

func (d *fakeDispatcher) Dispatch(m notify.Message) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.err != nil {
		return d.err
	}
	d.msgs = append(d.msgs, m)
	return nil
}

func (d *fakeDispatcher) count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.msgs)
}

// memStoreWithIncident 造一个 CreatedAt=base 的 open 事件。
func memStoreWithIncident(t *testing.T, base time.Time, id string) *incident.MemStore {
	t.Helper()
	ms := incident.NewMemStore()
	ms.SetClock(func() time.Time { return base })
	if _, err := ms.Create(id, "HighDisk", "critical", "tester"); err != nil {
		t.Fatalf("create incident: %v", err)
	}
	return ms
}

func TestEscalationFiresOnceForTimedOutOpen(t *testing.T) {
	base := time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC)
	ms := memStoreWithIncident(t, base, "INC-1")
	disp := &fakeDispatcher{}
	p := NewEscalationPoller(ms, disp, newMemEscalationLedger(), "t1", 15*time.Minute, time.Minute, nil)
	p.now = func() time.Time { return base.Add(20 * time.Minute) } // 已超阈值

	// 首轮：升级一次
	p.pollOnce(context.Background())
	if disp.count() != 1 {
		t.Fatalf("after first scan: %d dispatches, want 1", disp.count())
	}
	// 次轮：已认领 → 不重复
	p.pollOnce(context.Background())
	if disp.count() != 1 {
		t.Fatalf("after second scan: %d dispatches, want still 1 (only once)", disp.count())
	}
	// 消息形态：标题带升级前缀、严重级沿用事件
	m := disp.msgs[0]
	if m.Severity != "critical" || m.Title == "" {
		t.Fatalf("escalation msg = %+v", m)
	}
}

func TestEscalationSkipsRecentOpen(t *testing.T) {
	base := time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC)
	ms := memStoreWithIncident(t, base, "INC-2")
	disp := &fakeDispatcher{}
	p := NewEscalationPoller(ms, disp, newMemEscalationLedger(), "t1", 15*time.Minute, time.Minute, nil)
	p.now = func() time.Time { return base.Add(5 * time.Minute) } // 未到阈值

	p.pollOnce(context.Background())
	if disp.count() != 0 {
		t.Fatalf("recent open escalated: %d, want 0", disp.count())
	}
}

func TestEscalationSkipsAcked(t *testing.T) {
	base := time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC)
	ms := memStoreWithIncident(t, base, "INC-3")
	if _, err := ms.Transition("INC-3", incident.StateAcked, "oncall"); err != nil {
		t.Fatalf("ack: %v", err)
	}
	disp := &fakeDispatcher{}
	p := NewEscalationPoller(ms, disp, newMemEscalationLedger(), "t1", 15*time.Minute, time.Minute, nil)
	p.now = func() time.Time { return base.Add(time.Hour) }

	p.pollOnce(context.Background())
	if disp.count() != 0 {
		t.Fatalf("acked incident escalated: %d, want 0", disp.count())
	}
}

func TestEscalationReleaseOnDispatchFailure(t *testing.T) {
	base := time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC)
	ms := memStoreWithIncident(t, base, "INC-4")
	ledger := newMemEscalationLedger()
	disp := &fakeDispatcher{err: errors.New("sink down")}
	p := NewEscalationPoller(ms, disp, ledger, "t1", 15*time.Minute, time.Minute, nil)
	p.now = func() time.Time { return base.Add(20 * time.Minute) }

	// 首轮发送失败 → 回滚认领
	p.pollOnce(context.Background())
	if disp.count() != 0 {
		t.Fatalf("failed dispatch recorded: %d", disp.count())
	}
	// sink 恢复 → 下一轮重试成功（证明认领已回滚）
	disp.err = nil
	p.pollOnce(context.Background())
	if disp.count() != 1 {
		t.Fatalf("retry after recovery: %d dispatches, want 1", disp.count())
	}
}

func TestMemEscalationLedgerClaimRelease(t *testing.T) {
	l := newMemEscalationLedger()
	ctx := context.Background()
	if ok, err := l.Claim(ctx, "x"); err != nil || !ok {
		t.Fatalf("first claim = %v/%v, want true/nil", ok, err)
	}
	if ok, _ := l.Claim(ctx, "x"); ok {
		t.Fatal("second claim must be false")
	}
	if err := l.Release(ctx, "x"); err != nil {
		t.Fatalf("release: %v", err)
	}
	if ok, _ := l.Claim(ctx, "x"); !ok {
		t.Fatal("claim after release must be true")
	}
}

// TestPGEscalationLedgerClaim PG 台账幂等（门控）。
func TestPGEscalationLedgerClaim(t *testing.T) {
	dsn := os.Getenv("OPS_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("OPS_TEST_PG_DSN not set")
	}
	sink := pgSinkForTest(t)
	cleanup := func() { pgExec(t, sink, `DELETE FROM incident_escalation WHERE tenant_id = $1`, "esc-test") }
	cleanup()
	t.Cleanup(cleanup)

	l := newPGEscalationLedger(sink.pool, "esc-test")
	ctx := context.Background()
	ok, err := l.Claim(ctx, "INC-PG-1")
	if err != nil || !ok {
		t.Fatalf("first claim = %v/%v, want true/nil", ok, err)
	}
	if ok, _ := l.Claim(ctx, "INC-PG-1"); ok {
		t.Fatal("second claim must be false (PK idempotent)")
	}
	if err := l.Release(ctx, "INC-PG-1"); err != nil {
		t.Fatalf("release: %v", err)
	}
	if ok, _ := l.Claim(ctx, "INC-PG-1"); !ok {
		t.Fatal("claim after release must be true")
	}
}
