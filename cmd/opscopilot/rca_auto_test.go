// rca_auto_test.go 二期池波二 #4：升级联动自动 RCA 的触发器单测（假出口，
// 无 DB）。锁定口径：
//   - critical 升级 → 恰一触发（actor=auto 进 Analyze）；非 critical 零触发；
//   - 重复升级/重复触发不重跑（内存双保险 seen + 审计回读 PG 双保险）；
//   - 审计已有 rca/auto 行（重启场景等价物）→ 跳过分析；已有手动 rca 行
//     （actor≠auto）→ 自动仍跑一次（双保险只认 auto）；
//   - 审计后端故障 → 保守跳过（无法确认就不重跑）；
//   - 队列满 → 丢弃计数 opscopilot_rca_autotrigger_dropped_total，
//     Trigger 立即返回（绝不阻塞调用方 escalation）；
//   - escalation 集成：升级成功才回调、发送失败不回调。
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"opscopilot/internal/incident"
)

// fakeAnalyzer rcaAnalyzer 计数替身（记录 id+actor；err 非空则分析失败）。
type fakeAnalyzer struct {
	mu    sync.Mutex
	calls []string // "id|actor"
	err   error
}

func (f *fakeAnalyzer) Analyze(_ context.Context, id, actor string) (*RCAResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, id+"|"+actor)
	if f.err != nil {
		return nil, f.err
	}
	return &RCAResult{IncidentID: id}, nil
}

func (f *fakeAnalyzer) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

// fakeAnalyzerCalls 快照（防竞态读）。
func (f *fakeAnalyzer) got() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

// errAuditAudit 审计读故障替身（List 必败，Append 记录）。
type errAuditAudit struct {
	fakeAudit
}

func (e *errAuditAudit) List(string) ([]AuditEntry, error) {
	return nil, errors.New("audit backend down")
}

func criticalInc(id string) incident.Incident {
	return incident.Incident{ID: id, Severity: "critical", State: incident.StateOpen}
}

func TestRCATriggerTable(t *testing.T) {
	tests := []struct {
		name        string
		queueCap    int
		failAudit   bool
		setup       func(t *testing.T, tr *RCATrigger, an *fakeAnalyzer, audit AuditLog)
		acts        func(tr *RCATrigger)
		wantDropped float64
		check       func(t *testing.T, tr *RCATrigger, an *fakeAnalyzer)
	}{
		{
			name:     "critical 升级 → 恰一触发（actor=auto）",
			queueCap: 4,
			acts: func(tr *RCATrigger) {
				tr.Trigger(criticalInc("INC-1"))
			},
			check: func(t *testing.T, tr *RCATrigger, an *fakeAnalyzer) {
				if got := len(tr.queue); got != 1 {
					t.Fatalf("queue depth = %d, want 1 (恰一入队)", got)
				}
				drain(t, tr) // worker 消费一条
				if w := an.got(); len(w) != 1 || w[0] != "INC-1|auto" {
					t.Fatalf("analyze calls = %v, want [INC-1|auto]", w)
				}
			},
		},
		{
			name:     "非 critical 事件 → 零触发（升级联动只对 critical）",
			queueCap: 4,
			acts: func(tr *RCATrigger) {
				tr.Trigger(incident.Incident{ID: "INC-w", Severity: "warning"})
			},
			check: func(t *testing.T, tr *RCATrigger, an *fakeAnalyzer) {
				if got := len(tr.queue); got != 0 {
					t.Fatalf("queue depth = %d, want 0", got)
				}
				if an.count() != 0 {
					t.Fatalf("analyze must not run for warning")
				}
			},
		},
		{
			name:     "重复升级不重触发（内存双保险：入队即置 seen）",
			queueCap: 4,
			acts: func(tr *RCATrigger) {
				tr.Trigger(criticalInc("INC-dup"))
				tr.Trigger(criticalInc("INC-dup"))
				tr.Trigger(criticalInc("INC-dup"))
			},
			check: func(t *testing.T, tr *RCATrigger, an *fakeAnalyzer) {
				if got := len(tr.queue); got != 1 {
					t.Fatalf("queue depth = %d, want 1 (seen 拦截重投)", got)
				}
			},
		},
		{
			name:     "审计已有 rca/auto 行 → 分析跳过（PG 双保险：重启不重跑）",
			queueCap: 4,
			setup: func(_ *testing.T, _ *RCATrigger, _ *fakeAnalyzer, audit AuditLog) {
				audit.Append(AuditEntry{IncidentID: "INC-restart", Action: AuditRCA, Actor: autoTriggerActor})
			},
			acts: func(tr *RCATrigger) {
				tr.Trigger(criticalInc("INC-restart"))
			},
			check: func(t *testing.T, tr *RCATrigger, an *fakeAnalyzer) {
				drain(t, tr)
				if an.count() != 0 {
					t.Fatalf("analyze must skip when audit already has auto rca: %v", an.got())
				}
			},
		},
		{
			name:     "审计只有手动 rca 行（actor=tester）→ 自动仍跑（双保险只认 auto）",
			queueCap: 4,
			setup: func(_ *testing.T, _ *RCATrigger, _ *fakeAnalyzer, audit AuditLog) {
				audit.Append(AuditEntry{IncidentID: "INC-manual", Action: AuditRCA, Actor: "tester"})
			},
			acts: func(tr *RCATrigger) {
				tr.Trigger(criticalInc("INC-manual"))
			},
			check: func(t *testing.T, tr *RCATrigger, an *fakeAnalyzer) {
				drain(t, tr)
				if w := an.got(); len(w) != 1 || w[0] != "INC-manual|auto" {
					t.Fatalf("analyze calls = %v, want [INC-manual|auto]", w)
				}
			},
		},
		{
			name:      "审计后端故障 → 保守跳过（无法确认就不重跑）",
			queueCap:  4,
			failAudit: true,
			acts: func(tr *RCATrigger) {
				tr.Trigger(criticalInc("INC-ae"))
			},
			check: func(t *testing.T, tr *RCATrigger, an *fakeAnalyzer) {
				drain(t, tr)
				if an.count() != 0 {
					t.Fatalf("analyze must not run when audit unreadable: %v", an.got())
				}
			},
		},
		{
			name:     "队列满 → 丢弃计数 + Trigger 立即返回（不阻塞 escalation）",
			queueCap: 1,
			acts: func(tr *RCATrigger) {
				tr.Trigger(criticalInc("INC-q1")) // 入队占满
				start := time.Now()
				tr.Trigger(criticalInc("INC-q2")) // 满 → 丢弃
				if d := time.Since(start); d > time.Second {
					t.Fatalf("Trigger blocked %v — must never block escalation", d)
				}
			},
			wantDropped: 1,
			check: func(t *testing.T, tr *RCATrigger, an *fakeAnalyzer) {
				if got := len(tr.queue); got != 1 {
					t.Fatalf("queue depth = %d, want 1 (仅首条)", got)
				}
			},
		},
		{
			name:     "分析失败（超时/故障）→ 只记日志，不重投不 panic",
			queueCap: 4,
			setup: func(_ *testing.T, _ *RCATrigger, an *fakeAnalyzer, _ AuditLog) {
				an.err = errors.New("analyze boom")
			},
			acts: func(tr *RCATrigger) {
				tr.Trigger(criticalInc("INC-err"))
			},
			check: func(t *testing.T, tr *RCATrigger, an *fakeAnalyzer) {
				drain(t, tr)
				if an.count() != 1 {
					t.Fatalf("analyze calls = %d, want 1 (尽力而为，失败不自动重试)", an.count())
				}
			},
		},
		{
			name:     "空 ID 忽略",
			queueCap: 4,
			acts: func(tr *RCATrigger) {
				tr.Trigger(incident.Incident{ID: "", Severity: "critical"})
			},
			check: func(t *testing.T, tr *RCATrigger, an *fakeAnalyzer) {
				if got := len(tr.queue); got != 0 {
					t.Fatalf("queue depth = %d, want 0", got)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			an := &fakeAnalyzer{}
			m := NewAppMetrics()
			var audit AuditLog = NewMemAuditLog()
			if tc.name == "审计后端故障 → 保守跳过（无法确认就不重跑）" {
				audit = &errAuditAudit{}
			}
			tr := NewRCATrigger(an, audit, m, tc.queueCap, nil)
			if tc.setup != nil {
				tc.setup(t, tr, an, audit)
			}
			tc.acts(tr)
			tc.check(t, tr, an)
			if tc.wantDropped > 0 {
				rec := httptest.NewRecorder()
				m.Handler()(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
				want := fmt.Sprintf("opscopilot_rca_autotrigger_dropped_total %g", tc.wantDropped)
				if !strings.Contains(rec.Body.String(), want) {
					t.Fatalf("metrics missing %q", want)
				}
			}
		})
	}
}

// drain 模拟 worker 消费一条（不起 goroutine 的确定性测试姿势）。
func drain(t *testing.T, tr *RCATrigger) {
	t.Helper()
	select {
	case id := <-tr.queue:
		tr.process(context.Background(), id)
	default:
	}
}

// TestRCATriggerRunWorker Run 主循环：入队 → 异步消费；ctx 取消即退。
func TestRCATriggerRunWorker(t *testing.T) {
	an := &fakeAnalyzer{}
	tr := NewRCATrigger(an, NewMemAuditLog(), NewAppMetrics(), 4, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { tr.Run(ctx); close(done) }()

	tr.Trigger(criticalInc("INC-w"))
	deadline := time.Now().Add(2 * time.Second)
	for an.count() != 1 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if an.count() != 1 {
		t.Fatalf("worker did not process trigger: %v", an.got())
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run must exit on ctx cancel")
	}
}

// TestEscalationTriggersAutoRCAOnce escalation → 自动触发的集成闭环：
// 首轮扫描升级 critical 事件恰一次并入队一次；次轮扫描（台账幂等）既不
// 重发通知也不重触发；发送失败不回调 OnEscalate。
func TestEscalationTriggersAutoRCAOnce(t *testing.T) {
	base := time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC)
	ms := incident.NewMemStore()
	ms.SetClock(func() time.Time { return base })
	if _, err := ms.Create("INC-e1", "DiskFull", "critical", "t"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := ms.Create("INC-e2", "DiskLow", "warning", "t"); err != nil {
		t.Fatalf("create: %v", err)
	}
	trig := NewRCATrigger(&fakeAnalyzer{}, NewMemAuditLog(), NewAppMetrics(), 8, nil)
	disp := &fakeDispatcher{}
	p := NewEscalationPoller(ms, disp, newMemEscalationLedger(), "t1", 15*time.Minute, time.Minute, nil)
	p.now = func() time.Time { return base.Add(time.Hour) }
	// 与 assembly 同款接线（critical 过滤已收进 Trigger）。
	p.OnEscalate = trig.Trigger

	p.pollOnce(context.Background())
	// critical + warning 各升级一次（通知层面），但只有 critical 入队 RCA。
	if disp.count() != 2 {
		t.Fatalf("dispatches = %d, want 2", disp.count())
	}
	if got := len(trig.queue); got != 1 {
		t.Fatalf("rca enqueues = %d, want 1 (critical only)", got)
	}
	p.pollOnce(context.Background()) // 台账幂等：不重发也不重触发
	if got := len(trig.queue); got != 1 {
		t.Fatalf("rca enqueues after 2nd scan = %d, want still 1", got)
	}
}

// TestEscalationNoHookOnDispatchFailure 发送失败（回滚认领等下轮重试）不
// 触发自动 RCA——升级没成功，联动就不该发生。
func TestEscalationNoHookOnDispatchFailure(t *testing.T) {
	base := time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC)
	ms := memStoreWithIncident(t, base, "INC-f")
	called := 0
	p := NewEscalationPoller(ms, &fakeDispatcher{err: errors.New("boom")},
		newMemEscalationLedger(), "t1", 15*time.Minute, time.Minute, nil)
	p.now = func() time.Time { return base.Add(time.Hour) }
	p.OnEscalate = func(incident.Incident) { called++ }

	p.pollOnce(context.Background())
	if called != 0 {
		t.Fatalf("OnEscalate fired %d times despite dispatch failure", called)
	}
}

// TestEscalationNilHookZeroBehavior OnEscalate 未挂（默认/RCA off）：
// 升级链路逐字节保持现状。
func TestEscalationNilHookZeroBehavior(t *testing.T) {
	base := time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC)
	ms := memStoreWithIncident(t, base, "INC-n")
	disp := &fakeDispatcher{}
	p := NewEscalationPoller(ms, disp, newMemEscalationLedger(), "t1", 15*time.Minute, time.Minute, nil)
	p.now = func() time.Time { return base.Add(time.Hour) }
	p.pollOnce(context.Background())
	if disp.count() != 1 {
		t.Fatalf("dispatches = %d, want 1 (baseline unchanged)", disp.count())
	}
}

// TestRCATAutoAssemblyWiring 装配矩阵（开关 off 零行为）：
//   - Auto=off → RCATrigger 不构造、escalation 钩子不挂（升级链路逐字节现状）；
//   - Auto=on 且 escalation on → 钩子已挂；
//   - Auto=on 但 escalation off → 不构造（只 WARNING，不 panic）。
func TestRCATAutoAssemblyWiring(t *testing.T) {
	tests := []struct {
		name                  string
		auto, esc             bool
		wantTrigger, wantHook bool
	}{
		{"默认 off：零行为", false, true, false, false},
		{"on + escalation on → 挂钩", true, true, true, true},
		{"on 但 escalation off → 不挂", true, false, false, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testAssemblyConfig("tk")
			cfg.RCA.Enabled = true
			cfg.RCA.Auto = tc.auto
			cfg.Notify.EscalationEnabled = tc.esc
			asm, err := NewAssembly(newQuietLogger(), cfg)
			if err != nil {
				t.Fatalf("assembly: %v", err)
			}
			defer asm.Close()
			if (asm.RCATrigger != nil) != tc.wantTrigger {
				t.Fatalf("RCATrigger wired = %v, want %v", asm.RCATrigger != nil, tc.wantTrigger)
			}
			if tc.wantHook {
				if asm.Escalation == nil || asm.Escalation.OnEscalate == nil {
					t.Fatal("OnEscalate hook must be wired")
				}
			} else if asm.Escalation != nil && asm.Escalation.OnEscalate != nil {
				t.Fatal("OnEscalate must stay nil when auto off / no trigger point")
			}
		})
	}
}

// TestRCATriggerRealOrchestrator 复用真实编排器（#12 全链路，不复制逻辑）的
// 内存端到端：触发一次 → 审计出现 action=rca actor=auto 行；再模拟"重启"
// （新触发器、同一份审计）→ PG 双保险回读命中，不产生第二条 auto 分析。
func TestRCATriggerRealOrchestrator(t *testing.T) {
	e := newRCAEnv(t, nil)
	e.seedIncident(t, "INC-auto", "n1")

	tr := NewRCATrigger(e.orch, e.audit, NewAppMetrics(), 4, nil)
	tr.Trigger(criticalInc("INC-auto"))
	drain(t, tr)

	countAuto := func() int {
		n := 0
		for _, au := range e.audit.entries {
			if au.Action == AuditRCA && au.Actor == autoTriggerActor {
				n++
			}
		}
		return n
	}
	if n := countAuto(); n != 1 {
		t.Fatalf("auto rca audit rows = %d, want 1: %+v", n, e.audit.entries)
	}
	// 重启等价物：新触发器 seen 为空，仅靠审计回读兜住。
	tr2 := NewRCATrigger(e.orch, e.audit, NewAppMetrics(), 4, nil)
	tr2.Trigger(criticalInc("INC-auto"))
	drain(t, tr2)
	if n := countAuto(); n != 1 {
		t.Fatalf("auto rca audit rows after simulated restart = %d, want still 1", n)
	}
}
