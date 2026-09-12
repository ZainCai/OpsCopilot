// escalation.go W9-3 值班升级（最小版）：未 ack 超时重发一次。
//
// 动机：通知发出去 ≠ 有人接手。事件停在 open（未确认）超过阈值时，再发一次
// "超时未响应"通知，把"没人管"从沉默变成显式信号。这是值班闭环的最低要求
// ——不做排班/轮岗/多级升级（那些留 M3）。
//
// 口径与边界：
//   - **只对 state=open 升级**：acked 之后属人工已接手（R2 语义：人工干预过
//     的单置 manual_only），不再催——催已接手的单是纯噪声；
//   - **只发一次**：台账 incident_escalation 以 (tenant_id, incident_id) 主键
//     幂等，Claim 成功者才发；多实例部署下也不会重复发；
//   - **不经 Gate**：升级是"对既有事件再提醒"，与降噪判定（dedup/merge）
//     正交——Gate 管"该不该第一次通知"，升级管"发了没人理"；
//   - 严重级沿用事件自身 severity → 自然走 W9-3 的渠道路由（critical 进 IM、
//     info 只留日志）。
package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"opscopilot/internal/incident"
	"opscopilot/internal/notify"
	"opscopilot/pkg/memguard"
)

// EscalationLedger 升级台账：Claim 幂等认领 + Release 失败回滚。
//
// 两方法而非只 Claim：发送失败必须能回滚，否则一次网络抖动就让该事件
// 永久失去升级机会——"静默失败才是真问题"（项目纪律）。
type EscalationLedger interface {
	// Claim 原子认领。返回 true = 本轮由调用方负责发通知（首次认领）；
	// false = 已被认领过（他轮/他实例），不重复发。
	Claim(ctx context.Context, incidentID string) (bool, error)
	// Release 释放认领（发送失败时回滚，让下一轮重试）。
	Release(ctx context.Context, incidentID string) error
}

// memEscalationLedger 内存台账（无 DB 的 dev/测试；重启即丢、单实例）。
//
// 内存有界化（优化方案 #6）：Claim 的幂等键只进不出（Release 仅在发送
// 失败时回滚），无上限时是 DB 缺席降级路径上的无界 map。装护栏后按
// **最久未活跃**（认领时刻最早）淘汰——被逐条大概率早已 resolved
// （不再被扫描），复活代价是"理论上可能重复升级一次"，两害相权。
type memEscalationLedger struct {
	mu    sync.Mutex
	seen  map[string]time.Time // incidentID → 认领时刻（lastSeen）
	guard *memguard.Guard
}

func newMemEscalationLedger() *memEscalationLedger {
	return newMemEscalationLedgerWithLimits(nil)
}

// newMemEscalationLedgerWithLimits 构造并装配容量护栏（nil = 不设限）。
func newMemEscalationLedgerWithLimits(guard *memguard.Guard) *memEscalationLedger {
	l := &memEscalationLedger{seen: map[string]time.Time{}, guard: guard}
	guard.SetSize(l.Size)
	return l
}

// MemGuards 返回装配的护栏（供装配层注册指标）。
func (l *memEscalationLedger) MemGuards() []*memguard.Guard {
	if l.guard == nil {
		return nil
	}
	return []*memguard.Guard{l.guard}
}

// Size 当前台账条数（锁内读；gauge 回调）。
func (l *memEscalationLedger) Size() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.seen)
}

func (l *memEscalationLedger) Claim(_ context.Context, incidentID string) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.seen[incidentID]; ok {
		return false, nil
	}
	l.seen[incidentID] = time.Now()
	l.enforceLocked()
	return true, nil
}

// enforceLocked 超限淘汰（持锁）：认领时刻最早者优先，同刻按 ID 字典序。
func (l *memEscalationLedger) enforceLocked() {
	if l.guard == nil {
		return
	}
	excess := l.guard.Over(len(l.seen))
	removed := 0
	for removed < excess {
		var victim string
		var victimAt time.Time
		found := false
		for id, at := range l.seen {
			if !found || at.Before(victimAt) || (at.Equal(victimAt) && id < victim) {
				victim, victimAt, found = id, at, true
			}
		}
		if !found {
			break
		}
		delete(l.seen, victim)
		removed++
	}
	if removed > 0 {
		l.guard.Evicted(removed)
	}
}

func (l *memEscalationLedger) Release(_ context.Context, incidentID string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.seen, incidentID)
	return nil
}

// pgEscalationLedger PG 台账（多实例安全：INSERT ... ON CONFLICT DO NOTHING，
// 只有真正插入成功的那一次认领成立）。
type pgEscalationLedger struct {
	pool   *pgxpool.Pool
	tenant string
}

func newPGEscalationLedger(pool *pgxpool.Pool, tenant string) *pgEscalationLedger {
	return &pgEscalationLedger{pool: pool, tenant: tenant}
}

func (l *pgEscalationLedger) Claim(ctx context.Context, incidentID string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	tag, err := l.pool.Exec(ctx, `
INSERT INTO incident_escalation (tenant_id, incident_id) VALUES ($1, $2)
ON CONFLICT (tenant_id, incident_id) DO NOTHING`, l.tenant, incidentID)
	if err != nil {
		return false, fmt.Errorf("escalation ledger: claim %s: %w", incidentID, err)
	}
	return tag.RowsAffected() > 0, nil
}

func (l *pgEscalationLedger) Release(ctx context.Context, incidentID string) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := l.pool.Exec(ctx,
		`DELETE FROM incident_escalation WHERE tenant_id = $1 AND incident_id = $2`,
		l.tenant, incidentID)
	if err != nil {
		return fmt.Errorf("escalation ledger: release %s: %w", incidentID, err)
	}
	return nil
}

// EscalationDispatcher 升级通知出口（notify.Registry 满足；接口便于测试注入）。
type EscalationDispatcher interface {
	Dispatch(m notify.Message) error
}

// EscalationPoller 值班升级调度器：周期扫描超时未 ack 事件并升级一次。
type EscalationPoller struct {
	Store    incident.Store       // 事件源（List(StateOpen)）
	Notify   EscalationDispatcher // 通知出口（走严重级路由）
	Ledger   EscalationLedger     // 幂等台账
	Tenant   string
	After    time.Duration // 创建后多久仍未 ack 触发升级
	Interval time.Duration // 扫描周期（<=0 = 未启用）
	logf     func(string, ...any)
	now      func() time.Time // 可注入时钟（测试用）
	// OnEscalate 成功升级一条事件后的回调（二期池波二 #4：critical 事件
	// 自动 RCA 触发挂点）。nil = 无联动（保持现状）。
	//
	// **纪律**：本回调在 escalation 扫描循环内联调用，实现方**必须非阻塞**
	// （只做内存去重 + 有界队列投递，投递满即丢弃计数）——绝不允许在这里
	// 同步跑 RCA/查库/发通知，否则一次慢分析会拖垮整个升级节律。
	OnEscalate func(inc incident.Incident)
}

// NewEscalationPoller 构造。任何依赖缺失或周期 <=0 → Run 立即返回（不 panic）。
func NewEscalationPoller(store incident.Store, dispatcher EscalationDispatcher, ledger EscalationLedger,
	tenant string, after, interval time.Duration, logf func(string, ...any)) *EscalationPoller {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	return &EscalationPoller{
		Store: store, Notify: dispatcher, Ledger: ledger, Tenant: tenant,
		After: after, Interval: interval, logf: logf, now: time.Now,
	}
}

// Run 周期扫描，ctx 取消即退出。首轮立即扫（不等第一个 tick），
// 让服务一起来就能补上升级积压的存量事件。
func (p *EscalationPoller) Run(ctx context.Context) {
	if p.Store == nil || p.Notify == nil || p.Ledger == nil || p.Interval <= 0 || p.After <= 0 {
		p.logf("escalation: not started (disabled or unwired)")
		return
	}
	p.logf("escalation: started (after %v, interval %v)", p.After, p.Interval)
	p.pollOnce(ctx)
	ticker := time.NewTicker(p.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			p.logf("escalation: stopped")
			return
		case <-ticker.C:
			p.pollOnce(ctx)
		}
	}
}

// pollOnce 扫一轮：对"未 ack 且创建超时"的事件认领并升级。单轮失败只记日志。
func (p *EscalationPoller) pollOnce(ctx context.Context) {
	open, err := p.Store.List(incident.StateOpen)
	if err != nil {
		if ctx.Err() != nil {
			return // 停机中的取消失败，不刷噪声
		}
		p.logf("WARNING: escalation scan: %v", err)
		return
	}
	now := p.now()
	escalated := 0
	for _, inc := range open {
		if inc.CreatedAt.IsZero() || now.Sub(inc.CreatedAt) < p.After {
			continue // 未到升级阈值
		}
		claimed, err := p.Ledger.Claim(ctx, inc.ID)
		if err != nil {
			p.logf("WARNING: escalation claim %s: %v", inc.ID, err)
			continue
		}
		if !claimed {
			continue // 已升级过：只发一次
		}
		msg := notify.Message{
			TenantID:   p.Tenant,
			ClusterKey: escalationClusterKey(inc),
			Title:      "[超时未响应] " + inc.Title,
			Body: fmt.Sprintf("事件 %s 已创建 %s 仍未确认（state=open），请值班人员立即处置。",
				inc.ID, now.Sub(inc.CreatedAt).Round(time.Second)),
			Severity: inc.Severity,
		}
		if err := p.Notify.Dispatch(msg); err != nil {
			// 发送失败：回滚认领让下一轮重试；回滚也失败则记日志（该事件
			// 本轮放弃，避免"送达失败刷屏"）。
			p.logf("WARNING: escalation dispatch %s: %v", inc.ID, err)
			if rerr := p.Ledger.Release(ctx, inc.ID); rerr != nil {
				p.logf("WARNING: escalation release %s: %v", inc.ID, rerr)
			}
			continue
		}
		escalated++
		if p.OnEscalate != nil {
			// 升级成功联动（#4 自动 RCA 触发点）：回调约定非阻塞（见字段注释），
			// 内联调用不引入新串行成本；critical 过滤由实现方负责。
			p.OnEscalate(inc)
		}
	}
	if escalated > 0 {
		p.logf("escalation: scanned=%d escalated=%d", len(open), escalated)
	}
}

// escalationClusterKey 取首个簇键（通知消息用；无簇关联返回空）。
func escalationClusterKey(inc incident.Incident) string {
	if len(inc.ClusterKeys) > 0 {
		return inc.ClusterKeys[0]
	}
	return ""
}
