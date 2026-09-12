// leader.go 优化方案 #11 水平扩展 / ADR-012：拓扑单 owner（leader）选举与
// 后台循环门禁。协调逻辑按 ADR"模块边界"条款落在 cmd/（编排只汇合在 cmd，
// internal 各模块不感知选主）。
//
// 选举机制（ADR-012 选定方案）：PG **会话级 advisory lock**（固定键
// LeaderAdvisoryLockKey），零新增中间件：
//   - 实例持有一条**独占连接**（pgxpool.Acquire 出来的 pinned conn），循环
//     `pg_try_advisory_lock(key)` 竞选；成功即 leader，并在同连接周期性复检
//     ——复检用 `SELECT 1` 作**连接存活探针**（会话锁在会话存活期不可被抢，
//     探针报错 = 连接断 = 锁随会话消亡，立刻降级重选）。刻意不用"重跑
//     try-lock"当探针：PG 会话锁是引用计数的，重复 try 会重复持有，显式
//     解锁就还不干净（见 sqlLeaderProbe 注释）；
//   - 连接断 / 进程崩 → PG 自动释放会话锁，其余实例在下一竞选节拍
//     （≤ OPS_LEADER_RETRY_INTERVAL，默认 5s）接管；显式下野走
//     `pg_advisory_unlock` 归还，failover 不等过期；
//   - **开闸前必须完成簇态恢复**（failover 时序 3）：OnPromote 钩子按
//     Redis 镜像 → PG alert_cluster 顺序重建内存簇，恢复失败**不翻转 leader
//     标志、不启动任何 gated 循环**，持锁退避重试——恢复完成前本实例不产出
//     任何拓扑判决（宁漏勿杀）。
//
// 降级路径（与单实例现状逐字节一致）：无 DSN/pool 或 OPS_LEADER_ELECTION=off
// → 恒为 leader（构造即置位，选举环空转等停机）。
//
// 门禁矩阵（决策章"链路归属矩阵"，接线见 main.go）：
//   - leader 才跑：Host 采集（拓扑图唯一归属）→ 降噪判决链路随之、
//     AlertPoller 拉取入队、Retention 归档清扫；
//   - 所有实例常驻：webhook 入队 + IngestWorker（000016 认领租约互斥）、
//     REST/SSE/控制台、通知渠道、escalation（PG 台账主键认领）、
//     ChangePruner（内存 map 每实例各自增长，leader-only 反而无界）。
package main

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"opscopilot/internal/config"
)

// LeaderAdvisoryLockKey 选主锁键（ADR-012）："OCPL"（0x4F43504C）。
// 簇级全局固定常量——M1 单租户形态下 leader 是集群角色，不分租户。
const LeaderAdvisoryLockKey int64 = 0x4F43504C

const (
	// leaderLockOpTimeout 单次锁操作（try/复检/显式解锁）语句上限。复检走
	// 的是 leader 自己的 pinned 连接，DB 卡住绝不拖住选举环——超时按"锁不
	// 可保有"处理（降级重选），与 pgSinkTimeout 同款口径。
	leaderLockOpTimeout = 5 * time.Second
	// leaderAcquireMinTimeout 竞选轮取连接的超时下界（节拍 env 可以配得很
	// 小，取连接是另一回事——慢 DB 下 200ms 节拍会疯狂 Acquire 失败刷日志）。
	leaderAcquireMinTimeout = 2 * time.Second
	// leaderStopWaitTimeout Stop 等选举环退出的上限：Assembly.Close 关共享池
	// 之前必须让选举环归还 pinned 连接（pgxpool.Close 会等所有 Acquire 出去
	// 的连接归还，否则停机挂在池关闭上）。
	leaderStopWaitTimeout = 10 * time.Second
	// leaderPromoteTimeout OnPromote 钩子（簇恢复）单次上限——Redis 镜像 +
	// PG 真相源各一轮读，秒级；超时算恢复失败，持锁下一节拍重试。
	leaderPromoteTimeout = 30 * time.Second
)

// leaderSQL 锁操作语句（bigint 显式 cast，避免服务端类型推断歧义）。
//
// 复检刻意用 `SELECT 1` 而不是"同会话重跑 try-lock"：PG 会话锁是**引用计数**
// 的——同会话重复 pg_try_advisory_lock 每次都会再持有一层（文档明说计数包含
// try 版），一旦把 try 当探针用，leader 每个节拍都给锁加一格引用，单次
// unlock 就再也还不干净（接管者永远等不到释放）。`SELECT 1` 同样钉在
// pinned 连接上：报错 = 连接断 = 会话锁随之释放，探针语义等价且不动计数。
// （对 ADR"同会话重跑 try-lock 恒 true"口径的实现级修正，行为意图不变。）
const (
	sqlTryLeaderLock = `SELECT pg_try_advisory_lock($1::bigint)`
	sqlUnlockLeader  = `SELECT pg_advisory_unlock($1::bigint)`
	sqlLeaderProbe   = `SELECT 1`
)

// LeaderElector leader 选举器（实现细节）+ leader 状态源（对外只有
// IsLeader/Notify/SetOnPromote/Run/Stop）。零值不可用，必须走 NewLeaderElector。
type LeaderElector struct {
	pool    *pgxpool.Pool
	enabled bool          // false = 降级恒 leader（无 DB 或 OPS_LEADER_ELECTION=off）
	retry   time.Duration // 竞选节拍 = 持锁复检节拍 = 恢复失败退避重试节拍

	logf func(format string, args ...any)

	mu        sync.Mutex
	leader    bool
	changed   chan struct{} // 每次状态翻转 close + 换新（订阅广播）
	pinned    *pgxpool.Conn // 独占选举连接（竞选期即持有，见文件头）
	lockHeld  bool          // 本会话已持有 advisory lock（引用计数护栏：钩子重试期不再重跑 try-lock）
	onPromote func(context.Context) error
	started   bool
	stopNow   bool // Stop 先于 Run：后来的 Run 直接退出，不碰锁
	quit      chan struct{}
	quitOnce  sync.Once
	runDone   chan struct{}
	runDoneO  sync.Once
}

// NewLeaderElector 构造。enabled=false 或 pool=nil 走降级路径（恒 leader，
// 构造即置位——gauge 从装配完成第一秒就是 1，与单实例现状逐字节一致）。
// retry<=0 回退 config 默认（与 OPS_LEADER_RETRY_INTERVAL 缺省同源）。
func NewLeaderElector(pool *pgxpool.Pool, enabled bool, retry time.Duration, logf func(string, ...any)) *LeaderElector {
	if retry <= 0 {
		retry = config.DefaultLeaderRetryInterval
	}
	if logf == nil {
		logf = func(string, ...any) {}
	}
	l := &LeaderElector{
		pool:    pool,
		enabled: enabled && pool != nil, // ADR：无 DSN/pool 恒 leader，off 同
		retry:   retry,
		logf:    logf,
		changed: make(chan struct{}),
		quit:    make(chan struct{}),
		runDone: make(chan struct{}),
	}
	if !l.enabled {
		l.leader = true
	}
	return l
}

// SetOnPromote 登记开闸前钩子（ADR 时序 3：簇态恢复）。返回 error = 恢复
// 未完成：保持非 leader、持锁退避重试。Run 之后设置也生效（每轮读取）。
func (l *LeaderElector) SetOnPromote(fn func(context.Context) error) {
	l.mu.Lock()
	l.onPromote = fn
	l.mu.Unlock()
}

// IsLeader 当前是否 leader（gauge / 门禁循环监督的统一状态源）。
func (l *LeaderElector) IsLeader() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.leader
}

// Notify 返回状态翻转广播通道：每次 leader 状态变化，旧通道 close、换新
// 通道。订阅方标准姿势：先取通道、select 等待、醒来重读 IsLeader。
func (l *LeaderElector) Notify() <-chan struct{} {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.changed
}

// Run 选举主循环，直到 ctx 取消或 Stop。只应跑一个 goroutine（重复调用
// 第二次直接返回）。降级模式下不跑任何 PG 语句（行为与选举引入前完全一致）。
func (l *LeaderElector) Run(ctx context.Context) {
	l.mu.Lock()
	if l.started {
		l.mu.Unlock()
		l.runDoneO.Do(func() { close(l.runDone) })
		return
	}
	l.started = true
	stopped := l.stopNow
	l.mu.Unlock()
	defer l.runDoneO.Do(func() { close(l.runDone) })

	if stopped {
		return
	}
	if !l.enabled {
		// 恒 leader（构造已置位）：只等停机，不做任何锁操作。
		select {
		case <-ctx.Done():
		case <-l.quit:
		}
		return
	}

	for {
		if l.stopped(ctx) {
			l.stepDown("shutting down")
			return
		}
		if l.IsLeader() {
			// 复检 = 同连接重跑 try-lock：true 恒成立（会话锁不可被他抢），
			// 报错/超时 = 连接没了、锁随会话释放 → 降级重选。
			held, err := l.probe()
			switch {
			case err != nil:
				l.logf("ERROR: leader lock probe failed — connection lost or lock operation timed out, demoting: %v", err)
				slog.Error("leader lock probe failed, demoting", "err", err)
				l.stepDown("probe failed")
			case !held:
				// 理论上不可达（同会话恒 true）；若真发生（如人工 unlock）必须响亮。
				l.logf("ERROR: leader advisory lock no longer held by our session — demoting")
				slog.Error("leader advisory lock vanished, demoting")
				l.stepDown("lock lost")
			}
		} else {
			l.campaignRound(ctx)
		}
		if !l.sleepTick(ctx) {
			l.stepDown("shutting down")
			return
		}
	}
}

// campaignRound 一轮竞选：确保 pinned 连接在手 → try-lock → 拿到锁先过
// OnPromote 门禁再翻 leader。任何失败都保持非 leader，下一节拍重试。
func (l *LeaderElector) campaignRound(ctx context.Context) {
	conn, err := l.heldConn(ctx)
	if err != nil {
		l.logf("ERROR: leader election: acquire pinned connection: %v", err)
		slog.Error("leader election acquire failed", "err", err)
		return
	}
	held := false
	l.mu.Lock()
	already := l.lockHeld
	l.mu.Unlock()
	if !already {
		held, err = queryLeaderLock(ctx, conn, sqlTryLeaderLock)
		if err != nil {
			l.logf("ERROR: leader election: pg_try_advisory_lock: %v", err)
			slog.Error("leader election try-lock failed", "err", err)
			l.releasePinned(false) // 连接疑似已坏，直接丢弃（物理关断即释放锁）
			return
		}
		if held {
			l.mu.Lock()
			l.lockHeld = true
			l.mu.Unlock()
		}
	} else {
		held = true // 上一轮已持锁但没开闸（钩子失败重试中）：重跑 try 会多计一层引用
	}
	if !held {
		return // 他实例是 leader；pinned 连接留着，下一节拍再试
	}
	l.mu.Lock()
	hook := l.onPromote
	l.mu.Unlock()
	if hook != nil {
		hctx, cancel := context.WithTimeout(ctx, leaderPromoteTimeout)
		err := hook(hctx)
		cancel()
		if err != nil {
			if ctx.Err() == nil && !l.quitSignaled() {
				l.logf("ERROR: leader promote blocked: cluster restore failed — staying non-leader, "+
					"retrying every %s (topology verdicts stay OFF until restore succeeds, per ADR-012): %v", l.retry, err)
				slog.Error("leader promote cluster restore failed, staying non-leader", "err", err)
			}
			return // 锁留着但不开闸：恢复没成，判决链路绝不带残缺簇态启动
		}
	}
	l.setLeader(true)
}

// probe leader 持锁期的同连接复检（兼连接探针）。刻意不重跑 try-lock——
// 会话锁引用计数（见 sqlLeaderProbe 注释）；探针只需回答"这条连接还活着吗"：
// 活着 → true（会话锁不可能在会话存活期间被抢走/消失），断了 → err。
func (l *LeaderElector) probe() (bool, error) {
	l.mu.Lock()
	conn := l.pinned
	l.mu.Unlock()
	if conn == nil {
		return false, errors.New("leader flag set without pinned connection (invariant violated)")
	}
	pctx, cancel := context.WithTimeout(context.Background(), leaderLockOpTimeout)
	defer cancel()
	if _, err := conn.Exec(pctx, sqlLeaderProbe); err != nil {
		return false, err
	}
	return true, nil
}

// heldConn 返回选举连接（无则从池 Acquire 一条并钉住——"独占连接"的含义：
// 只归选举环用，不还池，保证复检/解锁都落在同一 PG 会话上）。
func (l *LeaderElector) heldConn(ctx context.Context) (*pgxpool.Conn, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.pinned != nil {
		return l.pinned, nil
	}
	timeout := l.retry
	if timeout < leaderAcquireMinTimeout {
		timeout = leaderAcquireMinTimeout
	}
	actx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	conn, err := l.pool.Acquire(actx)
	if err != nil {
		return nil, err
	}
	l.pinned = conn
	return conn, nil
}

// queryLeaderLock 在指定连接上跑一条锁操作语句（会话级：锁跟着这条连接的
// 后端进程走）。
func queryLeaderLock(ctx context.Context, conn *pgxpool.Conn, sql string) (bool, error) {
	lctx, cancel := context.WithTimeout(ctx, leaderLockOpTimeout)
	defer cancel()
	var ok bool
	if err := conn.QueryRow(lctx, sql, LeaderAdvisoryLockKey).Scan(&ok); err != nil {
		return false, err
	}
	return ok, nil
}

// stepDown 下野：显式解锁 + 归还 pinned 连接 + 翻标志。解锁语句用独立
// ctx（停机路径父 ctx 已取消，但显式解锁能让接管者少等一个 TCP 关断）。
// unlock 失败不致命：Release 检测到坏连接会物理关断，会话锁随之释放。
func (l *LeaderElector) stepDown(reason string) {
	l.releasePinned(true)
	l.setLeader(false)
	_ = reason // reason 只用于调用侧可读性；日志在 setLeader/releasePinned 处
}

// releasePinned 归还（可选先解锁）选举连接。幂等：并发下只有一方拿到句柄。
func (l *LeaderElector) releasePinned(unlock bool) {
	l.mu.Lock()
	conn := l.pinned
	l.pinned = nil
	l.lockHeld = false // 连接已离手（显式解锁清干净或随会话关断消亡），不再声称持有
	l.mu.Unlock()
	if conn == nil {
		return
	}
	if unlock {
		// 循环解锁直到"不再持有"：会话锁引用计数——竞选重试每一拍都会
		// 再持有一层（try-lock 对已持有同样计数），单次 unlock 还不干净。
		uctx, cancel := context.WithTimeout(context.Background(), leaderLockOpTimeout)
		holds := 0
		for i := 0; i < 64; i++ {
			var ok bool
			err := conn.QueryRow(uctx, sqlUnlockLeader, LeaderAdvisoryLockKey).Scan(&ok)
			if err != nil {
				l.logf("WARNING: leader advisory unlock failed after %d hold(s) (session close will release the lock anyway): %v", holds, err)
				break
			}
			if !ok {
				break // 已无持有（最后这次未持有调用会在 PG 日志记一条 WARNING，无害）
			}
			holds++
		}
		cancel()
	}
	conn.Release()
}

// setLeader 翻状态并广播（gauge 自动跟随）。每次真实翻转：slog WARN + logf
// 双口径——"当前谁是 owner"的变化是运维必须看见的事件，不许静默。
func (l *LeaderElector) setLeader(v bool) {
	l.mu.Lock()
	changed := l.leader != v
	if changed {
		l.leader = v
		close(l.changed)
		l.changed = make(chan struct{})
	}
	l.mu.Unlock()
	if !changed {
		return
	}
	if v {
		l.logf("leader: PROMOTED — this instance now owns topology collection/verdict chains (ADR-012)")
		slog.Warn("leader state changed", "is_leader", true)
	} else {
		l.logf("leader: DEMOTED — gated loops stop at safe points until next election round (ADR-012)")
		slog.Warn("leader state changed", "is_leader", false)
	}
}

// sleepTick 等一个节拍；返回 false = ctx 取消或已 Stop（调用方负责下野收尾）。
func (l *LeaderElector) sleepTick(ctx context.Context) bool {
	t := time.NewTimer(l.retry)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-l.quit:
		return false
	case <-t.C:
		return true
	}
}

func (l *LeaderElector) quitSignaled() bool {
	select {
	case <-l.quit:
		return true
	default:
		return false
	}
}

func (l *LeaderElector) stopped(ctx context.Context) bool {
	return ctx.Err() != nil || l.quitSignaled()
}

// Stop 同步停选举环：Run 在跑则等它完成下野（解锁 + 归还连接）——
// Assembly.Close 关共享池之前必须调（pgxpool.Close 会等所有被 Acquire 的
// 连接归还，pinned 连接不还池就关不掉）。Run 未启动则只置停用标记。幂等。
func (l *LeaderElector) Stop() {
	l.quitOnce.Do(func() { close(l.quit) })
	l.mu.Lock()
	started := l.started
	if !started {
		l.stopNow = true
	}
	done := l.runDone
	l.mu.Unlock()
	if !started {
		return
	}
	select {
	case <-done:
	case <-time.After(leaderStopWaitTimeout):
		// 选举环挂着没退（连接卡 IO）——响亮报错，不静默：池关闭可能被拖住。
		l.logf("ERROR: leader stop: election loop did not exit within %s — pinned connection may still block pool close", leaderStopWaitTimeout)
		slog.Error("leader stop timed out waiting for election loop", "timeout", leaderStopWaitTimeout)
	}
}
