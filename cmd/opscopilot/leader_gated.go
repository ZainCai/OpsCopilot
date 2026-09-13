// leader_gated.go leader 门禁监督器（runLeaderGated）与 OnPromote 簇态恢复钩子（ADR-012 时序 3）。
// 选举机制本体与门禁矩阵注释见 leader.go 文件头。
package main

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"

	"opscopilot/internal/noise"
)

// ---------- leader 门禁循环监督（矩阵"leader 才跑"一侧的接线原语） ----------

// leaderGate leader 状态源最小抽象（*LeaderElector 满足；单测可注入假实现）。
type leaderGate interface {
	IsLeader() bool
	Notify() <-chan struct{}
}

// runLeaderGated 门禁监督器：leader 在位时以每个 start 函数为入口各起一个
// goroutine（函数收 leader ctx，约定 ctx 取消即在安全点退出并返回）；失去
// leader → 取消并等全部退出（切换窗口内循环停在安全点）；重新当选 → 再拉起。
// starts 本身不产出常驻 goroutine——本函数返回即全部循环已退出。
func runLeaderGated(ctx context.Context, gate leaderGate, starts ...func(context.Context)) {
	for {
		if ctx.Err() != nil {
			return
		}
		// Notify 标准姿势（契约见 leader.go:142-148"先取通道、select 等待、
		// 醒来重读 IsLeader"）：**先捕获通道，再判 IsLeader**。两步各自加锁，
		// 顺序不可颠倒——若反过来"先判 IsLeader==true 再取通道"，一次
		// setLeader(false) 的 close-换新通道（leader.go:352-357）恰落在两者之间，
		// 取到的就是那张**不会再被 close 的新通道**，本次 demote 广播被漏掉，
		// 监督器挂等新翻转、gated 循环在非 leader 态继续跑（ADR-012 单 owner
		// 违例的双写窗口）。先捕获、后校验：捕获之后发生的任何翻转都会 close
		// 这张已在手的通道，select 必收。
		notify := gate.Notify()
		if !gate.IsLeader() {
			// 非 leader（fail-closed）：挂起等翻转或整体停机，醒来看当前态。
			select {
			case <-ctx.Done():
				return
			case <-notify:
			}
			continue // 醒来看当前态：成为 leader 才进入下一段，否则继续等
		}
		lctx, cancel := context.WithCancel(ctx)
		var wg sync.WaitGroup
		for _, s := range starts {
			start := s
			wg.Add(1)
			go func() {
				defer wg.Done()
				start(lctx)
			}()
		}
		// 等"失去 leader"或整体停机。进入本分支的 notify 已在捕获 IsLeader
		// 之前取好，漏不到这次 demote；每次被一次翻转唤醒后**重新捕获**
		// notify 再判状态（维持"捕获→判→等"顺序），既杜绝漏唤醒也不把同向
		// 事件数错（stale-able 广播：醒来看当前态）。
		for {
			if !gate.IsLeader() {
				break // 失去 leader：出等待，走 cancel + wg.Wait 停全部循环
			}
			select {
			case <-ctx.Done(): // 整体停机，交由下方 ctx.Err() 判定退出
			case <-notify: // 一次翻转，重读状态并重新捕获通道
			}
			if ctx.Err() != nil {
				break
			}
			notify = gate.Notify()
		}
		cancel()
		wg.Wait()
		// 循环回顶部：仍 leader（如抖动后回到 true）会立刻重启；非 leader
		// 则挂起等翻转。
	}
}

// ---------- OnPromote 钩子（ADR 时序 3：开闸前簇态恢复，宁漏勿杀） ----------

// clusterRestoreOnPromote 按 ADR 顺序重建内存簇：Redis 镜像（加速层）→
// PG alert_cluster（真相源）。取"第一个成功且非空"的来源；两路都失败才判
// 恢复失败（返回 error → 选举环不开闸、持锁退避重试）。
//
// "Redis 可达但为空、PG 不可读"放行（新开张/仅 Redis 部署形态，空簇 = 从零
// 开始，不是恢复失败）；反之（Redis 断、PG 在）走 PG 兜底 = ADR-001"清空后
// 可重建"的兑现。engine=nil（降噪关闭）没有簇态可恢复，直接开闸。
func clusterRestoreOnPromote(ctx context.Context, engine *NoiseEngine, redisSink *RedisClusterSink,
	pool *pgxpool.Pool, tenant string, logf func(string, ...any)) error {
	if engine == nil {
		return nil
	}
	var (
		redisRecs []noise.ClusterRecord
		redisErr  error = errors.New("redis mirror unwired")
	)
	if redisSink != nil {
		redisRecs, redisErr = redisSink.LoadClusters(ctx)
	}
	if redisErr == nil && len(redisRecs) > 0 {
		return engine.RestoreFrom(redisRecs)
	}
	if redisErr != nil {
		logf("WARNING: leader promote: redis mirror unreadable (%v), falling back to pg truth source", redisErr)
	}
	pgRecs, pgErr := loadAlertClustersFromPG(ctx, pool, tenant)
	if pgErr == nil {
		if redisErr == nil && len(redisRecs) == 0 && len(pgRecs) == 0 {
			logf("leader promote: cluster state empty on both mirrors (fresh start)")
		}
		return engine.RestoreFrom(pgRecs)
	}
	// PG 也读不了：若 redis 路是好的（只是空），以空簇开闸并响亮告警——
	// 两路全挂（redisErr != nil）才算恢复失败、阻塞开闸。
	if redisErr == nil {
		logf("WARNING: leader promote: pg alert_cluster unreadable (%v), promoting with redis mirror state (empty) — verdicts start from scratch", pgErr)
		return engine.RestoreFrom(nil)
	}
	return fmt.Errorf("cluster restore failed on both sources — redis: %w; pg: %w", redisErr, pgErr)
}
