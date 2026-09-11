// W9-5：变更事件库的保留窗清理接线（第八轮审核 C7 / 建议项 4）。
//
// 问题：`topology.ChangeStore` 是**进程内 map**，`PruneBefore` 早就写好并
// 有测试，但**生产从未调用**（只有 change_test.go 调）——而 `POST
// /api/v1/changes` 是公开可写的 webhook（CI/Jenkins 持续提交），长期运行
// 会让变更事件在内存里无限增长。降噪簇、工单、审计都有保留策略，唯独这条
// 没有，属典型"写了没接线"。
//
// 为什么放在 cmd 而不是 internal/topology：`PruneBefore` 的注释已定下契约——
// 本模块不内置后台 goroutine，定时策略属编排层。本文件就是那个编排层实现，
// 与 retention.go（工单归档）同一范式、同一取舍。
//
// 保留窗取值：7 天。依据是变更关联查询的最大回溯窗（"告警前 30 分钟"量级）
// 再乘两个数量级的余量——运维排障常要"上周那次发版"，7 天够用；而变更事件
// 单条只有几百字节，7 天量级（百台主机 × 每天几十次变更）完全在内存预算内。
package main

import (
	"context"
	"os"
	"time"

	"opscopilot/internal/topology"
)

// 变更库清理的环境变量。
const (
	// envChangeRetention 保留窗（Go duration 或 "<N>d"；默认 7d；off 关闭）。
	envChangeRetention = "OPS_CHANGE_RETENTION"
	// envChangePruneInterval 清理周期（默认 1h）。
	envChangePruneInterval = "OPS_CHANGE_PRUNE_INTERVAL"
)

const (
	// changeRetentionDefault 默认保留窗：7 天（见文件头）。
	changeRetentionDefault = 7 * 24 * time.Hour
	// changePruneIntervalDefault 默认清理周期。变更事件量级小，每小时一次
	// 足够；频率再高只是徒增一次 O(n) 遍历。
	changePruneIntervalDefault = time.Hour
	// changePruneStartupDelay 首轮延迟：避开启动风暴（与 retention 同）。
	changePruneStartupDelay = time.Minute
	// changeStoreWarnSize 容量告警阈值。超过它说明保留窗配得过长或写入
	// 速率异常——内存 map 的膨胀是不可回收的，必须在日志里看得见。
	changeStoreWarnSize = 100000
)

// ChangePruner 变更事件库的周期清理器。
type ChangePruner struct {
	store     *topology.ChangeStore
	retention time.Duration
	interval  time.Duration
	logf      func(string, ...any)
}

// NewChangePruner 构造。retention<=0 或 interval<=0 表示未启用（Run 立即返回）。
func NewChangePruner(store *topology.ChangeStore, retention, interval time.Duration, logf func(string, ...any)) *ChangePruner {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	return &ChangePruner{store: store, retention: retention, interval: interval, logf: logf}
}

// NewChangePrunerFromEnv 按环境变量构造。
//
//	OPS_CHANGE_RETENTION       保留窗，默认 7d；off/0 关闭
//	OPS_CHANGE_PRUNE_INTERVAL  清理周期，默认 1h
//
// 两者任一非法即返回错误（fail-fast，与保留策略解析同纪律：静默回退会让
// 运维以为配置生效了）。
func NewChangePrunerFromEnv(store *topology.ChangeStore, logf func(string, ...any)) (*ChangePruner, error) {
	retention, on, err := ParseRetentionDefault(os.Getenv(envChangeRetention), changeRetentionDefault)
	if err != nil {
		return nil, err
	}
	if !on {
		return NewChangePruner(nil, 0, 0, logf), nil
	}
	interval := changePruneIntervalDefault
	if raw := os.Getenv(envChangePruneInterval); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil || d <= 0 {
			return nil, &invalidChangePruneIntervalError{raw: raw, err: err}
		}
		interval = d
	}
	return NewChangePruner(store, retention, interval, logf), nil
}

type invalidChangePruneIntervalError struct {
	raw string
	err error
}

func (e *invalidChangePruneIntervalError) Error() string {
	if e.err == nil {
		return "change prune: invalid " + envChangePruneInterval + " " + e.raw + ": must be a positive duration"
	}
	return "change prune: invalid " + envChangePruneInterval + " " + e.raw + ": " + e.err.Error()
}

// Run 周期清理；首轮延迟一分钟（避开启动风暴），ctx 取消即退出。
func (p *ChangePruner) Run(ctx context.Context) {
	if p == nil {
		return // nil 接收者直接返回：不能碰 p.logf（nil 解引用）
	}
	if p.store == nil || p.retention <= 0 || p.interval <= 0 {
		p.logf("change prune: OFF")
		return
	}
	p.logf("change prune: ON (drop change events older than %s, every %v)", p.retention, p.interval)
	select {
	case <-ctx.Done():
		return
	case <-time.After(changePruneStartupDelay):
	}
	p.pruneOnce(time.Now())
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			p.logf("change prune: stopped")
			return
		case <-ticker.C:
			p.pruneOnce(time.Now())
		}
	}
}

// pruneOnce 清理一轮并记录容量。返回 (removed, size)——供测试直接断言。
//
// 每轮都打容量日志（而不是只在 removed>0 时打）：这个方法的价值一半在
// 清理、一半在让"内存里的变更库有多大"变成可观测量——C7 之所以长期没人
// 发现，就是因为这个数字从来没被打印过。
func (p *ChangePruner) pruneOnce(now time.Time) (removed, size int) {
	cutoff := now.Add(-p.retention)
	removed = p.store.PruneBefore(cutoff)
	size = p.store.Len()
	if size >= changeStoreWarnSize {
		p.logf("WARNING: change store size=%d exceeds %d — retention %s may be too long or write rate abnormal",
			size, changeStoreWarnSize, p.retention)
	}
	p.logf("change store: size=%d pruned=%d (retention %s)", size, removed, p.retention)
	return removed, size
}
