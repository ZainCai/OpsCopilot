// Package memguard 有界内存结构统一护栏（优化方案 #6 内存有界化）。
//
// 定位：本项目所有"只进不出"的进程内 map/slice 都过同一个 Guard 判定——
// maxEntries 上限 + 超限淘汰计数（onEvict 记账）+ 告警水位 WARN。
// 淘汰策略本身（挑哪个条目下手）由各持有方决定（通常按最久未活跃），
// 本包只回答三个问题：**超限多少条、淘汰了多少条、指标长什么样**。
//
// 指标（注册进 pkg/metrics 的 Registry，Prometheus 文本暴露）：
//   - gauge   opscopilot_mem_entries{store="..."}     当前规模（回调求值，不两处记账）
//   - counter opscopilot_mem_evictions_total{store=...} 累计淘汰条数
//
// 淘汰与水位都打 slog WARN：首次必打，此后限流（每分钟最多一条）——
// 正常规模永不触发；一旦触发，日志与 /metrics 两处同时可见。
//
// nil 安全：New(max<=0) 返回 nil，所有方法对 nil 调用均为 no-op
// （无限 = 不装护栏）。这样各持有方的热路径不需要 if 判空分支，
// 默认行为的"无上限"与实现层统一。
package memguard

import (
	"log/slog"
	"sync"
	"time"

	"opscopilot/pkg/metrics"
)

// warnInterval 淘汰/水位 WARN 的限流间隔。
const warnInterval = time.Minute

// 指标名（全仓唯一定义处）。
const (
	MetricEntries   = "opscopilot_mem_entries"
	MetricEvictions = "opscopilot_mem_evictions_total"
)

// Guard 单个有界结构的护栏。并发安全（自身互斥；持有方在自锁内调用也安全——
// Guard 的锁内不再回调持有方）。
type Guard struct {
	store string
	max   int64
	water int64 // 告警水位（size >= water 触发 WARN）；0 = 未启用

	mu         sync.Mutex
	size       func() int // 当前规模回调（RegisterTo 的 gauge 求值用）
	evictions  uint64
	counter    *metrics.Counter
	registered bool
	warnOnce   bool
	lastWarn   time.Time
	warnWater  bool
	lastWater  time.Time
}

// New 构造护栏。maxEntries <= 0 → 返回 nil（无限，不装护栏）。
// warnRatio ∈ (0,1) 时启用告警水位（size >= max*ratio 打 WARN）；
// 其余取值（含 0、>=1）不启用水位——水位不是第二道闸门。
func New(store string, maxEntries int, warnRatio float64) *Guard {
	if maxEntries <= 0 {
		return nil
	}
	g := &Guard{store: store, max: int64(maxEntries)}
	if warnRatio > 0 && warnRatio < 1 {
		g.water = int64(float64(maxEntries) * warnRatio)
		if g.water >= g.max {
			g.water = g.max - 1 // 留出一格：水位必须早于淘汰
		}
	}
	return g
}

// Store 护栏名（指标 store 标签值）。
func (g *Guard) Store() string {
	if g == nil {
		return ""
	}
	return g.store
}

// MaxEntries 容量上限；无限返回 0。
func (g *Guard) MaxEntries() int {
	if g == nil {
		return 0
	}
	return int(g.max)
}

// SetSize 绑定当前规模回调（gauge 求值；注册前调用一次即可）。
func (g *Guard) SetSize(f func() int) {
	if g == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.size = f
}

// Evictions 累计淘汰条数。
func (g *Guard) Evictions() uint64 {
	if g == nil {
		return 0
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.evictions
}

// Over 报告当前规模超出上限多少条（0 = 未超限；无限恒 0）。
// 顺带做告警水位检查（限流 WARN）——调用方只需在插入后问一次。
func (g *Guard) Over(size int) int {
	if g == nil {
		return 0
	}
	g.mu.Lock()
	if g.water > 0 && int64(size) >= g.water && g.shouldWarnLocked(&g.warnWater, &g.lastWater) {
		slog.Warn("memguard: store approaching capacity",
			"store", g.store, "size", size, "warn_at", g.water, "max", g.max)
	}
	g.mu.Unlock()
	if int64(size) <= g.max {
		return 0
	}
	return size - int(g.max)
}

// Evicted 记入一次淘汰：计数 + counter + WARN（首次必打，此后每分钟最多一条）。
// n 为本次淘汰条数。
func (g *Guard) Evicted(n int) {
	if g == nil || n <= 0 {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.evictions += uint64(n)
	if g.counter != nil {
		g.counter.Add(uint64(n))
	}
	if g.shouldWarnLocked(&g.warnOnce, &g.lastWarn) {
		slog.Warn("memguard: evicting least-recently-active entries (capacity reached)",
			"store", g.store, "evicted", n, "evictions_total", g.evictions, "max", g.max)
	}
}

// RegisterTo 把两类指标挂进注册表（幂等：同护栏重复注册只生效一次）。
// gauge 用 SetSize 绑定的回调实时求值；counter 若注册前已有淘汰，
// 在锁内一次性并入存量，不漏账。
func (g *Guard) RegisterTo(reg *metrics.Registry) {
	if g == nil || reg == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.registered {
		return
	}
	g.registered = true
	labels := metrics.LabelSet{"store": g.store}
	g.counter = reg.Counter(MetricEvictions,
		"Entries evicted from bounded in-memory stores (memory bounding, plan #6)", labels)
	if g.evictions > 0 {
		g.counter.Add(g.evictions)
	}
	size := g.size
	if size == nil {
		size = func() int { return 0 }
	}
	reg.GaugeFunc(MetricEntries,
		"Current entries held by bounded in-memory stores (memory bounding, plan #6)",
		labels, func() float64 { return float64(size()) })
}

// shouldWarnLocked 限流判定（必须持 g.mu 调用）：首次立即放行，
// 之后每 warnInterval 至多一条；放行时更新时间戳。
func (g *Guard) shouldWarnLocked(once *bool, last *time.Time) bool {
	now := time.Now()
	if *once && now.Sub(*last) < warnInterval {
		return false
	}
	*once = true
	*last = now
	return true
}
