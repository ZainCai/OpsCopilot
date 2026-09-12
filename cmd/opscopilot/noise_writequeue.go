// noise_writequeue.go 判决异步落库队列 + 批量 writer（优化方案 #8，队满丢弃取向）。
//
// 动机：ProcessAlerts 若在采集 goroutine 里同步逐条 SaveVerdict——PG 慢
// （连接池排队、语句超时 5s×N 条）会把整轮采集节拍占死。本文件把"判决
// 生成"与"判决落库"解耦：ProcessAlerts 生成判决后投递到带缓冲 channel
// 即返回；**单** writer goroutine 攒批 + 定时 flush，串行落已挂载的
// noise.VerdictSink（Redis 镜像 + PG 真相源的双写语义由 sink 侧决定，
// 接口契约不变——批量只发生在 writer 侧）。
//
// 顺序保证：单 writer 消费单 FIFO channel——批 N 的判决必然先于批 N+1
// 落库（alert_event 是追加语义，判决行之间无覆盖问题；簇快照仍走同步
// 路径，见 noise.go ProcessAlerts 锁边界注释）。Admit/延迟打点只依赖
// 内存判决（notify.Decision），不读判决存储——"入队即完成"不改变
// 降噪→Gate→通知的时序语义（见 docs/W9-4 §9 的耦合审查结论）。
//
// 降级取向（#8 修订版，对齐项目哲学"宁漏勿杀、多通知不静默丢"）：
// 落库是评估数据链，不是通知链路——**队满绝不回退阻塞/同步写占住采集
// goroutine**，而是丢弃该持久化任务：
//  1. 非阻塞投递：有空位直接进（正常路径）；
//  2. 队满 / writer 停机中 → 该条计 opscopilot_noise_sink_drops_total
//     {store=...} + slog ERROR，判决本身照常进内存态走 Gate 放行链路；
//  3. 停机 drain 超过上限（OPS_NOISE_SINK_DRAIN，默认 5s）→ channel 残量
//     同样计丢弃后返回，不再无限等。该计数是**丢失面的保守上界**：
//     预算耗尽后 writer 若仍在后台挣扎且 sink 恢复，残量可能最终落库，
//     计数不冲销——对账宁可偏悲观，不可静默乐观。
//
// ⚠️ 一致性声明：PG 判决流在极端洪峰下可缺条目；Redis 镜像 / PG 真相源
// 与内存态判决之间**非强一致**，对账以 drops 计数与 sink_queue 水位为准。
// DB 写失败（到达 DB 但语句报错）沿既有 verdictFailures 口径
// （opscopilot_noise_write_dropped_total gauge 镜像），与"队列侧未尝试"
// 的 drops 分账，不混计。
//
// 优雅退出：StopVerdictWriter 置 stopped（后续投递直接计丢弃）→ 通知
// writer drain 存量 → 最多等 OPS_NOISE_SINK_DRAIN；超时残量计丢弃并返回
// （writer 可能仍在后台挣扎落库，进程关池后其失败只计入 write_dropped，
// 属保守双记账的已知边角）。调用方保证该动作发生在 Redis 客户端 /
// pgxpool 关闭之前（main.go 停机序列、Assembly.Close）。
package main

import (
	"log/slog"
	"sync"
	"time"

	"opscopilot/internal/noise"
)

// verdictWriter 判决异步落库队列（引擎字段 n.vq；nil = 未启动，
// ProcessAlerts 走同步落库路径——测试/嵌入式兼容：不调 StartVerdictWriter
// 行为与异步化之前完全一致）。
type verdictWriter struct {
	ch   chan noise.VerdictRecord // 带缓冲判决通道（深度即缓冲占用）
	size int

	batch int           // 攒批阈值：攒满即刷
	flush time.Duration // 定时刷批：不满一批的最长滞留

	done     chan struct{} // stop() 关闭：通知 writer 进入 drain
	finished chan struct{} // writer 退出时关闭：stop() 的等待点
	stopOnce sync.Once

	// mu 同时保护 stopped 标志、sink 引用与 store 标签。不变式：push 在持
	// RLock 期间才向 ch 投递，stop 先拿写锁置 stopped 再关 done——done 关闭
	// 时所有在途 push 已完成，channel 不会再有"迟到的发送者"，writer 的
	// drain 判定"channel 已空即退出"因此是安全的。丢弃取向（非阻塞投递）
	// 让这条 RLock 路径不再有"持锁等待 DB"的情形，stop 无需等任何超时。
	mu      sync.RWMutex
	stopped bool
	sink    noise.VerdictSink
	store   string // 落库目标标签（drop 指标 {store=...}），随 setSink 更新

	// observeGroup 每刷一批回调一次（批大小）——仅测试注入（验证攒批分组），
	// 生产恒 nil。与 sink 同锁保护。
	observeGroup func(n int)

	// 以下参数在构造后只读。
	e *NoiseEngine
}

// newVerdictWriter 构造（不启动）。参数来自 config.NoiseSection（#2 通道）。
func newVerdictWriter(e *NoiseEngine, size, batch int, flush time.Duration) *verdictWriter {
	if size < 1 {
		size = 1
	}
	if batch < 1 {
		batch = 1
	}
	if batch > size {
		batch = size // 批阈值不超过缓冲容量（否则"攒满一批"永不成立，只靠 tick）
	}
	return &verdictWriter{
		ch:       make(chan noise.VerdictRecord, size),
		size:     size,
		batch:    batch,
		flush:    flush,
		done:     make(chan struct{}),
		finished: make(chan struct{}),
		e:        e,
	}
}

// push 投递一条判决：**永不阻塞**（这是与旧"宁慢不丢"取向的决裂点）。
// 返回 false = 队列已满或 writer 停机中，该条持久化任务被放弃（调用方
// 计 sink_drops + ERROR 日志；判决的内存态与 Gate 链路不受影响）。
func (q *verdictWriter) push(rec noise.VerdictRecord) bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if q.stopped {
		return false
	}
	select {
	case q.ch <- rec:
		return true
	default:
		return false // 队满：丢弃，不占采集节拍
	}
}

// setSink 更新判决落库出口（SetVerdictSink 装配/卸载）并同步 store 标签。
// 非 nil sink 一律按类型标注（pg / redis / other）；卸载（nil）保留最后
// 已知标签——detach 批次的丢弃记在它将去向的标签下。
func (q *verdictWriter) setSink(vs noise.VerdictSink) {
	q.mu.Lock()
	q.sink = vs
	if name := verdictSinkStore(vs); name != "" {
		q.store = name
	}
	q.mu.Unlock()
}

func (q *verdictWriter) getSink() noise.VerdictSink {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.sink
}

// storeLabel 当前 {store=...} 取值；从未挂过已知类型出口时为 "other"。
func (q *verdictWriter) storeLabel() string {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if q.store != "" {
		return q.store
	}
	return "other"
}

// verdictSinkStore 判决出口 → 落库目标标签（装配层类型对号入座；
// 测试假出口等未知类型归 "other"，nil 归空串=不改标签）。
func verdictSinkStore(vs noise.VerdictSink) string {
	switch vs.(type) {
	case nil:
		return ""
	case *PGClusterSink:
		return "pg"
	case *RedisClusterSink:
		return "redis"
	default:
		return "other"
	}
}

// setObserver 注入"每刷一批"观测回调（仅测试用；生产不触发）。
func (q *verdictWriter) setObserver(cb func(int)) {
	q.mu.Lock()
	q.observeGroup = cb
	q.mu.Unlock()
}

// flushBatch 刷一批：先报观测点，再落库。
func (q *verdictWriter) flushBatch(batch []noise.VerdictRecord) {
	q.mu.RLock()
	cb := q.observeGroup
	q.mu.RUnlock()
	if cb != nil {
		cb(len(batch))
	}
	q.e.flushVerdictBatch(batch)
}

// depth 队列当前缓冲判决条数（gauge 回调；len(chan) 对观测用途无需加锁）。
func (q *verdictWriter) depth() int { return len(q.ch) }

// stop 停机 drain，返回**超时残量**条数（0 = 排空成功）。新投递自 stopped
// 置起即全部走丢弃计数；writer 把 channel 存量落完才退出。drain <= 0 时
// 不设限由调用方兜底（构造保证正数，这里再防一手）。可重复调用，幂等。
func (q *verdictWriter) stop(drain time.Duration) int {
	q.mu.Lock()
	q.stopped = true
	q.mu.Unlock()
	q.stopOnce.Do(func() { close(q.done) })
	if drain <= 0 {
		drain = 30 * time.Second
	}
	t := time.NewTimer(drain)
	defer t.Stop()
	select {
	case <-q.finished:
		return 0
	case <-t.C: // 慢 DB 拖住 drain：残量按丢弃计（停机预算优先于库存完整）
		return len(q.ch)
	}
}

// run writer 主循环（单 goroutine，串行落库）：攒批 + 定时 flush + 停机 drain。
func (q *verdictWriter) run() {
	defer close(q.finished)
	ticker := time.NewTicker(q.flush)
	defer ticker.Stop()
	batch := make([]noise.VerdictRecord, 0, q.batch)
	for {
		select {
		case rec := <-q.ch:
			batch = append(batch, rec)
			// 机会性攒批：把已经在队列里的判决顺手取满一批，减少刷批次数
			// （非阻塞，不等待后到者——后到的由下一个循环/tick 处理）。
		gather:
			for len(batch) < q.batch {
				select {
				case rec2 := <-q.ch:
					batch = append(batch, rec2)
				default:
					break gather
				}
			}
			if len(batch) >= q.batch {
				q.flushBatch(batch)
				batch = batch[:0]
			}
		case <-ticker.C:
			if len(batch) > 0 {
				q.flushBatch(batch)
				batch = batch[:0]
			}
		case <-q.done:
			// drain：先清空 channel（push 的 RLock 不变式保证此刻之后不再有
			// 新发送者），再刷最后残批，然后退出。stop() 在等这个退出
			// （带上限超时，超时后残量由调用方计丢弃）。
			for {
				select {
				case rec := <-q.ch:
					batch = append(batch, rec)
					if len(batch) >= q.batch {
						q.flushBatch(batch)
						batch = batch[:0]
					}
				default:
					if len(batch) > 0 {
						q.flushBatch(batch)
						batch = batch[:0]
					}
					return
				}
			}
		}
	}
}

// StartVerdictWriter 启动判决异步落库队列（装配层在 SetMetrics 之后调用一次）。
// 幂等；spec.SinkQueue<=0（手工构造的零值配置，非 config.Load 路径）
// 视为不启用——引擎保持同步落库行为。
//
// 指标登记（pkg/metrics 注册范式，#8 修订命名）：
//   - opscopilot_noise_sink_queue       gauge：队列缓冲条数（len(chan)，水位观察点）
//   - opscopilot_noise_sink_drops_total{store=...} counter：队列侧未尝试落库而被
//     丢弃的判决数（队满 / 停机后投递 / drain 超时残量 / 出口卸载）——
//     维度集合在 NewAppMetrics 编译期固定（pg / redis / other）
//   - opscopilot_noise_write_dropped_total gauge：到达 DB 但写失败被丢弃的判决
//     数——镜像 n.verdictFailures 单一记账（"两处记账必然漂移"教训）
func (n *NoiseEngine) StartVerdictWriter() {
	if n == nil {
		return
	}
	n.mu.Lock()
	if n.vq != nil || n.vqSpec.SinkQueue <= 0 {
		n.mu.Unlock()
		return
	}
	spec := n.vqSpec
	q := newVerdictWriter(n, spec.SinkQueue, spec.SinkBatch, spec.SinkFlush)
	q.setSink(n.verdicts) // 队列晚于 sink 挂载时补齐（装配层：先 Start 后 SetVerdictSink，这里为 nil）
	n.vq = q
	m := n.m
	n.mu.Unlock()

	go q.run()

	if m != nil {
		if reg := m.Registry(); reg != nil {
			reg.GaugeFunc("opscopilot_noise_sink_queue",
				"Verdicts buffered in the async sink queue (drops counted once the ceiling is hit)", nil,
				func() float64 { return float64(q.depth()) })
			reg.GaugeFunc("opscopilot_noise_write_dropped_total",
				"Verdicts lost after failed persistence attempts (mirrors engine verdictFailures; queue-side drops are counted separately in opscopilot_noise_sink_drops_total)", nil,
				func() float64 { return float64(n.verdictFailures.Load()) })
		}
	}
}

// StopVerdictWriter 停机 drain（上限 vqSpec.SinkDrain，默认 5s）：新投递
// 即刻起计丢弃，writer 至多再落 SinkDrain 预算内的存量；超时残量计入
// sink_drops + ERROR 日志后返回（宁漏库存不丢停机预算）。**残量是丢失面
// 的保守上界**：预算耗尽不等于落库死刑——writer 可能仍在后台挣扎，sink
// 随后恢复它就把残量照常写完（判决不重复投递，最多计多不减，宁可对账
// 偏悲观不可静默乐观）。必须在 Redis 客户端 / pgxpool 关闭之前调用
// （main.go 停机序列、Assembly.Close 双保险，幂等）。
func (n *NoiseEngine) StopVerdictWriter() {
	if n == nil {
		return
	}
	n.mu.Lock()
	q := n.vq
	drain := n.vqSpec.SinkDrain
	n.mu.Unlock()
	if q == nil {
		return
	}
	if left := q.stop(drain); left > 0 {
		n.m.CountSinkDrop(q.storeLabel(), uint64(left))
		slog.Error("noise verdict sink drain timeout, undrained verdicts dropped",
			"store", q.storeLabel(), "undrained", left, "drain", drain.String())
	}
}

// enqueueVerdicts 把一批判决投递到异步队列（#8 修订：投递永不阻塞）。
// 被丢弃（队满 / 停机中）的条数计 opscopilot_noise_sink_drops_total
// {store=...} 并打一条 slog ERROR（一批最多一条，不逐条刷屏）——判决的
// 内存态与 Gate 放行链路照常推进。
func (n *NoiseEngine) enqueueVerdicts(q *verdictWriter, verdicts []noise.VerdictRecord) {
	dropped := 0
	for _, rec := range verdicts {
		if !q.push(rec) {
			dropped++
		}
	}
	if dropped > 0 {
		store := q.storeLabel()
		n.m.CountSinkDrop(store, uint64(dropped))
		slog.Error("noise verdict sink queue full, verdict persistence dropped (notification path unaffected)",
			"store", store, "dropped", dropped, "batch", len(verdicts), "queue_size", q.size)
	}
}

// flushVerdictBatch writer 刷批（锁外串行落库）。失败按既有降级语义处理：
// 计数 + WARN、不重试不中断——失败即该判决行丢失（评估数据缺行由
// verdictFailures / opscopilot_noise_write_dropped_total 观察），与同步路径
// 完全同一口径。出口被显式卸载（SetVerdictSink(nil)）时整批按"未尝试"
// 计入 sink_drops（队列侧丢弃，与写失败分账）。
func (n *NoiseEngine) flushVerdictBatch(recs []noise.VerdictRecord) {
	if len(recs) == 0 || n.vq == nil {
		return
	}
	vs := n.vq.getSink()
	if vs == nil {
		// 出口被显式卸载：无处可写，按队列侧丢弃计。
		store := n.vq.storeLabel()
		n.m.CountSinkDrop(store, uint64(len(recs)))
		slog.Error("noise verdict sink detached, queued verdicts dropped",
			"store", store, "dropped", len(recs))
		return
	}
	failed := 0
	for _, rec := range recs {
		if err := vs.SaveVerdict(rec); err != nil {
			failed++
			n.verdictFailures.Add(1)
		}
	}
	if failed > 0 {
		n.logf("WARNING: verdict persist failed for %d/%d (cumulative %d)",
			failed, len(recs), n.verdictFailures.Load())
	}
}
