// noise_writequeue.go 判决异步落库队列 + 批量 writer（优化方案 #8）。
//
// 动机：ProcessAlerts 原先在采集 goroutine 里同步逐条 SaveVerdict——PG 慢
// （连接池排队、语句超时 5s×N 条）会把整轮采集节拍占死。本文件把"判决
// 生成"与"判决落库"解耦：ProcessAlerts 生成判决后投递到带缓冲 channel
// 即返回；**单** writer goroutine 攒批 + 定时 flush，串行落现有
// noise.VerdictSink（装配层是 Redis 镜像 + PG 真相源双写出口，接口语义
// 不变——批量发生在 writer 侧，不改 sink 契约）。
//
// 顺序保证：单 writer 消费单 FIFO channel——批 N 的判决必然先于批 N+1
// 落库（alert_event 是追加语义，判决行之间无覆盖问题；簇快照仍走同步
// 路径，见 noise.go ProcessAlerts 锁边界注释）。
//
// 红线：判决不丢。三级兜底——
//  1. 非阻塞投递：队列有空位直接进（正常路径）；
//  2. 队满 → 阻塞投递带超时（backpressure，宁慢不丢）：writer 消费腾出
//     空位即入队，计 opscopilot_noise_writequeue_full_total；
//  3. 阻塞超时 / writer 已停机（drain 中）→ 该条退化为**同步落库**兜底
//     （记 WARN + 计数），绝不静默丢弃；同步写失败按既有语义计
//     verdictFailures（= opscopilot_noise_write_dropped_total）。
//
// 优雅退出：StopVerdictWriter 置 stopped（新批次自动走同步兜底）→ 通知
// writer → writer 把 channel 存量全部刷完才退出（drain）。调用方保证
// 该动作发生在 Redis 客户端 / pgxpool 关闭之前（main.go 停机序列、
// Assembly.Close）。
package main

import (
	"sync"
	"time"

	"opscopilot/internal/noise"
)

// verdictWriter 判决异步落库队列（引擎字段 n.vq；nil = 未启用，ProcessAlerts
// 走原同步路径——测试/嵌入式兼容：不调 StartVerdictWriter 行为与 #8 之前一致）。
type verdictWriter struct {
	ch   chan noise.VerdictRecord // 带缓冲判决通道（深度即缓冲占用）
	size int

	batch   int           // 攒批阈值：攒满即刷
	flush   time.Duration // 定时刷批：不满一批的最长滞留
	timeout time.Duration // 队满阻塞投递超时：超时退化同步写

	done     chan struct{} // stop() 关闭：通知 writer 进入 drain
	finished chan struct{} // writer 退出时关闭：stop() 的等待点
	stopOnce sync.Once

	// mu 同时保护 stopped 标志与 sink 引用。不变式：push 在持 RLock 期间
	// 才向 ch 投递，stop 先拿写锁置 stopped 再关 done——done 关闭时所有在途
	// push 已完成（或已注定走同步兜底），channel 不会再有"迟到的发送者"，
	// writer 的 drain 判定"channel 已空即退出"因此是安全的。
	// 代价：队满且 DB 卡死时 stop 最多排队等 timeout（宁慢不丢的自然结果）。
	mu      sync.RWMutex
	stopped bool
	sink    noise.VerdictSink

	// observeGroup 每刷一批回调一次（批大小）——仅测试注入（验证攒批分组），
	// 生产恒 nil。与 sink 同锁保护。
	observeGroup func(n int)

	// 以下参数在构造后只读。
	e *NoiseEngine
}

// pushResult 投递结果（ProcessAlerts 据此决定是否同步兜底）。
type pushResult int

const (
	pushOK          pushResult = iota // 直接入队（正常路径）
	pushFullThenOK                    // 队满 → 阻塞等待后入队成功（backpressure 生效）
	pushFullTimeout                   // 队满 → 阻塞超时，调用方同步兜底该条
	pushStopped                       // writer 停机中，调用方同步兜底本条及其后全部
)

func newVerdictWriter(e *NoiseEngine, size, batch int, flush, timeout time.Duration) *verdictWriter {
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
		timeout:  timeout,
		done:     make(chan struct{}),
		finished: make(chan struct{}),
		e:        e,
	}
}

// push 投递一条判决（不阻塞超过 q.timeout）。见 pushResult 各分支语义。
func (q *verdictWriter) push(rec noise.VerdictRecord) pushResult {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if q.stopped {
		return pushStopped
	}
	select {
	case q.ch <- rec:
		return pushOK
	default:
	}
	// 队满：阻塞投递（宁慢不丢）。writer 在消费，通常远快于超时；
	// 停机（done 关闭）与超时同样退化为调用方同步兜底。
	t := time.NewTimer(q.timeout)
	defer t.Stop()
	select {
	case q.ch <- rec:
		return pushFullThenOK
	case <-q.done:
		return pushStopped
	case <-t.C:
		return pushFullTimeout
	}
}

// setSink / getSink：判决落库出口的并发读写面（SetVerdictSink 装配/卸载，
// writer 刷批时取用）。nil sink 表示无出口——刷到的批按丢弃计数（唯一
// 允许丢判决的情形：出口是调用方显式拆掉的）。
func (q *verdictWriter) setSink(vs noise.VerdictSink) {
	q.mu.Lock()
	q.sink = vs
	q.mu.Unlock()
}

func (q *verdictWriter) getSink() noise.VerdictSink {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.sink
}

// setObserver 注入"每刷一批"观测回调（仅测试用；生产不触发）。
func (q *verdictWriter) setObserver(cb func(int)) {
	q.mu.Lock()
	q.observeGroup = cb
	q.mu.Unlock()
}

// flush 刷一批：先报观测点，再落库。
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

// stop 停机：置 stopped（后续 push 全部走同步兜底）→ 通知 writer drain →
// 等 writer 落完全部存量。可重复调用，幂等。
func (q *verdictWriter) stop() {
	q.mu.Lock()
	q.stopped = true
	q.mu.Unlock()
	q.stopOnce.Do(func() { close(q.done) })
	<-q.finished
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
			// 新发送者），再刷最后残批，然后退出。stop() 在等这个退出。
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
// 幂等；spec.WriteQueueSize<=0（手工构造的零值配置，非 config.Load 路径）
// 视为不启用——引擎保持同步落库行为。
//
// 指标登记（pkg/metrics 注册范式，命名对齐 #6 的 gate gauge 镜像口径）：
//   - opscopilot_noise_writequeue_depth        gauge：队列缓冲条数（len(chan)）
//   - opscopilot_noise_writequeue_full_total   counter：队满触发阻塞投递的次数
//     （cmd/opscopilot/metrics.go 注册；阻塞后成功与超时退化都计它）
//   - opscopilot_noise_write_dropped_total     gauge：最终落库失败被丢弃的判决
//     数——镜像 n.verdictFailures 单一记账（"两处记账必然漂移"教训，
//     与 gate suppressed/dispatched gauge 同款手法）
func (n *NoiseEngine) StartVerdictWriter() {
	if n == nil {
		return
	}
	n.mu.Lock()
	if n.vq != nil || n.vqSpec.WriteQueueSize <= 0 {
		n.mu.Unlock()
		return
	}
	spec := n.vqSpec
	q := newVerdictWriter(n, spec.WriteQueueSize, spec.WriteQueueBatch,
		spec.WriteQueueFlush, spec.WriteQueueEnqueueTimeout)
	q.setSink(n.verdicts) // 队列晚于 sink 挂载时补齐（装配层：先 Start 后 SetVerdictSink，这里为 nil）
	n.vq = q
	m := n.m
	n.mu.Unlock()

	go q.run()

	if m != nil {
		if reg := m.Registry(); reg != nil {
			reg.GaugeFunc("opscopilot_noise_writequeue_depth",
				"Verdicts buffered in the async write queue (drain target before shutdown)", nil,
				func() float64 { return float64(q.depth()) })
			reg.GaugeFunc("opscopilot_noise_write_dropped_total",
				"Verdicts lost after failed persistence attempts (mirrors engine verdictFailures; mirror droppable, truth-source failures surface here)", nil,
				func() float64 { return float64(n.verdictFailures.Load()) })
		}
	}
}

// StopVerdictWriter 停机 drain：新批次即刻起改走同步兜底，writer 把 channel
// 存量全部落库后才返回。必须在 Redis 客户端 / pgxpool 关闭之前调用
// （main.go 停机序列、Assembly.Close 双保险，幂等）。
func (n *NoiseEngine) StopVerdictWriter() {
	if n == nil {
		return
	}
	n.mu.Lock()
	q := n.vq
	n.mu.Unlock()
	if q != nil {
		q.stop()
	}
}

// enqueueVerdicts 把一批判决投递到异步队列，返回需要**同步兜底**的判决
// （队满超时 / writer 停机）。full>0 时记 opscopilot_noise_writequeue_full_total，
// 退化发生时打一条 WARN（一批最多一条，不逐条刷屏）。
func (n *NoiseEngine) enqueueVerdicts(q *verdictWriter, verdicts []noise.VerdictRecord) []noise.VerdictRecord {
	var fallback []noise.VerdictRecord
	var full, timedOut, stopped int
	for i, rec := range verdicts {
		switch q.push(rec) {
		case pushOK:
		case pushFullThenOK:
			full++
			n.m.CountWriteQueueFull()
		case pushFullTimeout:
			full++
			timedOut++
			n.m.CountWriteQueueFull()
			fallback = append(fallback, rec)
		case pushStopped:
			// 停机中：本条及其后全部同步兜底（不再逐条试投）。
			stopped = len(verdicts) - i
			fallback = append(fallback, verdicts[i:]...)
			break
		}
		if stopped > 0 {
			break
		}
	}
	if full > 0 {
		n.logf("WARNING: verdict write queue full: %d/%d enqueued under backpressure, %d timed out to sync fallback, %d diverted by shutdown (queue size %d; cumulative full events in opscopilot_noise_writequeue_full_total)",
			full, len(verdicts), timedOut, stopped, q.size)
	}
	return fallback
}

// flushVerdictBatch writer 刷批（锁外串行落库）。失败按既有降级语义处理：
// 计数 + WARN、不重试不中断——失败即该判决行丢失（评估数据缺行由
// verdictFailures / opscopilot_noise_write_dropped_total 观察），与同步路径
// 完全同一口径，"判决不丢"红线约束的是**队列不丢**，不承诺 DB 故障下不丢。
func (n *NoiseEngine) flushVerdictBatch(recs []noise.VerdictRecord) {
	if len(recs) == 0 || n.vq == nil {
		return
	}
	vs := n.vq.getSink()
	if vs == nil {
		// 出口被显式卸载（SetVerdictSink(nil)）：无处可写，按丢弃计。
		n.verdictFailures.Add(uint64(len(recs)))
		n.logf("WARNING: verdict sink detached, %d queued verdicts dropped (cumulative %d)",
			len(recs), n.verdictFailures.Load())
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
