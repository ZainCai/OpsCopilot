// noise_writequeue_test.go 判决异步落库队列（优化方案 #8，队满丢弃取向）
// 行为测试：慢 sink 不占采集 goroutine、攒批/定时刷、队满丢弃计数
// （投递永不阻塞）、停机有限 drain、sink 失败按既有降级（计数不中断）、
// 队列未启动保持同步。全部用假 sink，不依赖 DB/Redis
// （#4 测试纪律：直接构造参数，不碰全局 env）。
package main

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"opscopilot/internal/config"
	"opscopilot/internal/connector"
	"opscopilot/internal/noise"
)

// recVerdictSink 记录型判决假出口：fingerprint → 落库次数 + 到达顺序。
type recVerdictSink struct {
	mu       sync.Mutex
	got      map[string]int
	order    []string
	delay    time.Duration // 每条 SaveVerdict 的模拟耗时（测"不占采集"用）
	gate     chan struct{} // 非 nil：第一条之前阻塞等放行（测队满丢弃用）
	gateOnce sync.Once
}

func newRecSink() *recVerdictSink {
	return &recVerdictSink{got: map[string]int{}}
}

func (s *recVerdictSink) SaveVerdict(rec noise.VerdictRecord) error {
	if s.gate != nil {
		s.gateOnce.Do(func() { <-s.gate }) // 只有第一条经历阻塞（模拟 DB 卡住）
	}
	if s.delay > 0 {
		time.Sleep(s.delay)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.got[rec.Fingerprint]++
	s.order = append(s.order, rec.Fingerprint)
	return nil
}

func (s *recVerdictSink) snapshot() (map[string]int, []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make(map[string]int, len(s.got))
	for k, v := range s.got {
		cp[k] = v
	}
	return cp, append([]string(nil), s.order...)
}

func (s *recVerdictSink) total() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, v := range s.got {
		n += v
	}
	return n
}

// failVerdictSink 永远失败的出口（真相源故障注入）。
type failVerdictSink struct {
	mu    sync.Mutex
	calls int
}

func (f *failVerdictSink) SaveVerdict(noise.VerdictRecord) error {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	return errPersistFailed
}

func (f *failVerdictSink) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// queueEngine 构造"默认配置 + 覆写队列参数 + 挂假 sink + 启动 writer"的
// 引擎。mut 在 StartVerdictWriter 之前生效（参数在 Start 时快照进队列）。
func queueEngine(t *testing.T, sink noise.VerdictSink, mut func(*config.NoiseSection)) (*NoiseEngine, *AppMetrics) {
	t.Helper()
	ne := newTestNoiseEngine(t, noiseTestSink(t))
	am := NewAppMetrics()
	ne.SetMetrics(am)
	ne.SetVerdictSink(sink)
	if mut != nil {
		mut(&ne.vqSpec)
	}
	ne.StartVerdictWriter()
	t.Cleanup(ne.StopVerdictWriter) // 幂等；有限 drain 后 writer 退出
	return ne, am
}

func alertsN(prefix string, n int) []connector.Alert {
	now := time.Now()
	out := make([]connector.Alert, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, connector.Alert{
			Fingerprint: fmt.Sprintf("%s-%d", prefix, i),
			Labels:      map[string]string{"instance": "i1", "alertname": "T"},
			StartsAt:    now,
		})
	}
	return out
}

// TestVerdictQueueSlowSinkNotBlockingCollector 核心收益：慢 DB 不占死采集
// goroutine。30 条 × 10ms = 300ms 的 sink，ProcessAlerts 投递即返回；
// StopVerdictWriter drain（默认 5s 预算内）后全部落库且 FIFO
// （单 writer 串行保序）、逐条一次。
func TestVerdictQueueSlowSinkNotBlockingCollector(t *testing.T) {
	sink := newRecSink()
	sink.delay = 10 * time.Millisecond
	ne, _ := queueEngine(t, sink, nil)

	alerts := alertsN("fp-slow", 30)
	start := time.Now()
	ne.ProcessAlerts(alerts)
	if elapsed := time.Since(start); elapsed > 150*time.Millisecond {
		t.Fatalf("ProcessAlerts blocked %v on slow sink; async path expected immediate return", elapsed)
	}

	ne.StopVerdictWriter() // drain（预算内正常排空）
	got, order := sink.snapshot()
	if len(order) != 30 {
		t.Fatalf("persisted = %d, want 30 after drain", len(order))
	}
	for i := range order {
		if order[i] != alerts[i].Fingerprint {
			t.Fatalf("order[%d] = %s, want %s (single writer must preserve FIFO)", i, order[i], alerts[i].Fingerprint)
		}
	}
	for _, a := range alerts {
		if got[a.Fingerprint] != 1 {
			t.Fatalf("fingerprint %s persisted %d times, want exactly 1 (no dup)", a.Fingerprint, got[a.Fingerprint])
		}
	}
}

// TestVerdictQueueDrainNoLoss 大批量 + 立即停机：sink 快时 drain 预算内
// 存量必须全部落库，且逐条恰好一次。
func TestVerdictQueueDrainNoLoss(t *testing.T) {
	sink := newRecSink()
	ne, _ := queueEngine(t, sink, nil)
	ne.ProcessAlerts(alertsN("fp-drain", 200))
	ne.StopVerdictWriter()
	got, order := sink.snapshot()
	if len(order) != 200 || len(got) != 200 {
		t.Fatalf("persisted = %d (unique %d), want 200/200 (drain must flush every queued verdict)", len(order), len(got))
	}
	for fp, n := range got {
		if n != 1 {
			t.Fatalf("%s persisted %d times, want 1", fp, n)
		}
	}
}

// TestVerdictQueueBatchedByThreshold 攒批阈值可观察：batch=5 时每次刷批
// ≤5 条且至少出现一次"满 5 即刷"（不是逐条直写、也不是 12 条一把梭）；
// 全量到齐、顺序全 FIFO。
func TestVerdictQueueBatchedByThreshold(t *testing.T) {
	sink := newRecSink()
	sink.gate = make(chan struct{}) // 先卡住 writer，让队列攒批
	ne, _ := queueEngine(t, sink, func(s *config.NoiseSection) {
		s.SinkQueue = 100
		s.SinkBatch = 5
		s.SinkFlush = 24 * time.Hour // 排除定时刷干扰：只测阈值攒批
	})
	var gmu sync.Mutex
	var groups []int
	ne.vq.setObserver(func(n int) {
		gmu.Lock()
		groups = append(groups, n)
		gmu.Unlock()
	})
	go func() {
		time.Sleep(50 * time.Millisecond) // writer 阻塞在首批上；队列继续积
		close(sink.gate)
	}()
	ne.ProcessAlerts(alertsN("fp-batch", 12))
	ne.StopVerdictWriter()

	gmu.Lock()
	g := append([]int(nil), groups...)
	gmu.Unlock()
	sum, sawFull := 0, false
	for _, n := range g {
		if n > 5 {
			t.Fatalf("flush group %d exceeds batch threshold 5: %v", n, g)
		}
		if n == 5 {
			sawFull = true
		}
		sum += n
	}
	if sum != 12 || !sawFull {
		t.Fatalf("flush groups = %v (sum %d): want sum 12 with at least one full batch of 5", g, sum)
	}
	_, order := sink.snapshot()
	if len(order) != 12 {
		t.Fatalf("persisted = %d, want 12", len(order))
	}
	for i := range order {
		if want := fmt.Sprintf("fp-batch-%d", i); order[i] != want {
			t.Fatalf("order[%d] = %s, want %s", i, order[i], want)
		}
	}
}

// TestVerdictQueueTimedFlush 定时刷路径：1 条不满批（batch=50），
// 到 flush 周期即落库（不等攒批、不等停机）。
func TestVerdictQueueTimedFlush(t *testing.T) {
	sink := newRecSink()
	ne, _ := queueEngine(t, sink, func(s *config.NoiseSection) {
		s.SinkBatch = 50
		s.SinkFlush = 20 * time.Millisecond
	})
	ne.ProcessAlerts(alertsN("fp-tick", 1))
	deadline := time.Now().Add(2 * time.Second)
	for sink.total() == 0 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if sink.total() != 1 {
		t.Fatal("timed flush did not persist a sub-batch verdict")
	}
}

// TestVerdictQueueFullDrops 队满取向（#8 修订核心）：小队列 + 卡住的
// sink 首条 → ProcessAlerts **不阻塞**（不再有 backpressure 等待，更不回退
// 同步写），溢出条计 opscopilot_noise_sink_drops_total{store}。
// 定量边界：writer 在途批 ≤ batch(2) + channel 容量 2 ⇒ 入队 ≤ 4，
// 丢弃 ≥ 6；解除卡滞后入队部分全部落库，账目守恒 persisted + drops == 10。
func TestVerdictQueueFullDrops(t *testing.T) {
	sink := newRecSink()
	sink.gate = make(chan struct{})
	ne, am := queueEngine(t, sink, func(s *config.NoiseSection) {
		s.SinkQueue = 2
		s.SinkBatch = 2
		s.SinkFlush = 5 * time.Millisecond
	})

	start := time.Now()
	ne.ProcessAlerts(alertsN("fp-drop", 10)) // 投递永不阻塞：立即返回
	elapsed := time.Since(start)
	if elapsed > 150*time.Millisecond {
		t.Fatalf("ProcessAlerts blocked %v on full queue; drop policy expects immediate return", elapsed)
	}

	dropped := am.SinkDrops["other"].Value() // 假 sink → store="other"
	if dropped < 6 || dropped > 9 {
		t.Fatalf("sink_drops = %d, want in [6,9] (queue 2 + in-flight batch 2 ceiling)", dropped)
	}
	close(sink.gate) // 解除 sink 卡滞，writer 恢复消费
	ne.StopVerdictWriter()

	persisted := sink.total()
	if persisted+int(dropped) != 10 {
		t.Fatalf("persisted %d + drops %d != 10 — accounting must be conserved", persisted, dropped)
	}
	if persisted > 4 {
		t.Fatalf("persisted %d exceeds queue(2)+batch(2) ceiling — drop accounting raced?", persisted)
	}
}

// TestVerdictQueueDrainTimeoutBounded 停机预算：sink 卡死时
// StopVerdictWriter 必须在 SinkDrain 上限附近返回（不再无限等），未交给
// sink 的残量计 sink_drops；随后放行 gate，已进入 sink 的批次照常落完
// （cleanup 的二次 stop 幂等），守恒 persisted + drops == 投递总数。
// 首批大小 C∈{1,2} 取决于 writer 与投递的调度竞速，断言取区间。
func TestVerdictQueueDrainTimeoutBounded(t *testing.T) {
	sink := newRecSink()
	sink.gate = make(chan struct{})
	ne, am := queueEngine(t, sink, func(s *config.NoiseSection) {
		s.SinkQueue = 2
		s.SinkBatch = 2
		s.SinkFlush = 5 * time.Millisecond
		s.SinkDrain = 150 * time.Millisecond
	})
	// 首批 ≤2 条卡在 gate；channel 再容 ≤2；其余即时丢弃。
	ne.ProcessAlerts(alertsN("fp-tmo", 10))

	start := time.Now()
	ne.StopVerdictWriter() // drain 超时路径：150ms 预算 + 一点余量
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("StopVerdictWriter waited %v, drain bound 150ms ignored", elapsed)
	}
	drops := am.SinkDrops["other"].Value()
	if drops < 6 || drops > 9 {
		t.Fatalf(`sink_drops = %d, want in [6,9] (queue-full + drain-timeout residue, first batch 1..2 in flight)`, drops)
	}
	if p := sink.total(); p != 0 {
		t.Fatalf("persisted %d while sink gated, want 0", p)
	}
	close(sink.gate) // 放行：已交给 sink 的批次照常落库（预算之外的努力不算白做）
	ne.StopVerdictWriter()
	persisted := sink.total()
	// 账目只多不少：每条判决要么落库、要么至少计一次丢弃（drain 超时残量
	// 是保守上界——sink 恢复后可能既落库又保留丢弃计数，宁双计不漏计）。
	if persisted+int(drops) < 10 {
		t.Fatalf("persisted %d + drops %d < 10 — accounting must never under-count", persisted, drops)
	}
	if persisted > 4 {
		t.Fatalf("persisted %d exceeds queue(2)+batch(2) ceiling", persisted)
	}
}

// TestVerdictQueueSinkFailureCounted sink 故障（真相源写失败）：判决按既有
// 语义计数不重试不中断（对齐同步路径的降级口径），走 write_dropped 账
// 而非 sink_drops 账（"到达 DB 但失败"≠"队列侧未尝试"），告警链路照常返回。
func TestVerdictQueueSinkFailureCounted(t *testing.T) {
	sink := &failVerdictSink{}
	ne, am := queueEngine(t, sink, nil)
	ne.ProcessAlerts(alertsN("fp-fail", 5))
	ne.StopVerdictWriter()
	if sink.count() != 5 {
		t.Fatalf("SaveVerdict calls = %d, want 5", sink.count())
	}
	if got := ne.verdictFailures.Load(); got != 5 {
		t.Fatalf("verdictFailures = %d, want 5 (dropped on persistent sink failure)", got)
	}
	if got := am.SinkDrops["other"].Value(); got != 0 {
		t.Fatalf(`sink_drops{store="other"} = %d, want 0 (write failures are a separate ledger)`, got)
	}
	// opscopilot_noise_write_dropped_total 镜像 verdictFailures 单一记账；
	// 水位 gauge drain 后归零。
	var b bytes.Buffer
	if err := am.Registry().WritePrometheus(&b); err != nil {
		t.Fatalf("WritePrometheus: %v", err)
	}
	for _, want := range []string{
		"opscopilot_noise_write_dropped_total 5",
		"opscopilot_noise_sink_queue 0",
	} {
		if !strings.Contains(b.String(), want) {
			t.Fatalf("metrics text missing %q:\n%s", want, b.String())
		}
	}
}

// TestVerdictQueueMetricsRegistered 新指标按注册范式出现在 /metrics 文本：
// 水位 gauge、按 store 分桶的丢弃 counter（维度集合编译期固定）。
func TestVerdictQueueMetricsRegistered(t *testing.T) {
	sink := newRecSink()
	ne, am := queueEngine(t, sink, nil)
	ne.ProcessAlerts(alertsN("fp-exp", 3))
	ne.StopVerdictWriter()
	var b bytes.Buffer
	if err := am.Registry().WritePrometheus(&b); err != nil {
		t.Fatalf("WritePrometheus: %v", err)
	}
	for _, want := range []string{
		"# TYPE opscopilot_noise_sink_queue gauge",
		"# TYPE opscopilot_noise_sink_drops_total counter",
		`opscopilot_noise_sink_drops_total{store="pg"} 0`,
		`opscopilot_noise_sink_drops_total{store="redis"} 0`,
		`opscopilot_noise_sink_drops_total{store="other"} 0`,
		"# TYPE opscopilot_noise_write_dropped_total gauge",
	} {
		if !strings.Contains(b.String(), want) {
			t.Fatalf("metrics text missing %q:\n%s", want, b.String())
		}
	}
}

// TestVerdictQueueNotStartedStaysSync 兼容红线：不 StartVerdictWriter 的
// 引擎（测试/嵌入式）保持异步化之前的同步落库——ProcessAlerts 返回即落完。
func TestVerdictQueueNotStartedStaysSync(t *testing.T) {
	ne := newTestNoiseEngine(t, noiseTestSink(t))
	sink := newRecSink()
	ne.SetVerdictSink(sink)
	ne.ProcessAlerts(alertsN("fp-sync", 4))
	if n := sink.total(); n != 4 {
		t.Fatalf("sync path persisted %d, want 4 immediately after ProcessAlerts", n)
	}
}

// TestVerdictSinkDetachedCountsDrops 出口卸载（SetVerdictSink(nil)）：
// 已入队判决按"未尝试"计 sink_drops（store 标签沿用最后挂载目标），
// 不占 write_dropped 账。
func TestVerdictSinkDetachedCountsDrops(t *testing.T) {
	sink := newRecSink()
	ne, am := queueEngine(t, sink, func(s *config.NoiseSection) {
		s.SinkBatch = 1000 // 排除攒批抢跑：只靠停机/卸载触发路径计数
		s.SinkFlush = 24 * time.Hour
	})
	ne.ProcessAlerts(alertsN("fp-detach", 3))
	ne.SetVerdictSink(nil)
	ne.StopVerdictWriter() // drain 把这 3 条刷向已卸载的出口
	if got := am.SinkDrops["other"].Value(); got != 3 {
		t.Fatalf(`sink_drops{store="other"} = %d, want 3 (detached sink = queue-side drops)`, got)
	}
	if ne.verdictFailures.Load() != 0 {
		t.Fatal("detach drops must not touch verdictFailures ledger")
	}
}

// TestVerdictSinkStoreLabel 落库目标标签对号入座（{store=...} 维度）。
func TestVerdictSinkStoreLabel(t *testing.T) {
	if got := verdictSinkStore(&PGClusterSink{}); got != "pg" {
		t.Fatalf("pg sink label = %q, want \"pg\"", got)
	}
	if got := verdictSinkStore(&RedisClusterSink{}); got != "redis" {
		t.Fatalf("redis sink label = %q, want \"redis\"", got)
	}
	if got := verdictSinkStore(newRecSink()); got != "other" {
		t.Fatalf("unknown sink label = %q, want \"other\"", got)
	}
	var nilSink noise.VerdictSink
	if got := verdictSinkStore(nilSink); got != "" {
		t.Fatalf("nil sink label = %q, want \"\" (keeps last known store)", got)
	}
}
