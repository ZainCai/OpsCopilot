// noise_writequeue_test.go 判决异步落库队列（优化方案 #8）行为测试：
// 慢 sink 不占采集 goroutine、攒批/定时刷、队满 backpressure 判决不丢、
// 停机 drain 不丢、sink 失败按既有降级（计数不中断）。全部用假 sink，
// 不依赖 DB/Redis（#4 测试纪律：直接构造参数，不碰全局 env）。
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
	gate     chan struct{} // 非 nil：第一条之前阻塞等放行（测背压用）
	gateOnce sync.Once
}

func newRecSink() *recVerdictSink {
	return &recVerdictSink{got: map[string]int{}}
}

func (s *recVerdictSink) SaveVerdict(rec noise.VerdictRecord) error {
	if s.gate != nil {
		s.gateOnce.Do(func() { <-s.gate }) // 只有第一条经历阻塞（模拟 DB 卡住后恢复）
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
	t.Cleanup(ne.StopVerdictWriter) // 幂等；drain 后 writer 退出
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
// StopVerdictWriter drain 后全部落库且 FIFO（单 writer 串行保序）、逐条一次。
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

	ne.StopVerdictWriter() // drain
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
			t.Fatalf("fingerprint %s persisted %d times, want exactly 1 (no loss, no dup)", a.Fingerprint, got[a.Fingerprint])
		}
	}
}

// TestVerdictQueueDrainNoLoss 大批量 + 立即停机：channel 内存量必须全部
// 落库（drain 红线），且逐条恰好一次。
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
		s.WriteQueueSize = 100
		s.WriteQueueBatch = 5
		s.WriteQueueFlush = 24 * time.Hour // 排除定时刷干扰：只测阈值攒批
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
		s.WriteQueueBatch = 50
		s.WriteQueueFlush = 20 * time.Millisecond
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

// TestVerdictQueueBackpressureNoLoss 队满降级：小队列 + 卡住的 sink 首条，
// ProcessAlerts 走阻塞投递（宁慢不丢）/超时同步兜底；恢复后全部判决恰好
// 落库一次，full 计数非零。
func TestVerdictQueueBackpressureNoLoss(t *testing.T) {
	sink := newRecSink()
	sink.gate = make(chan struct{})
	ne, am := queueEngine(t, sink, func(s *config.NoiseSection) {
		s.WriteQueueSize = 2
		s.WriteQueueBatch = 2
		s.WriteQueueFlush = 5 * time.Millisecond
		s.WriteQueueEnqueueTimeout = 15 * time.Millisecond
	})

	done := make(chan struct{})
	go func() {
		ne.ProcessAlerts(alertsN("fp-bp", 10))
		close(done)
	}()

	// 队列容量 2 + writer 手里 1 条（阻塞在 gate 上）→ 后续条目触发队满。
	deadline := time.After(3 * time.Second)
	for am.WriteQueueFull.Value() == 0 {
		select {
		case <-deadline:
			t.Fatal("writequeue_full_total never incremented: backpressure path not exercised")
		case <-time.After(time.Millisecond):
		}
	}
	close(sink.gate) // 解除 sink 阻塞，writer 恢复消费
	<-done
	ne.StopVerdictWriter()

	got, order := sink.snapshot()
	if len(order) != 10 || len(got) != 10 {
		t.Fatalf("persisted = %d (unique %d), want 10/10 — queue must never drop verdicts", len(order), len(got))
	}
	for _, a := range alertsN("fp-bp", 10) {
		if got[a.Fingerprint] != 1 {
			t.Fatalf("%s count = %d, want 1", a.Fingerprint, got[a.Fingerprint])
		}
	}
}

// TestVerdictQueueSinkFailureCounted sink 故障（真相源写失败）：判决按既有
// 语义计数不重试不中断（对齐同步路径的降级口径），告警链路照常返回。
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
	// opscopilot_noise_write_dropped_total 镜像 verdictFailures 单一记账；
	// 深度 gauge drain 后归零。
	var b bytes.Buffer
	if err := am.Registry().WritePrometheus(&b); err != nil {
		t.Fatalf("WritePrometheus: %v", err)
	}
	for _, want := range []string{
		"opscopilot_noise_write_dropped_total 5",
		"opscopilot_noise_writequeue_depth 0",
	} {
		if !strings.Contains(b.String(), want) {
			t.Fatalf("metrics text missing %q:\n%s", want, b.String())
		}
	}
}

// TestVerdictQueueMetricsRegistered 三个新指标按注册范式出现在 /metrics 文本。
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
		"# TYPE opscopilot_noise_writequeue_depth gauge",
		"# TYPE opscopilot_noise_writequeue_full_total counter",
		"# TYPE opscopilot_noise_write_dropped_total gauge",
		"opscopilot_noise_writequeue_full_total 0",
	} {
		if !strings.Contains(b.String(), want) {
			t.Fatalf("metrics text missing %q:\n%s", want, b.String())
		}
	}
}

// TestVerdictQueueNotStartedStaysSync 兼容红线：不 StartVerdictWriter 的
// 引擎（测试/嵌入式）保持 #8 之前的同步落库——ProcessAlerts 返回即落完。
func TestVerdictQueueNotStartedStaysSync(t *testing.T) {
	ne := newTestNoiseEngine(t, noiseTestSink(t))
	sink := newRecSink()
	ne.SetVerdictSink(sink)
	ne.ProcessAlerts(alertsN("fp-sync", 4))
	if n := sink.total(); n != 4 {
		t.Fatalf("sync path persisted %d, want 4 immediately after ProcessAlerts", n)
	}
}
