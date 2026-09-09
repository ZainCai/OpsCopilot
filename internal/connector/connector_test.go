package connector

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// fakeConnector 测试用桩。
type fakeConnector struct {
	id          string
	typ         string
	health      Health
	healthErr   error
	collect     *CollectResult
	collectErr  error
	discover    *DiscoverResult
	discoverErr error
}

func (f *fakeConnector) ID() string   { return f.id }
func (f *fakeConnector) Type() string { return f.typ }
func (f *fakeConnector) HealthCheck(context.Context) (Health, error) {
	return f.health, f.healthErr
}
func (f *fakeConnector) Collect(context.Context, CollectRequest) (*CollectResult, error) {
	return f.collect, f.collectErr
}
func (f *fakeConnector) Discover(context.Context) (*DiscoverResult, error) {
	return f.discover, f.discoverErr
}

// discardLogger 静默日志，避免并发测试刷屏。
type discardLogger struct{}

func (discardLogger) Printf(string, ...interface{}) {}

// memSink 记录投递结果的 Sink 实现。并发安全（并发采集会同时投递）。
type memSink struct {
	mu        sync.Mutex
	collects  []CollectResult
	discovers []DiscoverResult
}

func (m *memSink) IngestCollect(_ context.Context, r *CollectResult) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.collects = append(m.collects, *r)
	return nil
}
func (m *memSink) IngestDiscover(_ context.Context, r *DiscoverResult) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.discovers = append(m.discovers, *r)
	return nil
}
func (m *memSink) len() (int, int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.collects), len(m.discovers)
}

func newHealthyFake(id string) *fakeConnector {
	return &fakeConnector{
		id:     id,
		typ:    "fake",
		health: Health{Status: HealthHealthy, Detail: "ok", CheckedAt: time.Now()},
		collect: &CollectResult{
			Alerts:      []Alert{{Fingerprint: id + "-a1", Source: id}},
			CollectedAt: time.Now(),
		},
		discover: &DiscoverResult{
			Nodes:        []ResourceNode{{Key: id + "-n1", Type: "host", Source: id}},
			DiscoveredAt: time.Now(),
		},
	}
}

func TestHost_Register(t *testing.T) {
	h := NewHost()
	if err := h.Register(nil); err != ErrNilConnector {
		t.Fatalf("nil should fail, got %v", err)
	}
	if err := h.Register(&fakeConnector{id: ""}); err != ErrEmptyID {
		t.Fatalf("empty id should fail, got %v", err)
	}
	c := newHealthyFake("c1")
	if err := h.Register(c); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := h.Register(c); err == nil {
		t.Fatal("duplicate id should fail")
	}
	if got, ok := h.Get("c1"); !ok || got.ID() != "c1" {
		t.Fatalf("Get mismatch: ok=%v", ok)
	}
	if len(h.List()) != 1 {
		t.Fatalf("List len = %d, want 1", len(h.List()))
	}
}

func TestHost_CheckAll(t *testing.T) {
	h := NewHost()
	good := newHealthyFake("good")
	bad := &fakeConnector{id: "bad", healthErr: errors.New("boom")}
	_ = h.Register(good)
	_ = h.Register(bad)

	health := h.CheckAll(context.Background())
	if health["good"].Status != HealthHealthy {
		t.Fatalf("good health = %v", health["good"].Status)
	}
	if health["bad"].Status != HealthDown {
		t.Fatalf("bad health = %v, want down", health["bad"].Status)
	}
}

func TestHost_RunOnce_Delivers(t *testing.T) {
	h := NewHost()
	_ = h.Register(newHealthyFake("c1"))
	sink := &memSink{}

	if err := h.RunOnce(context.Background(), sink); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	nc, nd := sink.len()
	if nc != 1 {
		t.Fatalf("collects = %d, want 1", nc)
	}
	if nd != 1 {
		t.Fatalf("discovers = %d, want 1", nd)
	}
}

func TestHost_RunOnce_BackoffOnError(t *testing.T) {
	h := NewHost()
	failing := &fakeConnector{
		id:         "fail",
		typ:        "fake",
		health:     Health{Status: HealthHealthy},
		collectErr: errors.New("source unreachable"),
	}
	_ = h.Register(failing)
	sink := &memSink{}

	// 第一次：采集失败 → 进入退避，不投递
	if err := h.RunOnce(context.Background(), sink); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if nc, _ := sink.len(); nc != 0 {
		t.Fatalf("expected no delivery during backoff, got %d", nc)
	}

	// 退避窗口内立即再跑，应被跳过（不重复尝试、不投递）
	if err := h.RunOnce(context.Background(), sink); err != nil {
		t.Fatalf("RunOnce#2: %v", err)
	}
	if nc, _ := sink.len(); nc != 0 {
		t.Fatalf("expected still no delivery, got %d", nc)
	}
}

// TestHost_RunOnce_ConcurrentSafe P0 回归测试：
// 多个 goroutine 并发驱动采集时，退避状态必须加锁，否则触发
// "fatal error: concurrent map writes"（不可 recover，直接杀进程）。
// 用极小 maxBackoff 让退避窗口立即过期，使每轮都写入调度 map，放大并发写窗口。
func TestHost_RunOnce_ConcurrentSafe(t *testing.T) {
	h := NewHost(WithMaxBackoff(time.Nanosecond), WithLogger(discardLogger{}))
	for i := 0; i < 8; i++ {
		c := &fakeConnector{
			id:         fmt.Sprintf("c%d", i),
			typ:        "fake",
			health:     Health{Status: HealthHealthy},
			collectErr: errors.New("boom"),
		}
		if err := h.Register(c); err != nil {
			t.Fatalf("register %s: %v", c.id, err)
		}
	}
	sink := &memSink{}

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				_ = h.RunOnce(context.Background(), sink)
			}
		}()
	}
	wg.Wait() // 若发生并发 map 写，进程会在此前 fatal 退出
}

// TestHost_ConcurrentRegisterAndCollect 注册表锁与调度锁分离：
// 采集（慢 IO 路径）不应阻塞 Register/Get/List/CheckAll。
func TestHost_ConcurrentRegisterAndCollect(t *testing.T) {
	h := NewHost(WithLogger(discardLogger{}))
	_ = h.Register(newHealthyFake("seed"))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			_ = h.RunOnce(context.Background(), &memSink{})
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			id := fmt.Sprintf("n%d", i)
			_ = h.Register(newHealthyFake(id))
			_, _ = h.Get(id)
			_ = h.List()
			_ = h.CheckAll(context.Background())
		}
	}()
	wg.Wait()
}

func TestHost_RunOnce_CanceledContext(t *testing.T) {
	h := NewHost(WithLogger(discardLogger{}))
	_ = h.Register(newHealthyFake("c1"))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// ctx 已取消：属调度器级错误，应返回非 nil
	if err := h.RunOnce(ctx, &memSink{}); err == nil {
		t.Fatal("RunOnce with canceled ctx should return error")
	}
}

func TestHost_Run_StopsOnCancel(t *testing.T) {
	h := NewHost(WithInterval(5*time.Millisecond), WithLogger(discardLogger{}))
	_ = h.Register(newHealthyFake("c1"))
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := h.Run(ctx, &memSink{})
	if err == nil {
		t.Fatal("Run should return ctx error after cancel")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
}

// TestZeroValueHost_Usable 零值 Host（不经 NewHost）也必须可用而非 panic：
// 7×24 值守组件不该"误用即崩溃"。覆盖 Register 写 nil map 与 RunOnce 用 nil 调度器的场景。
func TestZeroValueHost_Usable(t *testing.T) {
	var h Host
	if err := h.Register(newHealthyFake("c1")); err != nil {
		t.Fatalf("zero-value Host Register: %v", err)
	}
	if _, ok := h.Get("c1"); !ok {
		t.Fatal("zero-value Host Get failed")
	}
	sink := &memSink{}
	if err := h.RunOnce(context.Background(), sink); err != nil {
		t.Fatalf("zero-value Host RunOnce: %v", err)
	}
	if nc, nd := sink.len(); nc != 1 || nd != 1 {
		t.Fatalf("collects/discovers = %d/%d, want 1/1", nc, nd)
	}
}

// TestZeroValueHost_RunDoesNotPanic 零值 Host 的 interval 为 0，
// time.NewTicker(0) 会 panic，Run 必须兜底。
func TestZeroValueHost_RunDoesNotPanic(t *testing.T) {
	var h Host
	_ = h.Register(newHealthyFake("c1"))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := h.Run(ctx, &memSink{}); err == nil {
		t.Fatal("Run should return ctx error after timeout")
	}
}

// TestHost_RunOnce_PartialCollectStillDelivered G3 回归：连接器契约允许
// "带部分结果 + 错误"，Host 必须先投递已成功的数据再进退避——
// 告警时效性不该为指标查询失败陪葬。
func TestHost_RunOnce_PartialCollectStillDelivered(t *testing.T) {
	fake := newHealthyFake("partial")
	fake.collectErr = errors.New("one metric query failed")
	h := NewHost(WithLogger(discardLogger{}))
	if err := h.Register(fake); err != nil {
		t.Fatal(err)
	}
	sink := &memSink{}
	if err := h.RunOnce(context.Background(), sink); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	// 已成功的部分必须投递
	nCollects, _ := sink.len()
	if nCollects != 1 {
		t.Fatalf("collects delivered = %d, want 1 (partial result must be delivered)", nCollects)
	}
	if len(sink.collects[0].Alerts) != 1 {
		t.Errorf("alerts in delivered result = %d, want 1", len(sink.collects[0].Alerts))
	}
	// 退避照常生效：下一轮应跳过该连接器
	if !h.sched.shouldSkip("partial", time.Now()) {
		t.Error("connector must enter backoff after partial failure")
	}
}

// hangingHealthConnector 健康检查会挂起 hang 时长，用于验证连接器级超时。
type hangingHealthConnector struct {
	fakeConnector
	hang time.Duration
}

func (c *hangingHealthConnector) HealthCheck(ctx context.Context) (Health, error) {
	select {
	case <-time.After(c.hang):
		return c.health, c.healthErr
	case <-ctx.Done():
		return Health{}, ctx.Err()
	}
}

// TestHost_ConnTimeoutAbortsSlowConnector C3 回归：连接器级超时必须截断
// 挂死的连接器——一个慢连接器不能拖慢整轮巡检。
func TestHost_ConnTimeoutAbortsSlowConnector(t *testing.T) {
	fake := newHealthyFake("slow")
	hanging := &hangingHealthConnector{fakeConnector: *fake, hang: 2 * time.Second}
	h := NewHost(WithLogger(discardLogger{}), WithConnTimeout(150*time.Millisecond))
	if err := h.Register(hanging); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	err := h.RunOnce(context.Background(), &memSink{})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if elapsed > 800*time.Millisecond {
		t.Errorf("RunOnce took %v — connector timeout not enforced (hung 2s health check)", elapsed)
	}
	// 超时按失败处理 → 进入退避，下一轮跳过
	if !h.sched.shouldSkip("slow", time.Now()) {
		t.Error("timed-out connector must enter backoff")
	}
}
