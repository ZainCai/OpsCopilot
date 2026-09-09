package connector

import (
	"context"
	"log"
	"sync"
	"time"
)

// Logger 最小日志接口，便于在测试中替换。
type Logger interface {
	Printf(format string, args ...interface{})
}

// Sink 采集结果的下游消费者（W3 拓扑构建 / 降噪落库的前置点）。
// Host 不直接写库，只把归一化结果投递给 Sink，实现采集与消费解耦。
type Sink interface {
	// IngestCollect 接收一次 Collect 结果。
	IngestCollect(ctx context.Context, r *CollectResult) error
	// IngestDiscover 接收一次 Discover 结果。
	IngestDiscover(ctx context.Context, r *DiscoverResult) error
}

// scheduler 采集调度状态：每个连接器的退避窗口与当前退避时长。
//
// 为什么用独立互斥锁而不是复用 Host 保护 connectors 的那把锁：
// RunOnce 遍历过程中会调用连接器的 HealthCheck/Collect/Discover——这些都是
// 外部慢 IO。若与注册共用一把锁，一次慢采集会把 Register/Get 一起堵住。
// 调度状态与连接器注册表生命周期不同，理应分离加锁。
//
// 并发安全（P0 修复）：此前 RunOnce/backoffFor 直接读写裸 map，多个 goroutine
// 并发驱动采集会触发 Go 运行时的 "fatal error: concurrent map writes"——
// 这是**不可 recover 的致命错误**，会直接杀死进程。
// 现在所有读写统一经本类型方法加锁。
type scheduler struct {
	mu      sync.Mutex
	retryAt map[string]time.Time
	backoff map[string]time.Duration
}

func newScheduler() *scheduler {
	return &scheduler{
		retryAt: make(map[string]time.Time),
		backoff: make(map[string]time.Duration),
	}
}

// shouldSkip 该连接器是否仍处于退避窗口内（now 之前不重试）。
func (s *scheduler) shouldSkip(id string, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.retryAt[id]
	return ok && now.Before(t)
}

// fail 记录一次失败并指数退避（1s 起，翻倍，max > 0 时封顶）。
func (s *scheduler) fail(id string, max time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur := s.backoff[id]
	if cur == 0 {
		cur = time.Second
	} else {
		cur *= 2
	}
	if max > 0 && cur > max {
		cur = max
	}
	s.backoff[id] = cur
	s.retryAt[id] = time.Now().Add(cur)
}

// succeed 成功后清除该连接器的退避状态。
func (s *scheduler) succeed(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.retryAt, id)
	delete(s.backoff, id)
}

// Host 连接器宿主骨架（进程内实现）。
//
// 职责：
//   - 注册/查找连接器（按 ID 去重）；
//   - 周期性健康巡检与采集调度（失败退避、不丢任务）；
//   - 把归一化结果投递给 Sink。
//
// 并发契约：Host 可被多个 goroutine 安全使用。注册表由 h.mu 保护，
// 调度状态由 h.sched 内部锁保护，两者互不阻塞。
//
// 扩展点：W2 仅做进程内骨架；后续可让 Connector 经 go-plugin 远程加载，
// 只要远程包装同样实现本包 Connector 接口，Host 代码无需改动。
type Host struct {
	mu         sync.RWMutex
	connectors map[string]Connector

	logger     Logger
	interval   time.Duration
	maxBackoff time.Duration
	// connTimeout 单连接器单轮操作的超时上限（0 = 不设限，沿用连接器
	// 自身的 client timeout）。见 WithConnTimeout（全局审查 C3）。
	connTimeout time.Duration

	// sched 调度状态（退避），自带锁，见 scheduler 注释。
	sched *scheduler
}

// 调度默认值。零值 Host 未设置这些字段时，由 Host 自行兜底（见 NewHost / Run）。
const (
	// defaultInterval 默认采集轮询间隔。
	defaultInterval = 30 * time.Second
	// defaultMaxBackoff 默认单连接器最大退避。
	defaultMaxBackoff = 5 * time.Minute
)

// Option 配置项。
type Option func(*Host)

// WithLogger 注入日志器。
func WithLogger(l Logger) Option { return func(h *Host) { h.logger = l } }

// WithInterval 设置采集轮询间隔（默认 30s）。
func WithInterval(d time.Duration) Option {
	return func(h *Host) {
		if d > 0 {
			h.interval = d
		}
	}
}

// WithMaxBackoff 设置单连接器最大退避（默认 5m）。
func WithMaxBackoff(d time.Duration) Option {
	return func(h *Host) {
		if d > 0 {
			h.maxBackoff = d
		}
	}
}

// WithConnTimeout 设置单连接器单轮操作的超时上限（0 = 不设限，默认）。
// 连接器自身的 HTTP client 超时是第一道防线；本超时是第二道——
// 防止实现不良的连接器（无内部超时）拖慢整轮巡检（全局审查 C3）。
func WithConnTimeout(d time.Duration) Option {
	return func(h *Host) {
		if d > 0 {
			h.connTimeout = d
		}
	}
}

// NewHost 构造宿主。
func NewHost(opts ...Option) *Host {
	h := &Host{
		connectors: make(map[string]Connector),
		logger:     log.New(log.Writer(), "[connector-host] ", log.LstdFlags),
		interval:   defaultInterval,
		maxBackoff: defaultMaxBackoff,
		sched:      newScheduler(),
	}
	for _, o := range opts {
		o(h)
	}
	return h
}

// Register 注册连接器。拒绝 nil 与重复 ID。
func (h *Host) Register(c Connector) error {
	if c == nil {
		return ErrNilConnector
	}
	id := c.ID()
	if id == "" {
		return ErrEmptyID
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.connectors == nil {
		// 零值 Host 保护：避免写 nil map 直接 panic
		h.connectors = make(map[string]Connector)
	}
	if _, ok := h.connectors[id]; ok {
		return ErrDuplicateID(id)
	}
	h.connectors[id] = c
	return nil
}

// Get 按 ID 取连接器。
func (h *Host) Get(id string) (Connector, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	c, ok := h.connectors[id]
	return c, ok
}

// List 返回全部已注册连接器（快照）。
func (h *Host) List() []Connector {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make([]Connector, 0, len(h.connectors))
	for _, c := range h.connectors {
		out = append(out, c)
	}
	return out
}

// CheckAll 对所有连接器做一次健康巡检。
func (h *Host) CheckAll(ctx context.Context) map[string]Health {
	h.mu.RLock()
	cs := make([]Connector, 0, len(h.connectors))
	for _, c := range h.connectors {
		cs = append(cs, c)
	}
	h.mu.RUnlock()

	out := make(map[string]Health, len(cs))
	for _, c := range cs {
		health, err := c.HealthCheck(ctx)
		if err != nil {
			out[c.ID()] = Health{Status: HealthDown, Detail: err.Error(), CheckedAt: time.Now()}
			continue
		}
		out[c.ID()] = health
	}
	return out
}

// RunOnce 执行一轮"健康→采集→发现→投递"，不入睡眠，供调度循环与测试复用。
// 对处于退避窗口内的连接器直接跳过（不丢任务，下一轮再试）。
//
// 返回值语义（重要）：
//   - 仅当调度器自身不可继续（ctx 已取消）时返回非 nil error；
//   - **单个连接器失败不中断本轮、也不体现在返回值上**——
//     只记录日志并进入退避，以此保证"断连不丢任务"。
//
// 并发安全：可被多个 goroutine 同时调用。
func (h *Host) RunOnce(ctx context.Context, sink Sink) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	now := time.Now()
	// 取一次本地引用；ensureSched 同时为零值 Host 提供保护（见其注释）。
	sched := h.ensureSched()
	h.mu.RLock()
	cs := make([]Connector, 0, len(h.connectors))
	for _, c := range h.connectors {
		cs = append(cs, c)
	}
	h.mu.RUnlock()

	for _, c := range cs {
		id := c.ID()
		if sched.shouldSkip(id, now) {
			continue // 退避中，下一轮再试
		}
		h.runConnector(ctx, c, sink, sched, now)
	}
	return nil
}

// runConnector 执行单个连接器的一轮"健康→采集→发现→投递"。
// connTimeout > 0 时为该连接器派生带超时的独立 ctx——一个挂死的连接器
// 只损失自己的时间片，不拖慢整轮（全局审查 C3）。
func (h *Host) runConnector(ctx context.Context, c Connector, sink Sink, sched *scheduler, now time.Time) {
	id := c.ID()
	if h.connTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, h.connTimeout)
		defer cancel()
	}

	health, herr := c.HealthCheck(ctx)
	if herr != nil || health.Status == HealthDown {
		sched.fail(id, h.maxBackoff)
		// G4：herr 与 health.Detail 都要进日志——azure 风格的实现
		// 返回 (Down, nil)，只打 herr 会得到 info量为零的 "<nil>"。
		h.logf("connector %s health check failed: status=%s err=%v detail=%q",
			id, health.Status, herr, health.Detail)
		return
	}

	col, cerr := c.Collect(ctx, CollectRequest{})
	// 部分成功语义（全局审查 G3）：连接器契约允许"带结果 + 错误"
	// （如 prometheus 部分查询失败但告警全部成功）。Host 的取舍：
	// **先投递已成功的数据，再进退避**——告警的时效性不该为指标
	// 查询的失败陪葬；退避照常生效，避免带病连接器被打爆。
	if sink != nil && col != nil {
		if err := sink.IngestCollect(ctx, col); err != nil {
			sched.fail(id, h.maxBackoff)
			h.logf("connector %s sink ingest failed: %v", id, err)
			return
		}
	}
	if cerr != nil {
		sched.fail(id, h.maxBackoff)
		h.logf("connector %s collect failed (partial result delivered): %v", id, cerr)
		return
	}

	disc, derr := c.Discover(ctx)
	if derr != nil {
		sched.fail(id, h.maxBackoff)
		h.logf("connector %s discover failed: %v", id, derr)
		return
	}
	if sink != nil && disc != nil {
		if err := sink.IngestDiscover(ctx, disc); err != nil {
			sched.fail(id, h.maxBackoff)
			h.logf("connector %s sink discover failed: %v", id, err)
			return
		}
	}

	// 成功：清除退避
	sched.succeed(id)
}

// Run 启动调度循环，按 interval 周期性 RunOnce，直到 ctx 取消。
// 返回 ctx 的错误（正常取消即 context.Canceled）。
func (h *Host) Run(ctx context.Context, sink Sink) error {
	// 零值 Host 的 interval 为 0，而 time.NewTicker(0) 会 panic，故在此兜底。
	// 取局部值而非回写字段：避免多个 goroutine 并发 Run 时产生数据竞争。
	interval := h.interval
	if interval <= 0 {
		interval = defaultInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	// 立即跑一轮
	if err := h.RunOnce(ctx, sink); err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := h.RunOnce(ctx, sink); err != nil {
				return err
			}
		}
	}
}

// ensureSched 返回调度状态，必要时惰性初始化。
//
// 存在原因：零值 Host（不经 NewHost 直接 &Host{}）的 sched 为 nil，
// 直接使用会 panic。对 7×24 值守组件而言"误用即崩溃"不可接受，
// 故在此兜底，使 Host 的零值同样安全可用（与 Go 标准库零值可用惯例一致）。
//
// 注意：调用方不得在持有 h.mu 时调用本方法，否则死锁。
func (h *Host) ensureSched() *scheduler {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.sched == nil {
		h.sched = newScheduler()
	}
	return h.sched
}

func (h *Host) logf(format string, args ...interface{}) {
	if h.logger != nil {
		h.logger.Printf(format, args...)
	}
}
