// W4-1.4 影子降噪接线：connector.Alert → noise.Event 翻译 +
// topology.CausalSubgraph 故障域适配器 + TopologySink 挂载。
//
// 本文件是 connector / topology / noise 三个 internal 模块的唯一汇合点
// （v1.3 §5.2：internal 禁互 import，编排只落在 cmd/）。
//
// #2 配置收敛：本文件不再 os.Getenv——OPS_NOISE_* 的读取、默认值与非法值
// fail-fast 统一在 internal/config.Load；这里只消费解析好的 config.NoiseSection。
package main

import (
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"opscopilot/internal/config"
	"opscopilot/internal/connector"
	"opscopilot/internal/noise"
	"opscopilot/internal/notify"
	"opscopilot/pkg/memguard"
)

// 降噪模式常量（env 解析与白名单校验在 config；这里保留运行期比对取值）。
// shadow——只标注不拦截，告警全量放行（影子纪律）；
// enforce——WouldSuppress 判决交给 Gate 真拦截 + 放行通知（W9-1，ADR-011）。
// 判决落库两种模式都在跑——enforce 首周与 shadow 双跑对比的数据基础。
const (
	ModeShadow  = config.NoiseModeShadow
	ModeEnforce = config.NoiseModeEnforce
)

// NoiseEngine 影子降噪引擎的 cmd 侧持有者。
// 并发安全：内部互斥——域函数快照重建与 Process 串行化（采集宿主当前
// 串行调用 Sink，但本结构不依赖这一实现细节）。
type NoiseEngine struct {
	mu      sync.Mutex
	shadow  *noise.Shadow
	sink    *TopologySink // 取因果子图快照（锁内裁剪，见 CausalSnapshot）
	enabled bool
	logger  connector.Logger
	// total / converged 影子统计（启动以来累计）。
	total, converged int
	// tenant 租户标识（M1 单租户；落库记录与键空间前缀用）。
	// #10：由装配层构造参数显式注入，取代原包级变量 DefaultTenant
	// （init 读 env 的全局态：测试内 Setenv 改不动、跨测试互相污染）。
	tenant string
	// records 簇落库出口（W4-1.5，可选；nil = 只跑影子不落库）。
	records noise.RecordSink
	// verdicts 逐告警判决落库出口（W6-1 评估数据链，可选）。
	verdicts noise.VerdictSink
	// saveFailures / verdictFailures 落库失败累计（不中断告警链路）。
	// W9-5（第八轮审核建议 11）：改 atomic.Uint64。这两个计数在**锁外**自增
	// （persistClusters / persistVerdicts 都在 n.mu.Unlock() 之后运行），
	// 此前是裸 int++——采集宿主与任何潜在并发调用方同时进入就是数据竞争
	// （CI 的 -race 会抓）。落库路径本就该与引擎锁解耦，故选择原子计数
	// 而不是把它们挪回锁内。
	saveFailures, verdictFailures atomic.Uint64
	// persistedSig 已落库签名：clusterKey → state|lastSeenUnixNano|alertCount。
	// 第四轮扫描 F3：resolved 簇状态不再变化，全量重写会让 Redis 写放大
	// 随历史线性增长——按签名去重，只有变化过的簇才写。Restore 后清空
	// （重建自存储侧，签名必须重新建立）。
	persistedSig map[string]string
	// sigGuard persistedSig 的容量护栏（优化方案 #6）：簇从 Clusterer
	// 消失后其签名条目不会再被查到——超限即 GC 孤儿键（淘汰计数 + WARN）。
	sigGuard *memguard.Guard
	// enforce 转正模式（W9-1，ADR-011）：true = WouldSuppress 判决交
	// Gate 真拦截、放行项发通知；false = 影子（默认，不碰 Gate）。
	enforce bool
	// gate 通知闸门（enforce 模式的执行件；shadow 模式恒 nil）。
	// Admit 在锁外调用：渠道 Send 自带超时（Notifier 契约），不能拖住
	// 引擎锁——与落库 IO 同一锁边界纪律。
	gate *notify.Gate
	// gateStats Gate 计数真相源出口（nil = 只累计内存，重启清零）。
	gateStats GateStatsSink
	// gateFailures 计数持久化失败累计（尽力而为，不中断告警链路）。
	// W9-5：同 saveFailures —— admitDecisions 在锁外运行，原子计数。
	gateFailures atomic.Uint64
	// m 延迟/计数打点集（W9-4；nil = 不打点）。nil 安全：所有打点方法
	// 自带判空，指标缺失不得影响告警链路。
	m *AppMetrics
	// vq 判决异步落库队列（优化方案 #8；n.mu 保护引用）。nil = 未启动，
	// ProcessAlerts 走原同步落库路径——行为兼容：不调 StartVerdictWriter
	// 的测试/嵌入式用法与 #8 之前完全一致。
	vq *verdictWriter
	// vqSpec 队列参数快照（构造注入，StartVerdictWriter 消费；#2 通道：
	// 唯一装载点在 internal/config，这里只存解析好的值）。
	vqSpec config.NoiseSection
}

// NewNoiseEngine 按显式参数构造（#2：env 读取与非法值 fail-fast 已在
// config.Load 完成，构造函数不触环境；#4 测试直接构造参数、不依赖全局 env）。
// spec.Enabled=false 时返回 nil，表示影子降噪整体关闭（OPS_NOISE_SHADOW=off，
// 连影子判决都不产生，回到 W3 只计数行为）——调用方须容忍 nil
// （挂载点与 ProcessAlerts 均做 nil 检查）。
// spec.Window 默认 10m：30s 采集轮询下，10 分钟足以覆盖同一故障的重复告警，
// 又不至于把相隔较远的两次独立故障并成一簇。
// mem（#6 内存有界化）给去重指纹表/内存簇/落库签名缓存三个结构装容量
// 护栏；零值（上限 0）= 全部不设限，行为与设限前一致。
func NewNoiseEngine(sink *TopologySink, logger connector.Logger, tenant string, spec config.NoiseSection, mem config.MemLimitSection) *NoiseEngine {
	if !spec.Enabled {
		return nil
	}
	n := &NoiseEngine{
		sink:     sink,
		logger:   logger,
		tenant:   tenant,
		enforce:  spec.Mode == ModeEnforce,
		sigGuard: memguard.New("noise_sigcache", mem.NoiseSigCache, mem.WarnRatio),
		vqSpec:   spec,
	}
	n.shadow = noise.NewShadowWithLimits(spec.Window, nil, // 域函数按批经 SetDomain 注入
		memguard.New("noise_dedup", mem.NoiseDedup, mem.WarnRatio),
		memguard.New("noise_clusters", mem.NoiseClusters, mem.WarnRatio))
	n.enabled = true
	n.persistedSig = make(map[string]string)
	n.sigGuard.SetSize(n.sigCacheSize)
	return n
}

// MemGuards 本引擎持有的全部容量护栏（签名缓存 + 去重器 + 聚类器）。
func (n *NoiseEngine) MemGuards() []*memguard.Guard {
	if n == nil {
		return nil
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	out := append([]*memguard.Guard{n.sigGuard}, n.shadow.MemGuards()...)
	return out
}

// sigCacheSize 签名缓存当前条数（锁内读；gauge 回调）。
func (n *NoiseEngine) sigCacheSize() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.persistedSig)
}

// SetRecordSink 挂载簇落库出口（W4-1.5 双写管道）。传 nil 卸载。
// 必须在首条告警处理前挂好：落库语义是"批后全量幂等快照"，
// 挂载前的状态变更不会补写。
func (n *NoiseEngine) SetRecordSink(rs noise.RecordSink) {
	if n == nil {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.records = rs
}

// SetVerdictSink 挂载逐告警判决落库出口（W6-1）。传 nil 卸载。
// 异步队列已启动时同步更新 writer 持有的出口引用（#8）。
func (n *NoiseEngine) SetVerdictSink(vs noise.VerdictSink) {
	if n == nil {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.verdicts = vs
	if n.vq != nil {
		n.vq.setSink(vs)
	}
}

// GateStatsSink Gate 拦截/放行计数的真相源出口（R6-6：转正后拦截数是
// 核心运维指标，重启不得清零）。实现必须幂等——同租户重复 Save 结果
// 不变（覆盖写累计值）。nil = 只累计内存。
type GateStatsSink interface {
	SaveGateStats(tenant string, s notify.Stats) error
	LoadGateStats(tenant string) (notify.Stats, error)
}

// SetGate 挂载通知闸门（W9-1，enforce 模式执行件）。传 nil 卸载。
// 挂载时若配了真相源出口，先恢复历史累计（进程重启不清零）。
// 影子模式下闸门不会被调用，挂了也无副作用——但装配层应只在
// enforce 下挂载，让"配置意图"与"运行时行为"可从装配代码直接对读。
func (n *NoiseEngine) SetGate(g *notify.Gate, s GateStatsSink) {
	if n == nil {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.gate = g
	n.gateStats = s
	if g != nil && s != nil {
		if st, err := s.LoadGateStats(n.tenant); err != nil {
			n.logf("WARNING: gate stats restore skipped (starting from zero): %v", err)
		} else if st.Suppressed > 0 || st.Dispatched > 0 {
			g.SetStats(st)
			n.logf("gate stats restored: suppressed=%d dispatched=%d", st.Suppressed, st.Dispatched)
		}
	}
}

// batchView 一批采集的拓扑视图：因果邻接表 + instance→节点 Key 反查索引。
type batchView struct {
	adj      map[string][]string
	byInsEnv map[string]string // labels.instance → 节点 Key（首个命中，构建序决定）
}

// SetMetrics 挂载打点集（W9-4）。传 nil 卸载。只允许启动期调用一次
// （运行中热替换会让指标口径中途变化，无收益）。
func (n *NoiseEngine) SetMetrics(m *AppMetrics) {
	if n == nil {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.m = m
}

// pendingDecision 待放行判决 + 它的告警发射时刻（打点用）。
// fired 为零值表示上游没给 startsAt —— 不观测该条延迟（用合成时间打点
// 等于自欺，宁可少一个样本）。
type pendingDecision struct {
	d     notify.Decision
	fired time.Time
}

// ProcessAlerts 处理一轮采集的全部告警（影子模式：不拦截，只标注）。
// 每批开始时重建拓扑快照（CausalSubgraph），批内共用——一轮采集期间
// 拓扑视为不变。
//
// 锁边界（第四轮扫描 F3）：mu 保护处理段（快照/判决/签名比对），
// **不持有落库 IO**——慢存储不能拖住 Stats()。
//
// 落库路径（优化方案 #8 后）：
//   - 簇快照仍同步：快照是"最新覆盖"语义，异步会让批 N+1 先写、批 N
//     后写（旧数据覆盖新数据），必须保持调用方 goroutine 内串行；
//   - 判决（append-only）投递到带缓冲队列（vq），单 writer 攒批落库——
//     慢 DB 不再占死采集 goroutine。队列未启动（测试/嵌入式）或投递
//     退化（队满超时/停机 drain 中）时回落同步写，判决不丢是红线。
func (n *NoiseEngine) ProcessAlerts(alerts []connector.Alert) {
	if n == nil || len(alerts) == 0 {
		return
	}
	n.mu.Lock()

	view := n.snapshotView()
	n.shadow.SetDomain(func(a, b string) bool { return reachable(view.adj, a, b) })

	var suppressed, merged, created int
	var verdicts []noise.VerdictRecord
	// fired 与 verdicts 平行：告警发射时刻（零值 = 上游没给，不打点）。
	var fired []time.Time
	var pending []pendingDecision
	for _, a := range alerts {
		e := n.toEvent(a, view)
		v := n.shadow.Process(e)
		verdicts = append(verdicts, v.ToRecord(n.tenant))
		fired = append(fired, a.StartsAt) // 上游 startsAt 原值（toEvent 会兜底成 now，这里不兜）
		n.total++
		if n.m != nil {
			n.m.CountAlert()
			n.m.CountVerdict(v.Reason)
		}
		switch {
		case v.WouldSuppress:
			suppressed++
		case !v.ClusterCreated && v.ClusterKey != "":
			merged++
		}
		if v.WouldConverge {
			n.converged++
		}
		if v.ClusterCreated {
			created++
		}
		if n.enforce && n.gate != nil {
			// enforce 模式（ADR-011）：每条判决都过闸门——
			// W9-2 语义只有新事件放行（dedup/merge 都不重复通知）；
			// 判决照常落库：enforce 首周与影子基线对比的数据基础。
			pending = append(pending, pendingDecision{
				d: notify.Decision{
					TenantID:      n.tenant,
					ClusterKey:    v.ClusterKey,
					Severity:      v.Severity,
					Title:         v.Summary,
					WouldSuppress: v.WouldSuppress,
					Reason:        v.Reason,
				},
				fired: a.StartsAt,
			})
		}
	}
	n.logf("shadow verdict: %d alerts (new %d, dedup %d, merged %d) — cumulative converge %d/%d",
		len(alerts), created, suppressed, merged, n.converged, n.total)

	var dirty []noise.ClusterRecord
	if n.records != nil {
		dirty = n.collectDirtyClusters()
	}
	var vs noise.VerdictSink
	if n.verdicts != nil {
		vs = n.verdicts
	}
	q := n.vq // 异步判决队列（#8；nil = 未启动，走同步落库）
	gate, gs := n.gate, n.gateStats
	n.mu.Unlock()

	if len(dirty) > 0 {
		n.persistClusters(dirty)
	}
	if vs != nil {
		n.submitVerdicts(q, vs, verdicts, fired)
	}
	if len(pending) > 0 && gate != nil {
		n.admitDecisions(gate, gs, pending)
	}
}

// admitDecisions enforce 模式闸门执行（锁外，与落库 IO 同一锁边界纪律：
// 渠道 Send 自带超时，同步调用不 go 出去——异步会丢"放行失败"的
// 可观察性，且渠道间顺序没有契约值得保）。
// 拦截/放行计数持久化：每批批后保存累计值（覆盖写，幂等）；失败只
// 计数+日志，下一批覆盖写自然补齐（累计值语义下无脏写问题）。
func (n *NoiseEngine) admitDecisions(gate *notify.Gate, gs GateStatsSink, decisions []pendingDecision) {
	var admitted int
	for _, p := range decisions {
		ok, err := gate.Admit(p.d)
		if err != nil {
			// 空判决（无簇无标题）或全渠道失败：计数为闸门异常，
			// 不中断后续告警的闸门判定。
			n.gateFailures.Add(1)
			n.logf("WARNING: gate admit failed (cumulative %d): %v", n.gateFailures.Load(), err)
			continue
		}
		if ok {
			admitted++
			// W9-4 打点：Admit 返回 = 全渠道 Send 已完成 →
			// time.Since(fired) 即"发射 → 通知送达"的端到端延迟。
			// 批次内后闸门的判决含前面几条的发送耗时（同步顺序投递），
			// 这是真实发生的队列效应，不做修正。
			if !p.fired.IsZero() {
				n.m.ObserveNotifyLatency(time.Since(p.fired))
			} else {
				// 缺发射时刻：留痕（口径同上，见 submitVerdicts）。
				n.m.CountLatencySkipped("notify")
			}
		}
	}
	st := gate.Stats()
	if gs != nil {
		if err := gs.SaveGateStats(n.tenant, st); err != nil {
			n.gateFailures.Add(1)
			n.logf("WARNING: gate stats persist failed (cumulative %d): %v", n.gateFailures.Load(), err)
		}
	}
	n.logf("enforce gate: %d decisions (admitted %d, suppressed %d) — cumulative suppressed=%d dispatched=%d",
		len(decisions), admitted, len(decisions)-admitted, st.Suppressed, st.Dispatched)
}

// submitVerdicts 判决落库分发（W6-1，锁外）——优化方案 #8 的入口：
//  1. 先按**判决生成时刻**观测 fired→verdict 延迟（口径见 metrics.go 与
//     docs/W9-4 §9：异步化后"落库完成时刻"不再由采集 goroutine 掌握，
//     分位口径收敛为"判决生成"，落库滞后由队列深度/队满计数观察）；
//  2. 队列在跑 → 投递即返回；队满超时/停机 drain 中的判决同步兜底；
//     队列未启动 → 整批同步落库（#8 之前的原行为）。
//
// fired 与 verdicts 平行等长；某条 fired 为零值（上游没给 startsAt）
// 跳过观测但必须留痕（CountLatencySkipped），否则"processed 多、观测少"
// 会被误读成丢样本。
func (n *NoiseEngine) submitVerdicts(q *verdictWriter, vs noise.VerdictSink,
	verdicts []noise.VerdictRecord, fired []time.Time) {
	for i := range verdicts {
		if i < len(fired) && !fired[i].IsZero() {
			n.m.ObserveVerdictLatency(time.Since(fired[i]))
		} else {
			// 缺发射时刻（上游没给 startsAt）：观测不了但必须留痕——否则
			// "processed 多、观测少"会被误读成丢样本。
			n.m.CountLatencySkipped("verdict")
		}
	}
	if q != nil {
		if rest := n.enqueueVerdicts(q, verdicts); len(rest) > 0 {
			n.persistVerdicts(vs, rest) // 退化路径：同步兜底，判决不丢
		}
		return
	}
	n.persistVerdicts(vs, verdicts)
}

// persistVerdicts 同步落库路径（队列未启用，或投递退化时的兜底）。
// 失败只计数+日志——评估数据缺行会让准确率分母偏小，运维通过
// verdictFailures（/metrics 镜像为 opscopilot_noise_write_dropped_total）
// 观察丢失面。延迟打点不在此处（#8 后统一在 submitVerdicts 生成时刻）。
func (n *NoiseEngine) persistVerdicts(vs noise.VerdictSink, verdicts []noise.VerdictRecord) {
	failed := 0
	for _, rec := range verdicts {
		if err := vs.SaveVerdict(rec); err != nil {
			failed++
			n.verdictFailures.Add(1)
		}
	}
	if failed > 0 {
		n.logf("WARNING: verdict persist failed for %d/%d (cumulative %d)",
			failed, len(verdicts), n.verdictFailures.Load())
	}
}

// clusterSig 簇的落库签名：任何会影响存储行的字段变化都会改变签名。
func clusterSig(cl noise.Cluster) string {
	return string(cl.State) + "|" + strconv.FormatInt(cl.LastSeen.UnixNano(), 10) + "|" +
		strconv.Itoa(cl.AlertCount)
}

// collectDirtyClusters 锁内挑选签名变化的簇。
// 活跃簇 + 本批新 resolve 的簇（状态从 open/acked → resolved）都会入选；
// 早已 resolved 且未变的簇不再重复写（F3 写放大修复）。
// 收尾做签名缓存的容量 GC（#6）：簇已从 Clusterer 消失（内存淘汰或窗口
// 老化）后其签名永久无用——超上限先清这类孤儿键；活簇的签名不动
// （动了指纹缓存只是多写一次，但留孤儿才是只进不出的真泄漏）。
func (n *NoiseEngine) collectDirtyClusters() []noise.ClusterRecord {
	clusters := n.shadow.Clusterer().Clusters()
	dirty := make([]noise.ClusterRecord, 0, len(clusters))
	for _, cl := range clusters {
		sig := clusterSig(cl)
		if n.persistedSig[cl.Key] == sig {
			continue
		}
		n.persistedSig[cl.Key] = sig
		dirty = append(dirty, cl.ToRecord(n.tenant))
	}
	if excess := n.sigGuard.Over(len(n.persistedSig)); excess > 0 {
		live := make(map[string]struct{}, len(clusters))
		for _, cl := range clusters {
			live[cl.Key] = struct{}{}
		}
		dropped := 0
		for k := range n.persistedSig {
			if dropped >= excess {
				break
			}
			if _, ok := live[k]; !ok {
				delete(n.persistedSig, k)
				dropped++
			}
		}
		n.sigGuard.Evicted(dropped) // dropped=0（签名全对应活簇）时不计数——容量此时由簇上限护栏兜底
	}
	return dirty
}

// persistClusters 幂等写入脏簇。
// 第五轮审核 G1：签名在锁内 collectDirtyClusters 已更新，若 Save 失败
// 不回滚，下一批签名比对会说"无变化"——**丢失的写入永不补齐**（对
// resolved 簇是永久丢失），原注释"自动补齐"不成立。失败即回滚该 key
// 的签名，下一批重写。失败只计数+日志，不中断告警链路。
func (n *NoiseEngine) persistClusters(dirty []noise.ClusterRecord) {
	if n.records == nil {
		return
	}
	failed := 0
	for _, rec := range dirty {
		if err := n.records.SaveCluster(rec); err != nil {
			failed++
			n.saveFailures.Add(1)
			n.mu.Lock()
			delete(n.persistedSig, rec.ClusterKey)
			n.mu.Unlock()
		}
	}
	if failed > 0 {
		n.logf("WARNING: cluster persist failed for %d/%d clusters (rolled back signatures, cumulative failures %d)",
			failed, len(dirty), n.saveFailures.Load())
	}
}

// RestoreFrom 从落库记录重建内存簇状态（ADR-001：Redis 丢失后从真相源
// 拉平的唯一入口）。重建后继续 Process，命中同一批 cluster_key——
// 幂等链不断。失败返回错误且内存状态**不变**（半途而废的重建比不重建
// 更危险）。
func (n *NoiseEngine) RestoreFrom(records []noise.ClusterRecord) error {
	if n == nil {
		return nil
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.shadow.Clusterer().Restore(records); err != nil {
		return err
	}
	// 重建自存储侧：旧签名作废，下一批全量重建签名基线。
	n.persistedSig = make(map[string]string)
	return nil
}

// snapshotView 从当前拓扑图构建批次视图。
// ADR-007 low 门禁天然生效：CausalSubgraph 已剔除 low 节点/边，
// 它们进不了故障域判定。
//
// 走 Sink.CausalSnapshot（锁内裁剪 + 返回拷贝）：不能用
// graphSnapshot().CausalSubgraph()——那是在锁外遍历共享图，属无保护读。
func (n *NoiseEngine) snapshotView() batchView {
	g := n.sink.CausalSnapshot()
	view := batchView{
		adj:      make(map[string][]string, len(g.Nodes)),
		byInsEnv: make(map[string]string, len(g.Nodes)),
	}
	for _, e := range g.Edges {
		view.adj[e.SrcKey] = append(view.adj[e.SrcKey], e.DstKey)
		view.adj[e.DstKey] = append(view.adj[e.DstKey], e.SrcKey)
	}
	for k, node := range g.Nodes {
		if ins := node.Labels["instance"]; ins != "" {
			if _, ok := view.byInsEnv[ins]; !ok {
				view.byInsEnv[ins] = k
			}
		}
	}
	return view
}

// toEvent 翻译 connector.Alert → noise.Event。
// Fingerprint：上游为空时用 noise.Fingerprint(labels) 兜底（规范化哈希）。
// NodeKey：经 instance→Key 索引反查；反查不到置空——无法定位的告警
// 不参与域聚合，只能靠指纹命中簇。
// OccurredAt：上游 startsAt 缺失/格式坏时 parseTime 返回零值
// （第五轮审核 G2）——零值会掉进 1970 年窗口桶且被去重器判为窗口内
// 重复而吞掉，兜底为当前观察时刻。noise 保持纯语义（调用方给确定时间），
// 时间修补责任在装配层。
func (n *NoiseEngine) toEvent(a connector.Alert, view batchView) noise.Event {
	fp := a.Fingerprint
	if fp == "" {
		fp = noise.Fingerprint(a.Labels)
	}
	at := a.StartsAt
	if at.IsZero() {
		at = time.Now()
	}
	e := noise.Event{
		Fingerprint: fp,
		Severity:    a.Severity,
		OccurredAt:  at,
		NodeKey:     view.byInsEnv[a.Labels["instance"]],
	}
	if name := a.Labels["alertname"]; name != "" {
		e.Summary = name
	}
	return e
}

// reachable 邻接表 BFS 连通判定（同一故障域 = causal 图上可达）。
func reachable(adj map[string][]string, a, b string) bool {
	if a == b {
		return true
	}
	seen := map[string]bool{a: true}
	queue := []string{a}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, next := range adj[cur] {
			if next == b {
				return true
			}
			if !seen[next] {
				seen[next] = true
				queue = append(queue, next)
			}
		}
	}
	return false
}

// Stats 影子统计快照（运维自检）。
func (n *NoiseEngine) Stats() (total, converged int) {
	if n == nil {
		return 0, 0
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.total, n.converged
}

// Enabled 影子降噪是否启用。
func (n *NoiseEngine) Enabled() bool { return n != nil && n.enabled }

// Mode 降噪模式（shadow / enforce）。nil 引擎（整体关闭）返回空串。
func (n *NoiseEngine) Mode() string {
	if n == nil {
		return ""
	}
	if n.enforce {
		return ModeEnforce
	}
	return ModeShadow
}

func (n *NoiseEngine) logf(format string, args ...interface{}) {
	if n.logger != nil {
		n.logger.Printf(format, args...)
	}
}
