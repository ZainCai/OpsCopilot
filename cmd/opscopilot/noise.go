// W4-1.4 影子降噪接线：connector.Alert → noise.Event 翻译 +
// topology.CausalSubgraph 故障域适配器 + TopologySink 挂载。
//
// 本组文件（noise.go / noise_process.go / noise_view.go）是 connector /
// topology / noise 三个 internal 模块的唯一汇合点
// （v1.3 §5.2：internal 禁互 import，编排只落在 cmd/）。
//
// #2 配置收敛：本文件不再 os.Getenv——OPS_NOISE_* 的读取、默认值与非法值
// fail-fast 统一在 internal/config.Load；这里只消费解析好的 config.NoiseSection。
//
// #9 巨型文件拆分（波三收尾，已完成）：本文件原 ~600 行按职责同包切三块，
// 函数体/锁结构/逻辑逐字未动（同包切分，既有测试零修改）——
//
//	noise.go         本文件：NoiseEngine 结构、构造、挂载 setter（RecordSink/
//	                 VerdictSink/Gate/Metrics）、Stats/Enabled/Mode 统计面；
//	noise_process.go ProcessAlerts 处理段 + Gate 执行 + 判决/簇落库 +
//	                 RestoreFrom 重建；
//	noise_view.go    拓扑批次视图（snapshotView）与 Alert→Event 翻译
//	                 （toEvent/reachable/batchView）。
//
// 锁纪律（贯穿三个文件，review 前先读）：n.mu 的边界定义 = 本文件
// NoiseEngine 结构注释 + noise_process.go 中 ProcessAlerts 的"锁边界"注释。
// 处理段持锁、落库 IO 与 Gate 渠道 Send 一律锁外；各文件头各自重申同一
// 纪律并指回本权威声明。
package main

import (
	"sync"
	"sync/atomic"

	"opscopilot/internal/config"
	"opscopilot/internal/connector"
	"opscopilot/internal/incident"
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
	// ---- W10-6 簇→事件生产自动挂簇（OPS_AUTOATTACH，仅 enforce 生效）----
	// autoAttach 开关快照（构造注入）；incStore/auditLog 由装配层
	// SetAutoAttach 挂载（与 SetGate 同款"配置意图 ↔ 运行时行为"装配纪律：
	// 装配层只在 cfg.Noise.AutoAttach 时挂）。ProcessAlerts 的挂点条件恒为
	// n.enforce && n.autoAttach && incStore != nil——shadow 引擎即使误挂
	// 也零行为（防御在判决出口，不信任配置）。
	autoAttach bool
	incStore   incident.Store
	auditLog   AuditLog
	// attachFailures 自动挂簇"skipped"分支（建单/挂簇 IO 异常）累计——
	// 锁外自增，与 saveFailures 同款 atomic 纪律（W9-5/第八轮建议 11）。
	attachFailures atomic.Uint64
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
		sink:       sink,
		logger:     logger,
		tenant:     tenant,
		enforce:    spec.Mode == ModeEnforce,
		autoAttach: spec.AutoAttach,
		sigGuard:   memguard.New("noise_sigcache", mem.NoiseSigCache, mem.WarnRatio),
		vqSpec:     spec,
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

// SetAutoAttach 挂载 W10-6 自动挂簇的出口（事件 Store + 审计）。传 nil
// 卸载。与 SetGate 同款纪律：装配层只在 cfg.Noise.AutoAttach（on 已被
// Validate 强制 enforce）时挂载，"配置意图"与"运行时行为"从装配代码直接
// 对读。store 应传**装饰后**的事件 Store（PublishStore）——自动建单/挂簇
// 同样要推 SSE 给控制台。必须在首条告警处理前挂好（同 SetRecordSink）。
func (n *NoiseEngine) SetAutoAttach(store incident.Store, audit AuditLog) {
	if n == nil {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.incStore = store
	n.auditLog = audit
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
