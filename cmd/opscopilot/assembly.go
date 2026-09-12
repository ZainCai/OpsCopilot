// W3 启动装配（M1 W3 收官项）：把拓扑引擎与变更 webhook 装成一个可运行的服务。
//
// 装配清单：
//   - topology.Builder：拓扑图唯一归属（多轮发现共享同一张图）；
//   - TopologySink：连接器宿主 → 拓扑的发现数据入口（W2→W3 管道）；
//   - ChangeStore（严格节点校验）：变更事件库，校验钩子经 Sink.HasNode
//     锁内查图——"先发现、后变更"的关联语义在此闭环；后端可换
//     （topology.ChangeBackend）：有 DB 用 PG 真相源 + 启动回放（优化方案 #4），
//     无 DB 降级纯内存；
//   - ChangeWebhook：POST /api/v1/changes 手动提交变更事件。
//
// 装配放在 cmd/（package main）：跨模块引用的唯一合法汇合点（v1.3 §5.2）。
// #2 配置收敛：本文件不再 os.Getenv——全部配置经 NewAssembly 的 cfg 参数注入，
// env 解析与非法值 fail-fast 唯一发生在 internal/config.Load。
package main

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"

	"opscopilot/internal/config"
	"opscopilot/internal/connector"
	pb "opscopilot/internal/contracts/pb"
	"opscopilot/internal/incident"
	"opscopilot/internal/notify"
	"opscopilot/internal/topology"
	"opscopilot/pkg/memguard"
	"opscopilot/pkg/metrics"
)

// changeWebhookPath 变更事件提交路由（手动 curl / Git/Jenkins webhook 共用）。
const changeWebhookPath = "/api/v1/changes"

// Assembly W5 装配产物：各组件的持有者，供 main 做生命周期管理与测试断言。
type Assembly struct {
	Sink *TopologySink
	// Changes 变更事件库（优化方案 #4）：有 DB 时 PGChangeStore（真相源 +
	// 启动回放），否则纯内存 ChangeStore——消费方只认 topology.ChangeBackend。
	Changes topology.ChangeBackend
	Webhook *ChangeWebhook
	// Noise 影子降噪引擎（W4-1.4）；nil = Noise.Enabled=false 已关闭。
	Noise *NoiseEngine
	// GRPC 进程内 gRPC server（W5-2.1）：承载 SemanticModel 服务，
	// 供 transport.DialInProcess 消费。生命周期归 main（Stop 必调，
	// 否则 DialInProcess 的 Serve goroutine 泄漏，见其 R6 注释）。
	GRPC *grpc.Server
	// REST 只读查询网关（W5-2.2）。
	REST *RESTGateway
	// Incidents 事件域 Store（M2 主干 F-01/F-02；DB 后端 W9）。
	Incidents incident.Store
	// Ingest 链路 A 入队接收器（nil = 未配置 DB 队列，入队端点返回 503）。
	Ingest *AlertmanagerWebhook
	// Queue 导入队列（运维观察 Pending）。
	Queue *PGIngestQueue
	// Worker 队列消费者（Run 由 main 以 runCtx 驱动；autoCreate off 时立即返回）。
	Worker *IngestWorker
	// Poller 链路 A 拉取侧：定时从数据源拉告警入队（nil = 未启用）。
	Poller *AlertPoller
	// Escalation 值班升级（W9-3）：未 ack 超时重发一次（nil = 未启用）。
	Escalation *EscalationPoller
	// RCA 按需根因分析编排器（#12/ADR-014；nil = OPS_RCA=off，端点 503）。
	RCA *RCAOrchestrator
	// Events 实时广播器（W11：控制台事件页 SSE 订阅源）。
	Events *EventHub
	// NotifyReg 通知渠道注册表（W9-2）：渠道 CRUD 后由 reloadNotifyChannels
	// 整体重载；enforce 模式下 Gate 持有同一实例。
	NotifyReg *notify.Registry
	// Channels 渠道配置存储（nil = 无 DB，渠道配置不可管）。
	Channels *ChannelStore
	// Metrics 指标集与 /metrics 暴露（W9-4：告警链路延迟打点）。
	Metrics *AppMetrics
	// Leader 选主与 leader 状态源（#11/ADR-012，实现见 leader.go）。装配只
	// 构造不运行——Run/OnPromote 由 main 驱动（钩子要等 Redis/PG 出口就绪）。
	// 降级路径：无 DB 或 OPS_LEADER_ELECTION=off → 恒 leader。
	Leader *LeaderElector
	// audit 审计日志（人工操作与外部自动动作统一留痕）。
	audit AuditLog
	// logf 装配期日志函数（渠道重载等运行期回调需要，nil 安全）。
	logf func(string, ...any)
	// pool DB 连接池（事件 Store / 导入队列 / 审计共享；生命周期归装配）。
	pool *pgxpool.Pool
}

// reloadNotifyChannels 从 DB 重载启用渠道到注册表（先清后注册：
// 禁用/删除/改 URL 都不留残影）。装配期与渠道 CRUD 后调用。
// 返回**生效渠道总数**（含 console 兜底）——运维问的是"现在有几个
// 渠道在发"，不是"DB 里配了几个"。
func (a *Assembly) reloadNotifyChannels() (int, error) {
	if a.NotifyReg == nil {
		return 0, nil
	}
	a.NotifyReg.Clear()
	// 兜底 console 渠道必须重新挂上（Clear 会连它一起清掉）。
	a.NotifyReg.Register(&notify.ConsoleChannel{Logf: a.logf})
	if a.Channels == nil {
		return a.NotifyReg.Len(), nil
	}
	if _, err := loadChannelsIntoRegistry(context.Background(), a.Channels, a.NotifyReg,
		func(string, ...any) {}); err != nil { // 静默：调用方统一记日志
		return a.NotifyReg.Len(), err
	}
	return a.NotifyReg.Len(), nil
}

// Close 释放装配持有的资源：先有限排空判决异步落库队列（#8：StopVerdictWriter
// 至多等 OPS_NOISE_SINK_DRAIN，默认 5s；超时残量计 sink_drops 后返回，
// 宁漏库存不死等——但 writer 还在往 sink 写，排空动作必须早于共享池关闭；
// main 停机序列已提前调用一次，这里幂等兜底所有直接走 Assembly.Close 的
// 消费者），再停 leader 选举环（#11：释放 pinned 连接并显式解锁——
// pgxpool.Close 会等所有 Acquire 出去的连接归还，选举环不先退池就关不掉），
// 再停实时推送（关闭 SSE 连接），最后关 DB 池（共享池的唯一
// 所有者是装配，见 NewAssembly）。
func (a *Assembly) Close() {
	if a.Noise != nil {
		a.Noise.StopVerdictWriter()
	}
	if a.Leader != nil {
		a.Leader.Stop()
	}
	if a.Events != nil {
		a.Events.Close() // 让已连接的 SSE 订阅者立即结束，不悬挂
	}
	if a.pool != nil {
		a.pool.Close()
	}
}

// NewAssembly 组装 W3+W4+W5 组件并接线。配置唯一来源是 cfg（#2）：
//
//   - cfg.Security.WebhookToken：变更 webhook 的共享密钥；非空时
//     POST /api/v1/changes 必须携带匹配的 X-OpsCopilot-Token 头
//     （S1 写路径准入）。传空表示不鉴权——仅限回环/内网部署。
//   - cfg.Noise：影子降噪开关/窗口/模式（非法值已在 config.Load 拒绝启动，
//     装配层不再处理解析错误）。
//   - cfg.DB：共享连接池（DSN 空 = 内存降级；MaxConns 默认 16——事件 Store +
//     导入队列 + 审计三方共用，worker 批处理持一条事务连接再做 Store 写需要
//     第二条；pgx 默认 max(4,NumCPU) 在并发下易耗尽，D7 决策 A）。
//   - cfg.Tenant：全链路租户（#10：取代包级 DefaultTenant，显式注入）。
//
// 变更库的节点校验钩子经 Sink.HasNode（锁内读图）实现——变更事件只能
// 关联到拓扑图里真实存在的节点，防止"幽灵节点"静默失败。
func NewAssembly(logger connector.Logger, cfg *config.Config) (*Assembly, error) {
	// nil logger 容忍（装配测试惯用 nil）：本函数内日志一律走 logf，
	// 不再直接调 logger.Printf，避免 nil logger 触发空指针。
	logf := func(format string, args ...any) {
		if logger != nil {
			logger.Printf(format, args...)
		}
	}
	tenant := cfg.Tenant
	// 拓扑图容量护栏（#6）：默认 1e5 节点/4e5 边——演示与评估环境距触限
	// 差 2~3 个数量级，正常行为与不设限完全一致；触限按最久未活跃淘汰
	// （过度淘汰会伤降噪故障域计算与 RCA as_of 取证，所以上限保守）。
	builder := topology.NewBuilderWithLimits(
		memguard.New("builder", cfg.MemLimit.TopologyNodes, cfg.MemLimit.WarnRatio),
		memguard.New("builder_edges", cfg.MemLimit.TopologyEdges, cfg.MemLimit.WarnRatio))
	sink, err := NewTopologySink(builder, logger)
	if err != nil {
		return nil, err
	}
	// 变更库（内存 + 可选 PG 持久化）与 webhook 在共享池就绪后装配，
	// 见下方"变更库后端选型"（优化方案 #4）。

	// 影子降噪：挂在 sink 上（告警经 IngestCollect 转交），引擎持有
	// sink 引用做拓扑快照——互相引用只能后挂（见 AttachNoise 注释）。
	noiseEngine := NewNoiseEngine(sink, logger, tenant, cfg.Noise, cfg.MemLimit)
	sink.AttachNoise(noiseEngine)

	// W9-4 延迟打点集：挂在噪声引擎上——告警→判决→通知全链路都在它手里，
	// 是唯一能同时看到"发射时刻"（alert.startsAt）与"送达完成时刻"的位置。
	// 早于 asm 字面量创建（Assembly 要持有它；装配顺序敏感，见 W9-2 的坑）。
	appMetrics := NewAppMetrics()
	noiseEngine.SetMetrics(appMetrics) // noiseEngine 为 nil（降噪关闭）时方法自带判空

	// 优化方案 #8（队满丢弃取向）：判决异步落库队列 + 单 writer（慢 DB
	// 不占采集节拍；队列永不阻塞、队满丢持久化不丢通知）。必须在
	// SetMetrics 之后（水位/丢弃计数登记进同一注册表）。sink 在 main.go
	// 后挂（SetVerdictSink 会同步给 writer）；判决出口仅挂 DB 真相源
	// （Redis 镜像承载簇），双写语义由 sink 侧决定，此处不改。
	noiseEngine.StartVerdictWriter()

	// W5-2.2：REST 只读查询面（复用 SemanticModelServer 的校验与映射，
	// gRPC/REST 一套语义不漂移）。M2 主干：事件 Store 同源挂载。
	// W9：事件 Store 双实现；DB 可用时事件 Store / 导入队列 / 审计 / 变更库
	// **共用一条池**（R9：多池各占连接无收益）。pg 不可用不阻塞启动
	// （降级内存，Persistence 字段提醒消费者；见 R6-4）。
	//
	// #6 内存有界化：DB 缺席时的内存降级路径（事件 MemStore / 内存审计 /
	// 升级台账）全部装容量护栏；PG 生效时这些兜底结构不存在或不增长，
	// 护栏只在**真正启用降级路径时**注册进 /metrics（不制造恒零的误导指标）。
	memIncidents := incident.NewMemStoreWithLimits(
		memguard.New("incidents", cfg.MemLimit.Incidents, cfg.MemLimit.WarnRatio))
	memAudit := NewMemAuditLogWithLimits(
		memguard.New("audit", cfg.MemLimit.Audit, cfg.MemLimit.WarnRatio))
	var incStore incident.Store = memIncidents
	var audit AuditLog = memAudit
	var pgPool *pgxpool.Pool
	// W9-2：通知注册表与渠道存储（channel store 需 DB，注册表恒有）。
	var notifyReg *notify.Registry
	var chStore *ChannelStore
	if cfg.DB.DSN != "" {
		// D7 决策 A：显式配置连接池容量（cfg.DB.MaxConns，config 已校验正数）。
		poolCfg, err := pgxpool.ParseConfig(cfg.DB.DSN)
		if err != nil {
			logf("WARNING: db pool unavailable (bad DSN, incident memory only): %v", err)
		} else {
			poolCfg.MaxConns = int32(cfg.DB.MaxConns)
			pool, err := pgxpool.NewWithConfig(context.Background(), poolCfg)
			if err != nil {
				logf("WARNING: db pool unavailable (incident memory only): %v", err)
			} else if pgInc, err := incident.NewPGStoreWithPool(context.Background(), pool, tenant); err != nil {
				logf("WARNING: incident pg store unavailable (memory only): %v", err)
				pool.Close()
			} else {
				pgPool, incStore = pool, pgInc
				audit = NewPGAuditLog(pool, tenant, logf) // 审计随真相源持久化
				logf("incident persistence: timescaledb (shared pool: store+queue+audit+changes, max_conns=%d)", poolCfg.MaxConns)
			}
		}
	}
	// 注册 #6 护栏指标：拓扑恒有；内存降级兜底仅在未被 PG 替换时注册。
	reg := appMetrics.Registry()
	for _, g := range builder.MemGuards() {
		g.RegisterTo(reg)
	}
	if noiseEngine != nil {
		for _, g := range noiseEngine.MemGuards() {
			g.RegisterTo(reg)
		}
	}
	if incStore == memIncidents {
		for _, g := range memIncidents.MemGuards() {
			g.RegisterTo(reg)
		}
	}
	if audit == memAudit {
		for _, g := range memAudit.MemGuards() {
			g.RegisterTo(reg)
		}
	}
	// #11/ADR-012 leader 选举器：只构造不运行（Run/OnPromote 归 main）。
	// 降级路径与单实例现状逐字节一致：无共享池或 OPS_LEADER_ELECTION=off →
	// 恒 leader（构造即置位，gauge 从装配完成就是 1）。gauge 在**所有实例**
	// 的 /metrics 暴露本实例 leader 态 0/1（非 leader 恒 0——"当前谁是
	// owner"的唯一运行时口径）。opscopilot_is_leader 为主名（任务口径），
	// opscopilot_leader 为 ADR-012 观测章原名的等价别名，同一状态源。
	leader := NewLeaderElector(pgPool, cfg.Leader.Election, cfg.Leader.RetryInterval, logf)
	if cfg.Leader.Election && pgPool == nil {
		logf("WARNING: OPS_LEADER_ELECTION=on but no shared pool (no/unreachable DB) — permanently leader (single-instance semantics)")
	}
	leaderGauge := func() float64 {
		if leader.IsLeader() {
			return 1
		}
		return 0
	}
	reg.GaugeFunc("opscopilot_is_leader",
		"1 = this instance is the cluster leader running topology-gated loops (ADR-012 advisory-lock election; constant 1 when election off or no DB)", nil, leaderGauge)
	reg.GaugeFunc("opscopilot_leader",
		"Alias of opscopilot_is_leader (name used in ADR-012 observability section)", nil, leaderGauge)
	// 变更库后端选型（优化方案 #4）：恒先建内存实现——严格节点校验钩子经
	// Sink.HasNode 锁内查图，"先发现、后变更"的关联语义在此闭环。有共享池
	// 则包一层 PGChangeStore（PG=真相源、内存=读缓存）并**启动回放**保留窗
	// 内的历史证据：重启不丢 RCA 取证输入。回放失败只 WARNING、继续纯内存
	// ——变更持久化故障绝不阻塞启动（与事件 Store 同款降级纪律）。
	// 回放窗口与 OPS_CHANGE_RETENTION（清理器保留窗）对齐（cfg.Topology，
	// #4 新读取已并入 schema）：两侧同窗，"回放读到的"与"清理保留的"才
	// 一致（off = 不清理，则全量回放）。
	memChanges := topology.NewChangeStore(sink.HasNode)
	var changes topology.ChangeBackend = memChanges
	if pgPool != nil {
		pg := topology.NewPGChangeStore(memChanges, pgPool, tenant, logf)
		window, on := cfg.Topology.ChangeWindow, cfg.Topology.ChangeEnabled
		since := time.Time{}
		if on {
			since = time.Now().Add(-window)
		}
		if n, err := pg.LoadSince(since); err != nil {
			logf("WARNING: change pg store unavailable (memory only): %v", err)
		} else {
			changes = pg
			if on {
				logf("change persistence: timescaledb (replayed %d events within %s)", n, window)
			} else {
				logf("change persistence: timescaledb (full replay: %d events, retention off)", n)
			}
		}
	}
	hook, err := NewChangeWebhook(changes, "manual")
	if err != nil {
		return nil, err
	}
	hook.Token = cfg.Security.WebhookToken
	hook.MaxBodyBytes = cfg.Metrics.ChangeBodyLimit

	// W5-2.1：SemanticModel gRPC 服务（进程内形态，契约测试经
	// transport.DialInProcess 回环验证；独立进程形态只换 Dial 实现）。
	semantic := NewSemanticModelServer(sink, changes)
	grpcServer := grpc.NewServer()
	pb.RegisterSemanticModelServer(grpcServer, semantic)

	// W9-2 通知渠道：DB 为配置真相源，装配期装载启用渠道进注册表；
	// W9-1 闸门接线（ADR-011）：enforce 才挂（计数用共享池）。顺序要紧：
	// 先建注册表 → 装渠道 → 挂闸门（Gate 持有同一 registry，CRUD 后
	// reload 即可生效，无需重建 Gate）。**必须早于 asm 字面量**（否则
	// Assembly 抓到 nil 注册表——踩过）。
	notifyReg = notify.NewRegistry()
	// 兜底渠道：enforce 下零渠道 = 通知静默丢失，console 落日志至少留痕
	//（运维能在服务日志里看到"本该发出去的告警"）。真实渠道按需叠加。
	notifyReg.Register(&notify.ConsoleChannel{Logf: logf})
	webhookChannels := 0
	if pgPool != nil {
		chStore = NewChannelStore(pgPool, tenant)
		if n, err := loadChannelsIntoRegistry(context.Background(), chStore, notifyReg, logf); err != nil {
			logf("WARNING: notify channels load failed (console sink only): %v", err)
		} else {
			webhookChannels = n
			if n > 0 {
				logf("notify channels loaded: %d enabled", n)
			}
		}
	}
	gate := attachNoiseGate(noiseEngine, notifyReg, pgPool, logf)
	// W9-4：Gate 的实时计数直接读运行时状态暴露成 gauge——不在第二条
	// 路径上重复记账（两处记账必然漂移；真相源只有 notify_gate_stats
	// 与内存里这一个 Gate）。
	if gate != nil {
		appMetrics.Registry().GaugeFunc("opscopilot_noise_gate_suppressed_total",
			"Noise gate suppressed count (mirrors notify_gate_stats)", nil,
			func() float64 { return float64(gate.Stats().Suppressed) })
		appMetrics.Registry().GaugeFunc("opscopilot_noise_gate_dispatched_total",
			"Noise gate dispatched count (mirrors notify_gate_stats)", nil,
			func() float64 { return float64(gate.Stats().Dispatched) })
	}
	if noiseEngine != nil && noiseEngine.Mode() == ModeEnforce && webhookChannels == 0 {
		logf("WARNING: enforce mode with zero webhook channels — notifications are only logged (configure via POST /api/v1/notify/channels)")
	}

	// W11 实时推送：Hub + 事件 Store 装饰器（写成功即广播）。装饰必须早于
	// REST 网关与 IngestWorker 构造——worker 的自动建单（链路 A）也要推给
	// 控制台，否则"外部导入在页面上看不见"。
	hub := NewEventHub()
	incStore = NewPublishStore(incStore, hub)
	rest := NewRESTGateway(noiseEngine, semantic, incStore, cfg.Security.WebhookToken, audit, hub,
		RESTLimits{
			DedupWindow:       cfg.Noise.DedupWindow,
			IncidentBodyLimit: cfg.Metrics.IncidentBodyLimit,
			NotifyBodyLimit:   cfg.Metrics.NotifyBodyLimit,
		})
	// D2 决策 B+C：跨源放行改为显式白名单（默认不设置 = 仅同源）。
	// 合法性校验已在 config.Load 前置（"*"/畸形值启动失败）；SetCORSOrigin
	// 再校验一遍同源规则，装配期错误仍然拒绝启动（第七轮 M2：不能让错误
	// 配置静默生效）。
	if err := rest.SetCORSOrigin(cfg.Security.CORSOrigin); err != nil {
		return nil, err
	}
	rest.SetLogf(logf)
	// 告警中心数据源（仅 DB 部署有值；内存态该端点 503 并透出口径）。
	rest.SetDB(pgPool, tenant)

	asm := &Assembly{
		Sink:      sink,
		Changes:   changes,
		Webhook:   hook,
		Noise:     noiseEngine,
		GRPC:      grpcServer,
		REST:      rest,
		Incidents: incStore,
		Events:    hub,
		NotifyReg: notifyReg,
		Channels:  chStore,
		Metrics:   appMetrics,
		Leader:    leader,
		audit:     audit,
		logf:      logf,
		pool:      pgPool,
	}
	// 渠道 CRUD 后热生效：REST 写入配置即回调重载注册表（否则新渠道要
	// 重启才生效——"配置改了没反应"是运维最恨的一类 bug）。
	rest.SetChannels(asm.Channels, asm.reloadNotifyChannels)

	// W9-3 值班升级：未 ack 超时重发一次。需事件 Store（读 open）+ 通知
	// 注册表（出口）。台账优先 PG（多实例安全），无库降级内存并告警。
	asm.Escalation = buildEscalationPoller(cfg, asm.Incidents, asm.NotifyReg, pgPool, reg, logf)

	// #12/ADR-014 按需 RCA 最小链路：编排器是"incident × 拓扑/变更取证 ×
	// 六步流水线 × 审计"的唯一跨模块汇合点（internal/rca 只见纯 DTO）。
	// 取证走 SemanticModelServer 直调（REST 网关同款复用，一套语义）。
	// OPS_RCA=off → 不构造，端点 503（降级不阻塞启动纪律不变）。
	if cfg.RCA.Enabled {
		asm.RCA = NewRCAOrchestrator(cfg.RCA, asm.Incidents, semantic, noiseEngine, audit,
			cfg.Tenant, appMetrics, logf)
		rest.SetRCA(asm.RCA)
		// #3/ADR-015 llm-gateway 接线（ADR-014 conclude 挂点转正）：
		// OPS_LLM_ENDPOINT 空 = 不注入 Summarizer，conclude 维持 pending
		// 现状（行为与 ADR-014 逐字节一致）；已配置但构造失败只降级不拖垮
		// 启动——RCA 可用性不绑定 LLM，fail-open 纪律从装配点开始。
		if gw, gwErr := newLLMGateway(cfg.LLM); gwErr != nil {
			logf("WARNING: llm gateway rejected by config, rca conclude stays pending: %v", gwErr)
		} else if gw != nil {
			asm.RCA.SetSummarizer(newLLMSummarizer(gw, appMetrics, logf))
			logf("rca conclude wired to llm gateway: endpoint=%s model=%s timeout=%s (api key masked)",
				cfg.LLM.Endpoint, cfg.LLM.Model, cfg.LLM.Timeout)
		}
	} else {
		logf("rca on-demand analysis disabled (OPS_RCA=off)")
	}

	// W9 双链路链路 A（外部导入）：入队通道需要 DB 队列（持久化/可积压/可重放）。
	// 无 DB 时不注册队列——入队端点显式 503（见 Handler），比 404 可诊断。
	if pgPool != nil {
		// #11/ADR-012：认领租约参数来自 OPS_INGEST_LEASE_DURATION；owner 自动
		// 生成（host:pid:rand），多实例并起时靠它区分认领归属。
		queue := NewPGIngestQueue(pgPool, tenant, cfg.Ingest.BatchTimeoutPerItem,
			cfg.Ingest.LeaseDuration, "", logf)
		owner := &QueueOwner{}
		owner.SetWriter(queue)
		asm.Queue = queue
		asm.Ingest = &AlertmanagerWebhook{Owner: owner, Token: cfg.Security.WebhookToken,
			Tenant: tenant, BodyLimit: cfg.Ingest.AlertBodyLimit}
		asm.Worker = NewIngestWorker(queue, incStore, audit, cfg.Ingest.Interval, cfg.Ingest.Batch,
			cfg.Ingest.AutoCreate, cfg.Ingest.RateLimit, cfg.Ingest.RateWindow, logf)
		// W11 拉取侧：Pull.Enabled 且配了源地址时启用（需 DB 队列）。
		// 未配置即空转——不因"没接拉取源"而报错，骨架照常可用。
		asm.Poller = buildAlertPoller(cfg, owner, logf)
	} else {
		logf("WARNING: external import disabled (set OPS_DB_DSN to enable ingest queue)")
	}
	return asm, nil
}

// buildAlertPoller 按已装载的配置装配拉取调度器（未启用返回 nil）。
//
//	OPS_PULL_ALERTS=on        启用（默认关；避免与 push 并存时重复导入）
//	OPS_PROM_URL              源地址（复用连接器同款配置，见 ConnectorSection）
//	OPS_PROM_TOKEN            可选 Bearer
//	OPS_PULL_INTERVAL         轮询周期（默认 30s；非法值在 config.Load 即失败）
func buildAlertPoller(cfg *config.Config, owner *QueueOwner, logf func(string, ...any)) *AlertPoller {
	if !cfg.Pull.Enabled {
		return nil
	}
	url := cfg.Connector.PromURL
	if url == "" {
		logf("WARNING: OPS_PULL_ALERTS=on but OPS_PROM_URL unset — alert pull disabled")
		return nil
	}
	src := NewPrometheusAlertsSource(url, cfg.Connector.PromToken, cfg.Ingest.AlertBodyLimit)
	return NewAlertPoller(src, owner, incident.OriginPrometheus, cfg.Pull.Interval, logf)
}

// buildEscalationPoller 按已装载的配置装配值班升级调度器（未启用返回 nil）。
//
//	OPS_ESCALATION=on         启用（默认关；避免"没人管"在配置不当时刷通知）
//	OPS_ESCALATION_AFTER      创建后多久未 ack 触发升级（默认 15m；非法值启动失败）
//	OPS_ESCALATION_INTERVAL   扫描周期（默认 60s；非法值启动失败）
//
// 台账优先 PG（incident_escalation 主键幂等，多实例安全）；无库退化为内存
// 台账（重启即丢、仅单实例正确）——显式告警，不让运维误以为已持久化。
// 内存台账带容量护栏（#6，OPS_MEMLIMIT_ESCALATION_LEDGER），护栏指标注册进
// memReg（nil = 不暴露，测试/嵌入式装配容忍）。
func buildEscalationPoller(cfg *config.Config, store incident.Store, dispatcher EscalationDispatcher,
	pool *pgxpool.Pool, memReg *metrics.Registry, logf func(string, ...any)) *EscalationPoller {
	if !cfg.Notify.EscalationEnabled {
		return nil
	}
	var ledger EscalationLedger
	if pool != nil {
		ledger = newPGEscalationLedger(pool, cfg.Tenant)
	} else {
		logf("WARNING: escalation ledger is in-memory (no DB) — restart loses state, single-instance only")
		mem := newMemEscalationLedgerWithLimits(
			memguard.New("escalation_ledger", cfg.MemLimit.EscalationLedger, cfg.MemLimit.WarnRatio))
		for _, g := range mem.MemGuards() {
			g.RegisterTo(memReg)
		}
		ledger = mem
	}
	return NewEscalationPoller(store, dispatcher, ledger, cfg.Tenant,
		cfg.Notify.EscalationAfter, cfg.Notify.EscalationInterval, logf)
}

// Handler 装配 HTTP 路由：
//   - POST /api/v1/changes  提交变更事件（ChangeWebhook.ServeHTTP）
//   - GET  /healthz         存活探针（进程活着即 200，不探测下游）
//   - GET  /api/v1/*        REST 查询面（W5-2.2：簇/拓扑/变更；W9：事件读 + 写端点）
//   - POST /api/v1/ingest/* 外部导入入队（W9 链路 A：Alertmanager / 通用 webhook）
//   - GET  /metrics         Prometheus 指标（W9-4：告警链路延迟 + 判决/闸门计数）
//   - GET  /console（/ 跳转）控制台视图（W5-2.3；W10：事件页）
func (a *Assembly) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle(changeWebhookPath, a.Webhook)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /metrics", a.Metrics.Handler())
	a.REST.Register(mux)
	if a.Ingest != nil {
		a.Ingest.Register(mux) // 链路 A：POST /api/v1/ingest/{alertmanager,webhook}
	} else {
		// 未配置 DB 队列：显式 503（比 404 更可诊断——运维一眼知道是配置缺失）。
		unavailable := func(w http.ResponseWriter, _ *http.Request) {
			writeErr(w, http.StatusServiceUnavailable,
				"external import disabled: set OPS_DB_DSN to enable the ingest queue")
		}
		mux.HandleFunc("POST /api/v1/ingest/alertmanager", unavailable)
		mux.HandleFunc("POST /api/v1/ingest/webhook", unavailable)
	}
	registerConsole(mux)
	return mux
}
