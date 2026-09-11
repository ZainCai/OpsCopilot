// W3 启动装配（M1 W3 收官项）：把拓扑引擎与变更 webhook 装成一个可运行的服务。
//
// 装配清单：
//   - topology.Builder：拓扑图唯一归属（多轮发现共享同一张图）；
//   - TopologySink：连接器宿主 → 拓扑的发现数据入口（W2→W3 管道）；
//   - ChangeStore（严格节点校验）：变更事件库，校验钩子经 Sink.HasNode
//     锁内查图——"先发现、后变更"的关联语义在此闭环；
//   - ChangeWebhook：POST /api/v1/changes 手动提交变更事件。
//
// 装配放在 cmd/（package main）：跨模块引用的唯一合法汇合点（v1.3 §5.2）。
package main

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"

	"opscopilot/internal/connector"
	pb "opscopilot/internal/contracts/pb"
	"opscopilot/internal/incident"
	"opscopilot/internal/topology"
)

// changeWebhookPath 变更事件提交路由（手动 curl / Git/Jenkins webhook 共用）。
const changeWebhookPath = "/api/v1/changes"

// Assembly W5 装配产物：各组件的持有者，供 main 做生命周期管理与测试断言。
type Assembly struct {
	Sink    *TopologySink
	Changes *topology.ChangeStore
	Webhook *ChangeWebhook
	// Noise 影子降噪引擎（W4-1.4）；nil = envNoiseShadow=off 已关闭。
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
	// Events 实时广播器（W11：控制台事件页 SSE 订阅源）。
	Events *EventHub
	// audit 审计日志（人工操作与外部自动动作统一留痕）。
	audit AuditLog
	// pool DB 连接池（事件 Store / 导入队列 / 审计共享；生命周期归装配）。
	pool *pgxpool.Pool
}

// Close 释放装配持有的资源：先停实时推送（关闭 SSE 连接），再关 DB 池
// （共享池的唯一所有者是装配，见 NewAssembly）。
func (a *Assembly) Close() {
	if a.Events != nil {
		a.Events.Close() // 让已连接的 SSE 订阅者立即结束，不悬挂
	}
	if a.pool != nil {
		a.pool.Close()
	}
}

// NewAssembly 组装 W3+W4+W5 组件并接线。
//
// webhookToken：变更 webhook 的共享密钥；非空时 POST /api/v1/changes
// 必须携带匹配的 X-OpsCopilot-Token 头（S1 写路径准入）。传空表示
// 不鉴权——仅限回环/内网部署。
//
// 变更库的节点校验钩子经 Sink.HasNode（锁内读图）实现——变更事件只能
// 关联到拓扑图里真实存在的节点，防止"幽灵节点"静默失败。
//
// 影子降噪（W4-1.4）：OPS_NOISE_SHADOW=off 可整体关闭；OPS_NOISE_WINDOW
// 配置去重/聚类时间窗（默认 10m，解析失败启动失败）。
//
// dbDefaultMaxConns 共享连接池默认上限（D7 决策 A）：事件 Store + 导入队列 +
// 审计三方共用，worker 批处理持一条事务连接再做 Store 写需要第二条；
// pgx 默认 max(4,NumCPU) 在并发下易耗尽。可用 OPS_DB_MAX_CONNS 覆盖。
const dbDefaultMaxConns = 16

func NewAssembly(logger connector.Logger, webhookToken string) (*Assembly, error) {
	// nil logger 容忍（装配测试惯用 nil）：本函数内日志一律走 logf，
	// 不再直接调 logger.Printf，避免 nil logger 触发空指针。
	logf := func(format string, args ...any) {
		if logger != nil {
			logger.Printf(format, args...)
		}
	}
	builder := topology.NewBuilder()
	sink, err := NewTopologySink(builder, logger)
	if err != nil {
		return nil, err
	}
	store := topology.NewChangeStore(sink.HasNode)
	hook, err := NewChangeWebhook(store, "manual")
	if err != nil {
		return nil, err
	}
	hook.Token = webhookToken

	// 影子降噪：挂在 sink 上（告警经 IngestCollect 转交），引擎持有
	// sink 引用做拓扑快照——互相引用只能后挂（见 AttachNoise 注释）。
	noiseEngine, err := NewNoiseEngine(sink, logger)
	if err != nil {
		return nil, err
	}
	sink.AttachNoise(noiseEngine)

	// W5-2.1：SemanticModel gRPC 服务（进程内形态，契约测试经
	// transport.DialInProcess 回环验证；独立进程形态只换 Dial 实现）。
	semantic := NewSemanticModelServer(sink, store)
	grpcServer := grpc.NewServer()
	pb.RegisterSemanticModelServer(grpcServer, semantic)

	// W5-2.2：REST 只读查询面（复用 SemanticModelServer 的校验与映射，
	// gRPC/REST 一套语义不漂移）。M2 主干：事件 Store 同源挂载。
	// W9：事件 Store 双实现——OPS_DB_DSN 设置用 TimescaleDB（重启不丢），
	// 否则内存（Persistence 标注提醒）。pg 池错误不阻塞启动（降级内存）。
	// 事件 Store 双实现；DB 可用时事件 Store / 导入队列 / 审计**共用一条池**
	// （R9：多池各占连接无收益）。pg 不可用不阻塞启动（降级内存，Persistence
	// 字段提醒消费者；见 R6-4）。
	var incStore incident.Store = incident.NewMemStore()
	var audit AuditLog = NewMemAuditLog()
	var pgPool *pgxpool.Pool
	if dsn := os.Getenv("OPS_DB_DSN"); dsn != "" {
		// D7 决策 A：显式配置连接池容量。pgx 默认 max(4,NumCPU) 偏小——
		// 这个池被**事件 Store + 导入队列 + 审计**三方共用，且队列批处理
		// 会持一条连接（事务）再做 Store 写，压测下易"等连接直到超时"。
		poolCfg, err := pgxpool.ParseConfig(dsn)
		if err != nil {
			logf("WARNING: db pool unavailable (bad DSN, incident memory only): %v", err)
		} else {
			if raw := strings.TrimSpace(os.Getenv("OPS_DB_MAX_CONNS")); raw != "" {
				if n, e := strconv.Atoi(raw); e == nil && n > 0 {
					poolCfg.MaxConns = int32(n)
				} else {
					logf("WARNING: invalid OPS_DB_MAX_CONNS %q — using pgx default", raw)
				}
			} else {
				poolCfg.MaxConns = dbDefaultMaxConns
			}
			pool, err := pgxpool.NewWithConfig(context.Background(), poolCfg)
			if err != nil {
				logf("WARNING: db pool unavailable (incident memory only): %v", err)
			} else if pgInc, err := incident.NewPGStoreWithPool(context.Background(), pool, DefaultTenant); err != nil {
				logf("WARNING: incident pg store unavailable (memory only): %v", err)
				pool.Close()
			} else {
				pgPool, incStore = pool, pgInc
				audit = NewPGAuditLog(pool, DefaultTenant, logf) // 审计随真相源持久化
				logf("incident persistence: timescaledb (shared pool: store+queue+audit, max_conns=%d)", poolCfg.MaxConns)
			}
		}
	}
	// W11 实时推送：Hub + 事件 Store 装饰器（写成功即广播）。装饰必须早于
	// REST 网关与 IngestWorker 构造——worker 的自动建单（链路 A）也要推给
	// 控制台，否则"外部导入在页面上看不见"。
	hub := NewEventHub()
	incStore = NewPublishStore(incStore, hub)
	rest := NewRESTGateway(noiseEngine, semantic, incStore, webhookToken, audit, hub)
	// D2 决策 B+C：跨源放行改为显式白名单（默认不设置 = 仅同源）；
	// "*" 与非法形态直接让装配失败（第七轮 M2：不能让错误配置静默生效）。
	if err := rest.SetCORSOrigin(os.Getenv("OPS_CORS_ORIGIN")); err != nil {
		return nil, err
	}
	rest.SetLogf(logf)
	// 告警中心数据源（仅 DB 部署有值；内存态该端点 503 并透出口径）。
	rest.SetDB(pgPool, DefaultTenant)

	asm := &Assembly{
		Sink:      sink,
		Changes:   store,
		Webhook:   hook,
		Noise:     noiseEngine,
		GRPC:      grpcServer,
		REST:      rest,
		Incidents: incStore,
		Events:    hub,
		audit:     audit,
		pool:      pgPool,
	}

	// W9 双链路链路 A（外部导入）：入队通道需要 DB 队列（持久化/可积压/可重放）。
	// 无 DB 时不注册队列——入队端点显式 503（见 Handler），比 404 可诊断。
	// W9-1 转正（ADR-011）：闸门接线也在装配层（enforce 才挂，计数用共享池）。
	attachNoiseGate(noiseEngine, pgPool, logf)
	if pgPool != nil {
		queue := NewPGIngestQueue(pgPool, DefaultTenant)
		owner := &QueueOwner{}
		owner.SetWriter(queue)
		autoCreate := strings.EqualFold(strings.TrimSpace(os.Getenv("OPS_INCIDENT_AUTOCREATE")), "on")
		asm.Queue = queue
		asm.Ingest = &AlertmanagerWebhook{Owner: owner, Token: webhookToken, Tenant: DefaultTenant}
		asm.Worker = NewIngestWorker(queue, incStore, audit, 0, 0, autoCreate, 0, 0, logf)
		// W11 拉取侧：OPS_PULL_ALERTS=on 且配了源地址时启用（需 DB 队列）。
		// 未配置即空转——不因"没接拉取源"而报错，骨架照常可用。
		asm.Poller = buildAlertPoller(owner, logf)
	} else {
		logf("WARNING: external import disabled (set OPS_DB_DSN to enable ingest queue)")
	}
	return asm, nil
}

// buildAlertPoller 按环境变量装配拉取调度器（未启用返回 nil）。
//
//	OPS_PULL_ALERTS=on        启用（默认关；避免与 push 并存时重复导入）
//	OPS_PROM_URL              源地址（复用连接器同款 env）
//	OPS_PROM_TOKEN            可选 Bearer
//	OPS_PULL_INTERVAL         轮询周期（默认 30s；非法值告警后取默认）
func buildAlertPoller(owner *QueueOwner, logf func(string, ...any)) *AlertPoller {
	if !strings.EqualFold(strings.TrimSpace(os.Getenv("OPS_PULL_ALERTS")), "on") {
		return nil
	}
	url := strings.TrimSpace(os.Getenv("OPS_PROM_URL"))
	if url == "" {
		logf("WARNING: OPS_PULL_ALERTS=on but OPS_PROM_URL unset — alert pull disabled")
		return nil
	}
	interval := 30 * time.Second
	if raw := strings.TrimSpace(os.Getenv("OPS_PULL_INTERVAL")); raw != "" {
		if d, err := time.ParseDuration(raw); err == nil && d > 0 {
			interval = d
		} else {
			logf("WARNING: invalid OPS_PULL_INTERVAL %q — using 30s", raw)
		}
	}
	src := NewPrometheusAlertsSource(url, os.Getenv("OPS_PROM_TOKEN"))
	return NewAlertPoller(src, owner, incident.OriginPrometheus, interval, logf)
}

// Handler 装配 HTTP 路由：
//   - POST /api/v1/changes  提交变更事件（ChangeWebhook.ServeHTTP）
//   - GET  /healthz         存活探针（进程活着即 200，不探测下游）
//   - GET  /api/v1/*        REST 查询面（W5-2.2：簇/拓扑/变更；W9：事件读 + 写端点）
//   - POST /api/v1/ingest/* 外部导入入队（W9 链路 A：Alertmanager / 通用 webhook）
//   - GET  /console（/ 跳转）控制台视图（W5-2.3；W10：事件页）
func (a *Assembly) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle(changeWebhookPath, a.Webhook)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})
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
