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
	// Incidents 事件域内存 Store（M2 主干 F-01/F-02；DB 后端 W9）。
	Incidents incident.Store
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
func NewAssembly(logger connector.Logger, webhookToken string) (*Assembly, error) {
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
	var incStore incident.Store = incident.NewMemStore()
	if dsn := os.Getenv("OPS_DB_DSN"); dsn != "" {
		if pgInc, err := incident.NewPGStore(context.Background(), dsn, DefaultTenant); err != nil {
			logger.Printf("WARNING: incident pg store unavailable (memory only): %v", err)
		} else {
			incStore = pgInc
			logger.Printf("incident persistence: timescaledb")
		}
	}
	rest := NewRESTGateway(noiseEngine, semantic, incStore, webhookToken)

	return &Assembly{
		Sink:      sink,
		Changes:   store,
		Webhook:   hook,
		Noise:     noiseEngine,
		GRPC:      grpcServer,
		REST:      rest,
		Incidents: incStore,
	}, nil
}

// Handler 装配 HTTP 路由：
//   - POST /api/v1/changes  提交变更事件（ChangeWebhook.ServeHTTP）
//   - GET  /healthz         存活探针（进程活着即 200，不探测下游）
//   - GET  /api/v1/*        REST 只读查询面（W5-2.2：簇/拓扑/变更）
//   - GET  /console（/ 跳转）控制台最小视图（W5-2.3）
func (a *Assembly) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle(changeWebhookPath, a.Webhook)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})
	a.REST.Register(mux)
	registerConsole(mux)
	return mux
}
