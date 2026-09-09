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
	"encoding/json"
	"net/http"

	"opscopilot/internal/connector"
	"opscopilot/internal/topology"
)

// changeWebhookPath 变更事件提交路由（手动 curl / Git/Jenkins webhook 共用）。
const changeWebhookPath = "/api/v1/changes"

// Assembly W3 装配产物：各组件的持有者，供 main 做生命周期管理与测试断言。
type Assembly struct {
	Sink    *TopologySink
	Changes *topology.ChangeStore
	Webhook *ChangeWebhook
}

// NewAssembly 组装 W3 全部组件并接线。
//
// webhookToken：变更 webhook 的共享密钥；非空时 POST /api/v1/changes
// 必须携带匹配的 X-OpsCopilot-Token 头（S1 写路径准入）。传空表示
// 不鉴权——仅限回环/内网部署。
//
// 变更库的节点校验钩子经 Sink.HasNode（锁内读图）实现——变更事件只能
// 关联到拓扑图里真实存在的节点，防止"幽灵节点"静默失败。
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
	return &Assembly{Sink: sink, Changes: store, Webhook: hook}, nil
}

// Handler 装配 HTTP 路由：
//   - POST /api/v1/changes  提交变更事件（ChangeWebhook.ServeHTTP）
//   - GET  /healthz         存活探针（进程活着即 200，不探测下游）
func (a *Assembly) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle(changeWebhookPath, a.Webhook)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})
	return mux
}
