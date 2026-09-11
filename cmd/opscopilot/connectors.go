// W4-1.1 连接器装配：把数据源连接器注册进 Host 并启动调度。
//
// 装配纪律：
//   - 连接器按环境变量"按需装配"：未配置对应数据源就不注册，
//     Host 空转（Run 正常 tick，无连接器时 RunOnce 为 no-op），不报错——
//     骨架进程允许在没有数据源的机器上起服务（topology + webhook 照常可用）。
//   - 凭证一律走 credential.Store 的只读闸门（ADR-002）：env 里的裸 Token
//     先包装成只读 Credential 入库，再以 Credential 指针交给连接器，
//     构造期由 pkg/readonly.Validate 强制——不用 AllowInsecureToken 后门。
//   - 本文件只做"已装载配置 → 连接器"的装配与注册，不做业务逻辑
//     （保持 cmd/ 薄；#2 收敛后 env 读取唯一发生在 internal/config）。
package main

import (
	"fmt"
	"strings"
	"time"

	"opscopilot/internal/config"
	"opscopilot/internal/connector"
	"opscopilot/internal/connector/azure"
	"opscopilot/internal/connector/prometheus"
	"opscopilot/internal/credential"
	"opscopilot/internal/topology"
)

// nodeKeyPrefix 短名补全前缀（OPS_TOPOLOGY_EDGES 的 "src->dst" 短名归一化）。
const nodeKeyPrefix = "prometheus://nodes/"

// newConnectorHost 构造 Host 并注册 cfg 声明的连接器（#2：env 读取收敛进
// internal/config，这里只消费 ConnectorSection 的解析结果）。
//
// creds：凭证库。env Token 包装成只读凭证后 Put 入库再取用，使所有
// 连接器凭证在库内有据可查（W4-1.1 的 credential 接线点）。
// tenant：数据源归属租户（prometheus 发现结果的 TenantID，#10 去 DefaultTenant）。
//
// 返回已注册连接器 ID 列表（供启动日志与测试断言）。
// 单个连接器构造失败直接返回错误：配置了数据源却装不上属于启动期
// 致命问题，带病上线只会把故障推迟到第一轮采集。
func newConnectorHost(logger connector.Logger, creds *credential.Store,
	conn config.ConnectorSection, tenant string) (*connector.Host, []string, error) {
	host := connector.NewHost(
		connector.WithLogger(logger),
		// 采集轮询周期（默认 30s，OPS_CONNECTOR_INTERVAL 可调——原为写死的
		// 魔法数字，#10 进 schema）。
		connector.WithInterval(conn.Interval),
		// 单连接器单轮上限（C3 第二道防线，默认 30s，OPS_CONNECTOR_TIMEOUT）：
		// 无内部超时的坏连接器只损失自己的时间片，不拖垮整轮巡检。
		connector.WithConnTimeout(conn.OpTimeout),
	)
	registered := make([]string, 0, 2)

	// Prometheus（W4 降噪的主数据源：告警从 /api/v1/alerts 进管道）。
	if promURL := conn.PromURL; promURL != "" {
		cfg := prometheus.Config{
			ID:       "prometheus",
			BaseURL:  promURL,
			TenantID: tenant,
		}
		if tok := conn.PromToken; tok != "" {
			c := credential.NewReadOnlyBearer("prometheus", tok)
			if err := creds.Put(c); err != nil {
				return nil, nil, fmt.Errorf("prometheus credential rejected: %w", err)
			}
			// Get 再取：走与运行期一致的读取路径（过期/不存在都有统一语义）。
			stored, err := creds.Get("prometheus")
			if err != nil {
				return nil, nil, fmt.Errorf("prometheus credential unreadable after put: %w", err)
			}
			cfg.Credential = &stored
		}
		pconn, err := prometheus.New(cfg)
		if err != nil {
			return nil, nil, fmt.Errorf("prometheus connector: %w", err)
		}
		if err := host.Register(pconn); err != nil {
			return nil, nil, fmt.Errorf("register prometheus: %w", err)
		}
		registered = append(registered, pconn.ID())
	}

	// Azure（可选：只做发现，Collect 明确 ErrUnsupported，属设计内）。
	// 仅在订阅 ID 与令牌齐备时注册，缺一视为"未配置该数据源"。
	if sub, tok := conn.AzureSubscriptionID, conn.AzureToken; sub != "" && tok != "" {
		c := credential.NewReadOnlyBearer("azure", tok)
		if err := creds.Put(c); err != nil {
			return nil, nil, fmt.Errorf("azure credential rejected: %w", err)
		}
		stored, err := creds.Get("azure")
		if err != nil {
			return nil, nil, fmt.Errorf("azure credential unreadable after put: %w", err)
		}
		cn, err := azure.New(azure.Config{
			ID:             "azure",
			SubscriptionID: sub,
			Credential:     &stored,
		})
		if err != nil {
			return nil, nil, fmt.Errorf("azure connector: %w", err)
		}
		if err := host.Register(cn); err != nil {
			return nil, nil, fmt.Errorf("register azure: %w", err)
		}
		registered = append(registered, cn.ID())
	}

	return host, registered, nil
}

// parseStaticEdges 解析 OPS_TOPOLOGY_EDGES 为边输入（不落库）。
// 端点不存在不在此报错：发现数据稍后进图（首轮采集告警先于发现），
// 由 TopologySink 的挂起补边机制在节点进图后落边。
func parseStaticEdges(spec string) ([]topology.EdgeInput, error) {
	if spec == "" {
		return nil, nil
	}
	var inputs []topology.EdgeInput
	now := time.Now()
	for _, pair := range strings.Split(spec, ",") {
		parts := strings.SplitN(strings.TrimSpace(pair), "->", 2)
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return nil, fmt.Errorf("invalid OPS_TOPOLOGY_EDGES segment %q (want src->dst)", pair)
		}
		resolve := func(k string) string {
			if strings.Contains(k, "://") {
				return k
			}
			return nodeKeyPrefix + k
		}
		inputs = append(inputs, topology.EdgeInput{
			SrcKey:     resolve(strings.TrimSpace(parts[0])),
			DstKey:     resolve(strings.TrimSpace(parts[1])),
			Relation:   "depends_on",
			ObservedAt: now, Confidence: topology.ConfidenceMedium,
		})
	}
	return inputs, nil
}
