// W4-1.1 连接器装配：把数据源连接器注册进 Host 并启动调度。
//
// 装配纪律：
//   - 连接器按环境变量"按需装配"：未配置对应数据源就不注册，
//     Host 空转（Run 正常 tick，无连接器时 RunOnce 为 no-op），不报错——
//     骨架进程允许在没有数据源的机器上起服务（topology + webhook 照常可用）。
//   - 凭证一律走 credential.Store 的只读闸门（ADR-002）：env 里的裸 Token
//     先包装成只读 Credential 入库，再以 Credential 指针交给连接器，
//     构造期由 pkg/readonly.Validate 强制——不用 AllowInsecureToken 后门。
//   - 本文件只做"env → Config"的翻译与注册，不做业务逻辑（保持 cmd/ 薄）。
package main

import (
	"fmt"
	"os"
	"time"

	"opscopilot/internal/connector"
	"opscopilot/internal/connector/azure"
	"opscopilot/internal/connector/prometheus"
	"opscopilot/internal/credential"
)

// 连接器装配相关环境变量。
const (
	// envPromURL Prometheus 基址（如 http://localhost:9090）。设置即注册 prometheus 连接器。
	envPromURL = "OPS_PROM_URL"
	// envPromToken 可选 Bearer Token（无鉴权数据源留空）。
	envPromToken = "OPS_PROM_TOKEN"
	// envAzureSub Azure 订阅 ID；与 envAzureToken 同时设置才注册 azure 连接器。
	envAzureSub = "OPS_AZURE_SUBSCRIPTION_ID"
	// envAzureToken ARM 访问令牌（Secret，绝不打日志）。
	envAzureToken = "OPS_AZURE_TOKEN"
)

// newConnectorHost 构造 Host 并注册环境变量声明的连接器。
//
// creds：凭证库。env Token 包装成只读凭证后 Put 入库再取用，使所有
// 连接器凭证在库内有据可查（W4-1.1 的 credential 接线点）。
//
// 返回已注册连接器 ID 列表（供启动日志与测试断言）。
// 单个连接器构造失败直接返回错误：配置了数据源却装不上属于启动期
// 致命问题，带病上线只会把故障推迟到第一轮采集。
func newConnectorHost(logger connector.Logger, creds *credential.Store) (*connector.Host, []string, error) {
	host := connector.NewHost(
		connector.WithLogger(logger),
		// 单连接器单轮 30s 上限（C3 第二道防线）：无内部超时的坏连接器
		// 只损失自己的时间片，不拖垮整轮巡检。
		connector.WithConnTimeout(30*time.Second),
	)
	registered := make([]string, 0, 2)

	// Prometheus（W4 降噪的主数据源：告警从 /api/v1/alerts 进管道）。
	if promURL := os.Getenv(envPromURL); promURL != "" {
		cfg := prometheus.Config{
			ID:       "prometheus",
			BaseURL:  promURL,
			TenantID: "default",
		}
		if tok := os.Getenv(envPromToken); tok != "" {
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
		conn, err := prometheus.New(cfg)
		if err != nil {
			return nil, nil, fmt.Errorf("prometheus connector: %w", err)
		}
		if err := host.Register(conn); err != nil {
			return nil, nil, fmt.Errorf("register prometheus: %w", err)
		}
		registered = append(registered, conn.ID())
	}

	// Azure（可选：只做发现，Collect 明确 ErrUnsupported，属设计内）。
	// 仅在订阅 ID 与令牌齐备时注册，缺一视为"未配置该数据源"。
	if sub, tok := os.Getenv(envAzureSub), os.Getenv(envAzureToken); sub != "" && tok != "" {
		c := credential.NewReadOnlyBearer("azure", tok)
		if err := creds.Put(c); err != nil {
			return nil, nil, fmt.Errorf("azure credential rejected: %w", err)
		}
		stored, err := creds.Get("azure")
		if err != nil {
			return nil, nil, fmt.Errorf("azure credential unreadable after put: %w", err)
		}
		conn, err := azure.New(azure.Config{
			ID:             "azure",
			SubscriptionID: sub,
			Credential:     &stored,
		})
		if err != nil {
			return nil, nil, fmt.Errorf("azure connector: %w", err)
		}
		if err := host.Register(conn); err != nil {
			return nil, nil, fmt.Errorf("register azure: %w", err)
		}
		registered = append(registered, conn.ID())
	}

	return host, registered, nil
}
