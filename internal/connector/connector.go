// Package connector 定义 OpsCopilot 的数据源接入抽象（M1 W2）。
//
// 设计纪律（与全局架构一致）：
//   - 只读优先：Connector 接口只暴露采集/发现/健康三类读操作，
//     不提供任何写方法——写入路径属于后续阶段（拓扑构建/降噪落库），
//     由算子前置原则（ADR-002）与凭证只读强制共同约束。
//   - 云中立：接口与具体云厂商解耦，Prometheus / 阿里云 / AWS 等只作为
//     不同实现存在，宿主（Host）不感知差异。
//   - 归一化：所有实现必须把源数据归一化为本包类型，下游（W3 拓扑构建、
//     语义模型）只消费归一化结构，不直接接触各厂商 API。
//
// 归一化类型故意与 internal/contracts/pb 的 TopologyNode/TopologyEdge 形状对齐，
// 以便 W3 直接映射，避免二次转换。
package connector

import (
	"context"
	"errors"
	"time"
)

// HealthStatus 健康状态枚举（强类型，避免各处散落裸字符串字面量）。
type HealthStatus string

// 健康状态取值。
const (
	HealthHealthy  HealthStatus = "healthy"
	HealthDegraded HealthStatus = "degraded"
	HealthDown     HealthStatus = "down"
)

// ErrUnsupported 连接器不支持所请求的采集选项（如增量水位 Since、作用域过滤 Scope）。
//
// 用途：替代"静默忽略"。实现若不支持某选项，必须返回本错误（可用 errors.Is 判断），
// 让调用方明确知道过滤/增量未生效，而不是误以为已生效后按全量数据做下游决策。
var ErrUnsupported = errors.New("connector: unsupported collect option")

// ResourceNode 归一化后的资源节点（对应 pb.TopologyNode）。
// 一个 target / 一个云资源实例 = 一个 ResourceNode。
type ResourceNode struct {
	// Key 全局唯一键，如 "prometheus://scrape/<job>/<instance>" 或
	// "aliyun://ecs/i-xxxx"。下游用它做拓扑去重与关联。
	Key  string
	Type string // 资源类型：host / pod / db / lb / ...
	// Labels 资源标签。资源自身的健康状态（如 Prometheus target 的 up/down）
	// 也会保留在此，避免归一化过程丢失信息。
	Labels map[string]string
	Source string // 产生该节点的连接器 ID
	// ObservedAt 观测时间。注意：这是数据源侧观测到的时刻，
	// 实现应优先取数据源返回的抓取时间，而非本地处理时间。
	ObservedAt time.Time
}

// MetricSample 归一化后的指标样本。
type MetricSample struct {
	Name      string
	Labels    map[string]string
	Value     float64
	Timestamp time.Time
	Source    string
}

// Alert 归一化后的告警（对应告警簇降噪流水线的输入）。
// 字段对齐 Prometheus alertmanager 表达，便于直接消费。
type Alert struct {
	// Fingerprint 告警幂等键，用于告警簇去重（ADR-001 配套）。
	Fingerprint string
	// GeneratorURL 溯源链接（Prometheus 的 generatorURL）。
	GeneratorURL string
	Labels       map[string]string
	Annotations  map[string]string
	// StartsAt / EndsAt RFC3339 时间戳。
	StartsAt time.Time
	EndsAt   time.Time
	// Status：firing / resolved（Prometheus 语义）。
	Status string
	// Severity 从 Labels["severity"] 提取的副本，便于降噪排序。
	Severity string
	Source   string
}

// CollectResult Collect 的产出：本次拉取到的指标与告警。
type CollectResult struct {
	// Metrics 指标样本。若连接器未配置指标查询（如 Prometheus 未配 PromQL），
	// 本字段为空切片而非 nil 错误——那是配置选择，不是失败。
	Metrics     []MetricSample
	Alerts      []Alert
	TenantID    string
	CollectedAt time.Time
}

// DiscoverResult Discover 的产出：本次发现的资源节点。
type DiscoverResult struct {
	Nodes        []ResourceNode
	TenantID     string
	DiscoveredAt time.Time
}

// CollectRequest 采集请求。
type CollectRequest struct {
	TenantID string
	// Scope 可选过滤，如命名空间 / 项目；空表示全量。
	// 实现若不支持过滤，必须返回 ErrUnsupported，不得静默忽略。
	Scope string
	// Since 增量拉取水位（可选）；零值表示全量。
	// 实现若不支持增量，必须返回 ErrUnsupported，不得静默忽略。
	Since time.Time
}

// Health 健康检查结果。
type Health struct {
	Status    HealthStatus // Health* 常量之一
	Detail    string
	CheckedAt time.Time
}

// Connector 数据源接入的统一接口（纯只读）。
//
// 实现方可作为进程内实例注册到 Host，也可经 go-plugin 封装后由 Host 远程调用
// （W2 仅做进程内骨架，go-plugin 加载为后续扩展点）。
type Connector interface {
	// ID 全局唯一标识，如 "prometheus-prod"。Host 用它做注册键。
	ID() string
	// Type 实现类别，如 "prometheus" / "aliyun" / "aws"。
	Type() string
	// HealthCheck 探测数据源可达性与就绪状态。
	HealthCheck(ctx context.Context) (Health, error)
	// Collect 拉取指标与告警。必须只读（仅 GET 类操作）。
	// 遇到不支持的采集选项（Scope/Since）应返回 ErrUnsupported。
	Collect(ctx context.Context, req CollectRequest) (*CollectResult, error)
	// Discover 发现资源节点（用于 W3 拓扑自动发现）。
	Discover(ctx context.Context) (*DiscoverResult, error)
}
