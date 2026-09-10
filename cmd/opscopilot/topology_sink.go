// 编排装配层（M1 W3）：把连接器宿主的发现数据接入拓扑引擎。
//
// 为什么放在 cmd/（package main）而不放 internal/：
// 这层适配必须同时引用 internal/connector（Sink 接口与结果类型）与
// internal/topology（NodeInput/Builder），而 v1.3 §5.2 模块纪律规定
// internal/ 下各模块禁止互相 import（跨模块只能走 gRPC/transport）。
// 编排装配不属于任何 internal 模块，是两个模块之间唯一合法的汇合点。
package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"opscopilot/internal/connector"
	pb "opscopilot/internal/contracts/pb"
	"opscopilot/internal/topology"
)

// TopologySink 实现 connector.Sink：把宿主投递的发现结果喂进拓扑 Builder，
// 并提供覆盖率自检输出（W3 验收 1）。
//
// 转换规则：DiscoverResult.Nodes → topology.NodeInput，字段一一对应；
// Confidence 留零值——topology 侧对直接发现默认 high（连接器观测 = 事实，
// ADR-007 规则 1），把"事实 vs 推断"的判断留给数据的产生处而非搬运处。
//
// 并发安全：Host.RunOnce 当前为串行调用，但 Sink 不应依赖这一实现细节
// （W2 已有并发调度的测试先例），故内部加锁。
type TopologySink struct {
	mu      sync.Mutex
	builder *topology.Builder
	// alerts 累计收到的告警条数。
	alerts int
	logger connector.Logger
	// noise W4-1.4 影子降噪引擎（可选，nil = 只计数不处理）。
	noise *NoiseEngine
	// pendingEdges W6-0 静态拓扑边挂起队列：每次发现入图后锁内重试
	// 落边，成功即出队（Builder.AddEdge 端点必须在图的纪律不变——
	// 静态边只能等节点先进图）。
	pendingEdges []topology.EdgeInput
}

// AttachStaticEdges 挂载静态拓扑边（W6-0 评估环境，OPS_TOPOLOGY_EDGES）。
// 边不会立即落图——等端点节点经发现进图后，由 IngestDiscover 的
// 挂起补边机制落图。必须在首轮发现前调用（装配期）。
func (s *TopologySink) AttachStaticEdges(inputs []topology.EdgeInput) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pendingEdges = append(s.pendingEdges, inputs...)
}

// AttachNoise 挂载影子降噪引擎（W4-1.4）。传 nil 等价于卸载。
// 挂载点在装配层而非构造参数：NoiseEngine 依赖 sink 的拓扑快照，
// 二者互相引用，只能先建 sink 再挂引擎。
func (s *TopologySink) AttachNoise(ne *NoiseEngine) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.noise = ne
}

// NewTopologySink 构造。builder 须非 nil（拓扑图的归属在调用方，
// 便于多轮采集共享同一张图）。
func NewTopologySink(b *topology.Builder, logger connector.Logger) (*TopologySink, error) {
	if b == nil {
		return nil, errors.New("topology sink: nil builder")
	}
	return &TopologySink{builder: b, logger: logger}, nil
}

// IngestDiscover 实现 connector.Sink：发现节点 → 拓扑节点。
//
// 持锁范围说明：Builder.AddDiscovery 会写共享的节点 map，而本 Sink 的
// Graph()（供变更 webhook 的节点校验钩子等读方使用）读的是同一张图——
// 写入必须与 Graph() 用同一把锁互斥，否则装配层会出现数据竞争。
func (s *TopologySink) IngestDiscover(_ context.Context, r *connector.DiscoverResult) error {
	if r == nil {
		return nil
	}
	inputs := make([]topology.NodeInput, 0, len(r.Nodes))
	for _, n := range r.Nodes {
		inputs = append(inputs, topology.NodeInput{
			Key:        n.Key,
			Type:       n.Type,
			Labels:     n.Labels,
			Source:     n.Source,
			ObservedAt: n.ObservedAt,
		})
	}
	s.mu.Lock()
	err := s.builder.AddDiscovery(inputs)
	// W6-0 挂起补边：节点进图后逐条尝试落静态边，成功即出队。
	// 锁内执行——AddEdge 写共享边集合，与 Graph() 读方互斥。
	if err == nil && len(s.pendingEdges) > 0 {
		remaining := s.pendingEdges[:0]
		for _, e := range s.pendingEdges {
			if aerr := s.builder.AddEdge(e); aerr != nil {
				remaining = append(remaining, e) // 端点还没进图，下轮再试
			} else {
				s.logf("static topology edge registered: %s -> %s", e.SrcKey, e.DstKey)
			}
		}
		s.pendingEdges = remaining
	}
	s.mu.Unlock()
	if err != nil {
		return fmt.Errorf("topology sink: ingest discover: %w", err)
	}
	s.logf("discover ingested: %d nodes (tenant=%q)", len(inputs), r.TenantID)
	return nil
}

// IngestCollect 实现 connector.Sink：告警计数 + 影子降噪转交（W4-1.4）。
// 降噪处理在锁外调用：NoiseEngine 自带互斥，且其内部读拓扑快照
// （graphSnapshot）需要拿 s.mu——若在持锁区间内调用会自死锁。
func (s *TopologySink) IngestCollect(_ context.Context, r *connector.CollectResult) error {
	if r == nil {
		return nil
	}
	s.mu.Lock()
	s.alerts += len(r.Alerts)
	total := s.alerts
	ne := s.noise
	s.mu.Unlock()
	if ne != nil {
		ne.ProcessAlerts(r.Alerts)
	}
	if len(r.Alerts) > 0 {
		s.logf("collect ingested: %d alerts this round (total %d)", len(r.Alerts), total)
	}
	return nil
}

// graphSnapshot 返回当前拓扑图（W3 审查 P3：从公开 API 收窄为包内私有——
// 返回的是共享底层 map 的指针，对外暴露易被误用为无保护读；包外的
// 读需求走 HasNode / CoverageReport / TopologyAsOf 这类锁内方法，
// 包内仅测试使用）。
func (s *TopologySink) graphSnapshot() *topology.Graph {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.builder.Build()
}

// TopologyAsOf 时点拓扑查询（W5-2.1 gRPC GetTopology 的数据出口）。
//
// R2 契约：Build() 返回内部指针（零拷贝、无自身锁），AsOf/Neighborhood/
// ToProtoAsOf 全部读取共享 map——**整个读取+裁剪+映射必须发生在
// 持锁区间内**，锁外读图是无保护读（并发 IngestDiscover 会写同一 map）。
// root 非空时做邻域裁剪（root 不存在返回 ok=false，gRPC 层映射
// NotFound）；depth <= 0 且 root 非空 = 仅 root 单节点图（0 跳约定）。
func (s *TopologySink) TopologyAsOf(asOf time.Time, root string, depth int) (resp *pb.GetTopologyResponse, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g := s.builder.Build().AsOf(asOf)
	if root != "" {
		g, ok = g.Neighborhood(root, depth)
		if !ok {
			return nil, false
		}
	}
	return g.ToProtoAsOf(asOf), true
}

// HasNode 锁内判定节点是否存在（供变更 webhook 的严格节点校验钩子使用）。
// 读操作发生在持锁区间内，与 IngestDiscover 的写入互斥——不要用
// Graph().Nodes 在锁外做同样的事，那是无保护读。
func (s *TopologySink) HasNode(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.builder.Build().Nodes[key]
	return ok
}

// CoverageReport 覆盖率自检输出（W3 验收 1）：
// 对比 Nodes/Edges 与 CausalNodes/CausalEdges 即知被门禁拦下多少证据；
// 按置信度/来源的分桶用于 ADR-007 的后续演进决策。
//
// 锁纪律：Stats() 会遍历节点与边 map，必须发生在持锁区间内——
// 与 IngestDiscover 的写入互斥。不能写成 Graph().Stats()：
// 那是"持锁取引用、锁外遍历"的无保护读（W3 审查 P1-1）。
func (s *TopologySink) CoverageReport() topology.Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.builder.Build().Stats()
}

// AlertCount 返回累计收到的告警条数（供测试与运维自检）。
func (s *TopologySink) AlertCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.alerts
}

func (s *TopologySink) logf(format string, args ...interface{}) {
	if s.logger != nil {
		s.logger.Printf(format, args...)
	}
}
