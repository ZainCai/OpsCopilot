package topology

import (
	"errors"
	"fmt"
	"time"
)

// Builder 从发现结果与显式关系声明构建拓扑图。
//
// 合并规则（ADR-007 规则 4）：
//   - 节点同 Key 合并：Confidence 取高（有直接观测不降级）、
//     ValidFrom 取最早（bi-temporal 不回退，as_of 可重建）、
//     Source 与 Labels 以**后加入者**为准（调用方应按观测时间递增喂入，
//     Host 的调度轮次天然满足；仅非空值覆盖）；
//   - 边同 (Src,Dst,Relation) 合并：Confidence 取高、ValidFrom 取最早。
//
// 并发契约（第三轮复审 R2，调用方必读）：
//
//	Builder 自身**无锁**。AddDiscovery/AddEdge/AddNode 与 Build() 返回的
//	Graph 共享同一份可变状态——"写入方互斥 + 读操作全程持同一把锁"是
//	调用方的责任。规范用法见 cmd/opscopilot/topology_sink.go：单把互斥锁
//	覆盖 AddDiscovery 与所有 Build() 消费（HasNode/CoverageReport 等），
//	**禁止持锁取 Build() 引用后锁外遍历**（那是无保护读，-race 未必当场
//	暴露）。gRPC 服务（GetTopology/AsOf 路径）接线时同样必须锁内完成映射。
type Builder struct {
	g *Graph
}

// NewBuilder 构造空 Builder。
func NewBuilder() *Builder {
	return &Builder{g: &Graph{Nodes: make(map[string]*Node)}}
}

// AddDiscovery 并入一批发现节点。
//
// Confidence 为零值时按 high 处理（连接器直接发现的事实，见 NodeInput 注释）；
// 显式传入的非法取值会被拒绝。
func (b *Builder) AddDiscovery(in []NodeInput) error {
	if b == nil || b.g == nil {
		return errors.New("topology: nil builder")
	}
	for i, n := range in {
		if n.Key == "" {
			return fmt.Errorf("topology: discovery[%d]: empty key", i)
		}
		conf := n.Confidence
		if conf == "" {
			conf = ConfidenceHigh // 直接发现 = 观测事实
		}
		// 严格校验：非法取值（如拼写错误的档位）必须拒绝，不得静默降级。
		if _, err := ParseConfidence(string(conf)); err != nil {
			return fmt.Errorf("topology: discovery[%d]: %w", i, err)
		}
		observed := n.ObservedAt
		if observed.IsZero() {
			observed = time.Now()
		}

		if existing, ok := b.g.Nodes[n.Key]; ok {
			// 合并：取高置信度、最早 ValidFrom、最新 Source、新 Labels 覆盖
			existing.Confidence = maxConf(existing.Confidence, conf)
			if existing.ValidFrom.IsZero() || observed.Before(existing.ValidFrom) {
				existing.ValidFrom = observed
			}
			if n.Source != "" {
				existing.Source = n.Source
			}
			if existing.Labels == nil {
				existing.Labels = make(map[string]string, len(n.Labels))
			}
			for k, v := range n.Labels {
				if v != "" {
					existing.Labels[k] = v
				}
			}
			if n.Type != "" {
				existing.Type = n.Type
			}
			existing.ValidTo = time.Time{} // 仍被观测到 → 当前有效
			continue
		}
		b.g.Nodes[n.Key] = &Node{
			Key:        n.Key,
			Type:       n.Type,
			Labels:     n.Labels,
			Confidence: conf,
			Source:     n.Source,
			ValidFrom:  observed,
		}
	}
	return nil
}

// AddEdge 加入一条边。端点必须已在图中（先 AddDiscovery/AddNode），
// 否则拒绝——不允许构建出悬空引用的图。
// Confidence 为零值时按 low 处理（边几乎都是推断，见 EdgeInput 注释）。
func (b *Builder) AddEdge(e EdgeInput) error {
	if b == nil || b.g == nil {
		return errors.New("topology: nil builder")
	}
	if e.SrcKey == "" || e.DstKey == "" {
		return errors.New("topology: edge requires src and dst")
	}
	if e.Relation == "" {
		return fmt.Errorf("topology: edge %s->%s: empty relation", e.SrcKey, e.DstKey)
	}
	if _, ok := b.g.Nodes[e.SrcKey]; !ok {
		return fmt.Errorf("topology: edge src %q not in graph (add nodes first)", e.SrcKey)
	}
	if _, ok := b.g.Nodes[e.DstKey]; !ok {
		return fmt.Errorf("topology: edge dst %q not in graph (add nodes first)", e.DstKey)
	}
	conf := e.Confidence
	if conf == "" {
		conf = ConfidenceLow // 未声明的边证据按最低档（保守）
	}
	if _, err := ParseConfidence(string(conf)); err != nil {
		return fmt.Errorf("topology: edge %s->%s: %w", e.SrcKey, e.DstKey, err)
	}
	observed := e.ObservedAt
	if observed.IsZero() {
		observed = time.Now()
	}

	for _, existing := range b.g.Edges {
		if existing.SrcKey == e.SrcKey && existing.DstKey == e.DstKey && existing.Relation == e.Relation {
			existing.Confidence = maxConf(existing.Confidence, conf)
			if observed.Before(existing.ValidFrom) {
				existing.ValidFrom = observed
			}
			if e.Source != "" {
				existing.Source = e.Source
			}
			existing.ValidTo = time.Time{}
			return nil
		}
	}
	b.g.Edges = append(b.g.Edges, &Edge{
		SrcKey:     e.SrcKey,
		DstKey:     e.DstKey,
		Relation:   e.Relation,
		Confidence: conf,
		Source:     e.Source,
		ValidFrom:  observed,
	})
	return nil
}

// AddNode 加入单个节点（等价于单元素 AddDiscovery，便于显式加虚拟节点）。
func (b *Builder) AddNode(n NodeInput) error {
	return b.AddDiscovery([]NodeInput{n})
}

// Build 返回当前图。
//
// ⚠️ 语义澄清（第三轮复审 R2，原注释"浅拷贝快照"系失真描述）：本方法
// 返回的是**内部 Graph 的原指针，零拷贝**——
//   - 继续使用 Builder（AddDiscovery 等）**会**改变已返回图的内容；
//   - 修改返回图中的 Node/Edge 同样会影响 Builder；
//   - Node/Edge 指针与图共享（AsOf/CausalSubgraph 的结果同此约定）。
//
// "图是不可变约定，调用方不得修改返回图"依然成立；并发读写互斥是
// 调用方责任（见 Builder 类型级并发契约）。
func (b *Builder) Build() *Graph {
	return b.g
}
