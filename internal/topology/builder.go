package topology

import (
	"errors"
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	"opscopilot/pkg/memguard"
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
// 内存有界化（优化方案 #6）：NewBuilderWithLimits 注入容量护栏后，
// 每次写入收尾处做超限检查——节点按**最久未活跃**（最近一次被观测/合并
// 的时刻，而非入图时刻）淘汰，被删节点关联的边一并移除（悬空边纪律）；
// 边独立上限兜底（默认取节点×4，正常规模不可能触达）。淘汰计数与
// 水位 WARN 由 memguard.Guard 承担，经 MemGuards() 挂到 /metrics。
// 默认 NewBuilder()（无护栏）行为与此前完全一致：不设限、不维护 lastSeen。
//
// 并发契约（第三轮复审 R2，调用方必读）：
//
//	Builder 自身**无锁**。AddDiscovery/AddEdge/AddNode 与 Build() 返回的
//	Graph 共享同一份可变状态——"写入方互斥 + 读操作全程持同一把锁"是
//	调用方的责任。规范用法见 cmd/opscopilot/topology_sink.go：单把互斥锁
//	覆盖 AddDiscovery 与所有 Build() 消费（HasNode/CoverageReport 等），
//	**禁止持锁取 Build() 引用后锁外遍历**（那是无保护读，-race 未必当场
//	暴露）。gRPC 服务（GetTopology/AsOf 路径）接线时同样必须锁内完成映射。
//	护栏规模读数走原子镜像（nodeCount/edgeCount），/metrics 抓取因此
//	不会无锁遍历共享 map。
type Builder struct {
	g *Graph

	nodeGuard, edgeGuard *memguard.Guard
	// lastSeen/edgeSeen 节点/边的最近一次观测（合并）时刻——淘汰依据。
	// 仅在装配了护栏时维护（无界 Builder 不付这份记账成本）。
	lastSeen map[string]time.Time
	edgeSeen map[string]time.Time
	// 规模原子镜像：gauge 回调只读这两个值，不触碰共享 map。
	nodeCount, edgeCount atomic.Int64
}

// NewBuilder 构造空 Builder（无容量上限，行为与历史一致）。
func NewBuilder() *Builder {
	return NewBuilderWithLimits(nil, nil)
}

// NewBuilderWithLimits 构造带容量护栏的 Builder。护栏 nil = 对应维度不设限。
// guard 的规模回调在此绑定（调用方之后只需 MemGuards() 注册指标）。
func NewBuilderWithLimits(nodeGuard, edgeGuard *memguard.Guard) *Builder {
	b := &Builder{
		g:         &Graph{Nodes: make(map[string]*Node)},
		nodeGuard: nodeGuard,
		edgeGuard: edgeGuard,
	}
	if nodeGuard != nil || edgeGuard != nil {
		b.lastSeen = make(map[string]time.Time)
		b.edgeSeen = make(map[string]time.Time)
	}
	nodeGuard.SetSize(b.NodeEntries)
	edgeGuard.SetSize(b.EdgeEntries)
	return b
}

// MemGuards 返回装配的护栏（供装配层注册进指标注册表；nil 项已剔除）。
func (b *Builder) MemGuards() []*memguard.Guard {
	var out []*memguard.Guard
	if b.nodeGuard != nil {
		out = append(out, b.nodeGuard)
	}
	if b.edgeGuard != nil {
		out = append(out, b.edgeGuard)
	}
	return out
}

// NodeEntries 当前节点数（原子镜像，锁外可读；仅装配了护栏时维护，
// 无界 Builder 恒 0——调用方据此区分）。
func (b *Builder) NodeEntries() int {
	return int(b.nodeCount.Load())
}

// EdgeEntries 当前边数（同上）。
func (b *Builder) EdgeEntries() int {
	return int(b.edgeCount.Load())
}

// edgeKey 边的幂等身份（与 AddEdge 合并判据一致）。
func edgeKey(src, dst, relation string) string {
	return src + "\x1f" + dst + "\x1f" + relation
}

// enforceLimitsLocked 写入收尾的超限检查与淘汰。调用方持有本图的外层锁
// （Builder 并发契约）。淘汰策略：最久未活跃（lastSeen 最早）优先，
// 同刻按 key 字典序——决定性输出。
func (b *Builder) enforceLimitsLocked() {
	if b.nodeGuard != nil {
		b.nodeCount.Store(int64(len(b.g.Nodes)))
		total := 0
		for excess := b.nodeGuard.Over(len(b.g.Nodes)); excess > 0; excess = b.nodeGuard.Over(len(b.g.Nodes)) {
			n := b.evictNodesLocked(excess)
			if n == 0 {
				break
			}
			total += n
		}
		if total > 0 {
			b.nodeGuard.Evicted(total)
		}
	}
	if b.edgeGuard != nil {
		b.edgeCount.Store(int64(len(b.g.Edges)))
		total := 0
		for excess := b.edgeGuard.Over(len(b.g.Edges)); excess > 0; excess = b.edgeGuard.Over(len(b.g.Edges)) {
			n := b.evictEdgesLocked(excess)
			if n == 0 {
				break
			}
			total += n
		}
		if total > 0 {
			b.edgeGuard.Evicted(total)
		}
	}
}

// evictNodesLocked 淘汰最久未活跃的至多 excess 个节点，连带其关联边。
// 返回实际淘汰的节点数。
func (b *Builder) evictNodesLocked(excess int) int {
	if len(b.lastSeen) == 0 {
		return 0
	}
	type ks struct {
		key string
		at  time.Time
	}
	cands := make([]ks, 0, min(len(b.lastSeen), excess))
	for k, at := range b.lastSeen {
		if _, ok := b.g.Nodes[k]; !ok {
			continue
		}
		if len(cands) < excess {
			cands = append(cands, ks{k, at})
			continue
		}
		// 保留"更早"的一侧：新候选比当前最差还早则替换。
		worst := 0
		for i := range cands {
			if cands[i].at.After(cands[worst].at) ||
				(cands[i].at.Equal(cands[worst].at) && cands[i].key > cands[worst].key) {
				worst = i
			}
		}
		if at.Before(cands[worst].at) || (at.Equal(cands[worst].at) && k < cands[worst].key) {
			cands[worst] = ks{k, at}
		}
	}
	sort.Slice(cands, func(i, j int) bool {
		if !cands[i].at.Equal(cands[j].at) {
			return cands[i].at.Before(cands[j].at)
		}
		return cands[i].key < cands[j].key
	})
	evicted := 0
	droppedEdges := 0
	for _, c := range cands {
		if _, ok := b.g.Nodes[c.key]; !ok {
			continue
		}
		delete(b.g.Nodes, c.key)
		delete(b.lastSeen, c.key)
		droppedEdges += b.dropEdgesOfLocked(c.key)
		evicted++
	}
	// 节点淘汰的连带删边计入边护栏——边的 gauge/counter 与实际一致，
	// 不藏"看不见的删除"。
	if b.edgeGuard != nil && droppedEdges > 0 {
		b.edgeGuard.Evicted(droppedEdges)
	}
	b.nodeCount.Store(int64(len(b.g.Nodes)))
	b.edgeCount.Store(int64(len(b.g.Edges)))
	return evicted
}

// dropEdgesOfLocked 摘除与 key 关联的全部边（悬空边纪律）及其 lastSeen，
// 返回删除条数。
func (b *Builder) dropEdgesOfLocked(key string) int {
	kept := b.g.Edges[:0]
	dropped := 0
	for _, e := range b.g.Edges {
		if e.SrcKey == key || e.DstKey == key {
			delete(b.edgeSeen, edgeKey(e.SrcKey, e.DstKey, e.Relation))
			dropped++
			continue
		}
		kept = append(kept, e)
	}
	b.g.Edges = kept
	return dropped
}

// evictEdgesLocked 淘汰最久未观测的至多 excess 条边。返回实际条数。
// 逐轮找最旧：excess 在触限场景下是尾差（通常 1~个位数），
// O(E×excess) 只在病态规模出现，正常路径零成本。
func (b *Builder) evictEdgesLocked(excess int) int {
	drop := make(map[int]struct{}, excess)
	for removed := 0; removed < excess; removed++ {
		worstIdx, worstAt := -1, time.Time{}
		for i, e := range b.g.Edges {
			if _, gone := drop[i]; gone {
				continue
			}
			at, ok := b.edgeSeen[edgeKey(e.SrcKey, e.DstKey, e.Relation)]
			if !ok {
				at = e.ValidFrom
			}
			if worstIdx < 0 || at.Before(worstAt) {
				worstIdx, worstAt = i, at
			}
		}
		if worstIdx < 0 {
			break
		}
		drop[worstIdx] = struct{}{}
	}
	if len(drop) == 0 {
		return 0
	}
	kept := b.g.Edges[:0]
	for i, e := range b.g.Edges {
		if _, gone := drop[i]; gone {
			delete(b.edgeSeen, edgeKey(e.SrcKey, e.DstKey, e.Relation))
			continue
		}
		kept = append(kept, e)
	}
	b.g.Edges = kept
	b.edgeCount.Store(int64(len(b.g.Edges)))
	return len(drop)
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
			b.noteNodeSeen(n.Key, observed)
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
		b.noteNodeSeen(n.Key, observed)
	}
	// 写入收尾做超限检查（成功批次；错误早退时由下一批补上——
	// 触限本是病态规模，尾差一批无碍）。
	b.enforceLimitsLocked()
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
			b.noteEdgeSeen(e.SrcKey, e.DstKey, e.Relation, observed)
			b.enforceLimitsLocked()
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
	b.noteEdgeSeen(e.SrcKey, e.DstKey, e.Relation, observed)
	b.enforceLimitsLocked()
	return nil
}

// noteNodeSeen / noteEdgeSeen 维护"最久未活跃"判定依据（仅装配护栏时）。
func (b *Builder) noteNodeSeen(key string, at time.Time) {
	if b.lastSeen == nil {
		return
	}
	b.lastSeen[key] = at
}

func (b *Builder) noteEdgeSeen(src, dst, relation string, at time.Time) {
	if b.edgeSeen == nil {
		return
	}
	b.edgeSeen[edgeKey(src, dst, relation)] = at
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
