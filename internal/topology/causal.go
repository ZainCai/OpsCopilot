package topology

// 因果门禁与覆盖率自检（W3 验收：低置信度不进因果推理 / 覆盖率自检输出）。

// Stats 拓扑图统计（覆盖率自检输出）。
//
// 用法：对比 Nodes/Edges 与 CausalNodes/CausalEdges，即知被门禁拦下多少证据；
// 持续输出该报告是 ADR-007 后续演进（medium 是否收紧 / low 是否升级）的数据依据。
type Stats struct {
	// Nodes / Edges 图中全部节点与边数量。
	Nodes int
	Edges int
	// NodesByConfidence / EdgesByConfidence 按置信度分桶。
	NodesByConfidence map[Confidence]int
	EdgesByConfidence map[Confidence]int
	// NodesBySource 按来源统计节点数（多源合并后计入最新观测者）。
	NodesBySource map[string]int
	// CausalNodes / CausalEdges 通过因果门禁的数量。
	CausalNodes int
	CausalEdges int
}

// CausalSubgraph 返回允许进入因果推理的子图（ADR-007 硬门禁）：
//
//  1. 剔除 low 级边（即使两端节点都是 high——低质量关系一样会误导推理）；
//  2. 剔除 low 级节点；
//  3. 剔除因端点被剔除而悬空的边（保持图一致性）。
//
// 返回新图，不修改原图。
func (g *Graph) CausalSubgraph() *Graph {
	out := &Graph{Nodes: make(map[string]*Node)}
	// 1) 先滤 low 边
	keptEdges := make([]*Edge, 0, len(g.Edges))
	for _, e := range g.Edges {
		if e.Confidence != ConfidenceLow {
			keptEdges = append(keptEdges, e)
		}
	}
	// 2) 滤 low 节点
	for k, n := range g.Nodes {
		if n.Confidence != ConfidenceLow {
			out.Nodes[k] = n
		}
	}
	// 3) 剔悬空边
	for _, e := range keptEdges {
		if _, ok := out.Nodes[e.SrcKey]; !ok {
			continue
		}
		if _, ok := out.Nodes[e.DstKey]; !ok {
			continue
		}
		out.Edges = append(out.Edges, e)
	}
	return out
}

// Stats 计算统计信息（含因果门禁通过率）。
func (g *Graph) Stats() Stats {
	s := Stats{
		Nodes:             len(g.Nodes),
		Edges:             len(g.Edges),
		NodesByConfidence: make(map[Confidence]int),
		EdgesByConfidence: make(map[Confidence]int),
		NodesBySource:     make(map[string]int),
	}
	for _, n := range g.Nodes {
		s.NodesByConfidence[n.Confidence]++
		s.NodesBySource[n.Source]++
	}
	for _, e := range g.Edges {
		s.EdgesByConfidence[e.Confidence]++
	}
	causal := g.CausalSubgraph()
	s.CausalNodes = len(causal.Nodes)
	s.CausalEdges = len(causal.Edges)
	return s
}
