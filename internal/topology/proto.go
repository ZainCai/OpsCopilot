package topology

import (
	"sort"
	"time"

	pb "opscopilot/internal/contracts/pb"
)

// 契约层映射：拓扑图 → pb（gRPC API 与 W5 控制台直接消费）。

// fmtRFC3339 时间 → 契约字符串。零值输出空串
// （ValidTo 为空即"当前有效"的契约表达，与 as_of 语义配套）。
func fmtRFC3339(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(time.RFC3339)
}

// ToProto 映射到契约层 pb.TopologyNode。
//
// 注意：Labels **不映射**——pb 契约暂无该字段（ADR-007 交叉检查 2）。
// 若 W5 展示层需要标签，届时评估扩契约，不得挪用既有字段承载。
func (n *Node) ToProto() *pb.TopologyNode {
	return &pb.TopologyNode{
		NodeKey:    n.Key,
		NodeType:   n.Type,
		ValidFrom:  fmtRFC3339(n.ValidFrom),
		ValidTo:    fmtRFC3339(n.ValidTo),
		Confidence: string(n.Confidence),
		Source:     n.Source,
	}
}

// ToProto 映射到契约层 pb.TopologyEdge。
func (e *Edge) ToProto() *pb.TopologyEdge {
	return &pb.TopologyEdge{
		SrcKey:     e.SrcKey,
		DstKey:     e.DstKey,
		Relation:   e.Relation,
		Confidence: string(e.Confidence),
		Source:     e.Source,
	}
}

// ToProto 全图映射。
// 节点按 Key 排序、边保持插入序——输出稳定，便于测试与增量对比。
// ResolvedAsOf 由调用方按查询语义填写（W3 暂不支持历史时点，留空即"当前"）。
func (g *Graph) ToProto() *pb.GetTopologyResponse {
	resp := &pb.GetTopologyResponse{}
	keys := make([]string, 0, len(g.Nodes))
	for k := range g.Nodes {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		resp.Nodes = append(resp.Nodes, g.Nodes[k].ToProto())
	}
	for _, e := range g.Edges {
		resp.Edges = append(resp.Edges, e.ToProto())
	}
	return resp
}
