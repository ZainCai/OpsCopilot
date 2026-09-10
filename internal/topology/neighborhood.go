package topology

// Neighborhood 邻域裁剪（W5-2.1 GetTopology 的 node_key/depth 参数配套）：
// 返回以 root 为中心、depth 跳内的子图（含 root 自身）。
//
// 语义约定（契约未明确处在此定死，测试锁定）：
//   - root 不存在 → ok=false（gRPC 层映射 NotFound）；
//   - depth <= 0 → 仅含 root 的单节点图（0 跳 = 不扩展）；
//   - 边保留条件：边自身存在且两端点都在保留集内——与 AsOf 的
//     "悬空边剔除"纪律一致；
//   - 只读原图：Node/Edge 指针与原图共享（浅拷贝，与 Build/AsOf 同一
//     不可变约定），调用方不得修改；**调用方必须在拓扑锁内调用**
//     （R2：返回的是共享指针，锁外读图是无保护读）。
func (g *Graph) Neighborhood(root string, depth int) (*Graph, bool) {
	if _, ok := g.Nodes[root]; !ok {
		return nil, false
	}
	keep := map[string]struct{}{root: {}}
	if depth > 0 {
		frontier := []string{root}
		for d := 0; d < depth; d++ {
			next := make([]string, 0)
			for _, cur := range frontier {
				for _, e := range g.Edges {
					var other string
					switch {
					case e.SrcKey == cur:
						other = e.DstKey
					case e.DstKey == cur:
						other = e.SrcKey
					default:
						continue
					}
					if _, seen := keep[other]; !seen {
						keep[other] = struct{}{}
						next = append(next, other)
					}
				}
			}
			frontier = next
		}
	}
	out := &Graph{Nodes: make(map[string]*Node, len(keep))}
	for k, n := range g.Nodes {
		if _, ok := keep[k]; ok {
			out.Nodes[k] = n
		}
	}
	for _, e := range g.Edges {
		if _, ok := out.Nodes[e.SrcKey]; !ok {
			continue
		}
		if _, ok := out.Nodes[e.DstKey]; !ok {
			continue
		}
		out.Edges = append(out.Edges, e)
	}
	return out, true
}
