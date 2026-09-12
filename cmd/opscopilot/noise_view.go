// noise_view.go——拓扑批次视图与告警翻译（#9 同包拆分自 noise.go，
// 函数体逐字未动）：snapshotView 从 TopologySink 取因果子图并建邻接表/
// instance 反查索引；toEvent 翻译 connector.Alert → noise.Event；
// reachable 故障域 BFS 连通判定。
//
// 锁纪律声明：本包 n.mu 的锁边界跨文件仍然成立（权威定义见 noise.go 的
// NoiseEngine 结构注释与 noise_process.go 中 ProcessAlerts 的"锁边界"
// 注释）。本文件的拓扑读取只经 Sink.CausalSnapshot（其锁内裁剪 + 返回
// 拷贝），绝不在锁外遍历共享图；toEvent/reachable 是批内只读纯函数，
// 不触任何锁。
package main

import (
	"time"

	"opscopilot/internal/connector"
	"opscopilot/internal/noise"
)

// batchView 一批采集的拓扑视图：因果邻接表 + instance→节点 Key 反查索引。
type batchView struct {
	adj      map[string][]string
	byInsEnv map[string]string // labels.instance → 节点 Key（首个命中，构建序决定）
}

// snapshotView 从当前拓扑图构建批次视图。
// ADR-007 low 门禁天然生效：CausalSubgraph 已剔除 low 节点/边，
// 它们进不了故障域判定。
//
// 走 Sink.CausalSnapshot（锁内裁剪 + 返回拷贝）：不能用
// graphSnapshot().CausalSubgraph()——那是在锁外遍历共享图，属无保护读。
func (n *NoiseEngine) snapshotView() batchView {
	g := n.sink.CausalSnapshot()
	view := batchView{
		adj:      make(map[string][]string, len(g.Nodes)),
		byInsEnv: make(map[string]string, len(g.Nodes)),
	}
	for _, e := range g.Edges {
		view.adj[e.SrcKey] = append(view.adj[e.SrcKey], e.DstKey)
		view.adj[e.DstKey] = append(view.adj[e.DstKey], e.SrcKey)
	}
	for k, node := range g.Nodes {
		if ins := node.Labels["instance"]; ins != "" {
			if _, ok := view.byInsEnv[ins]; !ok {
				view.byInsEnv[ins] = k
			}
		}
	}
	return view
}

// toEvent 翻译 connector.Alert → noise.Event。
// Fingerprint：上游为空时用 noise.Fingerprint(labels) 兜底（规范化哈希）。
// NodeKey：经 instance→Key 索引反查；反查不到置空——无法定位的告警
// 不参与域聚合，只能靠指纹命中簇。
// OccurredAt：上游 startsAt 缺失/格式坏时 parseTime 返回零值
// （第五轮审核 G2）——零值会掉进 1970 年窗口桶且被去重器判为窗口内
// 重复而吞掉，兜底为当前观察时刻。noise 保持纯语义（调用方给确定时间），
// 时间修补责任在装配层。
func (n *NoiseEngine) toEvent(a connector.Alert, view batchView) noise.Event {
	fp := a.Fingerprint
	if fp == "" {
		fp = noise.Fingerprint(a.Labels)
	}
	at := a.StartsAt
	if at.IsZero() {
		at = time.Now()
	}
	e := noise.Event{
		Fingerprint: fp,
		Severity:    a.Severity,
		OccurredAt:  at,
		NodeKey:     view.byInsEnv[a.Labels["instance"]],
	}
	if name := a.Labels["alertname"]; name != "" {
		e.Summary = name
	}
	return e
}

// reachable 邻接表 BFS 连通判定（同一故障域 = causal 图上可达）。
func reachable(adj map[string][]string, a, b string) bool {
	if a == b {
		return true
	}
	seen := map[string]bool{a: true}
	queue := []string{a}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, next := range adj[cur] {
			if next == b {
				return true
			}
			if !seen[next] {
				seen[next] = true
				queue = append(queue, next)
			}
		}
	}
	return false
}
