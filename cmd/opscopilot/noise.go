// W4-1.4 影子降噪接线：connector.Alert → noise.Event 翻译 +
// topology.CausalSubgraph 故障域适配器 + TopologySink 挂载。
//
// 本文件是 connector / topology / noise 三个 internal 模块的唯一汇合点
// （v1.3 §5.2：internal 禁互 import，编排只落在 cmd/）。
package main

import (
	"os"
	"sync"
	"time"

	"opscopilot/internal/connector"
	"opscopilot/internal/noise"
)

// 影子降噪环境变量。
const (
	// envNoiseWindow 聚类/去重时间窗（Go duration，如 10m；默认 10m）。
	envNoiseWindow = "OPS_NOISE_WINDOW"
	// envNoiseShadow 影子开关："off" 关闭采集侧降噪处理（默认开）。
	// 关闭 = 完全不处理（连影子判决都不产生），回到 W3 只计数行为。
	envNoiseShadow = "OPS_NOISE_SHADOW"
)

// defaultNoiseWindow 影子期默认窗口：30s 采集轮询下，10 分钟足以覆盖
// 同一故障的重复告警，又不至于把相隔较远的两次独立故障并成一簇。
const defaultNoiseWindow = 10 * time.Minute

// NoiseEngine 影子降噪引擎的 cmd 侧持有者。
// 并发安全：内部互斥——域函数快照重建与 Process 串行化（采集宿主当前
// 串行调用 Sink，但本结构不依赖这一实现细节）。
type NoiseEngine struct {
	mu      sync.Mutex
	shadow  *noise.Shadow
	sink    *TopologySink // 取拓扑快照构建域函数（同包访问 graphSnapshot）
	enabled bool
	logger  connector.Logger
	// total / converged 影子统计（启动以来累计）。
	total, converged int
}

// NewNoiseEngine 按 env 构造。返回 nil 表示影子降噪关闭（envNoiseShadow=off），
// 调用方须容忍 nil（挂载点与 ProcessAlerts 均做 nil 检查）。
// 窗口解析失败视为配置错误返回 error——静默回退默认值会让运维以为
// 配置生效了。
func NewNoiseEngine(sink *TopologySink, logger connector.Logger) (*NoiseEngine, error) {
	if os.Getenv(envNoiseShadow) == "off" {
		return nil, nil
	}
	window := defaultNoiseWindow
	if raw := os.Getenv(envNoiseWindow); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil || d <= 0 {
			return nil, &invalidNoiseWindowError{raw: raw, err: err}
		}
		window = d
	}
	return &NoiseEngine{
		shadow:  noise.NewShadow(window, nil), // 域函数按批经 SetDomain 注入
		sink:    sink,
		enabled: true,
		logger:  logger,
	}, nil
}

type invalidNoiseWindowError struct {
	raw string
	err error
}

func (e *invalidNoiseWindowError) Error() string {
	return "noise: invalid " + envNoiseWindow + " " + e.raw + ": " + e.err.Error()
}

// batchView 一批采集的拓扑视图：因果邻接表 + instance→节点 Key 反查索引。
type batchView struct {
	adj      map[string][]string
	byInsEnv map[string]string // labels.instance → 节点 Key（首个命中，构建序决定）
}

// ProcessAlerts 处理一轮采集的全部告警（影子模式：不拦截，只标注）。
// 每批开始时重建拓扑快照（CausalSubgraph），批内共用——一轮采集期间
// 拓扑视为不变。
func (n *NoiseEngine) ProcessAlerts(alerts []connector.Alert) {
	if n == nil || len(alerts) == 0 {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()

	view := n.snapshotView()
	n.shadow.SetDomain(func(a, b string) bool { return reachable(view.adj, a, b) })

	var suppressed, merged, created int
	for _, a := range alerts {
		v := n.shadow.Process(n.toEvent(a, view))
		n.total++
		switch {
		case v.WouldSuppress:
			suppressed++
		case !v.ClusterCreated && v.ClusterKey != "":
			merged++
		}
		if v.WouldConverge {
			n.converged++
		}
		if v.ClusterCreated {
			created++
		}
	}
	n.logf("shadow verdict: %d alerts (new %d, dedup %d, merged %d) — cumulative converge %d/%d",
		len(alerts), created, suppressed, merged, n.converged, n.total)
}

// snapshotView 从当前拓扑图构建批次视图。
// ADR-007 low 门禁天然生效：CausalSubgraph 已剔除 low 节点/边，
// 它们进不了故障域判定。
func (n *NoiseEngine) snapshotView() batchView {
	g := n.sink.graphSnapshot().CausalSubgraph()
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
func (n *NoiseEngine) toEvent(a connector.Alert, view batchView) noise.Event {
	fp := a.Fingerprint
	if fp == "" {
		fp = noise.Fingerprint(a.Labels)
	}
	e := noise.Event{
		Fingerprint: fp,
		Severity:    a.Severity,
		OccurredAt:  a.StartsAt,
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

// Stats 影子统计快照（运维自检）。
func (n *NoiseEngine) Stats() (total, converged int) {
	if n == nil {
		return 0, 0
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.total, n.converged
}

// Enabled 影子降噪是否启用。
func (n *NoiseEngine) Enabled() bool { return n != nil && n.enabled }

func (n *NoiseEngine) logf(format string, args ...interface{}) {
	if n.logger != nil {
		n.logger.Printf(format, args...)
	}
}
