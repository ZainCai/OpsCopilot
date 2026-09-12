// 按需 RCA 编排器（优化方案 #12 最小链路，ADR-014）。
//
// 为什么在 cmd/（package main）：六步流水线（internal/rca）按边界纪律
// 不得 import topology/incident/noise/config——取证与数据搬运必须发生在
// 跨模块唯一合法汇合点（v1.3 §5.2）。本文件负责：
//
//	incident（Get）→ 故障域节点（noise 簇 NodeKeys，内存聚类器运行时视图）
//	→ 时点拓扑取证（SemanticModel.GetTopology as_of=T0，锁内映射 R2 纪律）
//	→ 变更窗口取证（SemanticModel.GetRecentChanges [T0-Window, T0]）
//	→ rca.NewDefaultPipeline 六步 → 审计留痕（append-only AuditLog）→ 报告。
//
// LLM 单出口（ADR-003）：conclude 步的 Summarizer 由装配注入——#3/ADR-015
// 起 llm-gateway 已接线（OPS_LLM_ENDPOINT 非空时经 internal/llmgw 唯一
// 出站调用点注入，见 assembly.go 与 llm_summarizer.go）；未配置时恒传
// nil，conclude 记 pending，报告以结构化证据链交付（禁止伪 RCA）。
//
// TODO(#12 二期挂点说明)：
//   - sessionstore（internal/sessionstore，P1-1 预留件）尚无运行时消费方——
//     若二期做"分析会话/网关不可用时延迟重跑队列"（ADR-003 二档降级：gateway 全不可用
//     时请求入延迟队列），挂点即在 cmd 构造 redis alert client 处
//     （main.go 的 Redis 出口）→ sessionstore.New(client, config.RedisAlert)；
//   - 自动触发（Escalation 联动）已随二期池波二 #4 落地：cmd/opscopilot/rca_auto.go
//     在 critical 升级成功后复用本编排器 Analyze(actor="auto")，本文件零改动。
package main

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"opscopilot/internal/config"
	pb "opscopilot/internal/contracts/pb"
	"opscopilot/internal/incident"
	"opscopilot/internal/noise"
	"opscopilot/internal/rca"
)

// ErrRCAIncidentNotFound 分析目标事件不存在（REST 映射 404）。
var ErrRCAIncidentNotFound = errors.New("rca: incident not found")

// rcaMaxDepth 邻域取证跳数上限——与 SemanticModelServer.maxTopologyDepth
// 同值同理由（BFS 空转防护）；两处如改动须同步（REST 与编排共用该口径）。
const rcaMaxDepth = 10

// RCAOrchestrator 单次"取 incident → 取证 → 六步 → 落审计"的编排器。
// 只读分析 + 一条审计追加，无其他副作用；并发安全（依赖组件均并发安全）。
type RCAOrchestrator struct {
	spec      config.RCASection
	incidents incident.Store
	sem       *SemanticModelServer // 取证唯一数据出口（gRPC 语义 REST 直调先例）
	noise     *NoiseEngine         // 故障域来源（簇 NodeKeys）；nil = 降噪关闭
	audit     AuditLog
	tenant    string
	m         *AppMetrics
	logf      func(string, ...any)
	// summarizer LLM 单出口挂点（ADR-003）；nil = conclude pending。
	summarizer rca.Summarizer
}

// NewRCAOrchestrator 构造。sem/incidents 必非 nil（装配是唯一入口）；
// noise 可为 nil（OPS_NOISE_SHADOW=off：无故障域，链路仍可跑通出
// "证据不足"报告）；audit 可为 nil（不落留痕，嵌入式测试用）。
func NewRCAOrchestrator(spec config.RCASection, incidents incident.Store, sem *SemanticModelServer,
	noise *NoiseEngine, audit AuditLog, tenant string, m *AppMetrics, logf func(string, ...any)) *RCAOrchestrator {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	return &RCAOrchestrator{spec: spec, incidents: incidents, sem: sem, noise: noise,
		audit: audit, tenant: tenant, m: m, logf: logf}
}

// SetSummarizer 注入 LLM 摘要出口（二期 llm-gateway 接线用；装配期调用）。
func (o *RCAOrchestrator) SetSummarizer(s rca.Summarizer) { o.summarizer = s }

// RCAResult 分析结果 + 编排层元信息（REST 契约的原料；JSON 序列化在
// rest_rca.go，本结构保持 Go 语义）。
type RCAResult struct {
	IncidentID  string
	ClusterKeys []string
	T0          time.Time
	Window      time.Duration
	Report      *rca.Report
}

// Analyze 执行一次按需 RCA。actor 为触发者（查询方提供，审计用；
// 空 = "system:rca"）。错误分类：
//   - ErrRCADisabled：OPS_RCA=off（调用方本不该构造，双保险）；
//   - ErrRCAIncidentNotFound：事件不存在（含 DB 故障外的真未找到）；
//   - context.DeadlineExceeded：整体超时（spec.Timeout 或调用方 ctx 更早）；
//   - 其余：incident Store / 取证内部错误（REST 侧脱敏为 500）。
func (o *RCAOrchestrator) Analyze(ctx context.Context, incidentID, actor string) (*RCAResult, error) {
	if !o.spec.Enabled {
		return nil, ErrRCADisabled
	}
	start := time.Now()
	res, err := o.analyze(ctx, incidentID, actor, start)
	o.m.CountRCA(boolOutcome(err))
	o.m.ObserveRCALatency(time.Since(start))
	return res, err
}

// ErrRCADisabled 开关关闭时的显式错误（REST 503 可诊断，不外泄 env 细节）。
var ErrRCADisabled = errors.New("rca: disabled by OPS_RCA=off")

func boolOutcome(err error) string {
	if err != nil {
		return "error"
	}
	return "ok"
}

func (o *RCAOrchestrator) analyze(ctx context.Context, incidentID, actor string, start time.Time) (*RCAResult, error) {
	// 整体超时：取证 IO + 六步纯计算。spec.Timeout 缺省兜底 10s。
	timeout := o.spec.Timeout
	if timeout <= 0 {
		timeout = config.DefaultRCATimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	inc, err := o.incidents.Get(incidentID)
	if err != nil {
		if errors.Is(err, incident.ErrNotFound) {
			return nil, fmt.Errorf("%w: %s", ErrRCAIncidentNotFound, incidentID)
		}
		return nil, err
	}
	// RCA 铁律：T0 = 事件创建时刻（故障被记录的时刻），禁止用"当前拓扑"。
	t0 := inc.CreatedAt
	if t0.IsZero() {
		t0 = time.Now()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	window := o.spec.Window
	if window <= 0 {
		window = config.DefaultRCAWindow
	}
	depth := o.spec.Depth
	if depth <= 0 {
		depth = config.DefaultRCADepth
	}
	if depth > rcaMaxDepth {
		depth = rcaMaxDepth
	}

	// ① 故障域：事件簇关联 → 内存聚类器 NodeKeys 并集。
	// 簇不在内存视图（重启后未 Restore / 已淘汰）→ 域为空，链路照常跑通
	// 输出"证据不足"报告（宁缺毋滥，不拿全图凑数）。
	domain, missingClusters := o.faultDomain(inc.ClusterKeys)

	// ② 时点拓扑取证（as_of=T0）+ 邻域裁剪。GetTopology 的映射在拓扑锁内
	// 完成（R2），返回的是 pb 值拷贝——锁外安全消费。
	// RFC3339**Nano**：契约解析吃小数秒；秒级截断会把 ValidFrom 带亚秒
	// 的刚观测节点系统性判成"t 时还没被观测到"（取证漏报）。
	topo, err := o.sem.GetTopology(ctx, &pb.GetTopologyRequest{
		TenantId: o.tenant,
		AsOf:     t0.Format(time.RFC3339Nano),
	})
	if err != nil {
		return nil, fmt.Errorf("rca: as_of topology: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	nodes, edges := neighborhoodFacts(topo, domain, depth)

	// ③ 变更窗口取证（[T0-Window, T0] 全局查询后按邻域过滤在步内做）。
	chResp, err := o.sem.GetRecentChanges(ctx, &pb.GetRecentChangesRequest{
		TenantId:    o.tenant,
		WindowStart: t0.Add(-window).Format(time.RFC3339Nano),
		WindowEnd:   t0.Format(time.RFC3339Nano),
	})
	if err != nil {
		return nil, fmt.Errorf("rca: change window: %w", err)
	}
	changes := make([]rca.ChangeFact, 0, len(chResp.GetChanges()))
	for _, c := range chResp.GetChanges() {
		at, perr := time.Parse(time.RFC3339, c.GetOccurredAt())
		if perr != nil {
			// 时间戳不可解析的变更不进证据链（脏数据不得参与归因），
			// 但必须留日志——静默丢证据是最难查的坑。
			o.logf("WARNING: rca skip change %s: bad occurred_at %q: %v", c.GetId(), c.GetOccurredAt(), perr)
			continue
		}
		changes = append(changes, rca.ChangeFact{
			ID: c.GetId(), NodeKey: c.GetNodeKey(), ChangeType: c.GetChangeType(),
			Source: c.GetSource(), Actor: c.GetActor(), Summary: c.GetSummary(),
			OccurredAt: at, Confidence: c.GetConfidence(),
		})
	}

	in := rca.Input{
		TenantID:     o.tenant,
		ClusterKey:   firstOf(inc.ClusterKeys),
		T0:           t0,
		Window:       window,
		AlertedNodes: domain,
		Evidence:     rca.Evidence{Nodes: nodes, Edges: edges, Changes: changes},
	}
	rep, rerr := o.runPipeline(ctx, in)
	if rerr != nil {
		return nil, rerr
	}
	if len(missingClusters) > 0 {
		o.logf("rca: incident %s clusters not in memory view: %s (evidence domain may be empty)",
			incidentID, strings.Join(missingClusters, ","))
	}

	// ④ 审计留痕（append-only；分析结论本身随响应返回，审计记"发生了什么"）。
	if o.audit != nil {
		done, pending, failed := 0, 0, 0
		for _, sr := range rep.Steps {
			switch sr.Status {
			case rca.StatusDone:
				done++
			case rca.StatusPending:
				pending++
			case rca.StatusFailed:
				failed++
			}
		}
		llmUsed := findingExists(rep, rca.StepConclude)
		if actor == "" {
			actor = "system:rca"
		}
		if len(actor) > 64 {
			actor = actor[:64]
		}
		o.audit.Append(AuditEntry{IncidentID: incidentID, Action: AuditRCA, Actor: actor,
			Detail: map[string]any{
				"t0":            t0.Format(time.RFC3339),
				"window":        window.String(),
				"cluster_keys":  inc.ClusterKeys,
				"domain_nodes":  len(domain),
				"evidence":      map[string]int{"nodes": len(nodes), "edges": len(edges), "changes": len(changes)},
				"steps_done":    done,
				"steps_pending": pending,
				"steps_failed":  failed,
				"root_causes":   len(rep.RootCauses),
				"llm_used":      llmUsed,
				"duration_ms":   time.Since(start).Milliseconds(),
			}})
	}
	return &RCAResult{IncidentID: incidentID, ClusterKeys: inc.ClusterKeys,
		T0: t0, Window: window, Report: rep}, nil
}

// runPipeline 在独立 goroutine 里跑六步（纯计算，但邻域遍历规模不可
// 先验保证），ctx 到期即放弃——分析超时是运维事实，不是悬挂请求的理由。
// goroutine 不随 ctx 终止（步骤无 IO 不可中断），但有 recover 且写入带
// 缓冲 channel，不会泄漏阻塞。
func (o *RCAOrchestrator) runPipeline(ctx context.Context, in rca.Input) (*rca.Report, error) {
	type out struct {
		rep *rca.Report
		err error
	}
	ch := make(chan out, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				ch <- out{err: fmt.Errorf("rca: step panic: %v", r)}
			}
		}()
		rep, err := rca.NewDefaultPipeline(o.summarizer).Run(in)
		ch <- out{rep, err}
	}()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-ch:
		return res.rep, res.err
	}
}

func findingExists(rep *rca.Report, step string) bool {
	for _, f := range rep.Findings {
		if f.Step == step {
			return true
		}
	}
	return false
}

func firstOf(ss []string) string {
	if len(ss) == 0 {
		return ""
	}
	return ss[0]
}

// faultDomain 事件簇 → 故障域节点并集（去重排序）与"内存视图缺失的簇"清单。
// 簇读取与 rest_topology.handleClusterDetail 同源：noise 内存聚类器
// （active+resolved），cmd 层合法持有（v1.3 §5.2 汇合点）。
func (o *RCAOrchestrator) faultDomain(clusterKeys []string) (nodes, missing []string) {
	seen := map[string]struct{}{}
	for _, key := range clusterKeys {
		var cl *noise.Cluster
		if o.noise != nil {
			cl = o.noise.shadow.Clusterer().Get(key)
		}
		if cl == nil {
			missing = append(missing, key)
			continue
		}
		for k := range cl.NodeKeys {
			if _, ok := seen[k]; !ok {
				seen[k] = struct{}{}
				nodes = append(nodes, k)
			}
		}
	}
	sort.Strings(nodes)
	return nodes, missing
}

// neighborhoodFacts 把 T0 全图裁剪到故障域邻域（值拷贝投影为 rca DTO）。
// keep = 域节点 ∪ depth 跳内可达节点；域内但 T0 不可见的节点也进
// AlertedNodes（collect 步据此报"不可见"）。edges 保留两端都在 keep 的边。
func neighborhoodFacts(topo *pb.GetTopologyResponse, domain []string, depth int) ([]rca.NodeFact, []rca.EdgeFact) {
	adj := map[string][]string{}
	for _, e := range topo.GetEdges() {
		adj[e.GetSrcKey()] = append(adj[e.GetSrcKey()], e.GetDstKey())
		adj[e.GetDstKey()] = append(adj[e.GetDstKey()], e.GetSrcKey())
	}
	keep := map[string]struct{}{}
	frontier := make([]string, 0, len(domain))
	for _, k := range domain {
		if _, ok := keep[k]; !ok {
			keep[k] = struct{}{}
			frontier = append(frontier, k)
		}
	}
	for d := 0; d < depth && len(frontier) > 0; d++ {
		var next []string
		for _, cur := range frontier {
			for _, nb := range adj[cur] {
				if _, ok := keep[nb]; !ok {
					keep[nb] = struct{}{}
					next = append(next, nb)
				}
			}
		}
		frontier = next
	}
	nodes := make([]rca.NodeFact, 0, len(keep))
	for _, n := range topo.GetNodes() {
		if _, ok := keep[n.GetNodeKey()]; ok {
			nodes = append(nodes, rca.NodeFact{
				Key: n.GetNodeKey(), Type: n.GetNodeType(),
				Confidence: n.GetConfidence(), Source: n.GetSource(),
			})
		}
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].Key < nodes[j].Key })
	edges := make([]rca.EdgeFact, 0, len(topo.GetEdges()))
	for _, e := range topo.GetEdges() {
		_, s := keep[e.GetSrcKey()]
		_, d := keep[e.GetDstKey()]
		if s && d {
			edges = append(edges, rca.EdgeFact{
				SrcKey: e.GetSrcKey(), DstKey: e.GetDstKey(),
				Relation: e.GetRelation(), Confidence: e.GetConfidence(),
			})
		}
	}
	return nodes, edges
}
