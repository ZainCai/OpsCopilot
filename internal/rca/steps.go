// 六步流水线的"规则 + 证据"实现（优化方案 #12 最小链路，ADR-014）。
//
// 实现度（六步各自，详见 ADR-014 决策 3）：
//   - collect / hypothesize / verify / attribution / recommend：真做（确定性
//     规则，可单测复现——步骤只读 Input，无 IO 无副作用）；
//   - conclude：唯一 LLM 挂点，Summarizer 未注入时返回 ErrNotImplemented
//     记 pending。ADR-003 硬性禁令"LLM 参与度不足不得输出结论形态"在这里
//     的落地方式就是：**没有出口就没有结论文本**，报告以结构化证据链交付。
//
// 置信度纪律（ADR-007 同源口径，封闭三档 high/medium/low）：
//   - 空/未知置信度一律按 low 处理（保守原则，与 ParseConfidence 同向）；
//   - 假设/归因的置信度 = 证据链上最弱一环（变更置信 × 节点因果门禁），
//     取低不取高；low 不进入归因（因果门禁硬约束）。
package rca

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// 步骤名（封闭集合；rootCauses 挑拣与 REST 契约共用这些值）。
const (
	StepCollect     = "collect"
	StepHypothesize = "hypothesize"
	StepVerify      = "verify"
	StepAttribution = "attribution"
	StepConclude    = "conclude"
	StepRecommend   = "recommend"
)

// ---------- 证据 DTO（编排层填充；rca 不 import 任何兄弟模块） ----------

// NodeFact 时点拓扑节点（拓扑 AsOf(T0) 的值拷贝投影）。
type NodeFact struct {
	Key        string
	Type       string
	Confidence string // high / medium / low（ADR-007）
	Source     string
}

// EdgeFact 时点拓扑边（邻域裁剪后两端必在 Evidence.Nodes 内）。
type EdgeFact struct {
	SrcKey     string
	DstKey     string
	Relation   string
	Confidence string // high / medium / low
}

// ChangeFact 窗口内变更事件（[T0-Window, T0]，ChangeBackend 值拷贝投影）。
type ChangeFact struct {
	ID         string // 幂等键（commit sha / build id / UUID）
	NodeKey    string
	ChangeType string // deploy / config_change / rollback（上游封闭集合）
	Source     string
	Actor      string
	Summary    string
	OccurredAt time.Time
	Confidence string // high / medium / low（外部声明默认 medium）
}

// Evidence 预采集证据集（拓扑邻域 + 窗口变更）。
type Evidence struct {
	Nodes   []NodeFact
	Edges   []EdgeFact
	Changes []ChangeFact
}

// Summarizer LLM 摘要唯一挂点（ADR-003：实现必须经 llm-gateway 单出口，
// 严禁在 internal 里直连模型）。#12 本期不接线——cmd 装配传 nil，
// conclude 步 pending；二期在 assembly 注入 gateway 客户端实现。
type Summarizer interface {
	Summarize(in Input, prev []Finding) (string, error)
}

// NewDefaultPipeline 构造 #12 最小链路的六步流水线（顺序即执行序）。
// summarizer 传 nil = conclude pending（无 LLM 的确定性证据报告形态）。
func NewDefaultPipeline(summarizer Summarizer) *Pipeline {
	return NewPipeline(
		collectStep{},
		hypothesizeStep{},
		verifyStep{},
		attributionStep{},
		concludeStep{summarizer: summarizer},
		recommendStep{},
	)
}

// ---------- 步 1：取证 ----------

type collectStep struct{}

func (collectStep) Name() string { return StepCollect }

// Run 汇总时点证据盘点：故障域节点在 T0 拓扑的可见性、因果门禁通过率、
// 窗口变更条数。本步只陈述"看到了什么"，置信度反映证据完整度。
func (collectStep) Run(in Input, _ []Finding) ([]Finding, error) {
	nodes := indexNodes(in.Evidence.Nodes)
	domain := dedupSorted(in.AlertedNodes)

	var invisible, gated []string
	for _, k := range domain {
		n, ok := nodes[k]
		if !ok {
			invisible = append(invisible, k)
			continue
		}
		if confRank(n.Confidence) == confRank("low") {
			gated = append(gated, k)
		}
	}
	causal := 0
	for _, n := range in.Evidence.Nodes {
		if confRank(n.Confidence) > confRank("low") {
			causal++
		}
	}
	windowNote := "不设下界"
	if in.Window > 0 {
		windowNote = fmt.Sprintf("%s 内", in.Window)
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "取证：故障域 %d 节点；T0 时点邻域 %d 节点 / %d 边（因果门禁通过 %d），窗口变更 %d 条（T0 前%s）",
		len(domain), len(in.Evidence.Nodes), len(in.Evidence.Edges), causal, len(in.Evidence.Changes), windowNote)
	if len(invisible) > 0 {
		fmt.Fprintf(&sb, "；%d 个故障域节点在 T0 拓扑不可见（%s）", len(invisible), strings.Join(invisible, ","))
	}
	if len(gated) > 0 {
		fmt.Fprintf(&sb, "；%d 个节点为 low 置信不进因果推理（%s）", len(gated), strings.Join(gated, ","))
	}

	conf := ConfidenceHigh
	switch {
	case len(domain) == 0:
		conf = ConfidenceLow // 无可定位故障域：证据盘点无锚点
	case len(invisible) > 0 || len(gated) > 0:
		conf = ConfidenceMedium // 域不完整或被门禁削边
	}
	return []Finding{{
		Step:       StepCollect,
		Summary:    sb.String(),
		NodeKeys:   domain,
		Confidence: string(conf),
	}}, nil
}

// ---------- 步 2：假设 ----------

type hypothesizeStep struct{}

func (hypothesizeStep) Name() string { return StepHypothesize }

// Run 规则假设："故障前窗口内，谁动了故障域（或其一跳邻域）？"
// 每条命中变更生成一条假设；置信度 = 变更置信与节点因果档取低，
// 邻域（非告警命中）再降一档。无命中 → 输出一条 low 的"无变更证据"
// 假设（如实陈述，不臆造根因）。
func (hypothesizeStep) Run(in Input, prev []Finding) ([]Finding, error) {
	nodes := indexNodes(in.Evidence.Nodes)
	domain := stringSet(in.AlertedNodes)
	adj := adjacency(in.Evidence.Edges)

	var out []Finding
	for _, c := range in.Evidence.Changes {
		if !withinWindow(c.OccurredAt, in.T0, in.Window) {
			continue // 防御性复核：窗口过滤是编排层职责，但步骤不信任输入
		}
		inDomain := domain[c.NodeKey] != ""
		if !inDomain && !neighborOf(adj, domain, c.NodeKey) {
			continue // 与故障域无关的变更不构成假设
		}
		conf := minConf(c.Confidence, confOf(nodes, c.NodeKey))
		if !inDomain {
			conf = downgrade(conf) // 只触及一跳邻域：降一档
		}
		if conf == ConfidenceLow && !inDomain {
			continue // low 假设且不在域内：噪声，不进链（ADR-007）
		}
		out = append(out, Finding{
			Step: StepHypothesize,
			Summary: fmt.Sprintf("假设：变更 %s（%s，%s %s）于故障前 %s 触及节点 %s%s",
				c.ID, c.ChangeType, c.Source, actorOf(c), agoText(in.T0, c.OccurredAt), c.NodeKey,
				summarySuffix(c.Summary)),
			NodeKeys:   []string{c.NodeKey},
			Confidence: string(conf),
			Ref:        c.ID,
		})
	}
	if len(out) == 0 {
		return []Finding{{
			Step:       StepHypothesize,
			Summary:    "窗口内无触及故障域（或其邻域）的变更记录——假设转向容量/依赖/外部因素，需补充数据源",
			NodeKeys:   dedupSorted(in.AlertedNodes),
			Confidence: string(ConfidenceLow),
		}}, nil
	}
	// 确定性输出序：先置信高后低，同级按变更时间贴近 T0。
	sort.SliceStable(out, func(i, j int) bool {
		ci, cj := confRank(out[i].Confidence), confRank(out[j].Confidence)
		if ci != cj {
			return ci > cj
		}
		return out[i].Ref < out[j].Ref
	})
	return out, nil
}

// ---------- 步 3：验证 ----------

type verifyStep struct{}

func (verifyStep) Name() string { return StepVerify }

// Run 逐条验证假设：时间序（变更不晚于 T0）、因果门禁（变更与节点均非
// low）、空间相关性（命中告警节点本体或仅一跳邻域）。三项全过且命中本体
// +变更 high → high；命中本体 + 变更 medium/high → medium；仅邻域或任一项
// 不过 → low（保留在链里做可解释性，但不进归因）。
func (verifyStep) Run(in Input, prev []Finding) ([]Finding, error) {
	nodes := indexNodes(in.Evidence.Nodes)
	changes := make(map[string]ChangeFact, len(in.Evidence.Changes))
	for _, c := range in.Evidence.Changes {
		changes[c.ID] = c
	}
	domain := stringSet(in.AlertedNodes)
	adj := adjacency(in.Evidence.Edges)

	var out []Finding
	for _, f := range prev {
		if f.Step != StepHypothesize || f.Ref == "" {
			continue
		}
		c, ok := changes[f.Ref]
		if !ok {
			out = append(out, verdict(f, "变更证据已不可查（假设悬空）", ConfidenceLow))
			continue
		}
		temporal := withinWindow(c.OccurredAt, in.T0, in.Window)
		gated := confRank(c.Confidence) > confRank("low") &&
			confRank(confOf(nodes, c.NodeKey)) > confRank("low")
		inDomain := domain[c.NodeKey] != ""
		spatial := inDomain || neighborOf(adj, domain, c.NodeKey)

		conf := ConfidenceLow
		switch {
		case !temporal || !gated || !spatial:
			// 任一不过：low（验证失败）
		case inDomain && confRank(c.Confidence) == confRank("high"):
			conf = ConfidenceHigh
		case inDomain:
			conf = ConfidenceMedium
		}
		why := fmt.Sprintf("时间序=%v 因果门禁=%v 空间相关=%v（%s 距 T0 %s）",
			temporal, gated, spatial, map[bool]string{true: "本体节点", false: "邻域节点"}[inDomain], agoText(in.T0, c.OccurredAt))
		out = append(out, verdict(f, why, conf))
	}
	if len(out) == 0 {
		// 上游是"无变更证据"（StepHypothesize 无 Ref）：验证步原样承认。
		return []Finding{{
			Step:       StepVerify,
			Summary:    "无可验证的变更假设（取证窗口内故障域无变更证据）",
			Confidence: string(ConfidenceLow),
		}}, nil
	}
	return out, nil
}

func verdict(hyp Finding, why string, conf Confidence) Finding {
	status := "未通过"
	if conf != ConfidenceLow {
		status = "通过"
	}
	return Finding{
		Step:       StepVerify,
		Summary:    fmt.Sprintf("验证%s：假设 %s（节点 %s）——%s", status, hyp.Ref, strings.Join(hyp.NodeKeys, ","), why),
		NodeKeys:   hyp.NodeKeys,
		Confidence: string(conf),
		Ref:        hyp.Ref,
	}
}

// ---------- 步 4：归因 ----------

type attributionStep struct{}

func (attributionStep) Name() string { return StepAttribution }

// Run 从验证通过的假设里挑根因（宁缺毋滥：low 一律不归因）。排序键：
// 置信度降序 → 距 T0 最近（变更越贴近故障越可疑）→ Ref 稳定序。
// 只归因 Top-1 之外的候选也保留为 medium/low 结论行，供结论步与前端呈现。
func (attributionStep) Run(in Input, prev []Finding) ([]Finding, error) {
	changes := make(map[string]ChangeFact, len(in.Evidence.Changes))
	for _, c := range in.Evidence.Changes {
		changes[c.ID] = c
	}
	var cands []Finding
	for _, f := range prev {
		if f.Step == StepVerify && f.Ref != "" && Confidence(f.Confidence) != ConfidenceLow {
			cands = append(cands, f)
		}
	}
	if len(cands) == 0 {
		return []Finding{{
			Step:       StepAttribution,
			Summary:    "证据链不足以归因（无通过验证的变更假设）——维持故障域现象描述，不指定根因",
			NodeKeys:   dedupSorted(in.AlertedNodes),
			Confidence: string(ConfidenceLow),
		}}, nil
	}
	sort.SliceStable(cands, func(i, j int) bool {
		ci, cj := confRank(cands[i].Confidence), confRank(cands[j].Confidence)
		if ci != cj {
			return ci > cj
		}
		if a, b := changes[cands[i].Ref], changes[cands[j].Ref]; !a.OccurredAt.Equal(b.OccurredAt) {
			return a.OccurredAt.After(b.OccurredAt) // 更贴近 T0 者优先
		}
		return cands[i].Ref < cands[j].Ref
	})
	out := make([]Finding, 0, len(cands))
	for i, f := range cands {
		c := changes[f.Ref]
		lead := "根因："
		if i > 0 {
			lead = "次要候选："
		}
		out = append(out, Finding{
			Step: StepAttribution,
			Summary: fmt.Sprintf("%s变更 %s（%s，%s %s，节点 %s）以 %s 置信归因%s",
				lead, c.ID, c.ChangeType, c.Source, actorOf(c), c.NodeKey, f.Confidence,
				map[bool]string{true: "（首要）", false: ""}[i == 0]),
			NodeKeys:   []string{c.NodeKey},
			Confidence: f.Confidence,
			Ref:        f.Ref,
		})
	}
	return out, nil
}

// ---------- 步 5：结论（LLM 唯一挂点） ----------

type concludeStep struct {
	summarizer Summarizer
}

func (concludeStep) Name() string { return StepConclude }

// Run LLM 结论。未注入 Summarizer（llm-gateway 尚未接线）时返回
// ErrNotImplemented → 记 pending：报告仍以完整结构化证据链交付，但
// **不产出结论形态文本**（ADR-003"禁止伪 RCA"）。
func (s concludeStep) Run(in Input, prev []Finding) ([]Finding, error) {
	if s.summarizer == nil {
		return nil, ErrNotImplemented
	}
	text, err := s.summarizer.Summarize(in, prev)
	if err != nil {
		// gateway 真失败：走 Pipeline 的失败中止语义（错误分类不得借
		// ErrNotImplemented 洗白成 pending——见其注释的 R6 约定）。
		return nil, fmt.Errorf("rca conclude: llm egress: %w", err)
	}
	conf := ConfidenceMedium
	for _, f := range prev {
		if f.Step == StepAttribution && Confidence(f.Confidence) == ConfidenceHigh {
			conf = ConfidenceHigh
			break
		}
	}
	return []Finding{{
		Step:       StepConclude,
		Summary:    text,
		NodeKeys:   dedupSorted(in.AlertedNodes),
		Confidence: string(conf),
	}}, nil
}

// ---------- 步 6：建议 ----------

type recommendStep struct{}

func (recommendStep) Name() string { return StepRecommend }

// Run 规则建议：首要根因是 deploy/config_change → 建议回滚并观察域恢复；
// 已是 rollback → 建议核查回滚完整性；无从归因 → 给取证改进建议
// （扩窗 / 补变更源 / 修拓扑覆盖），置信 low，明示"建议"而非"结论"。
func (recommendStep) Run(in Input, prev []Finding) ([]Finding, error) {
	changes := make(map[string]ChangeFact, len(in.Evidence.Changes))
	for _, c := range in.Evidence.Changes {
		changes[c.ID] = c
	}
	var primary *Finding
	for i := range prev {
		f := prev[i]
		if f.Step == StepAttribution && f.Ref != "" && (Confidence(f.Confidence) == ConfidenceHigh || Confidence(f.Confidence) == ConfidenceMedium) {
			primary = &prev[i]
			break // 归因步已按可疑度排序，取首个
		}
	}
	if primary != nil {
		c := changes[primary.Ref]
		msg := fmt.Sprintf("建议回滚变更 %s（%s，%s %s）并观察故障域 %v 是否恢复",
			c.ID, c.ChangeType, c.Source, actorOf(c), c.NodeKey)
		if c.ChangeType == "rollback" {
			msg = fmt.Sprintf("首要嫌疑本身是回滚（%s）：建议核查回滚完整性与残留旧配置，而非再次回退", c.ID)
		}
		return []Finding{{
			Step:       StepRecommend,
			Summary:    msg,
			NodeKeys:   primary.NodeKeys,
			Confidence: primary.Confidence,
			Ref:        primary.Ref,
		}}, nil
	}
	return []Finding{{
		Step:       StepRecommend,
		Summary:    "无从归因：建议扩大取证窗（OPS_RCA_WINDOW）、接入更多变更源（Git/Jenkins webhook）或补拓扑覆盖后复跑分析",
		NodeKeys:   dedupSorted(in.AlertedNodes),
		Confidence: string(ConfidenceLow),
	}}, nil
}

// ---------- 共享小工具（纯函数，包内单测覆盖） ----------

type Confidence string // 复用 ADR-007 三档字面量（本包自定义轻量类型，不 import topology）

const (
	ConfidenceHigh   Confidence = "high"
	ConfidenceMedium Confidence = "medium"
	ConfidenceLow    Confidence = "low"
)

// confRank 置信度排序值（空/未知按 low——保守原则；直接收 string，
// 免去调用方在 DTO 字段与 Confidence 之间来回转型）。
func confRank(c string) int {
	switch normalizeConf(c) {
	case ConfidenceHigh:
		return 3
	case ConfidenceMedium:
		return 2
	default:
		return 1
	}
}

// normalizeConf 空/未知置信度按 low（保守原则，与 topology.ParseConfidence 同向）。
func normalizeConf(s string) Confidence {
	switch Confidence(s) {
	case ConfidenceHigh, ConfidenceMedium, ConfidenceLow:
		return Confidence(s)
	default:
		return ConfidenceLow
	}
}

func minConf(a, b string) Confidence {
	ca, cb := normalizeConf(a), normalizeConf(b)
	if confRank(string(ca)) <= confRank(string(cb)) {
		return ca
	}
	return cb
}

func downgrade(c Confidence) Confidence {
	switch c {
	case ConfidenceHigh:
		return ConfidenceMedium
	default:
		return ConfidenceLow
	}
}

func indexNodes(ns []NodeFact) map[string]NodeFact {
	out := make(map[string]NodeFact, len(ns))
	for _, n := range ns {
		out[n.Key] = n
	}
	return out
}

func confOf(nodes map[string]NodeFact, key string) string {
	if n, ok := nodes[key]; ok {
		return n.Confidence
	}
	return string(ConfidenceLow) // 节点不在证据集 = 最保守
}

func stringSet(keys []string) map[string]string {
	out := make(map[string]string, len(keys))
	for _, k := range keys {
		out[k] = k
	}
	return out
}

// adjacency 无向邻接表（两端都在节点集内才有邻接；low 边保留——
// 门禁在 verify/attribution 步按置信度体现，不在这里物理删边，
// 让 collect 能如实报告"仅低置信边可达"）。
func adjacency(edges []EdgeFact) map[string]map[string]bool {
	out := make(map[string]map[string]bool)
	add := func(a, b string) {
		if out[a] == nil {
			out[a] = map[string]bool{}
		}
		out[a][b] = true
	}
	for _, e := range edges {
		add(e.SrcKey, e.DstKey)
		add(e.DstKey, e.SrcKey)
	}
	return out
}

func neighborOf(adj map[string]map[string]bool, domain map[string]string, key string) bool {
	for nb := range adj[key] {
		if _, ok := domain[nb]; ok {
			return true
		}
	}
	return false
}

// withinWindow [T0-Window, T0]；Window<=0 = 不设下界（只要求不晚于 T0）。
func withinWindow(at, t0 time.Time, window time.Duration) bool {
	if at.After(t0) {
		return false
	}
	if window > 0 && at.Before(t0.Add(-window)) {
		return false
	}
	return true
}

func dedupSorted(keys []string) []string {
	seen := make(map[string]struct{}, len(keys))
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		if k == "" {
			continue
		}
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func actorOf(c ChangeFact) string {
	if c.Actor != "" {
		return c.Actor
	}
	return "actor 未知"
}

func summarySuffix(s string) string {
	if s == "" {
		return ""
	}
	return fmt.Sprintf("：%s", s)
}

func agoText(t0, at time.Time) string {
	d := t0.Sub(at)
	if d < 0 {
		return "T0 之后（异常）"
	}
	if d < time.Minute {
		return fmt.Sprintf("%.0f 秒", d.Seconds())
	}
	return fmt.Sprintf("%.0f 分钟", d.Minutes())
}
