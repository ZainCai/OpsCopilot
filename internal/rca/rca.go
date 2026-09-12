// Package rca 根因分析流水线（M2 主干骨架 → 优化方案 #12 最小链路接线，
// 功能点 F-05/F-06，ADR-014）。
//
// 六步框架对齐原型 rca 页（pipeline 形态）：取证 → 假设 → 验证 → 归因 →
// 结论 → 建议。#12 落地"规则 + 证据"版最小可用链路（steps.go）：
// collect/hypothesize/verify/attribution/recommend 五步为确定性规则实现；
// conclude 是唯一 LLM 挂点（Summarizer 接口，ADR-003 单出口——未注入时
// 该步 pending，宁缺毋滥不出伪结论）。
//
// 边界纪律（v1.3 §5.2，scripts/check_module_boundaries.py 实测）：本包
// **不得** import internal/topology / internal/incident / internal/config——
// 数据由编排层（cmd/opscopilot/rca_orchestrator.go）取证后以纯 DTO
// （Input.Evidence / AlertedNodes）注入，本包只依赖标准库。
//
// 数据源（编排层负责采集，语义见 ADR-014）：
//   - topology AsOf(T0) 时点拓扑（RCA 铁律：as_of 用 T0，禁止当前拓扑）；
//   - ChangeBackend.ByNodeWithin/Within：故障窗口内变更证据；
//   - noise 簇 NodeKeys：故障域节点集合（告警实际命中处）。
package rca

import (
	"errors"
	"fmt"
	"sort"
	"time"
)

// Status 步骤状态。
type Status string

const (
	StatusPending Status = "pending" // 占位：算法未实现
	StatusDone    Status = "done"
	StatusFailed  Status = "failed"
)

// ErrNoInput 流水线缺必要输入（宁可报错不空跑）。
var ErrNoInput = errors.New("rca: missing required input")

// Input 单次分析输入（故障时刻 + 范围 + 证据）。
//
// Evidence/AlertedNodes 由编排层（cmd）按 ADR-007/ADR-014 纪律预采集注入：
// 拓扑为 T0 时点邻域（AsOf 语义），变更为 [T0-Window, T0] 窗口——
// rca 包自身不触任何数据源（internal 禁互 import，边界检查强制）。
type Input struct {
	TenantID   string
	ClusterKey string    // 触发分析的簇
	T0         time.Time // 故障时刻（RCA 铁律：as_of 用 T0，禁止当前拓扑）
	Window     time.Duration
	// AlertedNodes 故障域节点集合（告警簇 NodeKeys 并集，编排层去重排序）。
	// 空集 = 事件没有可定位的故障域（如无簇/簇已从内存聚类器淘汰），
	// 各步骤按"证据不足"降级输出，不臆造假设。
	AlertedNodes []string
	// Evidence 预采集证据（时点拓扑 + 窗口变更）。
	Evidence Evidence
}

// Finding 单步产出（证据/假设/结论的统一容器）。
type Finding struct {
	Step       string // 步骤名
	Summary    string
	NodeKeys   []string // 涉及节点（根因标注挂这里）
	Confidence string   // high / medium / low（ADR-007 同源口径）
	// Ref 关联证据标识（变更事件 ID 等），供跨步骤引用（hypothesize→verify
	// →attribution 链）。纯规则步骤的内部寻址键，不参与任何外部契约。
	Ref string
}

// Step 单步：输入 → 产出。实现须幂等且只读外部数据（不做副作用）。
type Step interface {
	Name() string
	Run(in Input, prev []Finding) ([]Finding, error)
}

// Pipeline 六步流水线（顺序执行；单步失败即中止——根因分析宁缺毋滥）。
type Pipeline struct {
	steps []Step
}

// NewPipeline 构造（步骤顺序即执行顺序）。
func NewPipeline(steps ...Step) *Pipeline { return &Pipeline{steps: steps} }

// Report 执行结果。
type Report struct {
	Input      Input
	Steps      []StepResult
	Findings   []Finding // 全部产出（按步骤序；编排层按上限截断后为截断版，见 TruncateFindings）
	RootCauses []Finding // 根因标注：Confidence=high 的归因结论（#12 规则版归因，见 steps.go）
	// FindingsTruncated 被 findings 上限（OPS_RCA_MAX_FINDINGS，二期池 #6）
	// 裁掉的条数。0 = 未截断。流水线自身不截断（Run 产全量），截断是编排
	// 层策略——本字段随报告走 REST/审计，让"少了几条 findings"成为可诊断
	// 的运维事实而非静默丢失。
	FindingsTruncated int
}

// StepResult 单步执行回执（原型 pipeline 节点：done/failed/pending + 耗时）。
type StepResult struct {
	Name     string
	Status   Status
	Duration time.Duration
	Err      string
}

// Run 顺序执行全部步骤。占位步骤（返回 ErrNotImplemented）记 pending
// 并继续——主干期整条流水线可跑通，产出空 Report；正式步骤失败则中止。
func (p *Pipeline) Run(in Input) (*Report, error) {
	if in.T0.IsZero() {
		return nil, ErrNoInput
	}
	rep := &Report{Input: in}
	for _, st := range p.steps {
		start := time.Now()
		fs, err := st.Run(in, rep.Findings)
		sr := StepResult{Name: st.Name(), Duration: time.Since(start)}
		switch {
		case err == nil:
			sr.Status = StatusDone
			rep.Findings = append(rep.Findings, fs...)
		case errors.Is(err, ErrNotImplemented):
			sr.Status = StatusPending
			sr.Err = "placeholder — llm egress not wired (ADR-003/ADR-014)"
		default:
			sr.Status = StatusFailed
			sr.Err = err.Error()
			rep.Steps = append(rep.Steps, sr)
			return rep, fmt.Errorf("rca step %q: %w", st.Name(), err)
		}
		rep.Steps = append(rep.Steps, sr)
	}
	rep.RootCauses = rootCauses(rep.Findings)
	return rep, nil
}

// rootCauses 从产出里挑根因标注：只挑 Confidence=high 且 Step="attribution"
// 的条目（#12 规则版：归因步只在"变更直接命中故障域节点 + 因果门禁全过 +
// 变更自身 high 置信"时才给 high——宁缺毋滥）。
func rootCauses(fs []Finding) []Finding {
	var out []Finding
	for _, f := range fs {
		if f.Step == "attribution" && f.Confidence == "high" {
			out = append(out, f)
		}
	}
	return out
}

// TruncateFindings 按上限保留 findings（二期池 #6 / ADR-014 findings>200
// 处理的最小方案——截断 + REST ?all=1 全量开关，替代游标分页：同步报告
// 本就在内存里，分页是过度设计）。选择规则：
//   - 置信度优先：high > medium > low（空/未知按 low，与 confRank 同口径）；
//   - 同档按原时序（流水线步序即产出时序，稳定排序保原序在前者）；
//   - 保留集按**原步骤序**重排输出——报告可读性/契约序不变，只是少了几条。
//
// max<=0 或 len(fs)<=max 时原样返回、截断数 0（"不限/未触限"零行为差异）。
// 纯函数不改入参切片，返回的保留集是新切片（入参可继续持有全量）。
func TruncateFindings(fs []Finding, max int) (kept []Finding, truncated int) {
	if max <= 0 || len(fs) <= max {
		return fs, 0
	}
	order := make([]int, len(fs))
	for i := range fs {
		order[i] = i
	}
	// 置信度降序优先；SliceStable 保证同档保持原时序。
	sort.SliceStable(order, func(i, j int) bool {
		return confRank(fs[order[i]].Confidence) > confRank(fs[order[j]].Confidence)
	})
	keep := order[:max]
	sort.Ints(keep) // 还原步骤序呈现
	kept = make([]Finding, 0, max)
	for _, i := range keep {
		kept = append(kept, fs[i])
	}
	return kept, len(fs) - max
}

// ErrNotImplemented 占位步骤统一哨兵（#12 起仅 conclude 步在 llm-gateway
// 未接线时返回它，见 ADR-003/ADR-014）：Pipeline 用 errors.Is 识别并记
// pending 继续。**正式步骤实现者不得复用此哨兵**表示真实失败；包装时
// 必须 %w 传递（如 fmt.Errorf("query as_of: %w", err)），否则 Is 判定
// 失效、错误分类漂移（R6 审核约定固化）。
var ErrNotImplemented = errors.New("rca: step not implemented (llm egress pending, ADR-003/ADR-014)")
