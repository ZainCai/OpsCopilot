// Package rca 根因分析流水线（M2 主干骨架，功能点 F-05/F-06）。
//
// 六步框架对齐原型 rca 页（pipeline 形态）：取证 → 假设 → 验证 → 归因 →
// 结论 → 建议。本文件只落**框架**：步骤接口、顺序执行、Findings 容器、
// 根因标注占位。各步骤的具体算法（as_of 拓扑取证、变更关联假设等）留
// W10 填充——占位步骤返回 StepPending，不臆造结论。
//
// 数据源（均已就绪，W10 直接接）：
//   - topology.AsOf(T0)：故障时刻时点拓扑（R2 锁内映射已封装）；
//   - ChangeStore.ByNodeWithin：故障窗口内变更证据；
//   - noise 簇：故障域节点集合。
package rca

import (
	"errors"
	"fmt"
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

// Input 单次分析输入（故障时刻 + 范围）。
type Input struct {
	TenantID   string
	ClusterKey string    // 触发分析的簇
	T0         time.Time // 故障时刻（RCA 铁律：as_of 用 T0，禁止当前拓扑）
	Window     time.Duration
}

// Finding 单步产出（证据/假设/结论的统一容器；W10 各步骤填充）。
type Finding struct {
	Step       string // 步骤名
	Summary    string
	NodeKeys   []string // 涉及节点（根因标注挂这里）
	Confidence string   // high / medium / low（ADR-007 同源口径）
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
	Findings   []Finding // 全部产出（按步骤序）
	RootCauses []Finding // 根因标注占位：Confidence=high 的归因结论（W10 实现）
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
			sr.Err = "placeholder — W10"
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

// rootCauses 从产出里挑根因标注（主干占位：只挑 Confidence=high 且
// Step="attribution" 的条目；归因算法 W10 实现）。
func rootCauses(fs []Finding) []Finding {
	var out []Finding
	for _, f := range fs {
		if f.Step == "attribution" && f.Confidence == "high" {
			out = append(out, f)
		}
	}
	return out
}

// ErrNotImplemented 占位步骤统一哨兵：Pipeline 用 errors.Is 识别并记
// pending 继续。**正式步骤实现者不得复用此哨兵**表示真实失败；包装时
// 必须 %w 传递（如 fmt.Errorf("query as_of: %w", err)），否则 Is 判定
// 失效、错误分类漂移（R6 审核约定固化）。
var ErrNotImplemented = errors.New("rca: step not implemented (W10)")
