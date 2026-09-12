// GET /api/v1/incidents/{id}/rca —— 按需根因分析（优化方案 #12 / ADR-014）。
//
// 契约（ADR-014 决策 5）：
//   - Token 门禁同现有写路径读法（authorized + X-OpsCopilot-Token；
//     未配置密钥 = 无鉴权，仅限回环部署口径）——分析会读全量证据链并按
//     请求写审计，不视作免费读端点；
//   - OPS_RCA=off 或组件未装配 → 503（可诊断，与 ingest 队列同款取向）；
//   - 事件不存在 → 404；超时 → 504；Store/取证故障 → 500（细节只进日志，
//     第七轮 M1 口径）；
//   - 证据不足**不报 501/404**：200 + 结构化证据报告（各步带状态/置信度/
//     依据，conclude pending）——"跑过但没结论"是可诊断的运维事实。
package main

import (
	"context"
	"errors"
	"net/http"
	"time"

	"opscopilot/internal/rca"
)

// rcaView REST 响应（snake_case 契约；原始证据不回显——体积不可控且
// 前端展示需要的是结论链不是全图，证据计数/故障域进 meta）。
type rcaView struct {
	IncidentID   string        `json:"incident_id"`
	Title        string        `json:"title"`
	ClusterKeys  []string      `json:"cluster_keys"`
	T0           time.Time     `json:"t0"`
	Window       string        `json:"window"`
	AlertedNodes []string      `json:"alerted_nodes"`
	Evidence     rcaCountsView `json:"evidence"`
	Steps        []rcaStepView `json:"steps"`
	Findings     []rcaFindView `json:"findings"`
	RootCauses   []rcaFindView `json:"root_causes"`
	// Conclusion LLM 结论文本；null = conclude pending（llm-gateway 未
	// 接线，ADR-003 禁止伪 RCA——契约层面明示"没有结论"）。
	Conclusion  *string `json:"conclusion"`
	LLMUsed     bool    `json:"llm_used"`
	Truncated   bool    `json:"truncated"` // findings 超上限被裁剪时为 true
	Persistence string  `json:"persistence"`
}

type rcaStepView struct {
	Name       string `json:"name"`
	Status     string `json:"status"`
	DurationMS int64  `json:"duration_ms"`
	Error      string `json:"error,omitempty"`
}

type rcaFindView struct {
	Step       string   `json:"step"`
	Summary    string   `json:"summary"`
	NodeKeys   []string `json:"node_keys,omitempty"`
	Confidence string   `json:"confidence"`
	Ref        string   `json:"ref,omitempty"`
}

type rcaCountsView struct {
	Nodes   int `json:"nodes"`
	Edges   int `json:"edges"`
	Changes int `json:"changes"`
}

// rcaFindingsLimit findings 回显上限（防御性裁剪：单节点大量变更时
// 假设/验证行会成百——报告可读性与响应体积优先，超出计 truncated）。
const rcaFindingsLimit = 200

func (g *RESTGateway) handleRCA(w http.ResponseWriter, r *http.Request) {
	if !authorized(r.Header.Get(AuthHeader), g.token) {
		writeErr(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if g.rca == nil {
		writeErr(w, http.StatusServiceUnavailable, "rca disabled (OPS_RCA=off)")
		return
	}
	id := r.PathValue("id")
	actor := r.URL.Query().Get("actor")
	res, err := g.rca.Analyze(r.Context(), id, actor)
	if err != nil {
		switch {
		case errors.Is(err, ErrRCAIncidentNotFound):
			writeErr(w, http.StatusNotFound, "incident not found: "+id)
		case errors.Is(err, ErrRCADisabled):
			writeErr(w, http.StatusServiceUnavailable, "rca disabled (OPS_RCA=off)")
		case errors.Is(err, context.DeadlineExceeded):
			writeErr(w, http.StatusGatewayTimeout, "rca analysis timed out")
		case errors.Is(err, context.Canceled):
			writeErr(w, http.StatusServiceUnavailable, "rca analysis canceled")
		default:
			g.logf("WARNING: rca analyze %s: %v", id, err)
			writeErr(w, http.StatusInternalServerError, "internal error")
		}
		return
	}
	writeJSON(w, http.StatusOK, newRCAView(res, g.incidents.Persistence()))
}

func newRCAView(res *RCAResult, persistence string) rcaView {
	rep := res.Report
	v := rcaView{
		IncidentID: res.IncidentID, ClusterKeys: res.ClusterKeys,
		T0: res.T0, Window: res.Window.String(),
		AlertedNodes: rep.Input.AlertedNodes,
		Evidence: rcaCountsView{
			Nodes:   len(rep.Input.Evidence.Nodes),
			Edges:   len(rep.Input.Evidence.Edges),
			Changes: len(rep.Input.Evidence.Changes),
		},
		Persistence: persistence,
	}
	for _, sr := range rep.Steps {
		v.Steps = append(v.Steps, rcaStepView{Name: sr.Name, Status: string(sr.Status),
			DurationMS: sr.Duration.Milliseconds(), Error: sr.Err})
	}
	fs := rep.Findings
	if len(fs) > rcaFindingsLimit {
		fs, v.Truncated = fs[:rcaFindingsLimit], true
	}
	for _, f := range fs {
		v.Findings = append(v.Findings, rcaFindView{Step: f.Step, Summary: f.Summary,
			NodeKeys: f.NodeKeys, Confidence: f.Confidence, Ref: f.Ref})
	}
	for _, f := range rep.RootCauses {
		v.RootCauses = append(v.RootCauses, rcaFindView{Step: f.Step, Summary: f.Summary,
			NodeKeys: f.NodeKeys, Confidence: f.Confidence, Ref: f.Ref})
	}
	for _, f := range rep.Findings {
		if f.Step == rca.StepConclude {
			text := f.Summary
			v.Conclusion, v.LLMUsed = &text, true
			break
		}
	}
	return v
}
