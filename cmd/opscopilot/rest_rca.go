// GET /api/v1/incidents/{id}/rca —— 按需根因分析（优化方案 #12 / ADR-014）。
//
// 契约（ADR-014 决策 5，findings 上限部分随二期池波二 #6 更新）：
//   - Token 门禁同现有写路径读法（authorized + X-OpsCopilot-Token；
//     未配置密钥 = 无鉴权，仅限回环部署口径）——分析会读全量证据链并按
//     请求写审计，不视作免费读端点；
//   - OPS_RCA=off 或组件未装配 → 503（可诊断，与 ingest 队列同款取向）；
//   - 事件不存在 → 404；超时 → 504；Store/取证故障 → 500（细节只进日志，
//     第七轮 M1 口径）；
//   - 证据不足**不报 501/404**：200 + 结构化证据报告（各步带状态/置信度/
//     依据，conclude pending）——"跑过但没结论"是可诊断的运维事实；
//   - findings 超限处理（#6 兑现 ADR-014"回显上限 200"的可配置化）：
//     编排器已按 OPS_RCA_MAX_FINDINGS 截断（置信度+时序），默认响应即
//     截断版并带 truncated 旗标 + findings_truncated 计数；`?all=1`
//     （仍在 Token 门禁内）返回未截断全量——同步报告在内存里现成，
//     游标分页是过度设计，一次性全量开关替代。
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
	Conclusion        *string `json:"conclusion"`
	LLMUsed           bool    `json:"llm_used"`
	Truncated         bool    `json:"truncated"`          // 本次回显的 findings 相对全量有裁剪时为 true（?all=1 恒 false）
	FindingsTruncated int     `json:"findings_truncated"` // 被 OPS_RCA_MAX_FINDINGS 裁掉的条数（全量报告口径，?all=1 也回显）
	Persistence       string  `json:"persistence"`
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
	// ?all=1：一次性全量回显开关（#6 对"分页"的最小替代，仍在 Token
	// 门禁内——走到这里已经过 authorized 校验）。任何非 "1" 取值都按
	// 截断版回显，宽松匹配避免前端拼参数的小差异变成 500。
	all := r.URL.Query().Get("all") == "1"
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
	writeJSON(w, http.StatusOK, newRCAView(res, g.incidents.Persistence(), all))
}

// newRCAView 序列化报告。截断策略在编排器落定（OPS_RCA_MAX_FINDINGS），
// 本函数只选择回显哪一份：默认 = Report.Findings（截断版，与审计同源）；
// all=true = res.FindingsFull（未截断全量）。truncated 旗标描述**本次回显**
// 相对全量是否有裁剪（all=true 恒 false）；findings_truncated 恒为策略
// 口径的被裁条数（all=true 时提醒调用方"默认视图少的是哪些数量"）。
func newRCAView(res *RCAResult, persistence string, all bool) rcaView {
	rep := res.Report
	fs := rep.Findings
	if all {
		fs = res.FindingsFull
	}
	v := rcaView{
		IncidentID: res.IncidentID, ClusterKeys: res.ClusterKeys,
		T0: res.T0, Window: res.Window.String(),
		AlertedNodes: rep.Input.AlertedNodes,
		Evidence: rcaCountsView{
			Nodes:   len(rep.Input.Evidence.Nodes),
			Edges:   len(rep.Input.Evidence.Edges),
			Changes: len(rep.Input.Evidence.Changes),
		},
		Truncated:         !all && rep.FindingsTruncated > 0,
		FindingsTruncated: rep.FindingsTruncated,
		Persistence:       persistence,
	}
	for _, sr := range rep.Steps {
		v.Steps = append(v.Steps, rcaStepView{Name: sr.Name, Status: string(sr.Status),
			DurationMS: sr.Duration.Milliseconds(), Error: sr.Err})
	}
	for _, f := range fs {
		v.Findings = append(v.Findings, rcaFindView{Step: f.Step, Summary: f.Summary,
			NodeKeys: f.NodeKeys, Confidence: f.Confidence, Ref: f.Ref})
	}
	for _, f := range rep.RootCauses {
		v.RootCauses = append(v.RootCauses, rcaFindView{Step: f.Step, Summary: f.Summary,
			NodeKeys: f.NodeKeys, Confidence: f.Confidence, Ref: f.Ref})
	}
	// conclusion/llm_used 从**全量** findings 探测：conclude 条即使被截断
	// 策略裁出默认列表，"LLM 出过结论"仍是既成的运维事实（结论进
	// 独立契约字段，不随 findings 列表消失）。
	for _, f := range res.FindingsFull {
		if f.Step == rca.StepConclude {
			text := f.Summary
			v.Conclusion, v.LLMUsed = &text, true
			break
		}
	}
	return v
}
