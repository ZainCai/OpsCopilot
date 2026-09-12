// GET/POST /api/v1/incidents/{id}/rca/session —— RCA 复盘会话（二期池 #7 S2，
// 拍板④定案端点契约；挂在 incident 子路径下，不设独立 /sessions 资源）。
//
// 契约（对齐 rest_rca.go 的取向）：
//   - Token 门禁同 RCA 写读法（authorized + X-OpsCopilot-Token；未配置密钥 =
//     无鉴权，仅限回环部署口径）。拍板⑤：沿用 ADR-009 单 Token 闸门 + 每轮
//     created_by 留身份钩子；**引入用户身份后，读会话须过 incident 权限**
//     （会话是 incident 的子资源，越权面随事件权限一并收口）。
//   - OPS_SESSION=off 或未装配 → 503（可诊断，与 RCA/ingest 同款取向）；
//   - 事件不存在 → 404；正文空/超限 → 400；超时 → 504；真相层故障 → 500
//     （细节只进日志，第七轮 M1 口径）；
//   - GET：读会话（热态命中即返；Redis 蒸发 → PG 懒恢复重建并回填，拍板①）；
//   - POST：{"content": "...", "actor": "..."} 追加一轮用户提问——user 轮
//     直落真相（PG rca_session_turn，行锁发号 seq）+ 热态覆盖写；assistant 轮
//     经 llmgw 产出后同样落库。**LLM 未配置/失败 → assistant_status=pending，
//     不伪造内容**（宁 pending 不假答，对齐 RCA conclude；降级面指标
//     opscopilot_session_llm_requests_total{outcome}）。
//   - 响应体上限复用建单体上限（Metrics.IncidentBodyLimit，#10 单一定义），
//     不新增键；llm_meta（哈希/长度）只存 PG 不回显——视图不绑内部卫生数据。
package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"
)

// sessionView 会话 REST 视图（snake_case 契约）。
type sessionView struct {
	IncidentID   string            `json:"incident_id"`
	CreatedBy    string            `json:"created_by,omitempty"`
	Participants []string          `json:"participants,omitempty"`
	Turns        []sessionTurnView `json:"turns"`
	// AssistantStatus 仅 POST 回显："answered" | "pending"。pending = LLM
	// 未配置/失败，assistant 轮**不在 turns 里**（契约层面明示"没答"）。
	AssistantStatus string `json:"assistant_status,omitempty"`
	Persistence     string `json:"persistence"`
}

type sessionTurnView struct {
	Seq       int64     `json:"seq"`
	Role      string    `json:"role"` // user | assistant（拍板②封闭集合，与 PG CHECK 对齐）
	Content   string    `json:"content"`
	CreatedBy string    `json:"created_by,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

func newSessionView(res *SessionResult) sessionView {
	v := sessionView{
		CreatedBy:       res.Meta.CreatedBy,
		Participants:    res.Meta.Participants,
		AssistantStatus: res.AssistantStatus,
		Persistence:     res.Persistence,
	}
	for _, t := range res.Turns {
		v.Turns = append(v.Turns, sessionTurnView{Seq: t.Seq, Role: t.Role, Content: t.Content,
			CreatedBy: t.CreatedBy, CreatedAt: t.CreatedAt})
	}
	if v.Turns == nil {
		v.Turns = []sessionTurnView{} // 空会话回显 []，不给前端 null 分支
	}
	return v
}

// sessionGate 两个 handler 共用的门禁：Token → 装配存在性。返回 false 时
// 响应已写完（401/503）。
func (g *RESTGateway) sessionGate(w http.ResponseWriter, r *http.Request) bool {
	if !authorized(r.Header.Get(AuthHeader), g.token) {
		writeErr(w, http.StatusUnauthorized, "unauthorized")
		return false
	}
	if g.session == nil {
		writeErr(w, http.StatusServiceUnavailable, "rca review session disabled (OPS_SESSION=off)")
		return false
	}
	return true
}

// writeSessionErr 错误 → HTTP 映射（与 handleRCA 同分类：404/504/500）。
func (g *RESTGateway) writeSessionErr(w http.ResponseWriter, id string, err error) {
	switch {
	case errors.Is(err, ErrRCAIncidentNotFound):
		writeErr(w, http.StatusNotFound, "incident not found: "+id)
	case errors.Is(err, ErrSessionTurnEmpty):
		writeErr(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		writeErr(w, http.StatusGatewayTimeout, "session request timed out")
	case errors.Is(err, context.Canceled):
		writeErr(w, http.StatusServiceUnavailable, "session request canceled")
	default:
		g.logf("WARNING: rca session %s: %v", id, err)
		writeErr(w, http.StatusInternalServerError, "internal error")
	}
}

// handleSessionRead GET /api/v1/incidents/{id}/rca/session —— 读会话，
// Redis 热态缺失时从 PG 懒恢复重建（对用户透明）。
func (g *RESTGateway) handleSessionRead(w http.ResponseWriter, r *http.Request) {
	if !g.sessionGate(w, r) {
		return
	}
	id := r.PathValue("id")
	res, err := g.session.ReadSession(r.Context(), id)
	if err != nil {
		g.writeSessionErr(w, id, err)
		return
	}
	v := newSessionView(res)
	v.IncidentID = id
	writeJSON(w, http.StatusOK, v)
}

// handleSessionWrite POST /api/v1/incidents/{id}/rca/session —— 追加一轮提问。
type sessionPostReq struct {
	Content string `json:"content"`
	Actor   string `json:"actor"`
}

func (g *RESTGateway) handleSessionWrite(w http.ResponseWriter, r *http.Request) {
	if !g.sessionGate(w, r) {
		return
	}
	if !requireJSON(w, r) {
		return
	}
	id := r.PathValue("id")
	var req sessionPostReq
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, g.limits.IncidentBodyLimit))
	dec.DisallowUnknownFields() // 写路径契约收紧：拼错字段静默丢比 400 更难查
	if err := dec.Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}
	if strings.TrimSpace(req.Content) == "" {
		writeErr(w, http.StatusBadRequest, ErrSessionTurnEmpty.Error())
		return
	}
	res, err := g.session.AddTurn(r.Context(), id, req.Actor, req.Content)
	if err != nil {
		g.writeSessionErr(w, id, err)
		return
	}
	v := newSessionView(res)
	v.IncidentID = id
	// 200（非 201）：资源语义是"追加并返回整会话"，幂等竞态下 201 反而误导。
	writeJSON(w, http.StatusOK, v)
}
