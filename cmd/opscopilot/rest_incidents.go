// 事件域 handler（D10 决策 B：自 rest_gateway.go 按域拆出）。
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"opscopilot/internal/incident"
	"strconv"
	"strings"
	"time"
)

// handleCreateIncident POST /api/v1/incidents —— 链路 B：人工建单。
// 写路径：必须携带共享密钥（复用 ChangeWebhook 的鉴权件，R6 应对），
// 且 origin 恒为 manual、auto_close_policy 恒为 manual_only（R2：人工单
// 不允许外部恢复自动关闭）。
func (g *RESTGateway) handleCreateIncident(w http.ResponseWriter, r *http.Request) {
	if !authorized(r.Header.Get(AuthHeader), g.token) {
		writeErr(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if !requireJSON(w, r) { // CSRF：拒绝免预检的跨站简单请求
		return
	}
	if g.incidents == nil {
		writeErr(w, http.StatusServiceUnavailable, "incident store not wired")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, g.limits.IncidentBodyLimit)
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		writeErr(w, http.StatusRequestEntityTooLarge, "body too large")
		return
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var in struct {
		ID         string `json:"id"`
		Title      string `json:"title"`
		Severity   string `json:"severity"`
		CreatedBy  string `json:"created_by"`
		SLAMinutes int    `json:"sla_minutes"` // W10-2 可选覆盖；0/缺省 = 按级默认
	}
	if err := dec.Decode(&in); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid body: "+err.Error())
		return
	}
	if in.SLAMinutes < 0 || in.SLAMinutes > slaMinutesMax {
		writeErr(w, http.StatusBadRequest,
			"sla_minutes must be 0.."+strconv.Itoa(slaMinutesMax)+" (0 = use per-severity default)")
		return
	}
	if strings.TrimSpace(in.Title) == "" {
		writeErr(w, http.StatusBadRequest, "title is required")
		return
	}
	if strings.TrimSpace(in.CreatedBy) == "" {
		writeErr(w, http.StatusBadRequest, "created_by is required (audit)")
		return
	}
	id := strings.TrimSpace(in.ID)
	if id == "" {
		id = "INC-" + time.Now().Format("20060102-150405.000")
	} else if !validIncidentID(id) {
		writeErr(w, http.StatusBadRequest,
			"id must be 1-128 chars of [A-Za-z0-9._:-]")
		return
	}
	// 严重级枚举化：此前是任意字符串直通，前端会按值拼 class、DB 无约束。
	severity := strings.ToLower(strings.TrimSpace(in.Severity))
	if severity == "" {
		severity = "info" // 与 DB 默认值一致，避免"没填就报错"破坏既有调用方
	} else if !validSeverity(severity) {
		writeErr(w, http.StatusBadRequest, "severity must be one of critical|warning|info")
		return
	}
	inc, err := g.incidents.Create(id, in.Title, severity, in.CreatedBy)
	if err != nil {
		writeErr(w, http.StatusConflict, err.Error())
		return
	}
	// 人工建单也留痕（与外部单的 worker create 审计对齐——审计轨迹不能只有
	// 自动动作，"这单是谁建的"必须可查）。
	if g.audit != nil {
		g.audit.Append(AuditEntry{IncidentID: inc.ID, Action: AuditCreate, Actor: in.CreatedBy,
			Detail: map[string]any{"origin": string(inc.Origin), "title": inc.Title}})
	}
	// W10-2：可选 SLA 覆盖随后单点落库（SetSLA 不触碰状态机字段）。失败时
	// 单已建但无覆盖——如实 500（细节进日志），绝不静默吞掉"配了却没生效"。
	if in.SLAMinutes > 0 {
		if err := g.incidents.SetSLA(inc.ID, in.SLAMinutes); err != nil {
			g.logf("WARNING: set sla after create (%s): %v", inc.ID, err)
			writeErr(w, http.StatusInternalServerError, "internal error")
			return
		}
		if fresh, gerr := g.incidents.Get(inc.ID); gerr == nil {
			inc = &fresh
		}
	}
	writeJSON(w, http.StatusCreated,
		incidentView{Incident: *inc, slaView: g.slaViewOf(*inc, time.Now())})
}

// handleDuplicates GET /api/v1/incidents/{id}/duplicates
// L2 疑似重复**提示**（方案决策：绝不自动合并）——只返回候选与理由，
// 由人决定是否走 POST /{id}/merge。
func (g *RESTGateway) handleDuplicates(w http.ResponseWriter, r *http.Request) {
	if g.incidents == nil {
		writeErr(w, http.StatusServiceUnavailable, "incident store not wired")
		return
	}
	id := r.PathValue("id")
	target, err := g.incidents.Get(id)
	if err != nil {
		writeErr(w, http.StatusNotFound, "incident not found: "+id)
		return
	}
	// 候选集：全部未解决事件（M2 规模下够用；量大时改按时间窗裁剪）。
	all, err := g.incidents.List("")
	if err != nil {
		// D5 决策 A：DB 故障必须如实暴露（500），但细节只进日志——
		// err.Error() 含主机/schema/约束名，回给客户端等于信息泄露（第七轮 M1）。
		g.logf("WARNING: duplicates list: %v", err)
		writeErr(w, http.StatusInternalServerError, "internal error")
		return
	}
	cands := incident.SimilarCandidates(target, all, g.limits.DedupWindow)
	writeJSON(w, http.StatusOK, map[string]any{
		"incident_id": id, "candidates": cands, "count": len(cands),
		"auto_merge": false, // 契约声明：本系统永不自动合并
	})
}

// handleMergeIncident POST /api/v1/incidents/{id}/merge —— 人工合并（L2 落地动作）。
// body: {"target_id":"...","actor":"..."}；鉴权必过。
func (g *RESTGateway) handleMergeIncident(w http.ResponseWriter, r *http.Request) {
	if !authorized(r.Header.Get(AuthHeader), g.token) {
		writeErr(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if !requireJSON(w, r) { // CSRF：拒绝免预检的跨站简单请求
		return
	}
	if g.incidents == nil {
		writeErr(w, http.StatusServiceUnavailable, "incident store not wired")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, g.limits.IncidentBodyLimit)
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		writeErr(w, http.StatusRequestEntityTooLarge, "body too large")
		return
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var in struct {
		TargetID string `json:"target_id"`
		Actor    string `json:"actor"`
	}
	if err := dec.Decode(&in); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid body: "+err.Error())
		return
	}
	if strings.TrimSpace(in.TargetID) == "" || strings.TrimSpace(in.Actor) == "" {
		writeErr(w, http.StatusBadRequest, "target_id and actor are required (audit)")
		return
	}
	srcID := r.PathValue("id")
	if err := g.incidents.MergeInto(srcID, in.TargetID); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if g.audit != nil {
		g.audit.Append(AuditEntry{IncidentID: srcID, Action: AuditMerge, Actor: in.Actor,
			Detail: map[string]any{"merged_into": in.TargetID}})
	}
	writeJSON(w, http.StatusOK, map[string]any{"merged": srcID, "into": in.TargetID})
}

func (g *RESTGateway) handleTransitionIncident(w http.ResponseWriter, r *http.Request) {
	if !authorized(r.Header.Get(AuthHeader), g.token) {
		writeErr(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if !requireJSON(w, r) { // CSRF：拒绝免预检的跨站简单请求
		return
	}
	if g.incidents == nil {
		writeErr(w, http.StatusServiceUnavailable, "incident store not wired")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, g.limits.IncidentBodyLimit)
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		writeErr(w, http.StatusRequestEntityTooLarge, "body too large")
		return
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var in struct {
		To         string `json:"to"`
		Actor      string `json:"actor"`
		SLAMinutes int    `json:"sla_minutes"` // W10-2 可选覆盖（流转同时改目标时长）
	}
	if err := dec.Decode(&in); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid body: "+err.Error())
		return
	}
	if in.SLAMinutes < 0 || in.SLAMinutes > slaMinutesMax {
		writeErr(w, http.StatusBadRequest,
			"sla_minutes must be 0.."+strconv.Itoa(slaMinutesMax)+" (0 = keep current)")
		return
	}
	if strings.TrimSpace(in.Actor) == "" {
		writeErr(w, http.StatusBadRequest, "actor is required (audit)")
		return
	}
	to := incident.State(strings.TrimSpace(in.To))
	switch to {
	case incident.StateAcked, incident.StateMitigated, incident.StateResolved:
	default:
		writeErr(w, http.StatusBadRequest, "to must be one of acked|mitigated|resolved")
		return
	}
	id := r.PathValue("id")
	inc, err := g.incidents.Transition(id, to, in.Actor)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if g.audit != nil {
		g.audit.Append(AuditEntry{IncidentID: id, Action: AuditTransition, Actor: in.Actor,
			Detail: map[string]any{"to": string(to)}})
	}
	// W10-2：流转可同时带 SLA 覆盖（同一鉴权门禁内完成，不再多一次往返）。
	if in.SLAMinutes > 0 {
		if err := g.incidents.SetSLA(id, in.SLAMinutes); err != nil {
			g.logf("WARNING: set sla after transition (%s): %v", id, err)
			writeErr(w, http.StatusInternalServerError, "internal error")
			return
		}
		if fresh, gerr := g.incidents.Get(id); gerr == nil {
			inc = fresh
		}
	}
	writeJSON(w, http.StatusOK, inc)
}

// handleAudit GET /api/v1/incidents/{id}/audit —— 单条事件的审计轨迹。
func (g *RESTGateway) handleAudit(w http.ResponseWriter, r *http.Request) {
	if g.audit == nil {
		writeErr(w, http.StatusServiceUnavailable, "audit not wired")
		return
	}
	list, err := g.audit.List(r.PathValue("id"))
	if err != nil {
		// D5 决策 A + 第八轮 D1：审计后端故障必须如实回 500，不能把
		// "查不到" 当成"没有记录"（空 200 会让人以为这一步没人操作过）。
		// 细节只进日志——err 含主机/schema/连接信息。
		g.logf("WARNING: audit list: %v", err)
		writeErr(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"incident_id": r.PathValue("id"), "entries": list, "count": len(list)})
}

// handleIncidents GET /api/v1/incidents?state=open|acked|mitigated|resolved
// （M2 主干：内存 Store，DB 后端 W9）。
// handleIncidents GET /api/v1/incidents —— 事件列表（D4 决策 A：游标分页）。
//
// 参数：state（""|active|open|acked|mitigated|resolved）、origin（精确过滤）、
// limit（默认 200，上限 1000）、cursor（上一页返回的 next_cursor）。
// 响应：{"incidents":[...], "count":N, "next_cursor":"", "stats":{...},
//
//	"persistence":"..."}；next_cursor 为空 = 已到末尾。
//
// 排序固定 created_at DESC（最新优先）。Stats 为全量聚合，供看板 KPI。
func (g *RESTGateway) handleIncidents(w http.ResponseWriter, r *http.Request) {
	if g.incidents == nil {
		writeErr(w, http.StatusServiceUnavailable, "incident store not wired")
		return
	}
	q := r.URL.Query()
	state := incident.State(q.Get("state"))
	switch state {
	case "", incident.StateActive, incident.StateOpen, incident.StateAcked,
		incident.StateMitigated, incident.StateResolved:
	default:
		writeErr(w, http.StatusBadRequest, "invalid state filter")
		return
	}
	limit := 0
	if raw := q.Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 0 {
			writeErr(w, http.StatusBadRequest, "limit must be a non-negative integer")
			return
		}
		limit = n
	}
	page, err := g.incidents.ListPage(incident.PageQuery{
		State:  state,
		Origin: q.Get("origin"),
		Limit:  limit,
		Cursor: q.Get("cursor"),
	})
	if err != nil {
		// 游标损坏是客户端错（400，但只回固定文案——不透出解码细节）；
		// 其余（含 DB 故障）一律 500 脱敏，细节进日志（第七轮 M1/M8）。
		if errors.Is(err, incident.ErrBadCursor) {
			writeErr(w, http.StatusBadRequest, "bad cursor")
			return
		}
		g.logf("WARNING: incidents list: %v", err)
		writeErr(w, http.StatusInternalServerError, "internal error")
		return
	}
	// R6-4：内存态重启即丢——把落库形态透出给消费者。
	// W10-2：items 是读视图（实体原样 + SLA 派生三字段），只读派生不落库。
	writeJSON(w, http.StatusOK, map[string]any{
		"incidents": g.incidentViews(page.Items, time.Now()), "count": len(page.Items),
		"next_cursor": page.NextCursor, "stats": page.Stats,
		"persistence": g.incidents.Persistence(),
	})
}

// handleIncidentDetail GET /api/v1/incidents/{id}。
// W10-2：响应是读视图（rest_sla.go）——实体字段原样 + sla_deadline/
// sla_remaining_seconds/sla_breached 派生三字段（只算不存）。
func (g *RESTGateway) handleIncidentDetail(w http.ResponseWriter, r *http.Request) {
	if g.incidents == nil {
		writeErr(w, http.StatusServiceUnavailable, "incident store not wired")
		return
	}
	inc, err := g.incidents.Get(r.PathValue("id"))
	if err != nil {
		writeErr(w, http.StatusNotFound, "incident not found: "+r.PathValue("id"))
		return
	}
	writeJSON(w, http.StatusOK, incidentView{Incident: inc, slaView: g.slaViewOf(inc, time.Now())})
}

// handleChanges GET /api/v1/changes?node_key=&window_start=&window_end=
// 复用 gRPC GetRecentChanges（窗口校验/映射一套语义）。
