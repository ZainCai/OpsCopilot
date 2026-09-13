// rest_runbook.go W11-4（F-12）Runbook 记录版 REST 面：事件挂载处置手册 +
// 执行记录展示，**只记不执行**（排期口径；runbook-engine 执行归二期池）。
//
// 端点（契约与字段以本文件为准，另见 docs/前端契约-runbook.md）：
//
//	GET    /api/v1/runbooks                                手册库列表
//	POST   /api/v1/runbooks                                新建手册（Token）
//	GET    /api/v1/incidents/{id}/runbooks                 事件挂载列表（聚合读）
//	POST   /api/v1/incidents/{id}/runbooks                 挂载（幂等，Token）
//	DELETE /api/v1/incidents/{id}/runbooks/{rid}           解挂（Token）
//	GET    /api/v1/incidents/{id}/runbooks/{rid}/executions      执行记录列表
//	POST   /api/v1/incidents/{id}/runbooks/{rid}/executions      追加一条执行记录（Token）
//
// 门禁与错误包络对齐现有口径（rest_rca.go / rest_notify.go 同取向）：
//   - 读路径（GET）默认无鉴权（S1 回环部署），写路径（POST/DELETE）必带
//     X-OpsCopilot-Token（authorized 常量时间比对）+ requireJSON（CSRF 免预检阻挡）；
//   - 无 DSN / store 未接线 → 显式 503（可诊断，对齐 RCA/渠道降级口径，绝不
//     把"存储故障"伪装成"没有手册"的空 200）；
//   - 事件不存在 → 404；手册不存在（挂载）→ 404；未挂载就记执行 → 404；
//     校验类错误 → 400 带原因；DB 故障 → 500 脱敏（细节只进日志，第七轮 M1）。
//
// "执行记录不算审计证据"（对齐 session 拍板②）：本面不写 incident_audit 哈希链，
// 执行只落 runbook_execution_log（append-only、无防篡改），响应带 auto_execution:false
// 契约声明——"记了一笔"是运维事实展示，不等于"系统替你执行了"，也不进合规取证面。
//
// 为什么读聚合放 cmd、写 CRUD 在 internal/runbook：聚合视图（挂载 join 手册 +
// 执行计数）是装配层的读侧组合（同 SLA 派生落 cmd 的先例），store 保持纯 CRUD、
// 不被任何 internal 模块 import（边界脚本纪律）。
package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"opscopilot/internal/runbook"
)

// runbook 请求体字段上限（防超长污染表与前端渲染；1MiB 体上限内再收字段级）。
const (
	runbookTitleMax   = 512
	runbookContentMax = 256 * 1024 // markdown 正文上限（256KiB，宽于 title，手册本就长）
	runbookScopeMax   = 128        // scope_service 标签长度
	runbookActorMax   = 128        // created_by / mounted_by / executed_by 身份串
	runbookResultMax  = 8 * 1024   // 执行结果自由文本
	runbookRefsMax    = 64 * 1024  // refs JSON 数组序列化后上限
	runbookRefsElem   = 100        // refs 数组元素个数上限
)

// validRunbookScope severity 适用性标签白名单（空 = 不限；否则同事件域三级）。
func validRunbookScope(sev string) bool {
	return sev == "" || validSeverity(sev)
}

// runbookWriteGate 写路径三道闸：Token → CSRF(需 JSON) → store 接线。
// 任一失败已写响应并返回 false。
func (g *RESTGateway) runbookWriteGate(w http.ResponseWriter, r *http.Request) bool {
	if !g.runbookTokenGate(w, r) {
		return false
	}
	if !requireJSON(w, r) {
		return false
	}
	return g.runbookStoreGate(w)
}

// runbookTokenGate DELETE 形态闸：Token → store 接线。无 body 的 DELETE 不强制
// Content-Type（对齐 notify/channels DELETE——requireJSON 的保护对象是"浏览器
// 免预检跨站表单请求"，DELETE 表单不存在；带 JSON 声明也照样通过）。
func (g *RESTGateway) runbookTokenGate(w http.ResponseWriter, r *http.Request) bool {
	if !authorized(r.Header.Get(AuthHeader), g.token) {
		writeErr(w, http.StatusUnauthorized, "unauthorized")
		return false
	}
	return true
}

// runbookStoreGate 读路径的接线闸（GET 无鉴权，仅判 store/事件源可用性）。
func (g *RESTGateway) runbookStoreGate(w http.ResponseWriter) bool {
	if g.runbooks == nil {
		writeErr(w, http.StatusServiceUnavailable, "runbook store not wired (needs DB DSN)")
		return false
	}
	if g.incidents == nil {
		writeErr(w, http.StatusServiceUnavailable, "incident store not wired")
		return false
	}
	return true
}

// runbookEnsureIncident 事件存在性前置（不存在写 404 返回 false）。挂载/执行都
// 锚在事件上，"给不存在的事件记手册"是客户端错，不该静默落悬挂行。
func (g *RESTGateway) runbookEnsureIncident(w http.ResponseWriter, id string) bool {
	if _, err := g.incidents.Get(id); err != nil {
		writeErr(w, http.StatusNotFound, "incident not found: "+id)
		return false
	}
	return true
}

// writeRunbookStoreErr store 错误 → HTTP 映射（404 挂载/手册缺失、409 重复、
// 500 脱敏）。校验类 400 已在入口提前拦截，不经此处。
func (g *RESTGateway) writeRunbookStoreErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, runbook.ErrRunbookNotFound), errors.Is(err, runbook.ErrNotMounted):
		writeErr(w, http.StatusNotFound, err.Error())
	case errors.Is(err, runbook.ErrDuplicateID):
		writeErr(w, http.StatusConflict, "runbook id already exists")
	default:
		g.logf("WARNING: runbook store: %v", err)
		writeErr(w, http.StatusInternalServerError, "internal error")
	}
}

// ---- 手册库（CRUD 从简：列表 + 创建；记录版无删改入口）----

// handleRunbookList GET /api/v1/runbooks —— 手册库列表（新→旧）。
func (g *RESTGateway) handleRunbookList(w http.ResponseWriter, r *http.Request) {
	if g.runbooks == nil {
		writeErr(w, http.StatusServiceUnavailable, "runbook store not wired (needs DB DSN)")
		return
	}
	list, err := g.runbooks.List(r.Context())
	if err != nil {
		g.logf("WARNING: runbook list: %v", err)
		writeErr(w, http.StatusInternalServerError, "internal error")
		return
	}
	if list == nil {
		list = []runbook.Runbook{} // 空库回显 []，不给前端 null 分支
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"runbooks": list, "count": len(list),
		"persistence": g.runbooks.Persistence(),
	})
}

// handleRunbookCreate POST /api/v1/runbooks —— 新建手册。
// body: {id?, title, content?, scope_severity?, scope_service?, created_by}。
// id 省略则自动生成（RBK-<时间戳>）；记录版库不做在线改删（改 = 建新版本手册）。
func (g *RESTGateway) handleRunbookCreate(w http.ResponseWriter, r *http.Request) {
	if !g.runbookWriteGate(w, r) {
		return
	}
	var in struct {
		ID            string `json:"id"`
		Title         string `json:"title"`
		Content       string `json:"content"`
		ScopeSeverity string `json:"scope_severity"`
		ScopeService  string `json:"scope_service"`
		CreatedBy     string `json:"created_by"`
	}
	if !decodeStrict(w, r, g.limits.IncidentBodyLimit, &in) {
		return
	}
	title := strings.TrimSpace(in.Title)
	if title == "" {
		writeErr(w, http.StatusBadRequest, "title is required")
		return
	}
	if len(title) > runbookTitleMax {
		writeErr(w, http.StatusBadRequest, "title too long (max "+strconv.Itoa(runbookTitleMax)+")")
		return
	}
	if len(in.Content) > runbookContentMax {
		writeErr(w, http.StatusBadRequest, "content too long (max "+strconv.Itoa(runbookContentMax)+" bytes)")
		return
	}
	createdBy := strings.TrimSpace(in.CreatedBy)
	if createdBy == "" {
		writeErr(w, http.StatusBadRequest, "created_by is required (identity hook)")
		return
	}
	if len(createdBy) > runbookActorMax {
		writeErr(w, http.StatusBadRequest, "created_by too long")
		return
	}
	sev := strings.ToLower(strings.TrimSpace(in.ScopeSeverity))
	if !validRunbookScope(sev) {
		writeErr(w, http.StatusBadRequest, "scope_severity must be one of critical|warning|info (or empty = any)")
		return
	}
	scopeSvc := strings.TrimSpace(in.ScopeService)
	if len(scopeSvc) > runbookScopeMax {
		writeErr(w, http.StatusBadRequest, "scope_service too long")
		return
	}
	id := strings.TrimSpace(in.ID)
	if id == "" {
		id = "RBK-" + time.Now().Format("20060102-150405.000")
	} else if !validIncidentID(id) { // 同 id 字符集口径（[A-Za-z0-9._:-] ≤128）
		writeErr(w, http.StatusBadRequest, "id must be 1-128 chars of [A-Za-z0-9._:-]")
		return
	}
	rb := &runbook.Runbook{
		ID: id, Title: title, Content: in.Content,
		ScopeSeverity: sev, ScopeService: scopeSvc, CreatedBy: createdBy,
	}
	if err := g.runbooks.Create(r.Context(), rb); err != nil {
		g.writeRunbookStoreErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"runbook": rb, "persistence": g.runbooks.Persistence(),
	})
}

// ---- 事件挂载（列表聚合读 / 挂载幂等 / 解挂）----

// handleIncidentRunbooksList GET /api/v1/incidents/{id}/runbooks —— 挂载列表
// （每条含手册正文 + 挂载元数据 + 执行计数，一次 join 到位，前端零 N+1）。
func (g *RESTGateway) handleIncidentRunbooksList(w http.ResponseWriter, r *http.Request) {
	if !g.runbookStoreGate(w) {
		return
	}
	id := r.PathValue("id")
	if !g.runbookEnsureIncident(w, id) {
		return
	}
	mounts, err := g.runbooks.ListMounts(r.Context(), id)
	if err != nil {
		g.logf("WARNING: runbook mounts %s: %v", id, err)
		writeErr(w, http.StatusInternalServerError, "internal error")
		return
	}
	if mounts == nil {
		mounts = []runbook.MountView{}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"incident_id": id, "runbooks": mounts, "count": len(mounts),
		"persistence": g.runbooks.Persistence(),
	})
}

// handleIncidentRunbookMount POST /api/v1/incidents/{id}/runbooks —— 挂载。
// body: {runbook_id, mounted_by}。重复挂载幂等（PK 去重，不刷新既有留痕）。
func (g *RESTGateway) handleIncidentRunbookMount(w http.ResponseWriter, r *http.Request) {
	if !g.runbookWriteGate(w, r) {
		return
	}
	var in struct {
		RunbookID string `json:"runbook_id"`
		MountedBy string `json:"mounted_by"`
	}
	if !decodeStrict(w, r, g.limits.IncidentBodyLimit, &in) {
		return
	}
	rbID := strings.TrimSpace(in.RunbookID)
	if rbID == "" {
		writeErr(w, http.StatusBadRequest, "runbook_id is required")
		return
	}
	by := strings.TrimSpace(in.MountedBy)
	if by == "" {
		writeErr(w, http.StatusBadRequest, "mounted_by is required (identity hook)")
		return
	}
	if len(by) > runbookActorMax {
		writeErr(w, http.StatusBadRequest, "mounted_by too long")
		return
	}
	id := r.PathValue("id")
	if !g.runbookEnsureIncident(w, id) {
		return
	}
	if err := g.runbooks.Mount(r.Context(), id, rbID, by); err != nil {
		g.writeRunbookStoreErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"incident_id": id, "runbook_id": rbID, "mounted": true,
		"persistence": g.runbooks.Persistence(),
	})
}

// handleIncidentRunbookUnmount DELETE /api/v1/incidents/{id}/runbooks/{rid} ——
// 解挂（只断挂载关系；执行记录作为事件的时间性事实保留，不随解挂消失）。
func (g *RESTGateway) handleIncidentRunbookUnmount(w http.ResponseWriter, r *http.Request) {
	if !g.runbookTokenGate(w, r) || !g.runbookStoreGate(w) {
		return
	}
	id, rid := r.PathValue("id"), r.PathValue("rid")
	if !g.runbookEnsureIncident(w, id) {
		return
	}
	if err := g.runbooks.Unmount(r.Context(), id, rid); err != nil {
		g.writeRunbookStoreErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"incident_id": id, "runbook_id": rid, "unmounted": true,
		"persistence": g.runbooks.Persistence(),
	})
}

// ---- 执行记录（append-only；只记不执行）----

// handleIncidentRunbookExecutionsList GET .../runbooks/{rid}/executions —— 某挂载
// 点的执行记录（旧→新）。契约层面 auto_execution:false 声明"本系统不代为执行"。
func (g *RESTGateway) handleIncidentRunbookExecutionsList(w http.ResponseWriter, r *http.Request) {
	if !g.runbookStoreGate(w) {
		return
	}
	id, rid := r.PathValue("id"), r.PathValue("rid")
	if !g.runbookEnsureIncident(w, id) {
		return
	}
	execs, err := g.runbooks.ListExecutions(r.Context(), id, rid)
	if err != nil {
		g.logf("WARNING: runbook executions %s/%s: %v", id, rid, err)
		writeErr(w, http.StatusInternalServerError, "internal error")
		return
	}
	if execs == nil {
		execs = []runbook.Execution{}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"incident_id": id, "runbook_id": rid, "executions": execs, "count": len(execs),
		"auto_execution": false, "persistence": g.runbooks.Persistence(),
	})
}

// handleIncidentRunbookExecutionAppend POST .../runbooks/{rid}/executions —— 追加
// 一条执行记录（挂载点必须存在，否则 404）。body: {executed_by, result, refs?}，
// refs 为自由 JSON 数组（链接/截图 URL/工单号等佐证）。系统只落库、不执行任何步骤。
func (g *RESTGateway) handleIncidentRunbookExecutionAppend(w http.ResponseWriter, r *http.Request) {
	if !g.runbookWriteGate(w, r) {
		return
	}
	var in struct {
		ExecutedBy string          `json:"executed_by"`
		Result     string          `json:"result"`
		Refs       json.RawMessage `json:"refs"`
	}
	if !decodeStrict(w, r, g.limits.IncidentBodyLimit, &in) {
		return
	}
	by := strings.TrimSpace(in.ExecutedBy)
	if by == "" {
		writeErr(w, http.StatusBadRequest, "executed_by is required (identity hook)")
		return
	}
	if len(by) > runbookActorMax {
		writeErr(w, http.StatusBadRequest, "executed_by too long")
		return
	}
	if strings.TrimSpace(in.Result) == "" {
		writeErr(w, http.StatusBadRequest, "result is required (what was done / outcome)")
		return
	}
	if len(in.Result) > runbookResultMax {
		writeErr(w, http.StatusBadRequest, "result too long (max "+strconv.Itoa(runbookResultMax)+" bytes)")
		return
	}
	refs, ok := normalizeRunbookRefs(w, in.Refs)
	if !ok {
		return
	}
	id, rid := r.PathValue("id"), r.PathValue("rid")
	if !g.runbookEnsureIncident(w, id) {
		return
	}
	exec, err := g.runbooks.AppendExecution(r.Context(), id, rid, by, in.Result, refs)
	if err != nil {
		g.writeRunbookStoreErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"incident_id": id, "runbook_id": rid, "execution": exec,
		"auto_execution": false, "persistence": g.runbooks.Persistence(),
	})
}

// normalizeRunbookRefs 校验并归一化 refs：缺省/空 → "[]"；必须是 JSON 数组且
// 元素数、序列化字节在限内（对象元素内容不限——前端可放 {url,label} 等自由结构）。
func normalizeRunbookRefs(w http.ResponseWriter, raw json.RawMessage) (json.RawMessage, bool) {
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" || trimmed == "null" {
		return json.RawMessage("[]"), true
	}
	if len(trimmed) > runbookRefsMax {
		writeErr(w, http.StatusBadRequest, "refs too large (max "+strconv.Itoa(runbookRefsMax)+" bytes)")
		return nil, false
	}
	var arr []json.RawMessage
	if err := json.Unmarshal(raw, &arr); err != nil {
		writeErr(w, http.StatusBadRequest, "refs must be a JSON array")
		return nil, false
	}
	if len(arr) > runbookRefsElem {
		writeErr(w, http.StatusBadRequest, "refs too many elements (max "+strconv.Itoa(runbookRefsElem)+")")
		return nil, false
	}
	return raw, true
}
