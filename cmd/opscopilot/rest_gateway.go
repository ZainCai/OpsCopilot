// W5-2.2 REST 网关：只读查询面（簇列表/详情/拓扑/变更）挂到现有 mux。
//
// 设计取舍：
//   - 直接方法调用而非绕 gRPC 回环：SemanticModelServer 的校验与映射
//     逻辑同进程复用，错误统一经 gRPC status → HTTP 映射——一套语义
//     两个门面，不会漂移；
//   - 读路径（GET）默认无鉴权：S1 下 bind loopback-only，非回环暴露
//     必须前置鉴权反代（main 启动警告已覆盖）；
//   - 写路径仅 POST /api/v1/incidents（人工建单，W9 双链路链路 B），
//     必须携带共享密钥（复用 ChangeWebhook 的 authorized 件）；
//   - Go 1.22+ ServeMux 增强路由：method + {key} 路径参数 + PathValue。
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "opscopilot/internal/contracts/pb"
	"opscopilot/internal/incident"
	"opscopilot/internal/noise"
)

// incidentBodyLimit 人工建单请求体上限（建单是几行 JSON，1MiB 足够）。
const incidentBodyLimit = 1 << 20

// dedupWindow L2 相似度的时间邻近窗口。
const dedupWindow = 30 * time.Minute

// RESTGateway 只读查询面。
// noise 可为 nil（影子降噪关闭 → 簇端点 503，其余端点照常）。
// incidents 可为 nil（M2 主干未接线时事件端点 503）。
type RESTGateway struct {
	noise     *NoiseEngine
	sem       *SemanticModelServer
	incidents incident.Store
	// token 写路径共享密钥（人工建单端点鉴权，R6）。
	token string
	// audit 审计日志（人工操作与外部自动动作统一留痕，二期）。
	audit AuditLog
	// hub 事件实时广播器（W11：GET /api/v1/events/stream 的订阅源）。
	// 由装配层注入（经 publishStore 装饰器接到写路径）；nil = 实时推送关闭。
	hub *EventHub
}

// NewRESTGateway 构造。hub 为事件广播器（W11 实时推送），可为 nil。
func NewRESTGateway(noise *NoiseEngine, sem *SemanticModelServer, incidents incident.Store, token string, audit AuditLog, hub *EventHub) *RESTGateway {
	return &RESTGateway{noise: noise, sem: sem, incidents: incidents, token: token, audit: audit, hub: hub}
}

// SetAudit 挂载审计（装配期可选）。
func (g *RESTGateway) SetAudit(a AuditLog) { g.audit = a }

// Register 把全部路由挂到 mux（装配层调用）。
// CORS：只读 GET 面放开跨源（Access-Control-Allow-Origin: *）——
// 支撑"静态打开 console.html + ?api= 指向运行中服务"的使用方式；
// 只读、无 cookie、无写路径（S1 写路径准入不涉及），泄露面为零。
// GET 简单请求不触发 CORS 预检，无需 OPTIONS 处理（注册 OPTIONS
// 通配会与 webhook 的写路径模式冲突——踩过）。
// 写路径（webhook）不经过此处，不受影响。
func (g *RESTGateway) Register(mux *http.ServeMux) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		g.route(w, r)
	})
	mux.Handle("GET /api/v1/clusters", h)
	mux.Handle("GET /api/v1/clusters/{key}", h)
	mux.Handle("GET /api/v1/topology", h)
	mux.Handle("GET /api/v1/changes", h)
	mux.Handle("GET /api/v1/incidents", h)
	mux.Handle("GET /api/v1/incidents/{id}", h)
	mux.HandleFunc("POST /api/v1/incidents", g.handleCreateIncident)
	mux.Handle("GET /api/v1/incidents/{id}/duplicates", h)
	mux.Handle("GET /api/v1/incidents/{id}/audit", h)
	mux.HandleFunc("POST /api/v1/incidents/{id}/merge", g.handleMergeIncident)
	mux.HandleFunc("POST /api/v1/incidents/{id}/transition", g.handleTransitionIncident)
	mux.HandleFunc("GET /api/v1/auth/status", g.handleAuthStatus)
	// W11 实时推送：SSE 事件流（控制台事件页订阅）。
	mux.Handle("GET /api/v1/events/stream", h)
}

// route 按 path 分发（CORS 包装层之下）。
func (g *RESTGateway) route(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/api/v1/clusters":
		g.handleClusters(w, r)
	case "/api/v1/topology":
		g.handleTopology(w, r)
	case "/api/v1/changes":
		g.handleChanges(w, r)
	case "/api/v1/events/stream":
		g.handleEventStream(w, r)
	case "/api/v1/incidents":
		if r.Method == http.MethodPost {
			g.handleCreateIncident(w, r)
			return
		}
		g.handleIncidents(w, r)
	default:
		// /api/v1/clusters/{key} 与 /api/v1/incidents/{id}：路径参数经 PathValue 取。
		if key := r.PathValue("key"); key != "" {
			g.handleClusterDetail(w, r)
			return
		}
		if id := r.PathValue("id"); id != "" {
			switch {
			case strings.HasSuffix(r.URL.Path, "/duplicates"):
				g.handleDuplicates(w, r)
			case strings.HasSuffix(r.URL.Path, "/audit"):
				g.handleAudit(w, r)
			default:
				g.handleIncidentDetail(w, r)
			}
			return
		}
		writeErr(w, http.StatusNotFound, "unknown endpoint")
	}
}

// writeJSON 统一成功响应。
func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// writeErr 统一错误响应（内部错误不外泄细节，请求错误带原因）。
func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

// grpcToHTTP gRPC status → HTTP status 映射（只读面只会遇到这三种）。
func grpcToHTTP(err error) (int, string) {
	switch status.Code(err) {
	case codes.InvalidArgument:
		return http.StatusBadRequest, status.Convert(err).Message()
	case codes.NotFound:
		return http.StatusNotFound, status.Convert(err).Message()
	default:
		return http.StatusInternalServerError, "internal error"
	}
}

// handleClusters GET /api/v1/clusters?state=active|resolved|all（默认 active）
// &limit=N（第五轮审核 G3：默认 200 上限 1000——7 天影子期 resolved
// 簇持续累积，无上限的列表响应会随历史膨胀）。
func (g *RESTGateway) handleClusters(w http.ResponseWriter, r *http.Request) {
	if g.noise == nil {
		writeErr(w, http.StatusServiceUnavailable, "noise engine disabled (OPS_NOISE_SHADOW=off)")
		return
	}
	state := strings.ToLower(r.URL.Query().Get("state"))
	switch state {
	case "", "active", "resolved", "all":
	default:
		writeErr(w, http.StatusBadRequest, "state must be one of: active, resolved, all")
		return
	}
	limit := 200
	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			writeErr(w, http.StatusBadRequest, "limit must be a positive integer")
			return
		}
		if n > 1000 {
			n = 1000
		}
		limit = n
	}
	var clusters []noise.Cluster
	if state == "active" {
		clusters = g.noise.shadow.Clusterer().ActiveClusters()
	} else {
		clusters = g.noise.shadow.Clusterer().Clusters()
		if state == "resolved" {
			filtered := clusters[:0]
			for _, cl := range clusters {
				if cl.State == noise.StateResolved {
					filtered = append(filtered, cl)
				}
			}
			clusters = filtered
		}
	}
	// API 契约用 ClusterRecord（snake_case json tags，集合导出为有序
	// 数组）——不直接输出内部 Cluster（Go 字段名 + map[string]struct{}
	// 的序列化形态不是给外部消费者的）。
	recs := make([]noise.ClusterRecord, 0, len(clusters))
	for _, cl := range clusters {
		recs = append(recs, cl.ToRecord(g.noise.tenant))
	}
	// Clusters() 按 key 有序 → 截断是确定性的（key 字典序前 limit 个）。
	truncated := false
	if len(recs) > limit {
		recs = recs[:limit]
		truncated = true
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"clusters": recs, "count": len(recs), "truncated": truncated,
	})
}

// handleClusterDetail GET /api/v1/clusters/{key}。
func (g *RESTGateway) handleClusterDetail(w http.ResponseWriter, r *http.Request) {
	if g.noise == nil {
		writeErr(w, http.StatusServiceUnavailable, "noise engine disabled (OPS_NOISE_SHADOW=off)")
		return
	}
	key := r.PathValue("key")
	if key == "" {
		writeErr(w, http.StatusBadRequest, "cluster key is required")
		return
	}
	cl := g.noise.shadow.Clusterer().Get(key)
	if cl == nil {
		writeErr(w, http.StatusNotFound, "cluster not found: "+key)
		return
	}
	writeJSON(w, http.StatusOK, cl.ToRecord(g.noise.tenant))
}

// handleTopology GET /api/v1/topology?as_of=&node_key=&depth=
// 复用 gRPC GetTopology（校验/裁剪/NotFound 语义一套）。
func (g *RESTGateway) handleTopology(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	depth := int32(0)
	if raw := q.Get("depth"); raw != "" {
		d, err := strconv.Atoi(raw)
		if err != nil || d < 0 {
			writeErr(w, http.StatusBadRequest, "depth must be a non-negative integer")
			return
		}
		depth = int32(d)
	}
	resp, err := g.sem.GetTopology(r.Context(), &pb.GetTopologyRequest{
		AsOf:    q.Get("as_of"),
		NodeKey: q.Get("node_key"),
		Depth:   depth,
	})
	if err != nil {
		code, msg := grpcToHTTP(err)
		writeErr(w, code, msg)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

// handleCreateIncident POST /api/v1/incidents —— 链路 B：人工建单。
// 写路径：必须携带共享密钥（复用 ChangeWebhook 的鉴权件，R6 应对），
// 且 origin 恒为 manual、auto_close_policy 恒为 manual_only（R2：人工单
// 不允许外部恢复自动关闭）。
func (g *RESTGateway) handleCreateIncident(w http.ResponseWriter, r *http.Request) {
	if !authorized(r.Header.Get(AuthHeader), g.token) {
		writeErr(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if g.incidents == nil {
		writeErr(w, http.StatusServiceUnavailable, "incident store not wired")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, incidentBodyLimit)
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		writeErr(w, http.StatusRequestEntityTooLarge, "body too large")
		return
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var in struct {
		ID        string `json:"id"`
		Title     string `json:"title"`
		Severity  string `json:"severity"`
		CreatedBy string `json:"created_by"`
	}
	if err := dec.Decode(&in); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid body: "+err.Error())
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
	}
	inc, err := g.incidents.Create(id, in.Title, in.Severity, in.CreatedBy)
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
	writeJSON(w, http.StatusCreated, inc)
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
	cands := incident.SimilarCandidates(target, g.incidents.List(""), dedupWindow)
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
	if g.incidents == nil {
		writeErr(w, http.StatusServiceUnavailable, "incident store not wired")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, incidentBodyLimit)
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

// handleAuthStatus GET /api/v1/auth/status —— 写权限探测（控制台据此决定是否
// 显示 Token 输入框）。
//
// 为什么不用服务端下发 cookie：本服务只有一枚共享密钥、**无用户体系**。服务端
// 下发"可写 cookie"等于把"知道密钥"降级为"能打开页面"——任何能访问 /console
// 的客户端都能拿到写权限，鉴权边界反而消失。正确做法是边界留在网络/代理层：
// 反代做鉴权后向下游注入 X-OpsCopilot-Token，浏览器侧不持有密钥。本端点让
// 控制台能识别这种情况并自动隐藏输入框。
//
// 安全：只回布尔与模式名，不回任何密钥材料；GET 无副作用。
func (g *RESTGateway) handleAuthStatus(w http.ResponseWriter, r *http.Request) {
	mode := "shared_secret"
	ok := false
	if strings.TrimSpace(g.token) == "" {
		mode = "open" // 未配置密钥（仅限回环/内网；main 启动已告警）
		ok = true
	} else {
		ok = authorized(r.Header.Get(AuthHeader), g.token)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"write_authorized": ok,
		"mode":             mode,
	})
}

// handleEventStream GET /api/v1/events/stream —— 事件实时推送（SSE）。
//
// 控制台事件页据此把"30s 轮询"换成"变更即推"。事件名固定 incident，
// data 为 SSEMessage（{type, incident}）JSON——前端按事件名订阅，无需
// 每种 type 各挂一个监听。读路径、无鉴权（与其余 GET 一致；S1 下
// loopback-only / 前置反代）。
//
// 连接管理：
//   - 首帧发注释行 ": connected" 立即刷新，让客户端确定连接已建立；
//   - 每 20s 发 ": ping" 心跳——穿过反代/代理的空闲超时，也便于客户端
//     感知连接存活；
//   - r.Context().Done() 触发（客户端断开/超时/服务停机）即退订返回。
//
// 慢客户端不阻塞写路径：Hub 缓冲满即丢该条（事件页是全量刷新语义，
// 丢一帧下一帧自愈）。
func (g *RESTGateway) handleEventStream(w http.ResponseWriter, r *http.Request) {
	if g.hub == nil {
		writeErr(w, http.StatusServiceUnavailable, "event stream disabled")
		return
	}
	fl, ok := w.(http.Flusher)
	if !ok {
		writeErr(w, http.StatusInternalServerError, "streaming unsupported by server")
		return
	}
	h := w.Header()
	h.Set("Content-Type", "text/event-stream; charset=utf-8")
	h.Set("Cache-Control", "no-store, must-revalidate")
	h.Set("Connection", "keep-alive")
	// 反代（nginx）默认缓冲响应，会攒够才下发——显式关闭，否则推送变"批量"。
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, ": connected\n\n")
	fl.Flush()

	ch, cancel := g.hub.Subscribe()
	defer cancel()

	ka := time.NewTicker(20 * time.Second)
	defer ka.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case msg, open := <-ch:
			if !open { // Hub 关闭（停机）：结束连接
				return
			}
			b, err := json.Marshal(msg)
			if err != nil {
				continue // 理论不可达；坏帧跳过，不断流
			}
			fmt.Fprintf(w, "event: incident\ndata: %s\n\n", b)
			fl.Flush()
		case <-ka.C:
			fmt.Fprint(w, ": ping\n\n")
			fl.Flush()
		}
	}
}

// handleTransitionIncident POST /api/v1/incidents/{id}/transition —— 事件状态流转
// （ack/mitigate/resolve）。写路径鉴权必过、actor 必填（审计）。
//
// R2 由状态机兜底：转 acked 会自动置 auto_close_policy=manual_only（人工接手后
// 外部恢复不得自动关单）——端点不重复做这个判断，避免两处口径漂移。
func (g *RESTGateway) handleTransitionIncident(w http.ResponseWriter, r *http.Request) {
	if !authorized(r.Header.Get(AuthHeader), g.token) {
		writeErr(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if g.incidents == nil {
		writeErr(w, http.StatusServiceUnavailable, "incident store not wired")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, incidentBodyLimit)
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		writeErr(w, http.StatusRequestEntityTooLarge, "body too large")
		return
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var in struct {
		To    string `json:"to"`
		Actor string `json:"actor"`
	}
	if err := dec.Decode(&in); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid body: "+err.Error())
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
	writeJSON(w, http.StatusOK, inc)
}

// handleAudit GET /api/v1/incidents/{id}/audit —— 单条事件的审计轨迹。
func (g *RESTGateway) handleAudit(w http.ResponseWriter, r *http.Request) {
	if g.audit == nil {
		writeErr(w, http.StatusServiceUnavailable, "audit not wired")
		return
	}
	list := g.audit.List(r.PathValue("id"))
	writeJSON(w, http.StatusOK, map[string]any{"incident_id": r.PathValue("id"), "entries": list, "count": len(list)})
}

// handleIncidents GET /api/v1/incidents?state=open|acked|mitigated|resolved
// （M2 主干：内存 Store，DB 后端 W9）。
func (g *RESTGateway) handleIncidents(w http.ResponseWriter, r *http.Request) {
	if g.incidents == nil {
		writeErr(w, http.StatusServiceUnavailable, "incident store not wired")
		return
	}
	state := incident.State(r.URL.Query().Get("state"))
	switch state {
	case "", incident.StateOpen, incident.StateAcked, incident.StateMitigated, incident.StateResolved:
	default:
		writeErr(w, http.StatusBadRequest, "invalid state filter")
		return
	}
	list := g.incidents.List(state)
	// R6-4：内存态重启即丢——把落库形态透出给消费者。
	writeJSON(w, http.StatusOK, map[string]any{
		"incidents": list, "count": len(list),
		"persistence": g.incidents.Persistence(),
	})
}

// handleIncidentDetail GET /api/v1/incidents/{id}。
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
	writeJSON(w, http.StatusOK, inc)
}

// handleChanges GET /api/v1/changes?node_key=&window_start=&window_end=
// 复用 gRPC GetRecentChanges（窗口校验/映射一套语义）。
func (g *RESTGateway) handleChanges(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	resp, err := g.sem.GetRecentChanges(r.Context(), &pb.GetRecentChangesRequest{
		NodeKey:     q.Get("node_key"),
		WindowStart: q.Get("window_start"),
		WindowEnd:   q.Get("window_end"),
	})
	if err != nil {
		code, msg := grpcToHTTP(err)
		writeErr(w, code, msg)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}
