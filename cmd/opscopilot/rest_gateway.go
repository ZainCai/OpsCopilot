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
	"encoding/json"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"net/http"
	"opscopilot/internal/incident"
	"strings"
	"time"
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
	// corsOrigin 跨源放行白名单（D2 决策：默认**不设置** = 仅同源）。
	// 空串时不返回 ACAO 头——浏览器按同源策略拦下跨源读。
	// 显式配置（如 https://ops.example.com）才放行该源。
	// 取代早期的 "*"：读端点含事件与审计数据，默认全放开等于把
	// "任意网页可在受害者浏览器内跨源读取"当作默认行为。
	corsOrigin string
}

// SetCORSOrigin 设置跨源放行白名单（装配期调用；空串 = 仅同源）。
func (g *RESTGateway) SetCORSOrigin(origin string) { g.corsOrigin = strings.TrimSpace(origin) }

// NewRESTGateway 构造。hub 为事件广播器（W11 实时推送），可为 nil。
func NewRESTGateway(noise *NoiseEngine, sem *SemanticModelServer, incidents incident.Store, token string, audit AuditLog, hub *EventHub) *RESTGateway {
	return &RESTGateway{noise: noise, sem: sem, incidents: incidents, token: token, audit: audit, hub: hub}
}

// SetAudit 挂载审计（装配期可选）。
func (g *RESTGateway) SetAudit(a AuditLog) { g.audit = a }

// Register 把全部路由挂到 mux（装配层调用）。
// CORS（D2 决策 B+C）：默认**不返回** ACAO 头 = 仅同源可读；运维显式配置
// OPS_CORS_ORIGIN（如 https://ops.example.com）才放行该源。
// 早期默认 "*" 的理由是支撑"静态打开 console.html + ?api= 指向运行中服务"，
// 但读端点含事件与审计数据，"任意网页可跨源读"不该是默认行为——该用法
// 现在需要显式配置 OPS_CORS_ORIGIN=null（或具体源）。
// GET 简单请求不触发 CORS 预检，无需 OPTIONS 处理（注册 OPTIONS
// 通配会与 webhook 的写路径模式冲突——踩过）。
// 写路径（webhook）不经过此处，不受影响。
func (g *RESTGateway) Register(mux *http.ServeMux) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if g.corsOrigin != "" {
			w.Header().Set("Access-Control-Allow-Origin", g.corsOrigin)
		}
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

// requireJSON 写路径的 CSRF 准入：要求 Content-Type: application/json。
//
// 为什么这能防 CSRF：跨站的"简单请求"（免预检）只允许 text/plain、
// application/x-www-form-urlencoded、multipart/form-data 三种类型，浏览器
// 不允许脚本把 Content-Type 设成 application/json 而不触发预检；而写路径
// 不返回任何 CORS 预检响应头 → 预检失败 → 浏览器拦下请求。因此"强制 JSON"
// 即可把"免预检的跨站写"挡在门外（本地 curl / AM 等非浏览器客户端不受影响）。
//
// 兼容 `application/json; charset=utf-8` 这类带参数的写法。
func requireJSON(w http.ResponseWriter, r *http.Request) bool {
	ct := strings.ToLower(strings.TrimSpace(r.Header.Get("Content-Type")))
	if !strings.HasPrefix(ct, "application/json") {
		writeErr(w, http.StatusUnsupportedMediaType, "Content-Type must be application/json")
		return false
	}
	return true
}

// incidentIDMaxLen 人工建单 id 的长度上限（防超长键污染表与前端渲染）。
const incidentIDMaxLen = 128

// validIncidentID 校验人工建单 id 的字符集与长度。
//
// 收窄到 [A-Za-z0-9._:-]：外部来源的 id 形如 `origin:sourceRef`（含冒号），
// 人工单形如 `INC-20260910-120000.000`（含连字符与点）。收窄字符集同时消除
// 控制台把 id 拼进 DOM/内联属性时的注入面（前端另有 esc，这里做防御纵深）。
func validIncidentID(id string) bool {
	if len(id) == 0 || len(id) > incidentIDMaxLen {
		return false
	}
	for _, r := range id {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		case r == '.' || r == '_' || r == ':' || r == '-':
		default:
			return false
		}
	}
	return true
}

// validSeverity 严重级白名单（与前端下拉、DB 默认值口径一致）。
func validSeverity(s string) bool {
	switch s {
	case "critical", "warning", "info":
		return true
	}
	return false
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
