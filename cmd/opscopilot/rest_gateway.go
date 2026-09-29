// W5-2.2 REST 网关：只读查询面（簇列表/详情/拓扑/变更）挂到现有 mux。
//
// 设计取舍：
//   - 直接方法调用而非绕 gRPC 回环：SemanticModelServer 的校验与映射
//     逻辑同进程复用，错误统一经 gRPC status → HTTP 映射——一套语义
//     两个门面，不会漂移；
//   - 读路径（GET）默认无鉴权：S1 下 bind loopback-only，非回环暴露
//     必须前置鉴权反代（main 启动警告已覆盖）；回环形态本身也不是无条件
//     安全——浏览器可从受害者机器上打进来（DNS rebinding），故另有 Host 头
//     白名单中间件（见本文件"Host 头校验"段，第十一轮 P1-1）；
//   - 写路径仅 POST /api/v1/incidents（人工建单，W9 双链路链路 B），
//     必须携带共享密钥（复用 ChangeWebhook 的 authorized 件）；
//   - Go 1.22+ ServeMux 增强路由：method + {key} 路径参数 + PathValue。
package main

import (
	"encoding/json"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"opscopilot/internal/config"
	"opscopilot/internal/incident"
	"opscopilot/internal/runbook"
)

// RESTLimits 网关侧的时间窗与请求体上限（#10 去魔法数字：值唯一来自
// config——Noise.DedupWindow 默认 30m、Metrics 各体上限；装配层注入）。
type RESTLimits struct {
	DedupWindow       time.Duration // L2 相似度的时间邻近窗口
	IncidentBodyLimit int64         // 人工建单等请求体上限（建单是几行 JSON，1MiB 足够）
	NotifyBodyLimit   int64         // 通知渠道配置请求体上限
	// SLA 按级默认目标时长（W10-2 F-04，OPS_SLA_*；覆盖逻辑见 rest_sla.go）。
	SLACritical time.Duration
	SLAWarning  time.Duration
	SLAInfo     time.Duration
	// KPIWindow GET /api/v1/kpis 默认观察窗（W10-3 F-07，OPS_KPI_WINDOW；
	// ?window= 按请求覆盖）。
	KPIWindow time.Duration
}

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
	// limits 时间窗与请求体上限（装配层从 config 注入）。
	limits RESTLimits
	// logf 服务端错误日志（500 脱敏后细节只进日志，不回客户端）。
	logf func(string, ...any)
	// db 告警中心数据源（alert_event 只读）。nil = 未接 DB，告警端点 503
	//（与 R6-4"落库形态透出"口径一致）。tenant 为告警查询租户。
	db     *pgxpool.Pool
	tenant string
	// channels 通知渠道配置存储（W9-2；nil = 无 DB，渠道端点 503）。
	channels *ChannelStore
	// reloadChannels 渠道写操作后的热重载回调（装配层注入；nil = 不重载）。
	reloadChannels func() (int, error)
	// rca 按需根因分析编排器（#12/ADR-014；nil = OPS_RCA=off，端点 503）。
	rca *RCAOrchestrator
	// session RCA 复盘会话编排器（二期池 #7 S2；nil = OPS_SESSION=off，
	// GET/POST /api/v1/incidents/{id}/rca/session 显式 503）。
	session *SessionOrchestrator
	// runbooks Runbook 记录版 store（W11-4 F-12；nil = 无 DB，runbook 端点
	// 显式 503——对齐 RCA 降级口径：可诊断的关闭态好过静默的空列表）。
	runbooks *runbook.Store
	// metrics 应用指标集（W12 审计解锁包：opscopilot_audit_reads_total 的
	// 记账入口；nil 安全——指标缺失不得影响读路径）。装配期 SetMetrics 注入。
	metrics *AppMetrics
}

// SetMetrics 挂载应用指标集（装配期调用；nil = 不打点）。
func (g *RESTGateway) SetMetrics(m *AppMetrics) { g.metrics = m }

// 审计读取计数的来源标签（与 AppMetrics.AuditReads 维度同源）。
const (
	auditSourceGlobal   = "global"   // GET /api/v1/audit
	auditSourceIncident = "incident" // GET /api/v1/incidents/{id}/audit
)

// countAuditRead 打点入口（nil 安全；调用点见两个审计读 handler）。
func (g *RESTGateway) countAuditRead(source string) { g.metrics.CountAuditRead(source) }

// SetRunbook 挂载 Runbook store（装配期调用；nil = 不接线，端点显式 503）。
func (g *RESTGateway) SetRunbook(s *runbook.Store) { g.runbooks = s }

// SetRCA 挂载按需 RCA 编排器（装配期调用；nil = 关闭，端点显式 503）。
func (g *RESTGateway) SetRCA(o *RCAOrchestrator) { g.rca = o }

// SetSession 挂载 RCA 复盘会话编排器（装配期调用；nil = 关闭，端点显式 503）。
func (g *RESTGateway) SetSession(o *SessionOrchestrator) { g.session = o }

// SetChannels 挂载通知渠道配置存储与重载回调（装配期调用）。
func (g *RESTGateway) SetChannels(store *ChannelStore, reload func() (int, error)) {
	g.channels, g.reloadChannels = store, reload
}

// SetCORSOrigin 设置跨源放行白名单（装配期调用；空串 = 仅同源）。
//
// 第七轮 M2：拒绝 "*"——那会静默恢复"任意网页可跨源读事件/审计"，
// 正是 D2 决策要消除的默认行为；其余值要求形如 scheme://host（或
// "null"——file:// 调试用法），非法值 fail-fast。
// 规则唯一来源是 config.ValidateCORSOrigin（与 OPS_CORS_ORIGIN 装载期
// 校验同一实现，两处不漂移）；装配调用保留作直接构造路径的兜底。
func (g *RESTGateway) SetCORSOrigin(origin string) error {
	if err := config.ValidateCORSOrigin(origin); err != nil {
		return err
	}
	g.corsOrigin = strings.TrimSpace(origin)
	return nil
}

// SetDB 挂载告警中心数据源（装配期；仅 DB 部署时有值）。
func (g *RESTGateway) SetDB(pool *pgxpool.Pool, tenant string) { g.db, g.tenant = pool, tenant }

// SetLogf 注入服务端错误日志（装配期调用；nil = 静默）。
func (g *RESTGateway) SetLogf(f func(string, ...any)) {
	if f == nil {
		f = func(string, ...any) {}
	}
	g.logf = f
}

// NewRESTGateway 构造。hub 为事件广播器（W11 实时推送），可为 nil。
// limits 为装配层注入的时间窗/体上限（零值字段回退 config 默认，同源单一定义）。
func NewRESTGateway(noise *NoiseEngine, sem *SemanticModelServer, incidents incident.Store,
	token string, audit AuditLog, hub *EventHub, limits RESTLimits) *RESTGateway {
	if limits.DedupWindow <= 0 {
		limits.DedupWindow = config.DefaultDedupWindow
	}
	if limits.IncidentBodyLimit <= 0 {
		limits.IncidentBodyLimit = config.DefaultIncidentBodyLimit
	}
	if limits.NotifyBodyLimit <= 0 {
		limits.NotifyBodyLimit = config.DefaultNotifyBodyLimit
	}
	// SLA/KPI 零值回退 config 默认（同源单一定义，#10 纪律；装配层正常会注入）。
	if limits.SLACritical <= 0 {
		limits.SLACritical = time.Duration(config.DefaultSLACriticalMinutes) * time.Minute
	}
	if limits.SLAWarning <= 0 {
		limits.SLAWarning = time.Duration(config.DefaultSLAWarningMinutes) * time.Minute
	}
	if limits.SLAInfo <= 0 {
		limits.SLAInfo = time.Duration(config.DefaultSLAInfoMinutes) * time.Minute
	}
	if limits.KPIWindow <= 0 {
		limits.KPIWindow = config.DefaultKPIWindow
	}
	return &RESTGateway{noise: noise, sem: sem, incidents: incidents, token: token,
		audit: audit, hub: hub, limits: limits}
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
	// W10-3（F-07）运维 KPI 聚合（MTTA/MTTR/吞吐，口径见 rest_kpi.go）。
	mux.Handle("GET /api/v1/kpis", h)
	mux.Handle("GET /api/v1/incidents/{id}/rca", h) // #12 按需根因分析（Token 门禁在 handler 内）
	// 二期池 #7 S2：RCA 复盘会话（incident 子路径，拍板④；Token 门禁在 handler 内）。
	mux.Handle("GET /api/v1/incidents/{id}/rca/session", h)
	mux.HandleFunc("POST /api/v1/incidents/{id}/rca/session", g.handleSessionWrite)
	mux.HandleFunc("POST /api/v1/incidents", g.handleCreateIncident)
	mux.Handle("GET /api/v1/incidents/{id}/duplicates", h)
	mux.Handle("GET /api/v1/incidents/{id}/audit", h)
	// W12 审计解锁包：全局审计检索（读路径鉴权口径同 /{id}/audit——无 Token，
	// handler 与契约见 rest_audit.go）。
	mux.Handle("GET /api/v1/audit", h)
	// W10-1（F-03）事件混合时间线：告警进出∥变更∥处置三源归并（读路径，
	// 鉴权口径同现有 GET）。
	mux.Handle("GET /api/v1/incidents/{id}/timeline", h)
	mux.HandleFunc("POST /api/v1/incidents/{id}/merge", g.handleMergeIncident)
	mux.HandleFunc("POST /api/v1/incidents/{id}/transition", g.handleTransitionIncident)
	// W11-4（F-12）Runbook 记录版：手册库 + 事件挂载 + 执行记录（只记不执行，
	// handler 与契约见 rest_runbook.go / docs/前端契约-runbook.md）。
	mux.Handle("GET /api/v1/runbooks", h)
	mux.HandleFunc("POST /api/v1/runbooks", g.handleRunbookCreate)
	mux.Handle("GET /api/v1/incidents/{id}/runbooks", h)
	mux.HandleFunc("POST /api/v1/incidents/{id}/runbooks", g.handleIncidentRunbookMount)
	mux.HandleFunc("DELETE /api/v1/incidents/{id}/runbooks/{rid}", g.handleIncidentRunbookUnmount)
	mux.Handle("GET /api/v1/incidents/{id}/runbooks/{rid}/executions", h)
	mux.HandleFunc("POST /api/v1/incidents/{id}/runbooks/{rid}/executions", g.handleIncidentRunbookExecutionAppend)
	mux.HandleFunc("GET /api/v1/auth/status", g.handleAuthStatus)
	// W11 实时推送：SSE 事件流（控制台事件页订阅）。
	mux.Handle("GET /api/v1/events/stream", h)
	// 告警中心：影子判决流（与 W6-3 评估同源数据，alerts.jsx 对齐）。
	mux.Handle("GET /api/v1/alerts", h)
	// W9-2 通知渠道配置（读 GET 走 CORS 包装；写 POST/DELETE 走独立 handler
	// 做 Token 鉴权 + requireJSON，与其他写端点一致）。
	mux.Handle("GET /api/v1/notify/channels", h)
	mux.HandleFunc("POST /api/v1/notify/channels", g.handleNotifyChannels)
	mux.HandleFunc("POST /api/v1/notify/channels/{name}/enabled", g.handleNotifyChannelItem)
	mux.HandleFunc("DELETE /api/v1/notify/channels/{name}", g.handleNotifyChannelItem)
}

// ---- Host 头校验（第十一轮 P1-1：DNS rebinding 穿透回环门禁）----
//
// 威胁模型：默认部署 = 回环监听 + 空 token（ADR-009 的"安全默认"）。此时
// authorized() 恒真、读路径本就无 Token，而"回环 = 只有本机能访问"这条前提
// 对浏览器不成立：受害者机器上的恶意网页可把自家域名 DNS rebinding 到
// 127.0.0.1 —— 请求从本机发出（监听方看是本机连接），对浏览器看是**同源**
// （CORS 不拦），于是能读全局审计（高敏、读路径无 Token）并写全部写端点。
// 这正是 ADR-009:7 自认的"代码层没有任何强制"缺口。
//
// 阻断点：浏览器不允许脚本伪造 Host，rebinding 请求必然携带**真实域名**的
// Host 头；而合法本机调用的 Host 只会是回环形态。故回环监听时把 r.Host 收进
// 白名单，不符即 403。
//
// 非回环监听**不启用**此校验：那种形态 ADR-009 已强制 token 门禁（无 token
// 直接启动失败），且前置鉴权反代/自定义域名会让 Host 白名单变成误杀源。
//
// 覆盖面：接线单点在 newHTTPServer（包在整棵 mux 之外），因此 REST 读端点、
// SSE 事件流、/console、换皮 UI 静态资源、/metrics、/healthz 与全部写路径
// 一视同仁——留一个豁免路由就是留一条穿透路径。

// hostGuardRejection 403 固定文案：不回显 r.Host（把用户可控输入反射进响应体
// 是 XSS/日志注入的老坑），也不说明期望形态（那等于给攻击者对齐口径）。
const hostGuardRejection = "forbidden: Host header not allowed"

// loopbackHostForms 回环 host 的三个规范形态。IPv6 带方括号：r.Host 用的是
// RFC 3986 host 形态（`[::1]:8080`），裸 `::1` 不会出现在 Host 头里。
var loopbackHostForms = []string{"127.0.0.1", "localhost", "[::1]"}

// listenAddrPort 取监听地址里的端口；拆不出（只写了 host）返回空串。
func listenAddrPort(listenAddr string) string {
	addr := strings.TrimSpace(listenAddr)
	if _, port, err := net.SplitHostPort(addr); err == nil {
		return port
	}
	return ""
}

// hostGuardAllowSet 构造回环监听下的 r.Host 白名单（字符串全等比对）。
//
// 集合构造规则（port 取配置的监听端口）：
//   - 三个回环形态各带配置端口：`127.0.0.1:P`、`localhost:P`、`[::1]:P`；
//   - 同时收进三个**裸形态**（无端口）：HTTP/1.1 允许客户端省略 :port
//     （`curl -H 'Host: localhost'` 与部分探活脚本就是这么发的）。裸回环形态
//     与 rebinding 域名（`evil.com[:P]`）不可能碰撞，放宽不削弱防护；
//   - 配置端口不可知（只写了 host，或 `:0` 让内核派随机端口）时，集合只含裸
//     形态，并由 portKnown=false 让 hostGuard 退化为按 host 部分判定——否则
//     合法本机调用会带着随机端口被整段拒掉（静默砖化比放行更糟）。
//
// 比对是**字符串全等、不做大小写归一**：IP 形态无大小写，`localhost` 由 HTTP
// 规范视为小写；大写形态一律拒（宁可让调用方改客户端，也不给 `LOCALHOST.x`
// 这类构造留缝）。
//
// 返回 portKnown 表示"配置端口是否可用于比对"（非空且非 "0"）。
func hostGuardAllowSet(listenAddr string) (allow map[string]struct{}, portKnown bool) {
	port := listenAddrPort(listenAddr)
	portKnown = port != "" && port != "0"
	allow = make(map[string]struct{}, len(loopbackHostForms)*2)
	for _, h := range loopbackHostForms {
		allow[h] = struct{}{} // 缺端口形态：按 r.Host 原样比对
		if portKnown {
			allow[h+":"+port] = struct{}{}
		}
	}
	return allow, portKnown
}

// hostGuard 包装 next，对回环监听启用 Host 头白名单校验（见上方威胁模型）。
// listenAddr 取 cfg.Security.ListenAddr（与 checkListenSecurity 同源，回环判定
// 复用 listen_guard.go 的 isLoopbackHost——两处判定不漂移）。
//
// 非回环监听原样返回 next（不启用校验）。
func hostGuard(listenAddr string, next http.Handler) http.Handler {
	addr := strings.TrimSpace(listenAddr)
	host := addr
	if h, _, err := net.SplitHostPort(addr); err == nil {
		host = h
	}
	if !isLoopbackHost(host) {
		return next
	}
	allow, portKnown := hostGuardAllowSet(addr)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, ok := allow[r.Host]; ok {
			next.ServeHTTP(w, r)
			return
		}
		// 服务端口不可知（host-only / `:0` 配置）：退化为只校验 host 部分，
		// 端口任意。isLoopbackHost 自带去方括号，`[::1]:随机端口` 同样命中。
		if !portKnown {
			if h, _, err := net.SplitHostPort(r.Host); err == nil && isLoopbackHost(h) {
				next.ServeHTTP(w, r)
				return
			}
		}
		// 403 而非 421 Misdirected Request：421 语义是"换条连接重试"，客户端
		// 会照做；这里是策略拒绝，要的是终态。
		writeErr(w, http.StatusForbidden, hostGuardRejection)
	})
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
	case "/api/v1/alerts":
		g.handleAlerts(w, r)
	case "/api/v1/notify/channels":
		// GET 列表（POST 由 mux 精确模式接管，不会进到这里）。
		g.handleNotifyChannels(w, r)
	case "/api/v1/incidents":
		// POST /api/v1/incidents 由 mux 精确模式（method+path）接管，不会进到这里。
		g.handleIncidents(w, r)
	case "/api/v1/runbooks":
		// POST /api/v1/runbooks 同上由精确模式接管；这里只会是 GET（手册库列表）。
		g.handleRunbookList(w, r)
	case "/api/v1/kpis":
		g.handleKPIs(w, r)
	case "/api/v1/audit":
		g.handleAuditGlobal(w, r)
	default:
		// /api/v1/clusters/{key} 与 /api/v1/incidents/{id}：路径参数经 PathValue 取。
		if key := r.PathValue("key"); key != "" {
			g.handleClusterDetail(w, r)
			return
		}
		if id := r.PathValue("id"); id != "" {
			// W11-4 二级路径参数 {rid}：executions 读（GET 走 h 包装进这里；
			// POST 记执行与 DELETE 解挂由 mux 精确模式接管）。必须先于
			// {id} 单参数分发——executions 路径同时携带 id 与 rid。
			if rid := r.PathValue("rid"); rid != "" && strings.HasSuffix(r.URL.Path, "/executions") {
				g.handleIncidentRunbookExecutionsList(w, r)
				return
			}
			switch {
			case strings.HasSuffix(r.URL.Path, "/duplicates"):
				g.handleDuplicates(w, r)
			case strings.HasSuffix(r.URL.Path, "/audit"):
				g.handleAudit(w, r)
			case strings.HasSuffix(r.URL.Path, "/timeline"):
				g.handleIncidentTimeline(w, r)
			case strings.HasSuffix(r.URL.Path, "/runbooks"):
				g.handleIncidentRunbooksList(w, r)
			case strings.HasSuffix(r.URL.Path, "/rca/session"):
				g.handleSessionRead(w, r) // 二期池 #7：复盘会话读（懒恢复在编排器内）
			case strings.HasSuffix(r.URL.Path, "/rca"):
				g.handleRCA(w, r)
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
