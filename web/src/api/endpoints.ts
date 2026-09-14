/**
 * 端点常量表 —— 唯一事实来源是 cmd/opscopilot/rest_*.go 的 Register()。
 * 内嵌 console.html 冻结在后端；本表若与 Go 侧漂移，以 Go 侧为准并修正本表。
 */
const V1 = "/api/v1";

export const ENDPOINTS = {
  // 降噪总览 / 簇（rest_gateway.go）
  clusters: `${V1}/clusters`, //        GET  ?state=active|resolved|all&limit=
  cluster: (key: string) => `${V1}/clusters/${encodeURIComponent(key)}`, // GET

  // 拓扑与变更（rest_gateway.go / rest_topology.go）
  topology: `${V1}/topology`, //        GET  ?node_key=&depth=&as_of=
  changes: `${V1}/changes`, //          GET  ?node_key=&window_start=

  // 事件双链路（rest_incidents.go）
  incidents: `${V1}/incidents`, //      GET  ?state=&origin=&limit=&cursor= | POST（写，带 Token）
  incident: (id: string) => `${V1}/incidents/${encodeURIComponent(id)}`, // GET
  incidentDuplicates: (id: string) =>
    `${V1}/incidents/${encodeURIComponent(id)}/duplicates`, // GET
  incidentAudit: (id: string) => `${V1}/incidents/${encodeURIComponent(id)}/audit`, // GET
  // W12 审计解锁包：全局审计检索（读路径无 Token，口径同 incidentAudit；
  // 参数 actor/action（封闭集合，未知 400）/since/until（RFC3339，[since,until) 半开）/
  // limit（默认 200 上限 1000）/cursor（keyset，坏 400）；rest_audit.go）。
  audit: `${V1}/audit`, //                 GET
  incidentTimeline: (id: string) =>
    `${V1}/incidents/${encodeURIComponent(id)}/timeline`, // GET（W10-1 混合时间线 ?limit=&cursor=）
  incidentMerge: (id: string) => `${V1}/incidents/${encodeURIComponent(id)}/merge`, // POST
  incidentTransition: (id: string) =>
    `${V1}/incidents/${encodeURIComponent(id)}/transition`, // POST（可带 sla_minutes 覆盖）

  // 按需根因分析（rest_rca.go，ADR-014）：GET ?actor=（写审计留痕）&all=1（全量 findings）；
  // OPS_RCA=off → 503，超时 → 504。分析按请求重跑（同步），前端按钮触发不自动拉。
  incidentRca: (id: string) => `${V1}/incidents/${encodeURIComponent(id)}/rca`, // GET
  // RCA 复盘会话（rest_rca_session.go，二期 #7 S2）：GET 读会话（OPS_SESSION=off → 503）；
  // POST body {content, actor}（DisallowUnknownFields，字段名以 Go 侧为准）。
  incidentRcaSession: (id: string) => `${V1}/incidents/${encodeURIComponent(id)}/rca/session`, // GET | POST

  // Runbook 记录版（rest_runbook.go E1–E7，契约 docs/前端契约-runbook.md，W11-4/F-12）：
  // 只记录与展示、不自动执行（执行响应恒带 auto_execution:false）；写路径 Token+JSON；
  // 无 DSN 时全端点显式 503（store 未接线，不伪装空 200）。
  runbooks: `${V1}/runbooks`, // E1 GET（库列表 新→旧）| E2 POST（新建，id 可省略）
  incidentRunbooks: (id: string) => `${V1}/incidents/${encodeURIComponent(id)}/runbooks`, // E3 GET（挂载+正文+计数）| E4 POST（挂载，幂等可重试）
  incidentRunbook: (id: string, rid: string) =>
    `${V1}/incidents/${encodeURIComponent(id)}/runbooks/${encodeURIComponent(rid)}`, // E5 DELETE（解挂；执行历史保留）
  incidentRunbookExecutions: (id: string, rid: string) =>
    `${V1}/incidents/${encodeURIComponent(id)}/runbooks/${encodeURIComponent(rid)}/executions`, // E6 GET（旧→新）| E7 POST（记一次执行；未挂载 404）

  // 运维 KPI（W10-3 F-07；?window=168h&severity=…，口径见 rest_kpi.go）
  kpis: `${V1}/kpis`, //                  GET

  // 告警中心 · 影子判决流（rest_alerts.go；需 OPS_DB_DSN）
  alerts: `${V1}/alerts`, //            GET  ?limit=（默认 200，上限 1000）&since=&until=&cursor=（keyset 游标，2026-09-14 起）

  // 鉴权探测（rest_gateway.go handleAuthStatus）
  authStatus: `${V1}/auth/status`, //   GET

  // SSE 实时推送（sse.go；事件名 "incident"）
  eventsStream: `${V1}/events/stream`, // GET（EventSource）

  // 通知渠道（rest_notify.go / notify_channels.go；需 OPS_DB_DSN）
  notifyChannels: `${V1}/notify/channels`, // GET | POST
  notifyChannelEnabled: (name: string) =>
    `${V1}/notify/channels/${encodeURIComponent(name)}/enabled`, // POST
  notifyChannel: (name: string) =>
    `${V1}/notify/channels/${encodeURIComponent(name)}`, // DELETE
} as const;

/** 写路径鉴权头（与内嵌 console 同口径：X-OpsCopilot-Token）。 */
export const TOKEN_HEADER = "X-OpsCopilot-Token";

/** SSE 事件名（服务端 EventHub 广播）。 */
export const SSE_EVENT_INCIDENT = "incident";
