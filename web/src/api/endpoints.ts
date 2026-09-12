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
  incidentTimeline: (id: string) =>
    `${V1}/incidents/${encodeURIComponent(id)}/timeline`, // GET（W10-1 混合时间线 ?limit=&cursor=）
  incidentMerge: (id: string) => `${V1}/incidents/${encodeURIComponent(id)}/merge`, // POST
  incidentTransition: (id: string) =>
    `${V1}/incidents/${encodeURIComponent(id)}/transition`, // POST（可带 sla_minutes 覆盖）

  // 运维 KPI（W10-3 F-07；?window=168h&severity=…，口径见 rest_kpi.go）
  kpis: `${V1}/kpis`, //                  GET

  // 告警中心 · 影子判决流（rest_alerts.go；需 OPS_DB_DSN）
  alerts: `${V1}/alerts`, //            GET  ?limit=（默认 200，上限 1000）

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
