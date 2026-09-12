/**
 * DTO 类型 —— 字段名逐一对齐 Go handler 的 json tag（rest_*.go）。
 */

// ---------- 告警中心（GET /api/v1/alerts，alertView） ----------
export interface ShadowAlert {
  occurred_at: string;
  fingerprint: string;
  cluster_key: string;
  reason: string; // new-incident | cluster-merge | dedup-window
  // 知识库富化字段
  title?: string;
  level?: string; // 中文级别：紧急 | 重要 | 一般
  error_code?: string;
  service?: string;
  summary?: string;
  metric?: string;
  threshold?: string;
  impact?: string;
  steps?: string[];
}
export interface AlertsResponse {
  alerts: ShadowAlert[];
  count: number;
}

// ---------- 簇（GET /api/v1/clusters[/{key}]） ----------
export type ClusterState = "open" | "acked" | "resolved";
export interface AlertCluster {
  cluster_key: string;
  state: ClusterState;
  severity?: string;
  alert_count: number;
  summary?: string;
  fingerprints?: string[];
  node_keys?: string[];
  first_seen_at?: string;
  last_seen_at?: string;
}
export interface ClustersResponse {
  clusters: AlertCluster[];
  count: number;
  truncated: boolean;
}

// ---------- 拓扑（GET /api/v1/topology） ----------
export type Confidence = "high" | "medium" | "low";
export interface TopoNode {
  node_key: string;
  node_type?: string;
  confidence?: Confidence;
}
export interface TopoEdge {
  src_key: string;
  dst_key: string;
  confidence?: Confidence;
  relation?: string;
}
export interface TopologyResponse {
  nodes: TopoNode[];
  edges: TopoEdge[];
  resolved_as_of?: string;
}

// ---------- 变更（GET /api/v1/changes） ----------
export interface ChangeRecord {
  node_key?: string;
  change_type: string;
  occurred_at: string;
  actor?: string;
  id?: string;
  source?: string;
  ref?: string;
  revision?: string;
  summary?: string;
  confidence?: Confidence;
}
export interface ChangesResponse {
  changes: ChangeRecord[];
  count?: number;
}

// ---------- 事件（rest_incidents.go） ----------
export type IncidentState = "open" | "acked" | "mitigated" | "resolved";
export type IncidentOrigin =
  | "manual"
  | "alertmanager"
  | "webhook"
  | "prometheus"
  | "azure"
  | "pull"
  | "api";
export interface Incident {
  id: string;
  title?: string;
  severity?: string;
  state: IncidentState;
  origin?: IncidentOrigin;
  source_ref?: string;
  created_by?: string;
  created_at?: string;
  updated_at?: string;
  cluster_keys?: string[];
  dedup_key?: string;
  merged_into?: string;
  auto_close_policy?: string;
}
export interface IncidentStats {
  active: number;
  resolved: number;
  manual: number;
  external: number;
}
export interface IncidentsResponse {
  incidents: Incident[];
  count: number;
  next_cursor: string; // 空 = 已到末尾
  stats: IncidentStats;
  persistence?: string;
}
export interface DuplicateCandidate {
  incident_id: string;
  title?: string;
  score: number;
  reasons?: string[];
}
export interface DuplicatesResponse {
  incident_id: string;
  candidates: DuplicateCandidate[];
  count: number;
}
export interface AuditEntry {
  action: string;
  actor?: string;
  occurred_at?: string;
  detail?: Record<string, string>;
}
export interface AuditResponse {
  incident_id: string;
  entries: AuditEntry[];
  count: number;
}

// ---------- 鉴权探测（GET /api/v1/auth/status） ----------
export interface AuthStatus {
  write_authorized: boolean;
  mode: "open" | "token" | string;
}

// ---------- 通知渠道（GET/POST /api/v1/notify/channels） ----------
export type ChannelKind = "generic" | "feishu" | "wecom";
export interface NotifyChannel {
  name: string;
  kind: ChannelKind;
  url: string;
  min_severity?: "info" | "warning" | "critical";
  enabled: boolean;
  updated_at?: string;
}
export interface ChannelsResponse {
  channels: NotifyChannel[];
  warning?: string;
}

// ---------- SSE（GET /api/v1/events/stream，事件名 "incident"） ----------
export interface SSEIncidentMessage {
  type: string; // created | updated | resolved | merged | attached
  incident: Incident;
}
