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
  // 时点有效性双时态（pb.TopologyNode json tag：valid_from/valid_to）与来源
  valid_from?: string;
  valid_to?: string;
  source?: string;
}
export interface TopoEdge {
  src_key: string;
  dst_key: string;
  confidence?: Confidence;
  relation?: string;
  source?: string;
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
  acked_at?: string; // 首次 ack 时刻（零值 "0001-01-01T00:00:00Z" = 从未 ack）
  sla_minutes?: number; // 单事件覆盖（0 = 按 severity 取服务端默认）
  // SLA 只读派生视图（GET detail/list 才有；SSE 广播是裸实体）
  sla_deadline?: string;
  sla_remaining_seconds?: number; // 可为负 = 已超时时长（终态冻结在闭环时刻）
  sla_breached?: boolean;
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

// ---------- 全局审计（GET /api/v1/audit，W12 审计解锁包） ----------
/** 列表行是后端视图（auditEntryView）：detail 全文只从单事件端点取，
 *  summary 由后端按 action 的既定 detail 键提取（缺键回退 detail 首 120 字符）。 */
export interface AuditGlobalEntry {
  incident_id: string;
  action: string; // 封闭集合：create|transition|attach_cluster|merge|external_recovery_ignored|rate_limited|ingest_failed|rca
  actor: string;
  occurred_at: string;
  summary: string;
}
export interface AuditListResponse {
  entries: AuditGlobalEntry[];
  count: number;
  next_cursor: string; // 空 = 已到末尾（keyset：(occurred_at, id) 倒序游标）
  persistence: string; // memory（无 DSN 镜像，易失）| timescaledb
  /** 仅内存镜像时出现：如实声明"重启即丢/护栏丢最旧"（R6-4 口径）。 */
  partial_hint?: string;
}

// ---------- 事件混合时间线（GET /api/v1/incidents/{id}/timeline，W10-1/F-03） ----------
/** kind 封闭集合（rest_timeline.go TimelineKind*，同刻稳定序 change<action<alert）。 */
export type TimelineKind = "alert_in" | "alert_out" | "change" | "action";
export interface TimelineItem {
  ts: string; // RFC3339
  kind: TimelineKind;
  source_id: string; // 源前缀唯一引用（alert:/change:/action:）
  summary?: string;
  severity?: string; // 仅告警源
  confidence?: string; // 仅变更源（ADR-007）
}
export interface TimelineResponse {
  incident_id: string;
  items: TimelineItem[];
  count: number;
  total: number; // 三源归并后的全量条数（不受分页影响）
  next_cursor: string; // 空 = 已到末尾
  partial: boolean; // true ⇒ 有依赖源缺席/不完整（见 missing）
  missing: Record<string, string>; // 源名 → 缺席/不完整原因
}

// ---------- 运维 KPI（GET /api/v1/kpis，W10-3/F-07） ----------
/** 口径（internal/incident/kpi.go）：队列 = created_at 入窗；MTTA=avg(acked−created)、
 * MTTR=avg(resolved−created)；**平均闭环时长与 MTTR 同口径（一个字段）**；
 * 均值 0 且 samples=0 表示"无样本"，不是"零耗时"。 */
export interface KPIStats {
  created_count: number;
  resolved_count: number;
  acked_samples: number;
  resolved_samples: number;
  mtta_seconds: number;
  mttr_seconds: number;
}
export interface KPIResponse {
  window: string; // 实际生效窗（Go duration 文本）
  window_start: string;
  generated_at: string;
  severity: string; // 空 = 未过滤
  stats: KPIStats;
}

// ---------- RCA 按需根因分析（GET /api/v1/incidents/{id}/rca，W11-2 · ADR-014） ----------
/** 六步流水线封闭集合（internal/rca/steps.go Step* 常量）。 */
export type RcaStepName = "collect" | "hypothesize" | "verify" | "attribution" | "conclude" | "recommend";
/** 步骤状态（internal/rca/rca.go Status*：pending=占位未实现，failed=本步出错但整体可诊断）。 */
export type RcaStepStatus = "done" | "pending" | "failed";
export interface RcaStep {
  name: string; // 理论上 ∈ RcaStepName，宽松处理防后端加步炸前端
  status: RcaStepStatus;
  duration_ms: number;
  error?: string;
}
/** 单条产出（rcaFindView）；confidence 三段口径与拓扑同源（ADR-007）。 */
export interface RcaFinding {
  step: string;
  summary: string;
  node_keys?: string[];
  confidence: Confidence;
  ref?: string;
}
export interface RcaEvidenceCounts {
  nodes: number;
  edges: number;
  changes: number;
}
export interface RcaResponse {
  incident_id: string;
  title?: string;
  cluster_keys?: string[];
  t0?: string;
  window?: string; // Go duration 文本（如 "24h0m0s"）
  alerted_nodes?: string[];
  evidence: RcaEvidenceCounts;
  steps?: RcaStep[]; // Go nil slice → JSON null（"跑过但为空"与"没跑"都不炸渲染）
  findings?: RcaFinding[];
  root_causes?: RcaFinding[];
  /** null = conclude pending（llm-gateway 未接线，ADR-003 禁止伪 RCA）。 */
  conclusion: string | null;
  llm_used: boolean;
  /** 本次回显的 findings 相对全量有裁剪（?all=1 恒 false）。 */
  truncated: boolean;
  /** 被 OPS_RCA_MAX_FINDINGS 裁掉的条数（全量报告口径）。 */
  findings_truncated: number;
  persistence: string; // memory | timescaledb
}

// ---------- RCA 复盘会话（GET/POST /api/v1/incidents/{id}/rca/session，二期 #7 S2） ----------
export interface SessionTurn {
  seq: number;
  role: "user" | "assistant"; // 拍板②封闭集合
  content: string;
  created_by?: string;
  created_at: string;
}
export interface SessionResponse {
  incident_id: string;
  created_by?: string;
  participants?: string[];
  turns: SessionTurn[]; // 后端保证空会话回显 []（不给 null 分支）
  /** 仅 POST 回显：pending = LLM 未配置/失败，assistant 轮不在 turns 里（宁 pending 不假答）。 */
  assistant_status?: "answered" | "pending";
  persistence: string;
}

// ---------- Runbook 记录版（E1–E7，W11-4/F-12；契约 docs/前端契约-runbook.md） ----------
/** 字段名逐一对齐契约 §2（rest_runbook.go）。总口径：只记录与展示、不自动执行；
 * 执行记录非审计证据（不进 incident_audit 哈希链，展示面不标"审计级"）。 */
export interface Runbook {
  id: string; // 业务键；E2 省略则服务端生成 "RBK-<时间戳>"
  title: string; // 必填，≤512
  content?: string; // markdown 正文，≤262144B；系统不解析不执行
  scope_severity?: string; // "" | critical | warning | info（"" = 不限级别）
  scope_service?: string; // 服务标签，"" = 不限
  created_by: string; // 必填（身份钩子）
  created_at?: string;
  updated_at?: string;
}
/** E1 GET /runbooks（库列表，新→旧）。 */
export interface RunbooksResponse {
  runbooks: Runbook[];
  count: number;
  persistence?: string;
}
/** E2 POST /runbooks 回显。 */
export interface RunbookCreateResponse {
  runbook: Runbook;
  persistence?: string;
}
/** E3 列表项 MountView：手册字段 + 挂载元数据 + 执行计数，平铺单层。
 * 注意键是 runbook_id（契约 §2 明示无 id 键——挂载视图的键，避免与手册主键二义）。 */
export interface RunbookMountView {
  runbook_id: string;
  title: string;
  content?: string;
  scope_severity?: string;
  scope_service?: string;
  created_by?: string; // 手册作者
  created_at?: string;
  updated_at?: string;
  mounted_by?: string; // 首次挂载人（幂等重挂不刷新）
  mounted_at?: string;
  execution_count: number; // 该挂载点已记录执行条数
}
export interface IncidentRunbooksResponse {
  incident_id: string;
  runbooks: RunbookMountView[];
  count: number;
  persistence?: string;
}
/** E4 挂载回显（成功与幂等重挂同码同形，可安全重试）。 */
export interface RunbookMountResponse {
  incident_id: string;
  runbook_id: string;
  mounted: boolean;
  persistence?: string;
}
/** E5 解挂回显（只断挂载关系，执行记录保留）。 */
export interface RunbookUnmountResponse {
  incident_id: string;
  runbook_id: string;
  unmounted: boolean;
  persistence?: string;
}
/** 执行记录行（append-only；seq/executed_at 服务端发号打点，前端只读不可传）。 */
export interface RunbookExecution {
  seq: number; // int64，挂载内严格递增
  executed_by: string; // 必填（身份钩子）
  executed_at: string;
  result: string; // 自由文本，≤8192B
  refs: unknown[]; // 元素自由（string 或 object）；缺省 []，≤100 元素
}
/** E6 GET executions（旧→新）；auto_execution 恒 false（契约 §0 总口径）。 */
export interface RunbookExecutionsResponse {
  incident_id: string;
  runbook_id: string;
  executions: RunbookExecution[];
  count: number;
  auto_execution: boolean;
  persistence?: string;
}
/** E7 POST executions 回显（只记不执行）。 */
export interface RunbookExecutionAppendResponse {
  incident_id: string;
  runbook_id: string;
  execution: RunbookExecution;
  auto_execution: boolean;
  persistence?: string;
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
/** POST upsert / POST enabled / DELETE 共用回包（rest_notify.go reloadAndReport）：
 *  写成功后热重载注册表，active_channels = 重载后启用渠道数；
 *  warning 非空 = 已落库但重载失败（提示"重启或下次写入补上"，必须透出）。 */
export interface ChannelWriteResponse {
  status: string;
  active_channels: number;
  warning?: string;
}

// ---------- SSE（GET /api/v1/events/stream，事件名 "incident"） ----------
export interface SSEIncidentMessage {
  type: string; // created | updated | resolved | merged | attached
  incident: Incident;
}
