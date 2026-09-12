# 前端分离说明 · console.html 冻结与 API 契约（优化方案 #9）

- **日期**：2026-09-13
- **关联**：ADR-013（前端独立工程）、`cmd/opscopilot/console.html`（go:embed 内嵌页）、
  `web/`（独立前端工程）、`console_source_sample/`（EagleOps 原型，设计种子、不入库构建）

## 1. console.html 冻结声明

`cmd/opscopilot/console.html` 自本方案起**功能冻结**：

1. 不新增页面、不新增交互、不扩端点消费面——**只修 bug**（安全/数据错误类）；
2. 它仍是二进制自带的**最小可用控制台**（离线评估/冒烟测试场景依赖 go:embed
   通道），在 `web/` 达到其功能超集并通过验收前**不移除**（届时另立 ADR）；
3. 它是 `web/` 的**冻结参照物**：视图口径、类名皮肤（tokens.css/base.css）、
   鉴权语义（X-OpsCopilot-Token / auth/status 探测）以它为准做 1:1 迁移；
4. 所有新页面、新交互一律进 `web/`；改 `console.html` 的 PR 若无 bugfix 理由，
   评审直接拒绝。

内嵌页现状盘点（4 视图，1213 行单文件）：

| 视图（#view-*） | 功能面 | web/ 承接 |
|---|---|---|
| overview 降噪总览 | 活跃簇 KPI×4、簇表格（state 过滤）、簇详情、拓扑图（SVG，节点置信度着色、>300 分桶聚合、节点邻域+时间切片+24h 变更） | `#/overview`（已迁移，拓扑图除外）+ `#/topology`（占位） |
| alerts 告警中心 | 影子判决流表格 + 知识库富化详情（级别/摘要/指标阈值/影响/措施） | `#/alerts`（已迁移） |
| events 事件 | KPI×4、状态/来源过滤、游标分页、人工建单、详情（转移/疑似重复合并/审计轨迹）、Token 输入、SSE 实时+30s 轮询降级 | `#/incidents`（已迁移） |
| settings 设置 | 通知渠道 CRUD（generic/feishu/wecom、最低严重级路由、启停、内置 console 兜底不可删） | `#/settings`（占位，端点已备） |

## 2. API 契约清单（唯一事实来源：`cmd/opscopilot/rest_*.go` 的 `Register()`）

Base：`/api/v1`。读路径无鉴权（受 CORS/监听门禁保护，ADR-009）；**写路径要求
`X-OpsCopilot-Token` 头等于 `OPS_WEBHOOK_TOKEN`**（`AuthHeader`，change_webhook.go），
且 `Content-Type: application/json`（CSRF 准入）。错误统一 `{"error": msg}`；
503 `not wired` = 依赖未装配（DB/噪声引擎/事件存储）。

| 端点 | 方法 | 参数/语义 | 写鉴权 | Go 源 |
|---|---|---|---|---|
| `/api/v1/clusters` | GET | `state=active\|resolved\|all`（默认 active）、`limit`（默认 200、上限 1000）→ `{clusters,count,truncated}` | — | rest_gateway.go |
| `/api/v1/clusters/{key}` | GET | ClusterRecord 全量（fingerprints/node_keys/evidence） | — | rest_topology.go |
| `/api/v1/topology` | GET | `node_key`、`depth`、`as_of`（时间切片）→ `{nodes,edges,resolved_as_of}` | — | rest_topology.go |
| `/api/v1/changes` | GET | `node_key`、`window_start`、`window_end` → `{changes}` | — | rest_topology.go |
| `/api/v1/incidents` | GET | `state=""\|active\|open\|acked\|mitigated\|resolved`、`origin`、`limit`、`cursor` → `{incidents,count,next_cursor,stats,persistence}` | — | rest_incidents.go |
| `/api/v1/incidents` | POST | 人工建单 `{title,severity,created_by,id?}` | ✅ | rest_incidents.go |
| `/api/v1/incidents/{id}` | GET | 单事件快照 | — | rest_incidents.go |
| `/api/v1/incidents/{id}/duplicates` | GET | L2 疑似重复候选（只提示不自动合并） | — | rest_incidents.go |
| `/api/v1/incidents/{id}/audit` | GET | 单事件审计轨迹（append-only + 哈希链，ADR-005） | — | rest_incidents.go |
| `/api/v1/incidents/{id}/merge` | POST | `{target_id,actor}` 人工合并 | ✅ | rest_incidents.go |
| `/api/v1/incidents/{id}/transition` | POST | `{to,actor}` 状态机流转 | ✅ | rest_stream.go |
| `/api/v1/alerts` | GET | 影子判决流（alert_event source='shadow'）`limit`（默认 200、上限 1000）→ `{alerts,count}`，含知识库富化字段 | — | rest_alerts.go |
| `/api/v1/notify/channels` | GET / POST | 渠道列表（含禁用）/ 幂等 upsert `{name,kind,url,min_severity}` | POST ✅ | rest_notify.go、notify_channels.go |
| `/api/v1/notify/channels/{name}/enabled` | POST | 软开关 `{"enabled":bool}` | ✅ | notify_channels.go |
| `/api/v1/notify/channels/{name}` | DELETE | 删除（内置 console 兜底渠道不可删） | ✅ | notify_channels.go |
| `/api/v1/auth/status` | GET | 写权限探测 `{write_authorized,mode}`（mode: open/shared_secret） | — | rest_stream.go |
| `/api/v1/events/stream` | GET | **SSE**（见 §3） | — | rest_stream.go、sse.go |

> 前端镜像表：`web/src/api/endpoints.ts`。与 Go 侧漂移时**以 Go 侧为准**并同步修正；
> 改路由的 PR 不同步端点表 = 验收不过（ADR-013 交叉检查）。

## 3. SSE 契约（GET /api/v1/events/stream）

- 传输：`text/event-stream`，20s 心跳注释帧；单实例订阅上限 200（超限 503）。
- 事件名：`incident`；data = `{"type":"created|updated|resolved|merged|attached","incident":{…完整快照}}`
  （`SSEMessage`，sse.go）。语义是幂等全量刷新：缓冲满丢帧无损，客户端下一帧自愈。
- 前端封装（`web/src/api/sse.ts`）：EventSource 原生重连之外，CLOSED 态指数退避
  重建（1s→2s→5s→10s 封顶）；降级期 UI 显示「轮询降级」并以 30s 轮询兜底——
  与内嵌页同口径。

## 4. 部署与门禁衔接（要点）

1. **开发**：`vite` 代理 `/api → 127.0.0.1:8080`，浏览器视角同源，CORS 门禁
   （`OPS_CORS_ORIGIN` 默认仅同源、禁 `*`）零改动；env 见 `web/.env.example`。
2. **生产二选一**（ADR-013 §5）：优先后端静态挂载 `web/dist/`（同源）；确需独立
   部署才显式配 `OPS_CORS_ORIGIN=https://<前端源>`，并复核监听门禁（非回环暴露
   必须前置鉴权反代，ADR-009）。
3. **Token**：sessionStorage 关页即清；`auth/status` 探测到 `write_authorized`
   （反代注入或未启用鉴权）时自动隐藏输入框。`VITE_OPS_TOKEN` 仅供内网调试，
   会打进构建产物，生产禁用。

## 5. 迁移路线图

| 批次 | 内容 | 依赖 |
|---|---|---|
| 已完成 | 告警中心、事件（含 SSE/写操作）、降噪总览（簇+KPI+详情） | — |
| 下一批 | 拓扑视图实装（SVG 环形布局、分桶聚合、节点邻域/时间切片/24h 变更） | 无（端点已备） |
| 下一批 | 设置·通知渠道 CRUD + 表单校验 | OPS_DB_DSN |
| 二期 | 全局审计视图 | **阻塞**：待后端新增审计列表端点（ADR-005） |
| 二期候选 | 原型独有页（RCA 根因分析、自动修复、AI 助手抽屉） | 待后端能力立项 |
| 收口 | web/ 功能超集 + 验收通过后，另立 ADR 移除 go:embed 通道 | 全部视图迁移完毕 |

验收口径：每个视图迁移需对照内嵌页同视图做**数据一致性**（同端点同参数渲染结果一致）
与**交互回归**（过滤/分页/写操作/SSE 降级）双查；通过后在 `web/README.md`
迁移表更新状态。
