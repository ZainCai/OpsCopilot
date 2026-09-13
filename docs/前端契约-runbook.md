# 前端契约 · Runbook 记录版（W11-4 / F-12）

> 版本：2026-09-14 · 基线：migration `000020_runbook` · 实现：`cmd/opscopilot/rest_runbook.go` + `internal/runbook/store.go`
> 消费方：并行的 web 前端任务（事件详情页「处置手册」卡片 + 执行记录展示）。**字段名以本文件为准**（后端实现即此契约，键名漂移后端测试会红）。

## 0. 总口径（前端必读）

- **只记录与展示，不自动执行**（排期 W11-4 定案，对齐 F-12 清单"M1 仅记录与展示"）。系统是"给人看的手册挂载台账 + 人事后手填的执行流水"，**没有**"点一下按钮让系统替你跑手册"这回事。执行相关响应恒带 **`auto_execution: false`** 契约声明（同 duplicates 端点 `auto_merge:false` 的取向），前端据此渲染"记录"而非"运行"语义。
- **执行记录不算审计证据**（对齐《sessionstore消费方与接线》拍板②口径）：`runbook_execution_log` 是独立表，不进 `incident_audit` 哈希链，`result`/`refs` 无防篡改。合规取证面仍以 `/audit` 为准；runbook 展示面**不要**标注"审计级"。
- **降级**：无 DB（无 `DB_DSN`）时 runbook store 不接线，**所有 runbook 端点显式 503**（不返回空 200 伪装"没有手册"）。前端对 503 应显示"该功能需持久化后端"，与 RCA/session 端点降级表现一致。
- **鉴权**：读路径（GET）无鉴权（S1 回环部署口径）；写路径（POST/DELETE）**必带** `X-OpsCopilot-Token` 请求头（值 = 服务端 `OPS_WEBHOOK_TOKEN`）。未配置密钥时写路径不设门禁（仅限回环部署）。写路径 POST 还要求 `Content-Type: application/json`（CSRF 免预检阻挡），否则 415。
- **租户**：单租户隐式（`OPS_TENANT`），前端不传 `tenant_id`，一切按后端装配期固定的租户读写。

## 1. 端点清单

| # | 方法 | 路径 | 鉴权 | 说明 |
|---|------|------|------|------|
| E1 | GET | `/api/v1/runbooks` | 无 | 手册库列表（新→旧） |
| E2 | POST | `/api/v1/runbooks` | Token+JSON | 新建手册 |
| E3 | GET | `/api/v1/incidents/{id}/runbooks` | 无 | 事件挂载手册列表（含正文+执行计数，一次到位） |
| E4 | POST | `/api/v1/incidents/{id}/runbooks` | Token+JSON | 挂载（幂等） |
| E5 | DELETE | `/api/v1/incidents/{id}/runbooks/{rid}` | Token | 解挂（执行历史保留） |
| E6 | GET | `/api/v1/incidents/{id}/runbooks/{rid}/executions` | 无 | 某挂载点执行记录列表（旧→新） |
| E7 | POST | `/api/v1/incidents/{id}/runbooks/{rid}/executions` | Token+JSON | 追加一条执行记录（只记不执行） |

- `{id}` = 事件 id；`{rid}` = 手册 id（runbook id）。
- 字符集：`{id}`/`{rid}`/手册 `id` 均为 `[A-Za-z0-9._:-]`，长度 ≤128（与事件 id 同口径）。

## 2. 数据结构（JSON shape）

### Runbook（手册库行）

```jsonc
{
  "id": "rb-disk",              // string，业务键；省略则服务端生成 "RBK-<时间戳>"
  "title": "磁盘清理手册",       // string，必填，≤512
  "content": "# 磁盘满\n1. df -h",// string，markdown 正文，≤262144 字节；系统不解析不执行
  "scope_severity": "warning",  // "" | "critical" | "warning" | "info"（"" = 不限级别）
  "scope_service": "mysql",     // string 标签，"" = 不限；≤128
  "created_by": "alice",        // string，必填，≤128（身份钩子）
  "created_at": "2026-09-14T10:00:00Z", // RFC3339
  "updated_at": "2026-09-14T10:00:00Z"  // RFC3339
}
```

### MountView（E3 列表项：手册展示字段 + 挂载元数据 + 执行计数，**平铺单层**）

```jsonc
{
  "runbook_id": "rb-disk",      // = 手册 id（挂载键）
  "title": "磁盘清理手册",
  "content": "# 磁盘满\n1. df -h",
  "scope_severity": "warning",
  "scope_service": "mysql",
  "created_by": "alice",        // 手册作者
  "created_at": "...",
  "updated_at": "...",
  "mounted_by": "alice",        // 首次挂载人（幂等重挂不刷新）
  "mounted_at": "...",          // 首次挂载时间（幂等重挂不刷新）
  "execution_count": 2          // int，该挂载点已记录执行条数
}
```

> 注意：E3 项用 `runbook_id`（**不是** `id`），且**无** `id` 键——这是挂载视图的键，避免与手册主键二义。

### Execution（执行记录行，append-only）

```jsonc
{
  "seq": 3,                     // int64，挂载内严格递增（服务端 IDENTITY 发号，前端只读不可传）
  "executed_by": "alice",       // string，必填，≤128（执行人，身份钩子）
  "executed_at": "2026-09-14T10:05:00Z", // RFC3339（服务端时间，前端只读）
  "result": "清理3天前日志，水位回到62%", // string，必填，≤8192 字节（自由文本）
  "refs": ["https://ticket/42", {"url":"https://shot/1","label":"df -h"}]
            // JSON 数组，元素自由（string 或 object）；缺省/空 → []；≤100 元素、≤65536 字节
}
```

## 3. 逐端点契约

### E1 · GET `/api/v1/runbooks`

- 200：
  ```json
  { "runbooks": [ /* Runbook... */ ], "count": 1, "persistence": "timescaledb" }
  ```
- 空库：`runbooks: []`（恒数组，不给 `null`）。
- store 未接线（无 DSN）：**503** `{ "error": "runbook store not wired (needs DB DSN)" }`。
- DB 故障：500 `{ "error": "internal error" }`。

### E2 · POST `/api/v1/runbooks`（Token + JSON）

- 请求体（未知字段拒绝 `DisallowUnknownFields`）：
  ```json
  { "id": "rb-disk", "title": "磁盘清理手册", "content": "# ...",
    "scope_severity": "warning", "scope_service": "mysql", "created_by": "alice" }
  ```
  - `id` 可省略（服务端生成）；`title`、`created_by` 必填；`content`/`scope_*` 可省（默认 ""）。
- **201**：
  ```json
  { "runbook": { /* Runbook，含回填的 created_at/updated_at */ }, "persistence": "timescaledb" }
  ```
- 错误：401 无/错 Token；415 非 JSON；400 校验失败（带原因，如 `title is required` / `scope_severity must be one of critical|warning|info (or empty = any)`）；**409** `{ "error": "runbook id already exists" }`；503 未接线。

### E3 · GET `/api/v1/incidents/{id}/runbooks`

- 200：
  ```json
  { "incident_id": "INC-...", "runbooks": [ /* MountView... */ ], "count": 1, "persistence": "timescaledb" }
  ```
- 空挂载：`runbooks: []`。
- 事件不存在：**404** `{ "error": "incident not found: <id>" }`。
- 503（store 或 incident store 未接线）。

### E4 · POST `/api/v1/incidents/{id}/runbooks`（Token + JSON）—— 挂载

- 请求体：
  ```json
  { "runbook_id": "rb-disk", "mounted_by": "alice" }
  ```
- **200**（成功与幂等重挂同码同形）：
  ```json
  { "incident_id": "INC-...", "runbook_id": "rb-disk", "mounted": true, "persistence": "timescaledb" }
  ```
- **幂等**：同一 `(event, runbook)` 重复挂载不产生第二条、不刷新 `mounted_by`/`mounted_at`（首挂留痕保留）。前端"重复挂载"按钮可安全重试。
- 错误：401/415；400（`runbook_id is required` / `mounted_by is required (identity hook)`）；**404** 事件不存在 或 手册不存在（`runbook: not found`）；503。

### E5 · DELETE `/api/v1/incidents/{id}/runbooks/{rid}`（Token）—— 解挂

- 无请求体（DELETE 不强制 `Content-Type`）。
- 200：
  ```json
  { "incident_id": "INC-...", "runbook_id": "rb-disk", "unmounted": true, "persistence": "timescaledb" }
  ```
- **解挂只断挂载关系，执行记录保留**（E6 仍能读到历史；重挂后 `execution_count` 继续累计，seq 不重置）。前端解挂确认框应提示"历史记录不删除"。
- 错误：401；**404** 事件不存在 或 未挂载（`runbook: not mounted`）；503。

### E6 · GET `/api/v1/incidents/{id}/runbooks/{rid}/executions`

- 200：
  ```json
  { "incident_id": "INC-...", "runbook_id": "rb-disk",
    "executions": [ /* Execution... 旧→新 */ ], "count": 1,
    "auto_execution": false, "persistence": "timescaledb" }
  ```
- 从未执行 / 未挂载：`executions: []`（读语义不判挂载存在性；解挂后历史仍在）。
- 事件不存在：404；503。

### E7 · POST `/api/v1/incidents/{id}/runbooks/{rid}/executions`（Token + JSON）—— 记录一次执行

- 请求体：
  ```json
  { "executed_by": "alice", "result": "清理3天前日志，水位回到62%",
    "refs": ["https://ticket/42", {"url":"https://shot/1","label":"df"}] }
  ```
  - `executed_by`、`result` 必填；`refs` 可省（默认 `[]`），**必须是 JSON 数组**（对象/标量 → 400）。
  - 不接收 `seq`/`executed_at`（服务端发号与打点，传了即"未知字段"400）。
- **201**：
  ```json
  { "incident_id": "INC-...", "runbook_id": "rb-disk",
    "execution": { /* 刚落库的 Execution */ },
    "auto_execution": false, "persistence": "timescaledb" }
  ```
- **前置**：挂载点必须存在。未挂载（含已解挂）就记执行 → **404** `{ "error": "runbook: not mounted" }`。
- 错误：401/415；400（`executed_by is required (identity hook)` / `result is required (what was done / outcome)` / `refs must be a JSON array` / 各类超长）；404 事件不存在 或 未挂载；503。

## 4. 错误包络（全端点统一）

任何非 2xx 响应体恒为：

```json
{ "error": "<人读原因>" }
```

状态码语义：`400` 入参校验（带原因）· `401` Token 缺/错 · `404` 事件不存在 / 手册不存在 / 未挂载 · `409` 手册 id 重复 · `415` 写路径非 `application/json` · `500` 存储故障（原因只进服务端日志，客户端只见 `internal error`）· `503` runbook/incident store 未接线（无 DSN）。

## 5. 验收闭环（对齐排期"手册挂载/查看闭环"，端到端测已锁）

`建单(E2建手册可选) → 挂手册(E4) → 记一次执行(E7) → GET 全链(E1/E3/E6 读回)`：
1. `POST /api/v1/incidents`（既有链路 B）→ 拿 `id`；
2. `POST /api/v1/runbooks` → 拿 `runbook.id`；重复 id 期望 409；
3. `POST /api/v1/incidents/{id}/runbooks` → `mounted:true`；再发一次 → 仍 `mounted:true`、`GET E3` 里 `count:1` 且 `mounted_by` 未变（幂等）；
4. `POST .../executions` → 201，`execution.seq>=1`、`auto_execution:false`；`GET E3` 的 `execution_count` 变 1；
5. `GET .../executions` → `count:1`，`refs` 原样往返；
6. `DELETE .../runbooks/{rid}` → `unmounted:true`；`GET E3` 空、`GET .../executions` **仍见历史**（不随解挂消失）；重复解挂 → 404。

对应测试：`cmd/opscopilot/rest_runbook_e2e_pg_test.go`（真库全链 + 错误面）、`internal/runbook/store_pg_test.go`（挂载幂等/解挂保历史/append-only）、`cmd/opscopilot/rest_runbook_test.go`（无 DSN 503/401/415 门禁）。
