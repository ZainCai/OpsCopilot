# 设计：sessionstore 消费方与接线（二期池 #7 前置评审件）

> 日期：2026-09-12 · 基线 HEAD `ee21403` · 状态：**已拍板（2026-09-12，§5 五个开放问题全部定案，
> 均取默认方案）——按本文与拍板记录实施 S1/S2**
> 关联：ADR-004（Redis 双实例/P1-1）、ADR-001（事件骨干）、ADR-003（LLM 单出口）、
> ADR-012（单 owner 水平扩展/#11）、ADR-014/015（RCA 链路与 llmgw 接线）、
> `docs/功能点清单-M2候选.md` F-13、二期池优先级 #3/#4/#7。

## 1. 它到底是什么（读实现判定）

`internal/sessionstore` 现实现 = **通用 KV 会话袋**：`Save/Load/Delete(sessionID, []byte)`，
Redis 告警实例（构造期强制 role="alert"，违反 panic）、键前缀 `opscopilot:session:`、
统一 TTL 2h、覆盖写刷新 TTL。状态是**不透明字节**——无轮次结构、无 incident/user 关联字段、
无追加语义、无按事件查询。测试载荷 `{"user":"ops","view":"clusters"}` 暴露了它的出身：
P1-1 当时防的是"控制台界面态会话被 LRU 逐出蒸发"，**既不是"对话会话"，也不是"告警实例会话"**。
结论：抽象**名大于实**——包注释讲"会话状态"，但没有任何运行时消费方赋予其语义（rca_orchestrator.go
文件头 TODO 与二期池 #7 均确认"零消费方"）。接线前必须先定消费方语义，本包大概率只需**加一层
envelope/键约定**而非重写；若消费方需要追加式轮次历史，Redis KV 只配当热态缓冲，真相须落 PG（同 ADR-001
"加速层非真相源"原则）。

## 2. 消费方候选评估（产品视角）

| 候选 | 场景 | 与现有能力的重叠 | 增量价值 | 成本 | 判定 |
|---|---|---|---|---|---|
| a) RCA 复盘会话 | 人 + LLM 围绕一个 incident 多轮追问证据链/结论（衔接 #3 摘要、#4 自动触发） | 无现成承载（审计只记单次动作，无"往返上下文"） | **高**：副驾驶定位的最后一环；#3/#4 刚落，正是消费窗口 | 中 | ★ 首发 |
| b) 告警处理会话 | ack/resolve 上下文留痕 | **已被覆盖**：状态机 + actor/AckBy 字段（statemachine.go）+ incident_audit append-only 留痕（ADR-005） | 低：再造会话=第二套"谁干了什么"真相源，违背审计分离纪律 | — | 不做 |
| c) 副驾驶对话历史 | 全局 AI 抽屉（F-13）跨事件的闲聊式问答 | 前端未立（web/ 只有簇/事件视图），LLM 出口无 UI 触达 | 中，但依赖 F-13 排期 | 高 | 远期 |

**首发建议：a) RCA 复盘会话。** 理由：① b 与审计正面冲突、c 无 UI 可依附；② a 的挂点已被
#3/#4 天然铺好——复盘追问复用 llmgw 单出口与 RCAOrchestrator 证据链，是"把已交付件用起来"
而非新造能力；③ a 以 incident 为中心，与事件骨干（ADR-001）、双实例语义（§3.3）对齐。

## 3. 接线形态

**数据流（internal 禁互 import 纪律下）**：复用 rca_orchestrator.go / llm_summarizer.go 先例——
**cmd/（package main）汇合装配**。新增 `cmd/opscopilot/rca_session.go`（暂名）：REST handler →
热态经 `sessionstore.New(alertRDB, config.RedisAlert)`（main.go 已有告警实例 Redis 出口），
取证/结论复用 RCAOrchestrator，LLM 轮次只走 llmgw（ADR-003 单出口，禁直连）。
**不进 contracts/gRPC**：SemanticModel 是取证面契约，会话是交互面，塞进去污染事件面
（同二期池"不建议做"里 SSE 的理由）。键约定：~~`<tenant>:<incidentID>:<userID>`~~
**按拍板③修正为 `<tenant>:<incidentID>`（incident 级共享，非 per-user）**，
envelope 为带 `schema_ver` 的 JSON。

**存储选型（PG 表草案要点，不上 migration 编号）**：Redis 只做热态缓冲（TTL 内进行中会话）；
会话/轮次真相落 TimescaleDB：
- `rca_session(session_id uuid PK, tenant, incident_id FK, user_id, state open|closed, created/updated)`，索引 `(incident_id, updated_at)`；
- `rca_session_turn(turn_id bigserial PK, session_id FK, seq, role user|assistant|system, content jsonb, redacted bool, llm_meta jsonb)`——`llm_meta` 只存模型名/token 数/内容 SHA-256（对齐 llm_summarizer.go 脱敏纪律），**正文入 PG 前必须已过 llmgw 入向 DLP**；
- 会话关闭时可选把最终结论以 `action='rca-review'` 追加进 incident_audit（审计仍 append-only，轮次历史不回灌审计）。

**双实例语义（#11/ADR-012 之后）**：**session 跟 incident 走，不跟 leader 走**。复盘会话属
事件链路（人经由 REST 交互），与判决/采集无关；Redis 告警实例与 PG 都是副本共享态，任意副本
可读同一会话，天然满足"非 leader 只跑事件链路"。例外：若将来做 ADR-003 二档"LLM 全不可用→
延迟重跑队列"，队列**消费侧**须 leader-only 或复用 ingest 的 `SKIP LOCKED` 认领租约模式
（000016 先例），防多副本重跑风暴——本期不做，仅立规矩。

## 4. 切片计划

| 步 | 内容 | 验收 | 估 |
|---|---|---|---|
| S1 装配骨架 | cmd 汇合点接线 `sessionstore.New(alertRDB, RedisAlert)`，定键/envelope 约定，开关 ~~`OPS_RCA_SESSION`~~ **`OPS_SESSION`（拍板定案，默认 off）** | 装配测试（miniredis）round-trip 过；off 时零行为变更；role 误绑在启动期 panic 有测试锁定 | 0.5–1d |
| S2 复盘会话 MVP | incident 维度 REST 端点 ~~开/查/关会话 +~~ 追加一轮（**拍板④定案契约：`GET/POST /api/v1/incidents/{id}/rca/session`，GET 读会话/懒恢复、POST 追加轮次**；用户追问→取证→llmgw→轮次落 PG/热态进 Redis） | 同 incident 多轮上下文跨请求可读；LLM 故障 fail-open pending、**不出伪结论**（ADR-003 硬禁令）；轮次内容已过 DLP；重启非 leader 副本仍能续会话 | 2d |
| S3（挂账不做） | 二档延迟重跑队列、F-13 全局对话抽屉复用本会话层 | 待真实追问量分布与前端排期出现再评审 | — |

**明确不做**：不替代 incident_audit；不做 b) 处理会话；不在 gRPC/contracts 表达会话；
不引入第三个 Redis 实例；不在本池内做前端 UI。

## 5. 拍板记录（已拍板，2026-09-12）

五个开放问题全部按默认方案定案（拍板日期 2026-09-12，随 S1+S2 落地）：

1. **TTL：不改 2h 默认、不加配置项，Redis 仅热态缓冲，真相在 PG，靠"懒恢复"续命。**
   TTL 蒸发后 GET 会话发现 Redis 无 key → 从 PG 真相表重建轮次并回填 Redis（刷新 TTL）。
   "隔天续聊的复盘"由 PG 真相保证，Redis 只承担 TTL 内的读加速与在线态。
   （修正 §1/§3 对 TTL 的担忧；实现见 `cmd/opscopilot/rca_session.go` 懒恢复路径。）
2. **轮次正文不算审计证据：正文落独立表 `rca_session_turn`，incident_audit 哈希链不动、
   不追加 'rca-review' 动作；`llm_meta` 只存哈希/长度（prompt SHA-256、字符/字节数、模型名），
   不存正文与 key。** §3 草案里"会话关闭可选追加审计"一项按此作废（不实施）；
   "llm_meta 存 token 数"口径按 llmgw.Stats 实际暴露面收敛为**长度（prompt 字符数/响应字节数）+
   SHA-256 + 模型名**（Stats 无 token 计数，不为会话功能改动 llmgw 契约）。
3. **会话 incident 级共享：键 = `(tenant, incident_id)`，非 per-user。** participants 列表
   （jsonb）与 created_by 存 `rca_session` 表；参与者随 POST 轮次追加去重。
   （修正 §3 键约定 `<tenant>:<incidentID>:<userID>` → `<tenant>:<incidentID>`；
   §3 草案的 user_id/state 列由 participants/created_by 取代，uuid 主键改复合自然键，
   与 incident 域 `(tenant_id, incident_id)` 幂等范式对齐，见 migration 000018。）
4. **S2 端点挂 incident 子路径：`/api/v1/incidents/{id}/rca/session`**——
   GET 读会话（含懒恢复）、POST 追加轮次（user 轮落库 + assistant 轮经 llmgw 产出，
   方法语义即本文 S2 契约）；不设独立 `/sessions` 资源。
5. **鉴权沿用 ADR-009 单 Token 闸门（`X-OpsCopilot-Token` + authorized 常量时间比对），
   每轮落库记 `created_by` 留身份钩子。** **引入用户身份（登录/JWT）后，读会话须过
   incident 权限（会话是 incident 的子资源，越权面随事件权限一并收口）**——本期不做
   用户级授权，但表结构与契约字段已为该扩展预留（created_by/participants 均非空可查）。

开关命名一并定案：`OPS_SESSION=on|off`（默认 **off**，全链路零行为变化），
替代 §4 S1 行原稿的 `OPS_RCA_SESSION` 暂名。
