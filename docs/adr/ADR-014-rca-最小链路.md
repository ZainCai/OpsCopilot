# ADR-014：按需 RCA 最小链路接线（优化方案 #12）

- 状态：已接受
- 日期：2026-09-12
- 关联：ADR-002（算子前置）、ADR-003（LLM 单出口）、ADR-005（审计分离）、ADR-007（置信度门禁）、ADR-010（审计随单长留）、v1.3 §5.2（模块边界）、v1.2 C15（as_of 时点拓扑）

## 背景

`internal/rca` 六步骨架（取证→假设→验证→归因→结论→建议）自 M2 落地后
从未被 cmd 装配，是"交付承诺未兑现"的断链件（优化方案盘点 #12）。数据面
此前已备齐：拓扑 `AsOf(T0)` 时点查询（R2 锁内映射）、`ChangeBackend`
变更取证（#4 已 TimescaleDB 持久化 + 重启回放）、noise 故障域簇、事件
双链路与 append-only 审计。

硬约束（实测确认，`scripts/check_module_boundaries.py` 强制）：
**internal 各模块禁止互相 import**——`rca` 不得引 `topology`/`incident`/
`config`，跨模块数据只能经 cmd 编排或 `internal/contracts` 流动。骨架
注释里"W10 直接接数据源"的设想在该纪律下不成立：步骤拿不到图与变更库，
必须换接线形态。

## 决策

### 1. 编排位置：cmd/opscopilot/rca_orchestrator.go（唯一汇合点）

`RCAOrchestrator` 落在 cmd（package main），与 TopologySink/publishStore
同一先例——编排装配不属于任何 internal 模块。六步算法本体留在
`internal/rca`（纯标准库），**证据以纯 DTO 注入**：编排层预采集
（拓扑 pb 值拷贝 + 变更 pb.ChangeRecord → `rca.NodeFact/EdgeFact/ChangeFact`），
步骤只见 `Input.Evidence`，零 IO、零兄弟模块依赖。被否决的替代方案：

- 步骤内回调 Provider 接口——多一层间接却没有收益（证据量小、一次取全
  即可），且把取证错误分类复杂度引入步骤契约；
- 把六步搬进 cmd——丢掉 internal/rca 的独立单测面与 M2 骨架资产。

### 2. 数据面复用 SemanticModel 直调，不新开私路

取证唯一经 `SemanticModelServer.GetTopology / GetRecentChanges`（进程内
直接方法调用，REST 网关同款先例）：锁内映射纪律（R2）、窗口校验、
ChangeEvent→pb 映射一套语义两个门面复用，不漂移。**RCA 铁律落地**：
`as_of=T0` 且 `T0 = incident.CreatedAt`（事件被记录的时刻），禁止用当前
拓扑分析历史事件；时间参数一律 RFC3339Nano（秒级截断会把带亚秒
ValidFrom 的刚观测节点系统性判为"当时不存在"——单测抓出的真实缺陷）。

### 3. 故障域与邻域裁剪

事件 `ClusterKeys` → noise 内存聚类器 `Cluster.NodeKeys` 并集 = 故障域
（告警实际命中处，不做无锚点的"全图扫"）；拓扑取证 = 域的 depth 跳邻域
（`OPS_RCA_DEPTH`，默认 2）。簇不在内存视图（重启未 Restore / 淘汰）→
域为空 → 输出"证据不足"报告，**不拿全图凑数**。二期可扩 alert_cluster
表回读（`rca_e2e` 已验证事件-簇关联在 PG 侧齐备）。

### 4. 六步实现度（本期）

| 步 | 形态 | 说明 |
|---|---|---|
| collect 取证 | **真做**（规则） | 域/T0 拓扑/因果门禁/窗口变更盘点，置信度反映证据完整度 |
| hypothesize 假设 | **真做**（规则） | "窗口内谁动了故障域（或一跳邻域）"；置信度=变更档×节点档取低，邻域再降档；low 非域内噪声丢弃 |
| verify 验证 | **真做**（规则） | 时间序/因果门禁/空间相关三查；全过且本体+high→high，本体→medium，其余 low（保留可解释性） |
| attribution 归因 | **真做**（规则） | 只从验证通过者挑 Top-1 首要 + 次要候选；`root_causes` 契约仍只收 high（宁缺毋滥） |
| conclude 结论 | **stub（LLM 挂点）** | `Summarizer` 接口未注入 → 返回 `ErrNotImplemented` 记 pending。**ADR-003 落地方式**：没有出口就没有结论文本（`conclusion=null`），报告以结构化证据链交付，绝不规则伪装"LLM 结论"（禁止伪 RCA） |
| recommend 建议 | **真做**（规则） | 根因是 deploy/config→建议回滚观察；嫌疑本身是 rollback→核查完整性；无从归因→取证改进建议（low，明示"建议"非"结论"） |

llm-gateway 二期接线：装配构造 Summarizer（经 transport 单出口调
gateway），`SetSummarizer` 注入即 conclude 转正——rca 包代码零改动。

### 5. 端点契约与状态码取向

`GET /api/v1/incidents/{id}/rca`：

- **Token 门禁按写路径读法**（`authorized` + `X-OpsCopilot-Token`，401）：
  分析读取全量证据链并按请求写审计行，不视作免费读端点（与 D2 收紧
  读面同向）；
- 事件不存在 → 404；`OPS_RCA=off`/未装配 → **503 显式可诊断**（对齐
  ingest 队列先例，不静默 404）；超时 → 504；Store/取证故障 → 500 脱敏
  （细节只进日志，第七轮 M1 口径）；
- **证据不足 = 200 + 结构化报告**（非 503/404）："跑过但没结论"是可诊断
  的运维事实；findings 回显上限 200 条（truncated 旗标），原始证据不回显
  （体积不可控，计数进 `evidence`）。

结果落审计：每次成功分析 append-only 追加 `action='rca'` 行（actor 取
查询参数，默认 `system:rca`；detail 记 t0/窗口/证据计数/步状态/根因数/
llm_used）。`incident_audit.action` 封闭 CHECK 由 **migration 000017**
扩入 `'rca'`（对齐 000002 "扩枚举必同步扩 CHECK"纪律；down 回 7 值集）。

### 6. 配置与观测

- `OPS_RCA_*` 组 4 键（开关默认 on / 超时 10s / 证据窗 30m / 邻域深度 2），
  进 `internal/config` schema + `.env.example` + 配置清单（55 键 / 15 组）；
  cmd 注入，rca 包不认 config。
- 指标：`opscopilot_rca_requests_total{outcome=ok|error}` +
  `opscopilot_rca_duration_seconds`（取证+六步端到端）。

### 7. 本期不做（明确留桩）

- **sessionstore 接线**：无运行时消费方；二期若做"分析会话/网关不可用时
  延迟重跑队列"（ADR-003 二档降级），挂点 = cmd 构造 redis alert client
  处（`rca_orchestrator.go` 文件头 TODO 注明）；
- **自动触发 RCA**（Escalation 超时未 ack 自动分析并随通知附证据链）：
  触发时机/频率门禁是独立决策，留 ADR 二期，本期只做按需；
- **SSE `rca` 事件**：现有 SSEMessage 契约绑定 incident 快照；RCA 是
  请求-响应型分析（结果当场返回，无"等待推送"场景），加事件反而诱导
  轮询式滥用——判定不做，二期若有"长任务完成通知"需求随任务化一起设计；
- llm-gateway 本体、告警历史序列取证（alert_event 参与度）、跨租户隔离
  （单租户常量注入，pb tenant 字段已备）。

## 链路图

```
GET /api/v1/incidents/{id}/rca  (Token 门禁)
        │
        ▼
RESTGateway.handleRCA ──nil──▶ 503 (OPS_RCA=off)
        │
        ▼
RCAOrchestrator.Analyze            ← cmd/（唯一跨模块汇合点）
  ├─ incident.Store.Get(id)         ← internal/incident   （404/500）
  ├─ noise Clusterer().Get(key)     ← NoiseEngine 内存视图（故障域 NodeKeys）
  ├─ SemanticModel.GetTopology(as_of=T0)      ← topology Graph R2 锁内映射
  ├─ SemanticModel.GetRecentChanges[T0-W, T0] ← ChangeBackend（#4 PG/内存双形态）
  ├─ neighborhoodFacts(pb → rca DTO 纯值)     （邻域裁剪，RFC3339Nano）
  ▼
rca.NewDefaultPipeline(summarizer=nil).Run     ← internal/rca（仅标准库）
  collect → hypothesize → verify → attribution → conclude(pending) → recommend
  ▼
Report{Steps, Findings, RootCauses(high only)}
  ├─ AuditLog.Append(action=rca)    ← incident_audit（append-only；000017 CHECK）
  ├─ opscopilot_rca_{requests,duration}
  └─ 200 JSON rcaView（conclusion=null ⇒ 证据报告形态）
```

## 验收标准

1. 边界脚本绿：`internal/rca` 仅依赖标准库；跨模块数据全部经 cmd；
2. `GET /api/v1/incidents/{id}/rca`：401/404/503/504/200 五态契约测试锁定；
3. 端到端（`OPS_TEST_PG_DSN` 真跑）：发现+变更入库+告警成簇+建单挂簇 →
   200 报告 `evidence.changes ≥ 1`、`root_causes[0].ref` = 注入的变更 ID、
   PG `incident_audit` 出现 `action='rca'` 行（migration 生效证据）；
4. 无 LLM 时 `conclusion=null` 且 conclude 步 pending——**任何路径不得
   产出无 LLM 的结论形态文本**（ADR-003）；
5. 无 DB 降级：内存 Store/ChangeStore 全链路仍可出报告，启动不受
   RCA 配置影响（降级不阻塞纪律）；
6. 置信度纪律单测：high 根因仅出自"high 变更 × 非 low 节点 × 命中本体 ×
   三查全过"；medium 声明止步候选（E2E 与编排单测双向锁定）。

## 后果

- 正面：rca 从死骨架变运行时链路；结论出口物理上只有一处（Summarizer
  接口），gateway 接线是纯装配动作；审计/指标/配置/文档四件套同步；
- 负面：故障域依赖内存聚类器视图（重启后未 Restore 的簇 → 证据不足），
  二期接 alert_cluster 表回读；分析同步执行占用请求 goroutine（邻域规模
  有 depth 上限与超时兜底，当前评估规模下毫秒级）；
- 撤销条件：若 llm-gateway 设计要求 RCA 步骤内多次调模型（假设排序逐步
  交互式），本 DTO 注入形态需重构为 provider 回调——届时另立 ADR。

## 交叉检查提醒

- **ADR-003**：任何"在 internal/rca 或 cmd 直连模型"的代码路径违反本
  ADR 决策 4——conclude 只认 `Summarizer` 接口注入；CI 边界检查 +
  review 拦截；
- **ADR-007**：取证/假设/验证全链消费置信度且 low 不进归因；拓扑侧改
  置信度语义须同步 `rca.Confidence` 保守映射（空=low）；
- **ADR-005 / 000006 CHECK**：新增 AuditAction 必须同步扩
  `incident_audit.action` CHECK（000017 先例）并补迁移；
- **v1.2 C15 / as_of 纪律**：RCA 取证的 `as_of` 语义变更（如 ValidTo
  淘汰机制落地）须重跑本 ADR 验收标准 3；时间参数保持 RFC3339Nano
  （秒截断漏报教训已入注释）；
- **配置清单三镜像**：`OPS_RCA_*` 已同步 config.go / load.go /
  .env.example / docs/配置清单-OpsEnv.md（55 键 15 组）。
