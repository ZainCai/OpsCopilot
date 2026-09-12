# OpsCopilot 配置清单（env 唯一装载表）

> 优化方案 #2「配置收敛」的盘点落表；运行时镜像为根目录 `.env.example`，
> 代码唯一事实源为 `internal/config/config.go`（schema/默认值）与
> `internal/config/load.go`（env 键名/解析）。三处如有出入以代码为准，并请回头修本文档。
>
> 统一解析策略（启动期 `config.Load`，一次性执行）：
> - **缺失**（未设置或仅空白）→ 取"默认值"列；
> - **非法值** → **启动失败**（fail-fast），且**聚合报错**：一次列出全部配置错误
>   （`config invalid (N error(s)): - KEY: ...`），不再"修一个报一个"；
> - on/off 开关只认 `on|off`（大小写不敏感、允许首尾空白）；`1/true/yes` 等一律拒绝；
> - duration 类必须能按 Go duration 解析且为正数；整数类必须为正整数；
>   比例类（OPS_MEMLIMIT_WARN_RATIO）必须是 (0,1] 内的浮点数；
> - 保留窗类接受 duration / `<N>d` / `off|0|never`（off 语义=关闭，非非法值）；
> - **例外**：`REDIS_ALERT_ADDR` / `REDIS_CACHE_ADDR` 必填——双实例物理隔离
>   （v1.2 C2 / G1"缺一拒绝启动"）不因收敛放松；两地址归一化后相同
>   （`localhost` ≡ `127.0.0.1`、大小写/空白不敏感）同样拒绝启动。

统计：**68 个运行时 env 键**（`OPS_*` 66 + `REDIS_*` 2），19 组。
（优化方案 #8 判决异步落库队列 `OPS_NOISE_SINK_*` 4 键，**队满丢弃取向**：
慢 DB/极端洪峰下丢持久化保采集节拍，宁漏库存不丢通知——**PG 判决流可缺
条目，Redis/PG 镜像与内存态非强一致**，丢弃面看
`opscopilot_noise_sink_drops_total{store}`；同时把 `fired→verdict` 打点
口径改为"判决生成"，见 docs/W9-4 §9。）
（优化方案 #6 新增 `OPS_MEMLIMIT_*` 组 9 键：无界内存结构的容量上限 + 告警水位。）
（优化方案 #11 新增 `OPS_INGEST_LEASE_DURATION`：ingest 认领租约，多实例并发消费互斥。）
（优化方案 #11/ADR-012 新增 `OPS_LEADER_*` 组 2 键：PG advisory lock 选主
（键 0x4F43504C），拓扑判决链路单 owner；无 DB 或 off → 恒 leader 降级。）
（优化方案 #12/ADR-014 新增 `OPS_RCA_*` 组 4 键：按需根因分析开关/超时/
证据窗/邻域跳数；只读链路，off 时端点显式 503。）
（二期池波二 #4 新增 `OPS_RCA_AUTO`（RCA 组第 5 键）：critical 升级成功→异步
自动跑一次 RCA 落审计（actor=auto），默认 off；on 须 RCA=on（Validate 强制），
防风暴=内存 + 审计回读双保险、有界队列满丢弃计
`opscopilot_rca_autotrigger_dropped_total`，绝不阻塞升级扫描。同波 #5：启动
簇恢复改走 Redis→PG alert_cluster 兜底（`clusterRestoreOnPromote` 与 leader
开闸钩子同函数），补上"跨重启故障域回读"缺口——纯启动路径行为，无新增键。）
（二期池波二 #3/ADR-015 新增 `OPS_LLM_*` 组 5 键：llm-gateway（通用
OpenAI-compatible，不绑厂商）**默认禁用**（Endpoint 空 = conclude 维持
pending）；密钥 Secret 绝不入库/入日志；超时预算 LLM ≤ RCA − 2s 硬预留；
LLM 协议出口唯一 internal/llmgw、物理发送唯一 internal/transport——
notify 渠道投递同步迁至 transport 路径，业务模块禁持 http.Client。）
（二期池波二 #6 新增 `OPS_RCA_MAX_FINDINGS`（RCA 组第 6 键，默认 200）：
报告 findings 超限按"置信度优先 + 同档时序"截断，被裁条数以
`findings_truncated` 记入报告与审计；REST 默认回显截断版、`?all=1`（Token
门禁内）取全量——以一次性全量开关替代游标分页（同步报告在内存里现成），
**长任务化（异步 job）仍不做**（ADR-014 留桩兑现说明）。conclude 步送
llm-gateway 的 prompt 证据行同守此上限（llmgw 输入侧预算防线）。）
（二期池 #7 sessionstore 接线新增 `OPS_SESSION`（复盘会话组首键，默认 **off**）：
RCA 复盘会话总开关（设计文档《sessionstore消费方与接线》拍板定案）。on 建告警
实例热态袋（sessionstore，2h TTL 仅缓冲、真相 PG migration 000018、懒恢复重建），
注册 GET/POST `/api/v1/incidents/{id}/rca/session`（GET 读会话、POST 追加轮次，
assistant 轮经 llmgw 产出；LLM 未配/失败 fail-open pending 不假答）；on 须
`OPS_RCA=on`（prompt 必携带 findings，Validate 强制）。超时/正文上限不新增键
（复用 `OPS_LLM_TIMEOUT`/`OPS_RCA_TIMEOUT`/建单体上限）。off 全链路零行为变化，
降级面看 `opscopilot_session_llm_requests_total{outcome}`。）
（W10-2 F-04 新增 `OPS_SLA_*` 组 3 键（事件 SLA 组首 3 键）：事件 SLA 时钟**按
严重级**（critical|warning|info）的默认目标时长；单事件可经 REST 建单/流转入参
`sla_minutes` 覆盖（incident.sla_minutes，迁移 000019），0=按级默认。deadline/
剩余/超时为 GET **只读派生**视图不落库（理由见迁移头注释与 rest_sla.go）；
M2 **只记录与展示**，不驱动自动升级（OPS_ESCALATION_* 是独立链路，本组零耦合）。
同迁移顺带补 `acked_at` 首戳列（状态机单点落戳只一次，MTTA 数据源——无新 env 键）。）
（W10-3 F-07 新增 `OPS_KPI_WINDOW`（运维 KPI 组首键，默认 168h）：
`GET /api/v1/kpis` 观察窗默认（?window= 按请求覆盖，非法值 400）。聚合口径
（队列=created_at 入窗；MTTA=avg(acked−created)、MTTR=avg(resolved−created)、
平均闭环与 MTTR 同口径合并为一个字段；空窗零值+样本数）唯一来源
`internal/incident/kpi.go`，PG 是其 SQL 翻译，双 Store 一致性由契约测试锁死；
数据源=incident 表本身（聚合 SQL，非时间线展示源）。）
另有 1 个测试门控键与 2 个独立 CLI 工具的键，见文末附录 B/C。

## 清单表

| # | env 名 | 组 | 类型 | 默认值 | 非法值行为（收敛后） | 主要消费方（收敛后注入点） |
|---|--------|----|------|--------|----------------------|-----------------------------|
| 1 | `REDIS_ALERT_ADDR` | Redis 双实例 | host:port（**必填**） | 无默认 | 缺失/非法/与 cache 同址 → 启动失败 | `main.go`（降噪落库 redis client 地址）、`config.Validate` |
| 2 | `REDIS_CACHE_ADDR` | Redis 双实例 | host:port（**必填**） | 无默认 | 同上 | `config.Validate`（装配层暂无 client 消费者，约束保留） |
| 3 | `OPS_TENANT` | 租户 | 字符串（trim） | `default` | 仅空白视为缺失取默认；无非法值概念 | `cfg.Tenant` 显式注入：`NoiseEngine`、incident/audit/change/channel/escalation 各 store、`PG/RedisClusterSink`、`AlertmanagerWebhook`、`RESTGateway.SetDB`、prometheus 连接器 `TenantID`（**#10：取代包级变量 `DefaultTenant`**） |
| 4 | `OPS_DB_DSN` | DB | 连接串（可空） | 空 = 无库 | 空值合法（内存降级、导入队列不启用）；DSN 语法错由 pgx 在建池时报 WARNING 降级（运行期语义，未收紧） | `assembly.go` 共享池选型、`main.go` PG 真相源 sink |
| 5 | `OPS_DB_MAX_CONNS` | DB | 正整数 | `16` | 原"WARNING+pgx 默认"→ **启动失败** | `assembly.go` `poolCfg.MaxConns` |
| 6 | `OPS_LISTEN_ADDR` | 安全门禁 | 字符串 | `127.0.0.1:8080` | 缺失取默认；监听地址合法性由 `checkListenSecurity`（回环判定）与 bind 失败把关 | `main.go`、`listen_guard.go`（D1 门禁） |
| 7 | `OPS_WEBHOOK_TOKEN` | 安全门禁 | 字符串（**原样不 trim**） | 空 = 写端点无鉴权 | 无非法值概念；空 + 非回环监听 → 启动失败（D1） | `NewAssembly`→`ChangeWebhook.Token`、`RESTGateway.token`、`AlertmanagerWebhook.Token` |
| 8 | `OPS_ALLOW_UNAUTHENTICATED` | 安全门禁 | on/off | `off` | 非 on/off → **启动失败**（原"任何非 on 值都算 off"） | `main.go` → `checkListenSecurity`（本机联调逃生门） |
| 9 | `OPS_CORS_ORIGIN` | 安全门禁 | 白名单源 | 空 = 仅同源 | `*`/畸形（无 `://` 且非 `null`）→ **启动失败**（校验上移 `config.ValidateCORSOrigin`，`RESTGateway.SetCORSOrigin` 同实现复用） | `RESTGateway`（D2 跨源白名单） |
| 10 | `OPS_PROM_URL` | 连接器 | URL（可空） | 空 = 不注册 prometheus | 无非法值概念（空即"未配置"） | `connectors.go` 注册、`assembly.go` 拉取源（**与拉取链路共用同一份，不再散读第二处**） |
| 11 | `OPS_PROM_TOKEN` | 连接器 | Secret | 空 = 无 Bearer | 同上 | `connectors.go`（只读闸门）、`PrometheusAlertsSource` |
| 12 | `OPS_AZURE_SUBSCRIPTION_ID` | 连接器 | 字符串 | 空 = 不注册 azure | 与令牌成对判断（缺一视为未配置） | `connectors.go` |
| 13 | `OPS_AZURE_TOKEN` | 连接器 | Secret | 空 | 同上 | `connectors.go`（只读闸门，绝不日志化） |
| 14 | `OPS_CONNECTOR_INTERVAL` | 连接器 | 正 duration | `30s`（**原为 internal/connector 写死的 defaultInterval，#10**） | **启动失败**（新增 env 通道） | `connectors.go` → `connector.WithInterval` |
| 15 | `OPS_CONNECTOR_TIMEOUT` | 连接器 | 正 duration | `30s`（原装配写死，C3 第二道防线，#10） | **启动失败** | `connectors.go` → `connector.WithConnTimeout` |
| 16 | `OPS_NOISE_SHADOW` | 降噪 | on/off | `on` | 非 on/off → **启动失败**（原"`!= off` 即开"） | `config.Noise.Enabled` → `NewNoiseEngine`（off = 引擎 nil） |
| 17 | `OPS_NOISE_WINDOW` | 降噪 | 正 duration | `10m` | 启动失败（原行为，实现移入 config） | `config.Noise.Window` → `noise.NewShadow` |
| 18 | `OPS_NOISE_MODE` | 降噪 | 枚举 | `shadow` | 白名单 `shadow|enforce`（大小写不敏感），白名单外 → 启动失败（原行为，实现移入 config） | `config.Noise.Mode` → `NewNoiseEngine.enforce`、`attachNoiseGate` |
| 19 | `OPS_DEDUP_WINDOW` | 降噪 | 正 duration | `30m`（**原 rest_gateway.go 常量 dedupWindow，#10**） | **启动失败**（新增 env 通道） | `RESTGateway.limits.DedupWindow`（L2 相似度，`rest_incidents.go`） |
| 20 | `OPS_NOISE_SINK_QUEUE` | 降噪 | 正整数 | `4096`（≈20 个满批；**优化方案 #8 判决异步落库队列，队满丢弃取向**） | **启动失败** | 判决异步落库队列缓冲条数（`NoiseEngine.StartVerdictWriter` → `verdictWriter.ch`；水位 gauge `opscopilot_noise_sink_queue`；溢出即丢持久化任务，计 `opscopilot_noise_sink_drops_total{store}`，**不阻塞采集、不回退同步写**） |
| 21 | `OPS_NOISE_SINK_BATCH` | 降噪 | 正整数 | `200`（同上） | **启动失败** | writer 攒批阈值（攒满即刷；单 writer 串行落现有 VerdictSink，不改 sink 语义） |
| 22 | `OPS_NOISE_SINK_FLUSH` | 降噪 | 正 duration | `2s`（同上） | **启动失败** | writer 定时刷批周期（不满一批的最长滞留） |
| 23 | `OPS_NOISE_SINK_DRAIN` | 降噪 | 正 duration | `5s`（同上；对齐 PG 语句超时 pgSinkTimeout 与停机预算） | **启动失败** | 停机 drain 上限超时：`StopVerdictWriter` 排空存量至多等本值，超时残量计 `opscopilot_noise_sink_drops_total{store}` 后放行停机（不再无限等） |
| 24 | `OPS_PULL_ALERTS` | 拉取链路 | on/off | `off` | 非 on/off → **启动失败** | `cfg.Pull.Enabled` → `buildAlertPoller`（on 但缺 `OPS_PROM_URL` 仍是 WARNING+禁用，非配置非法） |
| 25 | `OPS_PULL_INTERVAL` | 拉取链路 | 正 duration | `30s` | 原"WARNING 后取 30s"→ **启动失败** | `cfg.Pull.Interval` → `AlertPoller` |
| 26 | `OPS_INCIDENT_AUTOCREATE` | ingest 队列 | on/off | `off`（影子期） | 非 on/off → **启动失败** | `cfg.Ingest.AutoCreate` → `IngestWorker`（off = 只排队不建单） |
| 27 | `OPS_INGEST_INTERVAL` | ingest 队列 | 正 duration | `5s`（原构造函数写死默认） | **启动失败**（新增 env 通道） | `IngestWorker` 消费轮询 |
| 28 | `OPS_INGEST_BATCH` | ingest 队列 | 正整数 | `20`（同上） | **启动失败** | `IngestWorker.batch` / 批超时规模因子 |
| 29 | `OPS_INGEST_RATE_LIMIT` | ingest 队列 | 正整数 | `50`（**原 ingest_queue.go 魔法数字 burst 上限，#10**） | **启动失败** | `IngestWorker.applyRateLimit`（R1 风暴折叠聚合单） |
| 30 | `OPS_INGEST_RATE_WINDOW` | ingest 队列 | 正 duration | `5m`（同上魔法数字） | **启动失败** | 同上（窗口桶长） |
| 31 | `OPS_INGEST_BATCH_TIMEOUT_PER_ITEM` | ingest 队列 | 正 duration | `15s`（**原 batchTimeoutFor 写死 15s/条，第七轮 M-2 口径，#10**） | **启动失败** | `PGIngestQueue.batchPerItem`（整批超时 = 批量 × 本值，下限 30s、封顶 10m 仍为代码口径） |
| 32 | `OPS_INGEST_ALERT_BODY_LIMIT` | ingest 队列 | 正整数（字节） | `4194304`（4MiB；**原 `amWebhookBodyLimit` 与 `pullBodyLimit` 两处重复定义，#10 合并为单一定义 `config.DefaultAlertBodyLimit`**） | **启动失败** | `AlertmanagerWebhook.BodyLimit`（入站请求体）与 `PrometheusAlertsSource.BodyLimit`（拉取响应体）共用 |
| 33 | `OPS_INGEST_LEASE_DURATION` | ingest 队列 | 正 duration | `2m`（**优化方案 #11/ADR-012 新增 env**：认领租约时长，migration 000016） | **启动失败** | `PGIngestQueue.lease`（多实例并发消费互斥：过期行可被重领，at-least-once） |
| 34 | `OPS_ESCALATION` | 通知与升级 | on/off | `off` | 非 on/off → **启动失败** | `cfg.Notify.EscalationEnabled` → `buildEscalationPoller` |
| 35 | `OPS_ESCALATION_AFTER` | 通知与升级 | 正 duration | `15m` | 原"WARNING 后取默认"→ **启动失败** | `EscalationPoller.after` |
| 36 | `OPS_ESCALATION_INTERVAL` | 通知与升级 | 正 duration | `60s` | 同上 → **启动失败** | `EscalationPoller.interval`（扫描周期） |
| 37 | `OPS_INCIDENT_RETENTION` | 保留与归档 | 保留窗（duration/`<N>d`/off） | `90d` | 非法 → 启动失败（原行为，解析移入 `config.ParseRetention` 唯一实现） | `RetentionSweeper`（D8：resolved 满期归档，含死信 7d 清理） |
| 38 | `OPS_TOPOLOGY_EDGES` | 拓扑与变更 | 边声明 `src->dst,...` | 空 = 无静态边 | 段格式非法 → 启动失败（`parseStaticEdges` 留在 cmd，仍 fail-fast） | `main.go` → `Sink.AttachStaticEdges` |
| 39 | `OPS_CHANGE_RETENTION` | 拓扑与变更 | 保留窗（duration/`<N>d`/off） | `7d` | 非法 → 启动失败（原 `NewChangePrunerFromEnv` 报错，统一进 Load 聚合） | `ChangePruner` 保留窗 **与** `assembly.go` PG 回放窗口（两侧同窗，优化方案 #4） |
| 40 | `OPS_CHANGE_PRUNE_INTERVAL` | 拓扑与变更 | 正 duration | `1h` | 非法 → 启动失败（原行为，移入 Load） | `ChangePruner` 周期 |
| 41 | `OPS_MEMLIMIT_WARN_RATIO` | 内存有界化 | 比例浮点 (0,1] | `0.8` | 非数字/越界 → **启动失败** | `pkg/memguard.Guard` 告警水位（规模达 上限×比例 先打 WARN，不等淘汰） |
| 42 | `OPS_MEMLIMIT_TOPOLOGY_NODES` | 内存有界化 | 正整数 | `100000` | **启动失败** | `topology.NewBuilderWithLimits`（节点按最久未活跃淘汰；上限保守——过度淘汰伤降噪故障域与 RCA as_of 取证） |
| 43 | `OPS_MEMLIMIT_TOPOLOGY_EDGES` | 内存有界化 | 正整数 | `400000`（节点×4） | **启动失败** | 同上（边独立上限兜底；节点淘汰连带删边一并计数） |
| 44 | `OPS_MEMLIMIT_INCIDENTS` | 内存有界化 | 正整数 | `50000` | **启动失败** | `incident.NewMemStoreWithLimits`（**仅 DB 缺席降级路径**；resolved 最先出局，其次最久未活跃） |
| 45 | `OPS_MEMLIMIT_ESCALATION_LEDGER` | 内存有界化 | 正整数 | `100000` | **启动失败** | `newMemEscalationLedgerWithLimits`（无 DB 时升级台账按认领最早淘汰） |
| 46 | `OPS_MEMLIMIT_NOISE_DEDUP` | 内存有界化 | 正整数 | `200000` | **启动失败** | `noise.NewDedupWithLimits`（窗口清扫挡不住窗口内唯一指纹洪峰；逐条 fail-open=再放行一次） |
| 47 | `OPS_MEMLIMIT_NOISE_CLUSTERS` | 内存有界化 | 正整数 | `50000` | **启动失败** | `noise.NewClustererWithLimits`（活跃+历史总量；resolved 最先出局——真相源在 alert_cluster/Redis 镜像，可 Restore 重建） |
| 48 | `OPS_MEMLIMIT_NOISE_SIGCACHE` | 内存有界化 | 正整数 | `50000` | **启动失败** | `NoiseEngine.persistedSig` 孤儿键超限 GC（簇消失后的签名条目永久无用） |
| 49 | `OPS_MEMLIMIT_AUDIT` | 内存有界化 | 正整数 | `50000` | **启动失败** | `NewMemAuditLogWithLimits`（无 DB 时审计尾部截断丢最旧） |
| 50 | `OPS_LEADER_ELECTION` | leader 选举 | on/off | `on`（**优化方案 #11/ADR-012 新增**：有 DB 即竞选） | 非 on/off → **启动失败** | `config.Leader.Election` → `NewLeaderElector`（cmd/leader.go，PG advisory lock 0x4F43504C）：leader 才跑 Host 采集/判决链路/告警拉取/Retention；无 DB 或 off 恒 leader（单实例语义不变）；本实例态看 `/metrics` gauge `opscopilot_is_leader`（ADR 观测章原名 `opscopilot_leader` 作等价别名同时暴露） |
| 51 | `OPS_LEADER_RETRY_INTERVAL` | leader 选举 | 正 duration | `5s`（同上） | **启动失败** | 竞选节拍 = 持锁复检（连接探针）节拍 = failover 接管上界（ADR 验收标准 3 的 15s 限时 ≈ 3 个节拍余量） |
| 52 | `OPS_RCA` | RCA 按需分析 | on/off | `on`（**优化方案 #12/ADR-014 新增**：只读按需链路无副作用面） | 非 on/off → **启动失败** | `config.RCA.Enabled` → `NewAssembly`（有值才构造 `RCAOrchestrator` 并 `RESTGateway.SetRCA`；off → `GET /api/v1/incidents/{id}/rca` 显式 503） |
| 53 | `OPS_RCA_AUTO` | RCA 按需分析 | on/off | `off`（**二期池波二 #4 新增**：默认关，保持"只做按需"现状） | 非 on/off → **启动失败**；`on` 而 `OPS_RCA=off` → **启动失败**（`config.Validate`：自动触发复用按需链路，编排器 off 时不存在） | `config.RCA.Auto` → `NewAssembly`（on 且有 escalation 挂点时构造 `RCATrigger`、挂 `EscalationPoller.OnEscalate`；critical 升级成功→异步复用同一 `RCAOrchestrator.Analyze(actor="auto")`，落审计 `action='rca' actor='auto'`；防风暴=内存 seen + 审计回读双保险；分析走独立 goroutine + 有界队列，队满丢弃计 `opscopilot_rca_autotrigger_dropped_total`，绝不阻塞 escalation。无 `OPS_ESCALATION=on` 则打 WARNING 不生效） |
| 54 | `OPS_RCA_TIMEOUT` | RCA 按需分析 | 正 duration | `10s` | **启动失败** | 单次分析端到端预算（取证 + 六步；`context.WithTimeout` 阶段边界检查，超时 REST 504） |
| 55 | `OPS_RCA_WINDOW` | RCA 按需分析 | 正 duration | `30m` | **启动失败** | 证据窗 [T0−window, T0]（"告警前 30 分钟"取证口径，进 `rca.Input.Window` 与 GetRecentChanges 窗口参数） |
| 56 | `OPS_RCA_DEPTH` | RCA 按需分析 | 正整数（≤10 截断） | `2` | 非正整数 → **启动失败** | 故障域邻域取证跳数（`neighborhoodFacts` BFS 裁剪；上限与 `maxTopologyDepth` 同源，越界钳制不报错——取证参数非契约输入） |
| 57 | `OPS_RCA_MAX_FINDINGS` | RCA 按需分析 | 正整数 | `200`（**二期池波二 #6 新增**：转正前 `rest_rca.go` 硬编码回显上限 200 的可配置化，默认行为零漂移） | 非正整数 → **启动失败**（0 不承诺"不限"语义，防手滑关掉上限） | `config.RCA.MaxFindings` → `RCAOrchestrator`（产报告时按"置信度优先 + 同档时序"截断（`rca.TruncateFindings`），被裁条数以 `findings_truncated` 入报告与审计；REST 默认回显截断版 `truncated=true`，`?all=1` 在 Token 门禁内一次性取未截断全量——替代游标分页；`llm_summarizer` 的 prompt 证据行同守此上限，llmgw 输入侧预算防线） |
| 58 | `OPS_LLM_ENDPOINT` | LLM 网关 | http(s) 绝对 URL（可空） | **空 = 禁用**（**二期池波二 #3/ADR-015 新增**：默认不接线，conclude 维持 pending、`conclusion=null`，与 ADR-014 逐字节一致） | 非 http(s)/含 userinfo → **启动失败** | `assembly.go` → `newLLMGateway`（非空才构造 `llmgw.Client` 并 `SetSummarizer` 注入 `RCAOrchestrator`；物理发送经 `transport.NewHTTPClient`） |
| 59 | `OPS_LLM_API_KEY` | LLM 网关 | **Secret** | 空 = 不发 Authorization（本地无鉴权网关） | 无非法值概念；**绝不入库、绝不进日志/指标/审计**（`.env` 已在 `.gitignore`）；仅容忍行尾空白（`TrimRight`），其余原样 | `llmgw.Client` 构造参数（Bearer 头；错误值/日志均不携带） |
| 60 | `OPS_LLM_MODEL` | LLM 网关 | 字符串 | 无默认（**不给厂商预设模型名**） | Endpoint 非空而本键缺失 → **启动失败**（`config.Validate`） | `llmgw` 请求体 `model` 字段 |
| 61 | `OPS_LLM_TIMEOUT` | LLM 网关 | 正 duration | `8s` | 非正 duration → 启动失败；**超时预算**：`LLM + 2s ≤ OPS_RCA_TIMEOUT` 不满足 → 启动失败（2s 硬预留给取证 IO + 前四步规则计算，默认 8s+2s=10s 恰好压线） | `transport.NewHTTPClient(timeout)`（llmgw 不持 http.Client，发送器构造期锁定；超时 REST 侧不 5xx——conclude fail-open 回 pending） |
| 62 | `OPS_LLM_MAX_TOKENS` | LLM 网关 | 正整数 | `1024` | 非正整数 → **启动失败** | `llmgw` 请求体 `max_tokens` 字段（结论千字级顶天；响应侧另有 1MiB 业务上限） |
| 63 | `OPS_SESSION` | RCA 复盘会话 | on/off | `off`（**二期池 #7 新增**：sessionstore 接线总开关，默认 off 全链路零行为变化；会话端点显式 503） | 非 on/off → **启动失败**；`on` 而 `OPS_RCA=off` → **启动失败**（`config.Validate`：复盘会话 prompt 必携带 RCA findings，编排器 off 时不存在——同款 `OPS_RCA_AUTO` 约束） | `config.Session.Enabled` → `NewAssembly`（S1 骨架：构造告警实例 Redis 客户端 + `sessionstore.New(rdb, RedisAlert)` 热态袋，role 误绑启动期 panic 守卫继承 P1-1；S2：构造 `SessionOrchestrator`（PG 真相 000018 + 懒恢复 + llmgw assistant 轮）并 `RESTGateway.SetSession` 注册 GET/POST `/api/v1/incidents/{id}/rca/session`；无 DB 时真相退化内存并响亮 WARNING——升级台账同款纪律） |
| 64 | `OPS_AUTOATTACH` | 降噪 | on/off | `off`（**W10-6 新增**：簇→事件生产自动挂簇，默认 off 零行为变化） | 非 on/off → **启动失败**；`on` 而（降噪 off 或 `OPS_NOISE_MODE≠enforce`）→ **启动失败**（`config.Validate`：挂点在 enforce 判决 new-incident 出口，shadow 永不触发——同款 `OPS_RCA_AUTO`/`OPS_SESSION` fail-fast 纪律） | `config.Noise.AutoAttach` → `NewNoiseEngine.autoAttach` + `NewAssembly` `SetAutoAttach(装饰后 Incidents, audit)` → `ProcessAlerts`/`autoAttachClusters`（建单 `UpsertExternal(origin=prometheus, source_ref=promFingerprint(labels))` 与链路 A 同幂等键收敛一单；`AttachCluster` 挂簇 + `attach_cluster` 审计 actor=`system:autoattach`；共簇冲突 `errors.Is(ErrClusterTaken)` 跳过计 `opscopilot_autoattach_total{outcome=attached\|conflict\|skipped}` 不级联） |
| 65 | `OPS_SLA_CRITICAL_MINUTES` | 事件 SLA | 正整数（分钟） | `60`（**W10-2 F-04 新增**：critical 级 SLA 默认目标时长） | 非正整数/非数字 → **启动失败** | `config.SLA.CriticalMinutes` → 装配注入 `RESTLimits.SLACritical` → `rest_sla.go` 派生视图（无 `sla_minutes` 覆盖的 critical 事件） |
| 66 | `OPS_SLA_WARNING_MINUTES` | 事件 SLA | 正整数（分钟） | `240`（4h） | 同上 → **启动失败** | 同上（warning 档） |
| 67 | `OPS_SLA_INFO_MINUTES` | 事件 SLA | 正整数（分钟） | `1440`（24h；severity 白名单外同按此档兜底） | 同上 → **启动失败** | 同上（info 档 + 未知级兜底） |
| 68 | `OPS_KPI_WINDOW` | 运维 KPI | 正 duration | `168h`（7d；**W10-3 F-07 新增**） | 非正 duration/非法 → **启动失败**（REST `?window=` 非法是请求级 400，不动启动） | `config.KPI.Window` → `RESTLimits.KPIWindow` → `handleKPIs`（GET /api/v1/kpis 默认观察窗；聚合口径唯一来源 `internal/incident/kpi.go`，数据源 incident 表：MTTA/MTTR/吞吐，平均闭环与 MTTR 同口径合并） |

> #6 指标（非 env，登记于此便于对照）：每个有界结构两项——
> `opscopilot_mem_entries{store="builder|builder_edges|incidents|escalation_ledger|noise_dedup|noise_clusters|noise_sigcache|audit"}`（gauge）
> 与 `opscopilot_mem_evictions_total{store=…}`（counter），经 `pkg/memguard.Guard.RegisterTo`
> 挂进 `/metrics`；淘汰同时记 slog WARN（首次必打 + 每分钟限流）。

## 附录 A：进 schema 但**没有 env 通道**的口径（`MetricsSection` 等）

这些是安全/稳定性口径（改动应经代码评审而非运维改 env），收敛为 `config.Defaults()` 里的
单一定义值，消费方从 `cfg.Metrics` 取，不再在 cmd 各文件各写一份：

| 项 | 值（=现状不变） | 消费方 |
|----|-----------------|--------|
| `HTTPReadHeaderTimeout` | 5s | `main.newHTTPServer`（slow-loris） |
| `HTTPWriteTimeout` | 30s | 同上（SSE 在 handler 内显式清除，见 `rest_stream.go`） |
| `HTTPIdleTimeout` | 120s | 同上 |
| `IncidentBodyLimit` | 1MiB | `rest_incidents.go` 人工建单/合并/迁移请求体 |
| `ChangeBodyLimit` | 1MiB | `change_webhook.go` 变更事件请求体 |
| `NotifyBodyLimit` | 64KiB | `rest_notify.go` 渠道配置请求体 |

仍留在 cmd 的实现级常量（非本次范围，单一使用点、无重复）：`ingestTimeout 5s`、
`maxIngestAttempts 5`、`retentionInterval 6h`、`retentionBatchSize 500`、
`deadLetterRetention 7d`、`pullHTTPTimeout 15s`、`changeStoreWarnSize 10万`、
`changePruneStartupDelay/retention 首轮延迟 1m`、`credential 清扫 10m`、
批超时下限 30s/封顶 10m、停机 drain 10s。

## 附录 B：测试门控键（应用/装载器均不读取）

| 键 | 用途 |
|----|------|
| `OPS_TEST_PG_DSN` | PG 集成测试的存在性门控：未设置 → 相关测试按仓库惯例 skip。测试内的**行为**控制已全部改为直接构造 `*config.Config`（#10：测试不再 `t.Setenv` 运行时键）。**须指向独立测试库**（见下），不要复用开发库。 |
| `AZURE_ARM_TOKEN` / `AZURE_ARM_SUBSCRIPTION` / `AZURE_ARM_BASE_URL` | `internal/connector/azure` 真云集成测试门控（`t.Skip` if unset），与应用配置无关。 |

### 测试库隔离（二期池波一-1）

`OPS_TEST_PG_DSN` 若直接复用开发库（`DB_DSN`），incident 契约用例的翻页探针
`walkAll`（≤100 页硬上限，`internal/incident/store_contract_test.go`）会翻遍
过滤条件下的**全表**——开发库数据累积（压测/联调残留）超约 200 行即偶发变红。
d87d89a 后实测：旧开发库 `-count=2` 稳定红 3 例
（`TestContractUpsertExternalGenerations` / `TestContractListPage` /
`TestContractConcurrency` 的 pgstore 子测试，均报
`walk did not terminate within 100 pages`；`./cmd/...` 当时绿）。
处置：`bash scripts/reset_test_pg.sh [--wipe]` 在同实例上建独立测试库
`opscopilot_test` 并用纯 Go 助手 `scripts/migrate`（无 docker/psql 依赖）按序
应用 `migrations/*.up.sql`；切库后上述 3 例全绿。属数据敏感断言而非真 bug，
生产代码与测试逻辑零改动。

## 附录 C：独立 CLI 工具（未纳入统一 Config，保持各自 flag 默认）

`tools/verdicts`（评估导出）与 `tools/loadtest`（压测器）是独立 main 进程，
沿用 `-dsn`/`-tenant` flag（缺省回退读 `OPS_DB_DSN`/`OPS_TENANT`）。它们是排障工具、
不随服务部署，本次收敛不动它们（避免把评估工具绑进服务 schema）。

## 附录 D：行为变化点（收敛时唯一被批准的语义变化及其余等价性说明）

1. 非法 env 值统一 fail-fast（批准项）：`OPS_DB_MAX_CONNS`、`OPS_PULL_INTERVAL`、
   `OPS_ESCALATION_AFTER`、`OPS_ESCALATION_INTERVAL` 原"告警后静默取默认"→ 启动失败；
   `OPS_NOISE_SHADOW`、`OPS_ALLOW_UNAUTHENTICATED`、`OPS_PULL_ALERTS`、`OPS_ESCALATION`、
   `OPS_INCIDENT_AUTOCREATE` 收敛为 on/off 白名单（`OFF` 大写原会被当"开"，`1/true` 原会被
   当关或开、口径不一，现一律拒绝）；错误一次性**聚合**列出全部。
2. 其余键默认值与解析结果逐一保持不变（表驱动测试锁死，见 `internal/config/load_test.go`）；
   双 Redis 强校验、D1 安全门禁、D2 CORS 白名单、保留窗两侧同窗等约束原样并入 `Load`。
3. `OPS_LISTEN_ADDR` 等字符串类键：空白值 = 缺失（取默认），与此前 `TrimSpace` 语义一致；
   `OPS_WEBHOOK_TOKEN` 保持原样不 trim（避免"配了密钥却对不上"）。
4. `parseStaticEdges` 对 `OPS_TOPOLOGY_EDGES` 的语法校验仍由 cmd 承担（依赖 topology 类型），
   时机从"装配中"变为"装配前"，同样 fail-fast。
