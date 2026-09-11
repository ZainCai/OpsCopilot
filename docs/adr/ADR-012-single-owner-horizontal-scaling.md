# ADR-012: 水平扩展——拓扑单 owner（leader）与后台循环门禁

- **状态**：已接受
- **日期**：2026-09-12
- **关联**：优化方案 #11（多副本为已确认规划）、ADR-001（簇真相源/Restore）、ADR-007（拓扑置信）、ADR-011（enforce 判决链路）、migration 000016（ingest 认领租约）

## 背景

多副本目标：**并起 ≥2 个 opscopilot 实例无重复判决、无台账分裂、无后台循环互踩**。
当前代码里三处与"多实例"存在冲突：

1. **拓扑 Builder 归属**：`internal/topology.Builder` 的图是进程内存态。降噪判决
   （故障域聚类）依赖图上的因果可达性——两个实例各持一份图，采集时序不同则
   图内容分歧，同一批告警可能判出不同的簇（判决分歧 = 台账分裂的上游根因）。
2. **ingest 队列**：`FOR UPDATE SKIP LOCKED` 的行锁只在事务内有效，原实现
   "领取→处理→落状态"整批一个事务，注释自注多实例部署需改造（列入 M2）。
3. **后台循环**：Host 采集 / AlertPoller / Escalation / Retention / ChangePruner
   在多副本下若无协调，会出现重复采集投递、重复归档、互相覆盖等"互踩"。

## 备选方案与决策

### 拓扑 Builder 归属

- **方案 A（否决）：采集按节点分片，各实例自建子图**。
  每个实例认领一部分节点、只发现/入图自己那一片。否决理由：
  1. 故障域聚类是**跨节点**语义——交换机故障同时命中 n1/n2/n3，若三节点
     分属不同分片，没有任何一份本地图能看到完整故障域，各实例必然判分歧；
     要做对就得引入跨实例的簇状态协商或全局判决仲裁，复杂度远超 M2 需求；
  2. 分片本身还需要一套分片归属协调（又是一次选主），改动面大、收益不成立。
- **方案 B（选定）：单 owner（leader）跑连接器采集与拓扑判决链路**。
  leader 实例独占 Host 采集循环 → 拓扑图只有一份 → 降噪判决只有一处产出，
  与单实例语义逐字节一致。非 leader 只跑事件链路：webhook/pull 入队 →
  IngestWorker 消费建单（不依赖本地图）；拓扑取证走 PG 真相源
  （`alert_cluster` / `change_record` 的 as_of 查询），不读 leader 的内存图。

  **选定依据**：B 在 M2 规模（双实例）下是**最简正确**解；与降噪"宁漏勿杀"
  同向——判决分歧是最坏的失败模式（同一故障两个结论），单 owner 从根上消灭它，
  代价只是判决吞吐不随实例数扩展，而吞吐从来不是多副本的动机（动机是高可用）。

### leader 选举

- **选定：PG advisory lock（`pg_try_advisory_lock` 固定键）**。实例持有一条
  **独占连接**（pgxpool Acquire 出来的 pinned conn），循环 `pg_try_advisory_lock(key)`
  竞选；成功即 leader 并周期性在同一连接上复检（同会话重跑 try-lock 恒 true，
  兼作连接存活探针）；连接断开 / 进程崩溃 / 显式释放 → PG 自动释放会话级锁，
  其余实例在下一轮竞选中接管。**零新增中间件**（复用 TimescaleDB 真相源）。
- **否决：租约表 + 心跳行**。需要自建续约/过期判定/时钟偏斜处理，代码量与
  出错面显著大于 advisory lock，且没有换来额外保证（本项目单 PG 部署形态下
  锁的失效检测由 PG 会话机制完成，比应用层心跳更及时可靠）。
- 锁键为固定常量 `LeaderAdvisoryLockKey`（0x4F43504C，"OCPL"），簇级全局——
  M1 单租户形态下 leader 是集群角色，不分租户。
- **无 DB（pgPool=nil）或 `OPS_LEADER_ELECTION=off`**：恒为 leader（单实例
  降级路径，行为与本 ADR 之前完全一致）。

## 决策：链路归属矩阵

**leader 才跑**（切换窗口内暂停，恢复后继续）：

| 链路 | 为什么必须单 owner |
|------|-------------------|
| Host 连接器采集（`host.Run`） | 拓扑图唯一归属；重复采集 = 重复投递判决 |
| 拓扑降噪判决链路（NoiseEngine/ enforce Gate） | 判决由采集驱动，随 Host 走；双实例各判 = 簇态分裂 |
| AlertPoller 拉取入队（`OPS_PULL_ALERTS`） | 入队本身幂等、多实例也安全，但拉取判决的去重 `seen` 是每实例内存态——单 owner 免重复抓取源端、且入队计数可解释 |
| Retention 归档清扫 | 有 FOR UPDATE 复核、多实例不致错，但归档是全局单写者语义——省掉无谓的锁竞争 |

**所有实例都跑**：

| 链路 | 多实例安全依据 |
|------|---------------|
| webhook ingest 入队 + IngestWorker 消费 | migration 000016 认领租约（§下文）；下游 `UpsertExternal` 事务内 FOR UPDATE 幂等 |
| REST / SSE / 控制台 | 只读 + 本实例事件流；无共享可变态 |
| 通知渠道投递（建单侧） | 由 ingest 消费出口驱动，跟随消息认领互斥 |
| ChangePruner 变更库清理 | **例外说明**：任务清单原列 leader-only，评估后改为全实例——`ChangeStore` 内存 map 是**每实例各自增长**的（webhook 打到谁谁存），只让 leader 清会重新引入 W9-5 修掉的无界增长；PG 侧 `DELETE WHERE occurred_at<cutoff` 幂等，多实例重复执行无害 |
| Escalation 升级扫描 | PG 台账 `(tenant_id, incident_id)` 主键认领（下节）；内存台账维持"仅单实例正确" |

## ingest 队列认领租约（migration 000016）

队列表加 `locked_by` / `locked_until` 两列，消费从"整批一事务的行锁"改为
**认领租约**三段式：

1. **认领**：单条语句 `UPDATE ... FROM (SELECT ... FOR UPDATE SKIP LOCKED LIMIT n) RETURNING`
   ——把一批至多 `OPS_INGEST_BATCH` 行原子地写上本实例 owner 标识与
   `locked_until = now() + OPS_INGEST_LEASE_DURATION`，锁租约期内其他实例的
   认领条件（`locked_until IS NULL OR locked_until <= now()`）自动跳过它们；
2. **处理**：逐条走既有 `UpsertExternal` 幂等路径（与领取事务解耦——
   原实现的"行锁覆盖处理全程"在跨实例场景并不成立，租约才是跨实例互斥原语）；
3. **确认**：`UPDATE ... SET processed_at=now() WHERE id=$1 AND locked_by=$owner`
   ——**条件更新校验 owner**：若本实例处理超过租约时长、行已被他人重领
   （`locked_by` 已变），确认落空只记日志，绝不覆盖新 owner 的状态。失败同理
   `attempts+1` 也带 owner 条件。

语义是 **at-least-once**：处理中途崩溃 → 租约到期 → 别的实例重领重做——
下游本就幂等（`UpsertExternal` 命中未解决代只刷新），重领安全、不重复建单。
`OPS_INGEST_LEASE_DURATION` 默认 2m：应 ≥ 单批最坏处理时长（默认批量 20 ×
15s/条超时上限下，正常 DB 毫秒级远触不到；慢 DB 触发重领也只是幂等重做）。

## escalation 多实例语义

- **PG 台账在位**：`Claim` 是 `INSERT ... ON CONFLICT DO NOTHING` 单语句原子
  认领，主键即互斥——两个实例（含 leader-failover 后的新旧 leader）并发扫描
  同一 open 事件，只有真正插入成功者发通知，**无需额外 CAS**（核对结论：
  现有实现天然多实例安全；认领不随租约到期回收，"只发一次"是台账语义而非
  尽力交付语义——认领后崩溃则该事件本轮放弃、台账留痕，与单实例既有口径一致）。
- **DB 缺席的内存台账**：维持"重启即丢、仅单实例正确"注释——降级路径本来
  就不承诺多实例，不为此引入伪协调。

## failover 时序与判决保护

1. 旧 leader 进程死 / 连接断 → PG 释放会话锁（即刻，TCP 层或探针失败检测）；
2. 新 leader 在下一次竞选节拍（≤ `OPS_LEADER_RETRY_INTERVAL`，默认 5s）拿到锁；
3. **开闸前必须完成簇态恢复**：`Leader.OnPromote` 钩子按 Redis 镜像
   （`LoadClusters`）→ PG `alert_cluster`（`LoadClusters`，000001 表）顺序重建
   内存簇，恢复失败则**不翻转 leader 标志、不启动任何 leader 循环**，退避重试
   ——恢复完成前本实例不产出任何拓扑判决（宁漏勿杀）；
4. 恢复成功 → leader 标志翻转 → Host 采集/降噪/Poller/Retention 启动。

**代价（明确接受）**：leader 故障切换窗口（≈ 秒级：锁释放 + 一个竞选节拍 +
恢复往返）内拓扑判决链路暂停——期间 push 告警照常入队建单（事件链路不受
影响），仅"该不该再发一条重复通知"的判决停摆，方向与"宁漏勿杀"一致。

## 观测

- `opscopilot_leader` gauge（0/1）：**所有实例**的 `/metrics` 都暴露，
  非 leader 值恒 0——这是"当前谁是 owner"的唯一运行时口径；
- 入队/建单/认领失败等计数沿用既有指标；租约重领以
  "确认落空" WARN 日志可观察。

## 双实例语义验收标准

1. 同库并起 ≥2 实例：并发入队 N 条（各自 source_ref 唯一），最终
   incident 恰 N 单、每行 `ingest_queue` 至多一个 owner、全部 processed；
2. 任意时刻抽样两实例 `opscopilot_leader` 之和恒 = 1（advisory lock 互斥）；
3. 杀掉 leader 后另一实例在限时（≤ 15s）内接管；接管完成前新 leader 不产出
   拓扑判决（判决循环未启动），接管后从 PG/Redis 恢复的簇态继续判决；
4. 同一 open 事件在两实例并发升级扫描下**恰发一次**升级通知（PG 台账认领）；
5. migration 000016 up/down 幂等可往返（列可加可撤，不触既有行）。

## 后果

- 正面：双实例即可部署——事件链路水平扩展，判决链路单 owner 无分歧；
  零新增中间件（选举、租约、台账全部落在既有 TimescaleDB）。
- 代价：判决吞吐不随实例数扩展（明确不需要）；leader 切换窗口的判决空窗
  （秒级，事件链路不受影响）；恢复失败会阻塞判决链路开闸（响亮日志，
  Redis 与 PG 双双不可达时才会发生——与"不产出错误判决"的取向一致）。
- **已知未决（超出本 ADR 范围，明说）**：
  1. **SSE 跨实例**：`EventHub` 只广播**本实例**产生的事件变更——用户订阅
     实例 B，而实例 A 消费建单，B 的页面不会实时刷新（REST 全量拉取仍正确）。
     候选修复为 PG LISTEN/NOTIFY 或共享 Redis Streams 扇出，另立议题；
  2. 多写者端点（人工建单/合并）天然由 DB 行锁互斥，无需门禁，但
     `applyRateLimit` 建单限流窗计数是**每实例内存态**——多实例下全局限流
     实际上限 ≈ 实例数 × rateLimit。M2 规模可接受，收敛为共享计数留待后续；
  3. 通知渠道配置热重载（`reloadNotifyChannels`）只刷本实例注册表——
     渠道 CRUD 打到哪个实例哪个生效，其余实例重启后同步。运维口径：
     改渠道后逐实例重启（或等 ADR-011 补充机制）。

## 交叉检查提醒

- ADR-001：Redis 是加速层非真相源——本 ADR 的恢复顺序"Redis 镜像 → PG"与之
  一致；PG `alert_cluster` 兜底路径即"库才是重建依据"的兑现。
- ADR-004：双 Redis 实例约束不变，选主**不占用** Redis（用 PG advisory lock）。
- ADR-006：无新增外部依赖。
- ADR-011：enforce 判决链路现随 leader 走——影子/转正对比数据的连续性在
  failover 窗口会中断（秒级），评估脚本按时间线对照不受影响。
- 模块边界：全部新协调逻辑在 `cmd/opscopilot`（leader.go / ingest_queue.go），
  internal 各模块不感知选主（边界纪律：编排只落在 cmd/）。
