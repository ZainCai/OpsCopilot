# ADR-016: autoCreate 转正与灰度（`OPS_INCIDENT_AUTOCREATE` 默认值决策）

- **状态**：**提议——待团队拍板**（最终 ON/OFF 是用户/团队决策；本 ADR 只给推荐案与依据，**不改任何默认值**）
- **日期**：2026-09-13
- **关联**：双链路方案（`docs/方案-双链路事件来源.md` 第八节决策件 2 + R8）、待决策清单 D12/D13（M2 排期 W10-4 所称"D13 收尾"即这条链）、ADR-008（M9 代际）、ADR-011（shadow/enforce）、ADR-012（认领租约/多副本消费）、W9-2/W9-3（通知闭环）、`docs/容量基线-2026-09-12.md`、`docs/M2执行排期-W9到W11.md` W10-4

> **勘误**：排期 W10-4 行"产出"栏写的是 **ADR-012 + 开关**——笔误。
> ADR-012 编号已被"水平扩展：拓扑单 owner + ingest 认领租约"占用，
> 且其内容与建单开关默认值无关。本决策正式落号 **ADR-016**，排期行同步修正。

## 背景

**现状（读事实，代码零改动可复核）**：

- **开关与默认值**：`OPS_INCIDENT_AUTOCREATE` 默认 **off**
  （`internal/config/load.go` `p.onOff(EnvIngestAutoCreate, false)`，on/off 白名单、
  非法值启动失败）。off 时 `IngestWorker.Run` 立即返回不消费
  （`cmd/opscopilot/ingest_queue.go:289-293`）——消息在 `ingest_queue` 表堆积不丢，
  开启后从存量继续消费建单（"转正后回放历史消息"是队列的既有语义）。
- **消费点**：链路 A 两个进入方式在队列汇流后共用同一个 worker——
  push（`POST /api/v1/ingest/alertmanager`）与 pull（`AlertPoller`，
  `OPS_PULL_ALERTS`，载荷同形 amAlert）→ `process` → `UpsertExternal`
  （L0 幂等）→ 审计 / 自动关单策略（R2）。装配接线 `assembly.go:534`
  （`cfg.Ingest.AutoCreate` → `NewIngestWorker`）。
- **限流（burst 50/5min）**：单窗新建 >`OPS_INGEST_RATE_LIMIT`（默认 50）后
  新消息折叠进聚合单 `source_ref=burst:<窗口起点>`（`applyRateLimit`），不丢、可回放，
  `rate_limited` 审计留痕。容量基线 §2.3 已实测折叠路径：92k 事件压入仅 ~150 真单 +
  3 聚合单，台账不被风暴打爆。
- **代际（M9 / ADR-008）**：复发即新建一代，限流判据是
  `ExternalActive`（有没有**未解决**的单），复发不会被误计为"刷新"而绕开风暴防护。
- **通知闭环（W9-2/W9-3）**：建出来的单"有人看得见"的前提已就绪——
  Gate 只放行 new-incident 通知、三形态渠道 + 热配置、severity 路由、
  未 ack 超时升级一次（`OPS_ESCALATION`）。
- **决策原始上下文**：双链路方案第八节第 2 件"外部自动建单的开关默认值：
  影子期 off / 转正后 on（建议）"+ R8（影子期不该真建单）。D12（先评估再转）
  与 D13（先收口 M1 再按清单推进）的"先评估"前置已由 W6-3 100% 达标兑现。

## 决策（推荐案）

**默认保持 off，灰度转正**——不直接翻全局默认，按三段灰度推进，每一段有
退出判据，最终"是否翻默认"单独拍板。

1. **段一：demo/压测租户 on**。对演示与压测形态的部署实例
  （当前单租户形态 = 按部署实例）设 `OPS_INCIDENT_AUTOCREATE=on` **并携带下方
  调优参数组**（开关与参数是一体的，见"理由"）。观察项：
  `ingest_queue` Pending 水位（`Queue.Pending` 口径/SQL）、burst 折叠占比
  （`incident_audit.action='rate_limited'` 与 `source_ref LIKE 'burst:%'`）、
  `opscopilot_autoattach_total{outcome=attached|conflict|skipped}` 三计数、
  链路 A `create` 审计量 vs 通知投递量。
2. **段二：观察一周**。≥7 天审计与指标对账：误建单争议 = 0、`conflict` 不增长
  （多指纹共簇口径不破坏）、无持续增长积压、折叠段排空时长可接受（见风险 B）。
3. **段三：评估是否翻默认**。达标 → 团队拍板把出厂默认改 `on`（本 ADR 状态改
  "已接受"并回写 README/配置清单/`.env.example` 三处默认值描述）；不达标 → 维持
  off、回到段一调参。回退成本 ≈ 0：改回 off 重启即回影子态，已建单不删、队列语义不变。

**建议调优参数组（转正随附，实测口径来自容量基线 §2.2/§9）**：

```bash
OPS_INCIDENT_AUTOCREATE=on
OPS_INGEST_BATCH=500            # 默认 20 → 500
OPS_INGEST_INTERVAL=1s          # 默认 5s → 1s
#   以上组实测端到端建单 ≈160 ev/s（默认参数仅 ≈4 ev/s）；容量红线：≤100 ev/s 持续
#   限流保持默认不动：OPS_INGEST_RATE_LIMIT=50 / OPS_INGEST_RATE_WINDOW=5m（R1 兜底）
#   OPS_INGEST_LEASE_DURATION=2m 维持默认即可——实测单条 UpsertExternal ≈6ms、
#   500 条一批 ≈3s，正常 DB 触不到租约；仅慢 DB 环境建议随批量上调
#   （批超时上限 = batch×15s 封顶 10m 是异常路径上界，不是常态耗时）
```

### 理由：容量基线的 4 ev/s 事实决定"不能默默翻默认"

`docs/容量基线-2026-09-12.md` §2.2：**默认 `OPS_INGEST_BATCH=20 / INTERVAL=5s`
下消费侧排空仅 ≈4 events/s，而入队侧 ≥4000 ev/s——相差三个数量级；灌 10 ev/s
就开始积压（实测 70s 积 3,040 条）**。默认值是按"影子期不建单"设定的（§10-A），
`AUTOCREATE=on` 时**没有任何联动调参提示**。因此：告警风暴场景下"默认配置 + on"
= 建单消息无界积压（不丢，但"告警→事件可见"时延失去上界，恰恰是风暴时最要
"马上见到单"的时刻）。转正必须是**开关与参数绑定**的动作——这也是推荐案坚持
"默认先不动、灰度带参跑一周"而非一把梭翻 on 的核心依据。§9 红线表已把
"上线必查 `OPS_INGEST_BATCH/INTERVAL`"列为部署必查项。

### 开关无新代码 + 配置矩阵测试（本 ADR 随附的唯一代码级动作）

开关、队列、限流、代际、幂等在 D13 收尾前全部已存在，本决策**零产品代码**。
补的是一对**配置矩阵测试**，锁定 `AUTOCREATE=on + AUTOATTACH=on + enforce` 的
联动行为（此前三者各自有测试，组合口径只在注释里断言、无测试锁）：

- 配置层 `TestValidateAutoCreateAutoAttachMatrix`（`internal/config/load_test.go`）：
  两开关 × 模式的合法性矩阵——`AUTOCREATE=on + AUTOATTACH=on + enforce` 合法且
  两值如实装载；`AUTOATTACH=on + shadow` 无论 AUTOCREATE 开关如何都被 Validate
  拒绝（挂点在不跑的链路上 = 配置矛盾 fail-fast，AUTOCREATE 自身无约束）。
- 行为层 `TestAutoCreateAutoAttachConvergeSingleIncident`
  （`cmd/opscopilot/autocreate_matrix_test.go`）：enforce+AUTOATTACH 判决直写建单挂簇
  （`origin=prometheus + source_ref=promFingerprint(labels)`）后，链路 A worker
  （autoCreate=on）消费同一告警必须**收敛同一单**（计数仍 1、单 ID 不变、首挂簇与
  审计不增副本）；同时不同指纹告警照常新建（证明收敛靠幂等键、不是全局压制）。
  即排期 W10-6 注记"两路同开天然收敛一单、无硬依赖"的可执行版。

## 风险与转正前置回归

- **"若 on 则压测回归"（验收口径）**：本轮**不新跑全量压测**——引用容量基线 M1
  段（矩阵 1）数据即可：瓶颈在消费侧 `UpsertExternal` 的逐条 ≈6ms 串行路径，
  调优 500/1s 实测 ≈160 ev/s、逼近单 worker 串行上限；双 worker 排空 2.1× 线性
  （§3c，ADR-012 认领租约的活体复证）。**但段三拍板"翻默认"前，必须带
  `AUTOCREATE=on + 调优参数组` 复跑 `scripts/run_capacity_baseline.sh`**
  （M1 消费阶梯 + §2.3 风暴折叠路径复证），作为硬性关口写入灰度纪律。
- **B（热行放大）**：burst 折叠期所有消息 upsert 同一聚合行，行锁串行化使排空
  跌至 ~30/s（基线 §10-B/§2.3 观察）——风暴折叠段的排空时长是段二观察项，
  不改变"折叠优于打爆台账"的设计结论。
- **C（积压回放洪峰）**：切 on 时存量 stale 消息会集中消费——限流 50/5min 兜底
  折叠，但灰度纪律要求切开关前先查 `Pending` 水位，必要时先清理过期影子期积压。
- **D（双身份风险已闭环）**：链路 A worker 与 W10-6 autoattach 幂等键同源
  （`origin=prometheus+source_ref`），配置矩阵测试（上节）自此锁定该口径。

## 替代方案（否决理由)

- **A：直接翻默认为 on**。否决：默认参数 4 ev/s + 无联动提示（容量基线 §10-A），
  告警风暴下建单时延无界劣化；且"一次误建比十次漏报更伤信任"（D12 教训沿袭）。
- **B：做运行时/租户级热开关**。否决于 M2：与 ADR-011"env 开关 + 重启级仪式感"
  纪律一致（防误触、不在建单路径引入运行时可变状态）；按租户灰度属多租户形态
  （M3 候选），届时开关语义不变、只是作用域细化。
- **C：永久保持 off（人工建单 + W10-6 autoattach 一条路）**。否决：链路 A 的
  push/pull 导入价值归零，队列只涨不排空，且与双链路方案"转正后开启"的
  设计承诺相悖。

## 交叉检查提醒

- **README env 表 / `docs/配置清单-OpsEnv.md` #26 / `.env.example`**：段三若翻默认，
  三处"默认 off"描述同步改写，且 README `OPS_INGEST_BATCH/INTERVAL` 需加"on 时必查"注记。
- **ADR-012**：认领租约是"on 且多副本"的正确性前置（at-least-once + 幂等），
  本 ADR 不触碰其语义；灰度实例数变化时容量红线按其"PG 侧连接数×实例数"口径复核。
- **ADR-011**：`AUTOATTACH=on ⇒ enforce` 的 Validate 强制不变；本 ADR 的矩阵测试
  补的是"AUTOCREATE 与它组合时的行为"，不改约束本身。
- **排期**：W10-4 行"ADR-012"笔误 → ADR-016（文首勘误），行标 ✅（决策留痕完成，
  灰度按本 ADR 执行）。
