# RCA 评测集报告——证据版基线（`--no-llm`：LLM 未接线，conclude 恒 pending——ADR-003 禁止伪 RCA）

- run：`20260912-233220`　租户：`rca-eval-20260912-233220`　生成：2026-09-12T15:37:31Z
- golden：`tools/rca_eval/golden.json`（逐场景抄自 tools/faultinjector/main.go playbook() L54-87）
- 剧本缩放：answerbook 每段 30s（golden 原始 300s）；评测周期锚 23:33:51 ~ 23:36:51
- **形态声明**：证据版基线（`--no-llm`：LLM 未接线，conclude 恒 pending——ADR-003 禁止伪 RCA）。本报告命中率是**规则+证据链**的归因上限参考，不是 LLM 结论质量，**不单独构成转正依据**（见 §6）。

## 0. TL;DR

| 指标 | 值 |
|---|---|
| RCA 评测单元（golden 6 段中 3 个故障段 → 4 个簇单元） | 4 跑通 / 4 计划 |
| **top-1 命中率** | **100%**（4/4，门禁线 85%） |
| top-3 命中率 | 100%（4/4） |
| 证据完整率 | 100%（4/4） |
| 静默守护（防误报） | 3/3 通过 |
| GET /rca 延迟 P50/P95 | 5ms / 9ms |
| 出口判定 | PASS |

## 1. 管线与口径

```
golden 段起点前注入变更(POST /api/v1/changes, occurred_at 钉在段前)
  → faultinjector 剧本告警被 app 拉取：ProcessAlerts 成簇(影子判决落 alert_event)
  → 拉取链路入队建单(ingest_queue → UpsertExternal, origin=prometheus)
  → 评测器轮询 PG 等簇/单成（alert_event.cluster_key + incident.source_ref=promFingerprint(labels)）
  → 生产自动挂簇 OPS_AUTOATTACH（W10-6，评测接线 enforce+on）验证挂上；未开开关回退按 AttachCluster 语义直写桥接（见 §5）
  → GET /api/v1/incidents/{id}/rca → root_causes[].ref 对照 golden 根因变更
  → 直读 incident_audit(action='rca') 校验 #4 审计落库形态
```

- **top-1**：`root_causes[0].ref == 期望根因变更 id`；**top-3**：期望 id 出现在 `root_causes` 前 3 位。
- **证据完整**（单元素，全真才算）：故障域 == golden 域 ∧ 取证节点/变更数非空达标 ∧
  六步形态正确（证据版：5 done + conclude pending + conclusion=null + llm_used=false）∧
  审计行存在。
- 延迟三档：`detect_lag`（段起点→事件创建 T0，含采集/队列节拍）、`rca_http`（REST 端到端）、
  `audit_duration_ms`（编排层自报，取直读审计值——两者之差即 REST 链路开销）。

## 2. 六场景 × top-1 / top-3 命中矩阵

| 段 | 场景（golden→main.go 行号） | 评测单元 | top-1 | top-3 | 证据完整 | 备注 |
|---|---|---|---|---|---|---|
| 0 | A 单节点磁盘故障（L56-60） | A-n1（[prometheus://nodes/n1]） | ✅ | ✅ | ✅ | OK；挂簇走生产自动路径（OPS_AUTOATTACH new-incident 联动，W10-6）；成员 [fp-a-disk fp-a-inode] 中仅 fp-a-disk 的事件挂了簇（一簇一事件约束，见报告 §5） |
| 1 | C 静默窗口（防误报守护）（L61-65） | 静默守护 | — | — | — | GUARD_PASS |
| 2 | B 交换机故障（多节点同发）（L66-70） | B-n123（[prometheus://nodes/n1 prometheus://nodes/n2 prometheus://nodes/n3]） | ✅ | ✅ | ✅ | OK；挂簇走生产自动路径（OPS_AUTOATTACH new-incident 联动，W10-6）；成员 [fp-b-lat-n1 fp-b-lat-n2 fp-b-lat-n3] 中仅 fp-b-lat-n1 的事件挂了簇（一簇一事件约束，见报告 §5） |
| 3 | C 静默窗口（防误报守护）（L71-75） | 静默守护 | — | — | — | GUARD_PASS |
| 4 | D 无关噪声（两独立事件）（L76-80） | D-n4（[prometheus://nodes/n4]） | ✅ | ✅ | ✅ | OK；挂簇走生产自动路径（OPS_AUTOATTACH new-incident 联动，W10-6） |
| 4 | D 无关噪声（两独立事件）（L76-80） | D-n5（[prometheus://nodes/n5]） | ✅ | ✅ | ✅ | OK；挂簇走生产自动路径（OPS_AUTOATTACH new-incident 联动，W10-6） |
| 5 | C 静默窗口（防误报守护）（L81-85） | 静默守护 | — | — | — | GUARD_PASS |

## 3. 延迟分布

| 指标 | n | min | P50 | P95 | max |
|---|---|---|---|---|---|
| detect_lag(ms) | 4 | 9474 | 9478 | 9498 | 9498 |
| rca_http(ms) | 4 | 5 | 5 | 9 | 9 |
| audit_duration(ms) | 4 | 2 | 2 | 3 | 3 |

## 4. 逐单元明细

| 段 | 簇单元 | 事件 | 期望根因 | 实际 root_causes 序 | detect_lag | rca_http | 状态 |
|---|---|---|---|---|---|---|---|
| A | A-n1 | prometheus:prom:c321d299a5ec298f106059ea6d9b2129 | chg-20260912-233220-A-root | [chg-20260912-233220-A-root chg-20260912-233220-A-d1] | 9s | 9ms | OK |
| B | B-n123 | prometheus:prom:0ec5cc0814d8bb21d504cc073c08f037 | chg-20260912-233220-B-root | [chg-20260912-233220-B-root chg-20260912-233220-A-root chg-20260912-233220-B-d1 chg-20260912-233220-A-d1] | 9s | 5ms | OK |
| D | D-n4 | prometheus:prom:4eb447955137d5c392d895c3721836f2 | chg-20260912-233220-D4-root | [chg-20260912-233220-D4-root chg-20260912-233220-B-d3 chg-20260912-233220-D4-d1] | 9s | 5ms | OK |
| D | D-n5 | prometheus:prom:87b7ec79fb7f3b21051ac17650124957 | chg-20260912-233220-D5-root | [chg-20260912-233220-D5-root chg-20260912-233220-D5-d1 chg-20260912-233220-A-d3] | 9s | 7ms | OK |

## 5. 暴露的问题

- （本轮无未通过项）

**结构性口径（W10-6 起）**：

- 簇→事件生产自动挂簇已落地：`OPS_AUTOATTACH=on` + `OPS_NOISE_MODE=enforce` 时 new-incident
  判决联动建单+挂簇+`attach_cluster` 审计（评测接线默认如此）。本评测**优先验证生产路径**；
  桥接直写仅作未开开关 app 的回退——单元 note 出现"回退桥接"即本轮生产挂簇路径未被验证。
- 一簇一事件（`idx_incident_cluster_unique`）：多指纹共簇仅**首单**持故障域；其余指纹判决走
  cluster-merge 不再触发挂簇，其自建事件无域——处置口径 = 人工 `MergeInto` 归并到首单
  （L2 只提示不自动级联，见 `docs/M2执行排期-W9到W11.md` W10-6 注记）；RCA 只能逐簇一单。
- 变更证据链已按装配租户隔离：`PGChangeStore.LoadSince` 回放恒带 `tenant_id` 过滤
  （评测 0129078 的跨 run 泄漏实锤后收口，#4 遗留的"M3 补"提前兑现）——`foreign_refs`
  探测保留为回归防线：本轮 findings 再出现跨 run 泄漏即过滤失守/回滚，转正前必须查。

## 6. ≥85% 转正门禁（W12）

- 门禁语义：**LLM 对外转正**要求带 LLM 结论的评测集准确率 ≥85%（二期池文档 / ADR-015），
  完整判定见 §7 三门禁（G1 归因不回退 ∧ G2 结论产出 ∧ G3 结论-证据一致性）。
- 本轮为**证据版基线**（本机 LLM 未配置，`--no-llm` 跑分并在报告标注）：conclude 恒 pending、
  conclusion=null 属预期正确形态；**转正判定不适用本轮**。
- 规则链参考读数：top-1 100% / top-3 100%（85% 线）——证据链与归因排序先行达标，
  LLM 接线后同集重跑（`run_rca_eval.sh --with-llm` + .env 配 OPS_LLM_*）才是转正证据。

## 7. §转正判定（W12 · LLM 三门禁）

**本轮不适用**——证据版基线（`--no-llm`）：LLM 未接线，conclude 恒 pending 是预期形态，
三门禁无任何一分可判；转正判定只在带 LLM 跑分（`--with-llm`）时生效。

## 8. golden 维护协议

- 1) 场景告警/期望簇/expected_new_incidents 的唯一权威是 tools/faultinjector/main.go playbook()(L54-87) 与 alerts()(L169-203)；本文件逐条抄录并标行号。
- 2) 修改注入器剧本后，先跑 tools/rca_eval 的启动期 drift 校验（对照 injector /answerbook 的段序/场景码/expected_new_incidents），不一致评测直接退出（退出码 2），防答案漂移。
- 3) changes[] 是评测侧注入的变更证据（faultinjector 不产变更）：root=场景标注根因，distractor_*=干扰项；role 语义见 tools/rca_eval/main.go 注释。
- 4) 变更逻辑 ID 会在运行时拼上租户后缀（chg-<run>-<logical_id>）：独立租户隔离事件/判决/审计，变更证据同样隔离（PGChangeStore.LoadSince 回放恒带 tenant_id 过滤——评测 0129078 实锤后收口）——历史 run 的变更不再残留进 root_causes；若 §5 再出现 foreign_refs 即租户过滤回归，转正前必须查清。
- promFingerprint 算法双定义：`tools/rca_eval/main.go` 与
  `cmd/opscopilot/pull_alerts.go`——改标签构成时以 INCIDENT_MISS 暴露，两处必须同步。

## 9. 复跑

```
bash scripts/run_rca_eval.sh                    # 默认证据版基线（--no-llm, scale=0.1）
bash scripts/run_rca_eval.sh --with-llm         # LLM 转正跑分（OPS_LLM_* 配在 .env，未配置拒跑不降级）
bash scripts/run_rca_eval.sh stop               # 清理残留进程（pidfile 兜底）
```

