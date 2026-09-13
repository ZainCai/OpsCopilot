# 架构决策记录（ADR）

任何 P0 级修订或影响其他决策的变更，**先写 ADR 再改文档**。每份 ADR 末尾的"交叉检查提醒"记录该决策的耦合项——这是 v1.3 §5.2 的流程要求（源于 C17 部署策略与数据层特性冲突的事故）。

| 编号                                        | 决策                                    | 状态           |
| ----------------------------------------- | ------------------------------------- | ------------ |
| [ADR-001](ADR-001-event-backbone.md)      | 事件骨干用 Redis Streams（加速层非真相源），不用 Kafka | 已接受          |
| [ADR-002](ADR-002-operator-first.md)      | 算子前置、LLM 兜底（成本控制架构级决策）                | 已接受          |
| [ADR-003](ADR-003-llm-single-egress.md)   | llm-gateway 单出口 + 双向内容过滤 + 三档降级       | 已接受          |
| [ADR-004](ADR-004-redis-dual-instance.md) | Redis 双实例物理隔离（alert 持久化 / cache 可逐出）  | 已接受          |
| [ADR-005](ADR-005-audit-separation.md)    | 审计存储与业务库分离（append-only + 哈希链）         | 已接受          |
| [ADR-006](ADR-006-database-deployment.md) | 数据库部署策略 + **关键外部依赖清单**                | 已接受（法务确认收尾中） |
| [ADR-007](ADR-007-topology-confidence.md) | 拓扑证据三级置信度，low 硬门禁不进因果推理 | 已接受 |
| [ADR-008](ADR-008-external-incident-generations.md) | 外部事件按"代"演进：复发即新建，不重开旧单（M9） | 已接受 |
| [ADR-009](ADR-009-listen-security-gate.md) | 监听×写密钥组合启动期 fail-fast 门禁 + CORS 白名单（D1/D2） | 已接受 |
| [ADR-010](ADR-010-incident-archival.md) | 工单 resolved 满期归档（JSONB 快照），审计随单长留（D8） | 已接受 |
| [ADR-011](ADR-011-noise-enforce-mode.md) | 影子降噪转正模式开关（`OPS_NOISE_MODE=shadow\|enforce`）+ Gate 计数持久化 | 已接受 |
| [ADR-012](ADR-012-single-owner-horizontal-scaling.md) | 水平扩展：拓扑单 owner（PG advisory lock 选主）+ 后台循环门禁 + ingest 认领租约 | 已接受 |
| [ADR-013](ADR-013-前端独立工程.md) | 前端独立工程 web/：内嵌 console 冻结、后端只留 REST/SSE、开发代理免 CORS 改动 | 已接受 |
| [ADR-014](ADR-014-rca-最小链路.md) | 按需 RCA 最小链路：cmd 编排 + DTO 注入破禁互 import、六步规则+证据、conclude 留 llm-gateway 单出口挂点 | 已接受 |
| [ADR-015](ADR-015-llm-gateway-conclude接线.md) | llm-gateway 接线转正 conclude：通用 OpenAI-compatible 出口（llmgw 纯协议 + transport 唯一发送）、默认禁用、LLM≤RCA−2s 预算、fail-open 回 pending、脱敏与出口 CI 锁 | 已接受 |
| [ADR-016](ADR-016-autoCreate转正与灰度.md) | `OPS_INCIDENT_AUTOCREATE` 转正与灰度：默认保持 off、三段灰度（demo/压测 on 带调优参数 → 观察一周 → 评估翻默认）；W10-4（D13 收尾）决策留痕 | **提议——待团队拍板** |

## 写新 ADR 时的检查清单

1. 是否影响已有 ADR？若是，在受影响 ADR 中补充"交叉检查提醒"；
2. 是否引入新的外部依赖？若是，同步更新 ADR-006 的关键外部依赖清单；
3. 是否有替代方案被否决？记录否决理由（避免重复争论）。

