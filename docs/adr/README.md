# 架构决策记录（ADR）

任何 P0 级修订或影响其他决策的变更，**先写 ADR 再改文档**。每份 ADR 末尾的"交叉检查提醒"记录该决策的耦合项——这是 v1.3 §5.2 的流程要求（源于 C17 部署策略与数据层特性冲突的事故）。

| 编号 | 决策 | 状态 |
|---|---|---|
| [ADR-001](ADR-001-event-backbone.md) | 事件骨干用 Redis Streams（加速层非真相源），不用 Kafka | 已接受 |
| [ADR-002](ADR-002-operator-first.md) | 算子前置、LLM 兜底（成本控制架构级决策） | 已接受 |
| [ADR-003](ADR-003-llm-single-egress.md) | llm-gateway 单出口 + 双向内容过滤 + 三档降级 | 已接受 |
| [ADR-004](ADR-004-redis-dual-instance.md) | Redis 双实例物理隔离（alert 持久化 / cache 可逐出） | 已接受 |
| [ADR-005](ADR-005-audit-separation.md) | 审计存储与业务库分离（append-only + 哈希链） | 已接受 |
| [ADR-006](ADR-006-database-deployment.md) | 数据库部署策略 + **关键外部依赖清单** | 已接受（法务确认收尾中） |

## 写新 ADR 时的检查清单

1. 是否影响已有 ADR？若是，在受影响 ADR 中补充"交叉检查提醒"；
2. 是否引入新的外部依赖？若是，同步更新 ADR-006 的关键外部依赖清单；
3. 是否有替代方案被否决？记录否决理由（避免重复争论）。
