- 状态：已接受
- 日期：2026-09-11
- 关联：M9（用户决策）、ADR-001（事件骨干）、ADR-005（审计分离）、migrations/000008

## 背景

外部链路（链路 A）的幂等键原为 `(tenant_id, origin, source_ref)` **全局唯一**：同一外部告警恢复后再次触发，只会去刷新那条**已 resolved** 的旧单（标题/严重级被覆盖、状态仍是 resolved）——新故障对运维完全不可见。

## 决策

**外部事件按"代"（generation）演进：复发即新建，不重开旧单。**

1. `incident.generation` 列（默认 1）；幂等唯一索引改为 `(tenant_id, origin, source_ref, generation)`（migrations/000008）。
2. incident_id 规则：第 1 代保持裸 `origin:sourceRef`（兼容历史数据与反查习惯）；复发第 N 代为 `origin:sourceRef#N`——代数在 ID 里直接可见。
3. 语义：命中**未解决**的当前代 → 就地刷新（重推不新建）；命中**已解决**的 → 新开一代。旧代绝不被复发改动。
4. 并发：同号插入撞唯一索引 → 整事务回滚、重读当前代重试（有界 8 次）；刷新路径与"读当前代"同事务并 `FOR UPDATE`（第七轮 M-1）。
5. **约束：sourceRef 禁止含 `#`**——它是代际后缀分隔符，含 `#` 会让不同告警的代链精确碰撞（第七轮 H2，存量已查证 0 行）。
6. 配套新增 `Store.ExternalActive(origin, source_ref)`（是否存在**未解决**代）：建单限流的判据必须用它，否则复发被当作"刷新"、风暴防护失效。

## 后果

- 好处：新故障对运维可见；旧单保持历史原貌（含标题与审计）；代数在 ID 中自描述。
- 代价：incident_id 不再是"全局一单"的稳定键——消费者不能假设 `(origin, source_ref)` 唯一；拉取路径"只上报变化"意味着从未消失的告警不会复发（复发主要来自 push）。
- 交叉检查提醒：ADR-005 的审计 append-only —— 归档（ADR-010）会把事件审计随单移入 `incident_archive.payload`，热表中不再保留；读取归档属独立路径。

## 替代方案（否决理由）

- **重开已 resolved 的单**：丢失"上一次故障"的历史（标题/审计被覆盖），且 resolved 单重新出现会让"已处理完"的工单复活，扰乱人的工作队列。
- **前缀通配（LIKE 'base#%'）找当前代**：sourceRef 可含 `#`，会把别的告警的代串进来——已否决，改逐代精确查找 / `ORDER BY generation DESC`。
