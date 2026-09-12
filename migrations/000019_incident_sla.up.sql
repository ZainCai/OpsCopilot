-- 000019 事件 SLA 时钟 + acked_at 首戳（W10-2 F-04 / W10-3 F-07 前置）。
--
-- sla_minutes：事件级 SLA **目标时长覆盖**（分钟）。0 = 无覆盖，按 severity
-- 取 config 默认（OPS_SLA_CRITICAL/WARNING/INFO_MINUTES，装载点在 cmd 装配
-- 层——internal 不 import config，边界纪律）。deadline/剩余/超时是**只读派生**
-- 视图（GET 时按 created_at + 有效目标计算，见 cmd/opscopilot/rest_sla.go），
-- 不落冗余列：写少读多可接受，且避免"改 severity/改默认值后物化列与派生值
-- 漂移"的第二真相源——deadline 的真相只有 (created_at, severity, sla_minutes)。
--
-- acked_at：首次进入 acked 的时间戳（MTTA = avg(acked_at - created_at) 的
-- 分子来源，W10-3 KPI 数据源）。落戳单点在状态机（statemachine.go
-- planTransition/applyTransitionPlan + PG 同语义 CASE），**只落一次**
-- （重复 acked 被转移表拒绝；CASE 的 IS NULL 守卫兜底并发/后门写入）。
-- 未走 acked 的单（open→mitigated→resolved 直达）保持 NULL——KPI 聚合
-- 按样本数计，NULL 不进 MTTA 均值（不算 0，不污染均值）。
-- resolved_at/created_at 已在（000004），不重复。
ALTER TABLE incident ADD COLUMN IF NOT EXISTS sla_minutes INTEGER NOT NULL DEFAULT 0;
ALTER TABLE incident ADD COLUMN IF NOT EXISTS acked_at   TIMESTAMPTZ;
