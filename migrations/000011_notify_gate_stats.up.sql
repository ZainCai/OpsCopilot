-- 000011 通知闸门计数表（W9-1 影子降噪转正，消第六轮 R6-6）。
--
-- 背景：notify.Gate 的 suppressed/dispatched 此前只在内存（进程重启清零），
-- 转正（enforce）后"拦截了多少噪声"是核心运维指标，必须跨重启累计。
--
-- 设计：单行/租户 upsert 累计（应用层每批降噪处理后保存）。
--   - 不用时序表：计数是单调累计值，覆盖写即可，无历史需求；
--   - 权威明细仍在 alert_event（每告警一条 verdict，would_suppress 已落库），
--     本表只是 Gate 视角的快速计数（Gate 拦截数 = WouldSuppress 判决数，
--     两者可互为对账）。
CREATE TABLE IF NOT EXISTS notify_gate_stats (
  tenant_id   TEXT PRIMARY KEY,
  suppressed  BIGINT NOT NULL DEFAULT 0,   -- 累计拦截（噪声判决）
  dispatched  BIGINT NOT NULL DEFAULT 0,   -- 累计放行通知
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
