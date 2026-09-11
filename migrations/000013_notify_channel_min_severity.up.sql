-- 000013 通知渠道最低严重级（W9-3 值班路由）。
--
-- 设计：
--   1) min_severity 表达"本渠道接收的最低严重级"——critical 只在 IM 响、
--      info 只留日志，由 (severity, min_severity) 比较决定是否投递；
--   2) 默认 'info' = 全收，与"未配置即不过滤"的保守姿态一致：路由的失败
--      模式必须是**多通知**而非静默丢弃；
--   3) CHECK 收窄取值域，与 notify.ValidSeverity 同口径（DB 层兜底，
--      防止绕过 API 直写脏值）；
--   4) 单独一个迁移（不改已应用的 000012）：golang-migrate 版本不可回改。
ALTER TABLE notify_channel
  ADD COLUMN IF NOT EXISTS min_severity TEXT NOT NULL DEFAULT 'info';

ALTER TABLE notify_channel
  DROP CONSTRAINT IF EXISTS notify_channel_min_severity_chk;

ALTER TABLE notify_channel
  ADD CONSTRAINT notify_channel_min_severity_chk
  CHECK (min_severity IN ('critical', 'warning', 'info'));
