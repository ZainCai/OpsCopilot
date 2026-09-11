-- 000013 回滚：去掉 min_severity 列（CHECK 随列删除自动消失）。
ALTER TABLE notify_channel
  DROP CONSTRAINT IF EXISTS notify_channel_min_severity_chk;

ALTER TABLE notify_channel
  DROP COLUMN IF EXISTS min_severity;
