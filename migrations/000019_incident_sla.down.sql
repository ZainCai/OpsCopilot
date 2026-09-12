-- 000019 回滚：删除 SLA 覆盖列与 acked_at 首戳列。
-- 派生视图（deadline/剩余/超时）本就不落列，无需回滚任何东西；
-- acked_at 是确定性新行为的唯一物化，DROP 即彻底回到 W10-1 形态。
ALTER TABLE incident DROP COLUMN IF EXISTS sla_minutes;
ALTER TABLE incident DROP COLUMN IF EXISTS acked_at;
