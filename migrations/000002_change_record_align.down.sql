-- 000002_change_record_align.down.sql
-- 回滚 N2 修复：移除 event_id 列、唯一索引、CHECK 约束。
-- 注意：本回滚会把所有 event_id 数据丢弃（无 event_id → 无幂等键），属于破坏性。
ALTER TABLE change_record DROP CONSTRAINT IF EXISTS chk_change_record_type;
DROP INDEX IF EXISTS uq_change_record_tenant_event;
ALTER TABLE change_record DROP COLUMN IF EXISTS event_id;
ALTER TABLE change_record DROP COLUMN IF EXISTS confidence;
ALTER TABLE change_record DROP COLUMN IF EXISTS summary;
ALTER TABLE change_record DROP COLUMN IF EXISTS revision;
ALTER TABLE change_record DROP COLUMN IF EXISTS ref;
