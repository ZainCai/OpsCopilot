-- 000015_change_record_persist.down.sql
-- 回滚只删本迁移新增的索引；change_record 表与数据属 000001/000002，不动。
COMMENT ON TABLE change_record IS NULL;
DROP INDEX IF EXISTS idx_change_record_tenant_occurred;
