-- 回滚 000010：删除分页/归档索引（性能回退，无数据风险）。
DROP INDEX IF EXISTS idx_incident_tenant_resolved;
DROP INDEX IF EXISTS idx_incident_tenant_created_id;
