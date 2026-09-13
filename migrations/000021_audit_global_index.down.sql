-- 回滚 000021：删除全局审计读路径索引（性能回退，无数据风险；
-- 读语义不依赖索引存在，只快慢有别）。
DROP INDEX IF EXISTS idx_incident_audit_tenant_actor_time;
DROP INDEX IF EXISTS idx_incident_audit_tenant_time;
