-- 回滚 000006 事件审计。
DROP INDEX IF EXISTS idx_incident_audit_inc;
DROP TABLE IF EXISTS incident_audit;
