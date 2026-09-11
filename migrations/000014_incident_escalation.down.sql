-- 000014 回滚：删除升级台账表。
DROP INDEX IF EXISTS idx_incident_escalation_time;
DROP TABLE IF EXISTS incident_escalation;
