-- 000017 回退：action CHECK 收回 000006 原始 7 值集合。
-- 若库内已有 action='rca' 行，ADD CONSTRAINT 会因校验失败而报错——
-- 回退前先 DELETE FROM incident_audit WHERE action='rca'（或接受失败）。

ALTER TABLE incident_audit
    DROP CONSTRAINT IF EXISTS incident_audit_action_check;

ALTER TABLE incident_audit
    ADD CONSTRAINT incident_audit_action_check
    CHECK (action IN ('create','transition','attach_cluster','merge',
                      'external_recovery_ignored','rate_limited','ingest_failed'));
