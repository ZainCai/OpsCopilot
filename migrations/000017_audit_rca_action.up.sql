-- 000017 审计动作扩 'rca'（优化方案 #12 / ADR-014）。
--
-- 按需根因分析结果落审计（append-only 现有 incident_audit）：每次分析
-- 追加一条 action='rca' 记录（触发者 + 证据链体检计数进 detail JSONB）。
-- 000006 的 action CHECK 是封闭集合，新增动作必须同步扩约束——对齐
-- change_record 的 change_type CHECK 纪律（000002："扩展枚举须同步扩
-- 此 CHECK"）。

ALTER TABLE incident_audit
    DROP CONSTRAINT IF EXISTS incident_audit_action_check;

ALTER TABLE incident_audit
    ADD CONSTRAINT incident_audit_action_check
    CHECK (action IN ('create','transition','attach_cluster','merge',
                      'external_recovery_ignored','rate_limited','ingest_failed',
                      'rca'));

COMMENT ON COLUMN incident_audit.action IS
    'create | transition | attach_cluster | merge | external_recovery_ignored | rate_limited | ingest_failed | rca —— 与 cmd/audit.go AuditAction 封闭集合一致（迁移 000017 扩 rca）';
