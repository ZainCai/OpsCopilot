-- 回滚 000005 双链路事件来源。
DROP INDEX IF EXISTS idx_ingest_pending;
DROP INDEX IF EXISTS idx_ingest_pending_idem;
DROP TABLE IF EXISTS ingest_queue;
DROP INDEX IF EXISTS idx_incident_external_idem;
ALTER TABLE incident DROP CONSTRAINT IF EXISTS incident_auto_close_policy_check;
ALTER TABLE incident DROP CONSTRAINT IF EXISTS incident_origin_check;
ALTER TABLE incident
  DROP COLUMN IF EXISTS auto_close_policy,
  DROP COLUMN IF EXISTS merged_into,
  DROP COLUMN IF EXISTS created_by,
  DROP COLUMN IF EXISTS dedup_key,
  DROP COLUMN IF EXISTS source_meta,
  DROP COLUMN IF EXISTS source_ref,
  DROP COLUMN IF EXISTS origin;
