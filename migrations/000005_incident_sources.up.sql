-- 000005 双链路事件来源（外部自动导入 ∥ 人工建单）。
-- 决策：单表统一 + 来源字段区分；外部链路靠 (origin, source_ref) 幂等。

ALTER TABLE incident
  ADD COLUMN IF NOT EXISTS origin            TEXT NOT NULL DEFAULT 'manual',
  ADD COLUMN IF NOT EXISTS source_ref        TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS source_meta       JSONB NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS dedup_key         TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS created_by        TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS merged_into       TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS auto_close_policy TEXT NOT NULL DEFAULT 'auto';

ALTER TABLE incident DROP CONSTRAINT IF EXISTS incident_origin_check;
ALTER TABLE incident ADD CONSTRAINT incident_origin_check
  CHECK (origin IN ('manual','webhook','alertmanager','prometheus','azure','pull','api'));
ALTER TABLE incident DROP CONSTRAINT IF EXISTS incident_auto_close_policy_check;
ALTER TABLE incident ADD CONSTRAINT incident_auto_close_policy_check
  CHECK (auto_close_policy IN ('auto','manual_only'));

-- 外部链路幂等键：同一来源同一外部 ID 只一单（人工单 source_ref 为空不受约束）。
CREATE UNIQUE INDEX IF NOT EXISTS idx_incident_external_idem
  ON incident (tenant_id, origin, source_ref) WHERE source_ref <> '';

-- 导入队列：接收即落盘（可积压、可重放、可审计）。
CREATE TABLE IF NOT EXISTS ingest_queue (
  id           BIGSERIAL PRIMARY KEY,
  tenant_id    TEXT NOT NULL,
  origin       TEXT NOT NULL,
  source_ref   TEXT NOT NULL DEFAULT '',
  payload      JSONB NOT NULL DEFAULT '{}'::jsonb,
  received_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  attempts     INT NOT NULL DEFAULT 0,
  last_error   TEXT NOT NULL DEFAULT '',
  processed_at TIMESTAMPTZ
);
-- 待处理幂等：同来源同外部 ID 的未处理消息只入队一次（重推不堆积）。
CREATE UNIQUE INDEX IF NOT EXISTS idx_ingest_pending_idem
  ON ingest_queue (tenant_id, origin, source_ref)
  WHERE source_ref <> '' AND processed_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_ingest_pending
  ON ingest_queue (processed_at, received_at) WHERE processed_at IS NULL;
