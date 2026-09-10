-- 000006 事件审计（双链路二期）。
-- 审计是双链路的信任基础：人工操作与外部自动动作统一留痕，
-- 谁在什么时候对哪一单做了什么（含被拒绝的自动动作）。

CREATE TABLE IF NOT EXISTS incident_audit (
  id           BIGSERIAL PRIMARY KEY,
  tenant_id    TEXT NOT NULL,
  incident_id  TEXT NOT NULL,
  action       TEXT NOT NULL
               CHECK (action IN ('create','transition','attach_cluster','merge',
                                 'external_recovery_ignored','rate_limited','ingest_failed')),
  actor        TEXT NOT NULL,          -- 人（账号）或系统标识（system:*）
  detail       JSONB NOT NULL DEFAULT '{}'::jsonb,
  occurred_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_incident_audit_inc
  ON incident_audit (tenant_id, incident_id, occurred_at DESC);
