-- 000004 incident 域（M2 主干骨架，功能点 F-01/F-02）。
-- 事件是人的工单，簇是机器的降噪产物——两层解耦，
-- incident 以 UNIQUE(tenant_id, incident_id) 幂等，簇关联存关联表。

CREATE TABLE IF NOT EXISTS incident (
  id           BIGSERIAL PRIMARY KEY,
  tenant_id    TEXT NOT NULL REFERENCES tenant(id),
  incident_id  TEXT NOT NULL,
  title        TEXT NOT NULL,
  severity     TEXT NOT NULL DEFAULT 'info',
  state        TEXT NOT NULL DEFAULT 'open'
               CHECK (state IN ('open','acked','mitigated','resolved')),
  ack_by       TEXT NOT NULL DEFAULT '',
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  resolved_at  TIMESTAMPTZ,
  UNIQUE (tenant_id, incident_id)
);

CREATE INDEX IF NOT EXISTS idx_incident_state
  ON incident (tenant_id, state, updated_at DESC);

-- 簇→事件关联（一簇最多挂一事件；F-02）。
CREATE TABLE IF NOT EXISTS incident_cluster (
  incident_row_id BIGINT NOT NULL REFERENCES incident(id) ON DELETE CASCADE,
  cluster_key     TEXT NOT NULL,
  linked_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (incident_row_id, cluster_key)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_incident_cluster_unique
  ON incident_cluster (cluster_key);
