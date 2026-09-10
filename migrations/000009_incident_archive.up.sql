-- 000009 事件归档表（D8 决策 C：工单 resolved 后 N 天归档，审计随单入档）。
--
-- 设计：**单表 JSONB 快照**，而不是按列镜像 incident。理由：
--   1) schema 稳定——incident 以后加列不需要同步改归档表；
--   2) 自描述——一行的 payload 里带齐 事件本体 + 关联簇 + 审计轨迹，
--      归档后无需跨表拼接即可还原完整上下文；
--   3) 保留主键 id，保证与原表可追溯。
--
-- 归档动作由应用层周期任务执行（cmd/opscopilot/retention.go）：
-- 事务内 选行(FOR UPDATE SKIP LOCKED) → 写归档 → 删审计 → 删事件
-- （incident_cluster 由 FK ON DELETE CASCADE 级联清除）。
CREATE TABLE IF NOT EXISTS incident_archive (
  id          BIGINT PRIMARY KEY,          -- 沿用原 incident.id（可追溯）
  tenant_id   TEXT NOT NULL,
  incident_id TEXT NOT NULL,
  payload     JSONB NOT NULL,              -- {incident, clusters, audit, reason}
  archived_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_incident_archive_lookup
  ON incident_archive (tenant_id, incident_id, archived_at DESC);
