-- 回滚 000008：去掉 generation，恢复"全局一单"的幂等键。
-- 注意：若已产生多代数据，回滚会因唯一键冲突失败——先归档/合并多余代。
DROP INDEX IF EXISTS idx_incident_external_cur;
DROP INDEX IF EXISTS idx_incident_external_idem;
CREATE UNIQUE INDEX IF NOT EXISTS idx_incident_external_idem
  ON incident (tenant_id, origin, source_ref) WHERE source_ref <> '';
ALTER TABLE incident DROP COLUMN IF EXISTS generation;
