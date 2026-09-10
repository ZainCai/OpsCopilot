-- 000008 外部事件按"代"演进（M9：复发即新建，用户拍板）。
--
-- 背景：此前 (tenant_id, origin, source_ref) 全局唯一——同一外部告警恢复后
-- 再次触发，只会去刷新那条**已 resolved** 的旧单（标题/严重级/updated_at
-- 被刷掉），新故障对运维完全不可见。
--
-- 方案：引入 generation。同一 (origin, source_ref) 的第 1 代沿用裸
-- incident_id = origin:sourceRef（兼容历史数据与反查习惯）；复发时新开一代
-- （generation+1），incident_id 追加 #N 后缀。代内仍幂等（重推只刷新）。
-- incident_id 里看得见代数，无需额外查询。

ALTER TABLE incident ADD COLUMN IF NOT EXISTS generation INT NOT NULL DEFAULT 1;

-- 幂等键从"全局一单"改为"每代一单"。
DROP INDEX IF EXISTS idx_incident_external_idem;
CREATE UNIQUE INDEX IF NOT EXISTS idx_incident_external_idem
  ON incident (tenant_id, origin, source_ref, generation) WHERE source_ref <> '';

-- "找当前代"的查询路径（ORDER BY generation DESC LIMIT 1）。
CREATE INDEX IF NOT EXISTS idx_incident_external_cur
  ON incident (tenant_id, origin, source_ref, generation DESC);
