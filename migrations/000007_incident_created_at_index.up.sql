-- 000007 事件列表查询索引。
--
-- 背景：PGStore.List 的查询是 `WHERE tenant_id=$1 AND (...) ORDER BY created_at`，
-- 而 000004 只建了 (tenant_id, state, updated_at DESC) —— created_at 无索引，
-- 随事件数增长会退化为全表排序（甚至排序落盘）。压测口径下这是列表接口的
-- 首个性能拐点。
--
-- 复合列顺序 (tenant_id, created_at)：tenant_id 等值 + created_at 有序，
-- 一次索引扫描即可满足过滤与排序，无需额外 sort。
CREATE INDEX IF NOT EXISTS idx_incident_tenant_created
  ON incident (tenant_id, created_at);
