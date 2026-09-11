-- 000010 分页游标与归档扫描索引（第七轮 M6/M7）。
--
-- ListPage 的游标是 (created_at, incident_id) 行值比较，排序为
-- created_at DESC, incident_id DESC；000007 的 (tenant_id, created_at)
-- 缺 incident_id tiebreak —— 同秒多行或深翻页时无法一次 seek 定位，
-- 且 tiebreaker 需要额外 sort。
CREATE INDEX IF NOT EXISTS idx_incident_tenant_created_id
  ON incident (tenant_id, created_at DESC, incident_id DESC);

-- retention 候选查询（state='resolved' AND resolved_at < cutoff
-- ORDER BY resolved_at）：resolved_at 此前无索引，事件表增长后
-- 每 6h 一轮的归档扫描退化为全表过滤 + 排序。部分索引只覆盖
-- resolved 行（归档候选集），体积小、维护开销低。
CREATE INDEX IF NOT EXISTS idx_incident_tenant_resolved
  ON incident (tenant_id, resolved_at) WHERE state = 'resolved';
