-- 000015_change_record_persist.up.sql
-- 优化方案 #4：ChangeStore 持久化（PG 真相源 + 启动回放，重启不丢变更取证）。
--
-- 表结构决策：**复用** change_record（000001 建表、000002 对齐列映射），
-- 不新建表——000002 已备齐实现所需的每一列：
--   event_id（幂等键，承载 ChangeEvent.ID）+ UNIQUE(tenant_id, event_id)
--   （PGChangeStore 的 ON CONFLICT 目标）、change_type CHECK（封闭集合）、
--   ref/revision/summary/confidence 证据列。本迁移只补持久化路径缺的索引。
--
-- 回放与双清用的查询是「按时间扫全库保留窗」：
--   LoadSince:   WHERE occurred_at >= $1 ORDER BY occurred_at
--   PruneBefore: DELETE WHERE occurred_at < $1
-- 现有 idx_change_node_time 以 node_key 打头，这两条查询用不上——变更表
-- 随保留窗清理体量有界（7d × 每天几十次），但 DELETE 无索引会锁扫全表，
-- 补一个 (tenant_id, occurred_at DESC)：回放/清理走索引，也为多租户
-- 落地后的按租户窗口查询预留（当前查询不限租户，见 change_pg.go 注释）。
CREATE INDEX IF NOT EXISTS idx_change_record_tenant_occurred
    ON change_record (tenant_id, occurred_at DESC);

COMMENT ON TABLE change_record IS
    '变更事件真相源（优化方案 #4）：写路径 ON CONFLICT(tenant_id,event_id) 幂等，启动按保留窗回放进内存读缓存，PruneBefore 双清（internal/topology/change_pg.go）';
