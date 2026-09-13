-- 000021 全局审计视图索引（W12 审计解锁包）。
--
-- 背景：GET /api/v1/audit（全局审计检索）按 tenant + 时间倒序翻页，过滤维度是
-- actor / action / 时间窗。000006 只有 (tenant_id, incident_id, occurred_at DESC)
-- ——单事件反查索引，全局"最近 N 条"查询走不到它（incident_id 不在谓词里，
-- 只能全 tenant 扫描 + 显式排序）。本迁移补全局读路径的两条索引。
--
-- 排序契约与 cmd/opscopilot/audit.go PGAuditLog.ListPage 逐字对应：
-- (occurred_at DESC, id DESC) 决定性全序，游标是同款行值比较——索引列序与
-- 游标谓词 (occurred_at, id) < ($n,$n) 对齐，深翻页一次 seek，不随历史膨胀
-- 退化（对齐 000010 给 incident ListPage 建 tiebreaker 索引的同款纪律）。

-- 主读路径：tenant 内时间倒序翻页（无 actor/action 过滤时的默认列表，
-- 也是带时间窗过滤列表的驱动索引）。
CREATE INDEX IF NOT EXISTS idx_incident_audit_tenant_time
  ON incident_audit (tenant_id, occurred_at DESC);

-- actor 过滤索引（过滤条"操作人"输入框的主路径）。
-- 写放大取舍——audit 是高频写面，为什么仍然可接受：
--   1) incident_audit 的写入频率是"动作级"（建单/流转/合并/挂簇/RCA），
--      不是告警洪流级（alert_event 才承担逐告警写入，且是 hypertable）；
--      单行 INSERT 多维护一条 B 树的边际成本在此量级下可忽略；
--   2) actor 低基数（运维账号 + system:* 少数取值），相同 (tenant_id, actor)
--      前缀的键在页内高度聚集，索引体积远小于行数线性；
--   3) 没有它，"某人做过什么"只能靠 idx_incident_audit_tenant_time 扫时间窗
--      再过滤——全历史 actor 检索随保留期（180 天目标）线性变慢，正是审计
--      最典型的追问形状。
-- 若未来实测写入成为瓶颈，撤销本条即可（down 已备好），读路径退化为
-- tenant_time 索引 + 过滤，语义不变只慢不坏。
CREATE INDEX IF NOT EXISTS idx_incident_audit_tenant_actor_time
  ON incident_audit (tenant_id, actor, occurred_at DESC);

COMMENT ON INDEX idx_incident_audit_tenant_time IS
  '全局审计列表主读路径：(tenant, occurred_at DESC)，与 PGAuditLog.ListPage 游标 (occurred_at,id) 对齐（000021）';
COMMENT ON INDEX idx_incident_audit_tenant_actor_time IS
  'actor 过滤主读路径（写放大取舍见 000021 头注释：动作级写频 + 低基数 actor，可接受）';
