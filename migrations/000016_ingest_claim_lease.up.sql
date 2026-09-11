-- 000016 ingest 队列认领租约（优化方案 #11 水平扩展 / ADR-012）。
--
-- 背景：原消费语义是"领取→处理→落状态"整批一个事务，靠 FOR UPDATE 行锁
-- 互斥——行锁只在事务内有效，注释自注"多实例部署时需改为按条短事务"。
-- 多副本并起时两个实例可以领到同一批消息并发处理（幂等兜住建单，但
-- attempts/last_error 互相覆盖、日志重复、限流窗口失真）。
--
-- 方案：认领租约（claim lease）——
--   * locked_by：当前持有认领的实例标识（host:pid:rand）；
--   * locked_until：租约到期时刻。认领条件 = 未处理 & attempts 未达上限 &
--     （从未认领 OR 租约已过），过期行自动可被任何实例重领（at-least-once，
--     下游 UpsertExternal 幂等，重领安全）；
--   * 确认/失败回写都带 `locked_by = 认领者` 条件——租约被抢走后旧持有者
--     的写自动落空，不会覆盖新 owner 的状态。
-- 幂等：ADD COLUMN IF NOT EXISTS / CREATE INDEX IF NOT EXISTS，可重复执行。
ALTER TABLE ingest_queue
  ADD COLUMN IF NOT EXISTS locked_by    TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS locked_until TIMESTAMPTZ;

-- 认领扫描沿用既有 idx_ingest_pending (processed_at, received_at)
-- WHERE processed_at IS NULL；租约过滤（locked_until <= now()）含非常量
-- 表达式，不做部分索引，作行级谓词即可（待处理集本就是小集合）。

COMMENT ON COLUMN ingest_queue.locked_by IS
  '认领租约持有者实例标识；确认/失败回写以此做条件更新（CAS），'' = 从未认领';
COMMENT ON COLUMN ingest_queue.locked_until IS
  '租约到期时刻（now() + OPS_INGEST_LEASE_DURATION）；过期行可被重领';
