-- 000016 回滚：撤掉认领租约列。
-- 消费侧 SQL 若仍在引用这两列会直接报错（列不存在）——回滚前须回退代码，
-- 与 000014/000015 同纪律（schema 与代码同进退，不静默降级）。
ALTER TABLE ingest_queue
  DROP COLUMN IF EXISTS locked_until,
  DROP COLUMN IF EXISTS locked_by;
