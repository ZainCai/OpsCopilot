-- 000020 回滚：撤掉 Runbook 记录版三表（与 000018 同纪律：schema 与代码同进退，
-- 回滚前须先回退读写这三张表的代码，不静默降级）。
-- 先删执行记录（引用挂载对）与挂载表（引用手册库），最后删手册库本体。
DROP TABLE IF EXISTS runbook_execution_log;
DROP TABLE IF EXISTS incident_runbook;
DROP TABLE IF EXISTS runbook;
