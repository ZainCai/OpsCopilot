-- 000018 回滚：撤掉 RCA 复盘会话真相表。
-- 先删从表 rca_session_turn（外键依赖方）再删主表 rca_session；
-- 若消费方代码仍在读写这两张表会直接报错（表不存在）——回滚前须先回退代码，
-- 与 000014/000015 同纪律（schema 与代码同进退，不静默降级）。
DROP TABLE IF EXISTS rca_session_turn;
DROP TABLE IF EXISTS rca_session;
