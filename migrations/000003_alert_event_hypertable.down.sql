-- 000003_alert_event_hypertable.down.sql
-- 回滚 N3 修复：移除策略、解除压缩配置。
-- 注意：TimescaleDB 没有"un-hypertable"操作 —— 要把 alert_event 还原为普通表，
-- 需 pg_dump + 重建表 + 导入。M1 阶段若需回滚，建议直接重做整个 DB 而非降级。
SELECT remove_retention_policy('alert_event', if_exists => TRUE);
SELECT remove_compression_policy('alert_event', if_exists => TRUE);
DO $do$
BEGIN
    IF EXISTS (
        SELECT 1
          FROM timescaledb_information.compression_settings
         WHERE hypertable_name = 'alert_event'
    ) THEN
        ALTER TABLE alert_event RESET (
            timescaledb.compress,
            timescaledb.compress_segmentby,
            timescaledb.compress_orderby
        );
    END IF;
END $do$;
