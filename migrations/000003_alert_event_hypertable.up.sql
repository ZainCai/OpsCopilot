-- 000003_alert_event_hypertable.up.sql
-- 将 alert_event 转为 TimescaleDB hypertable（N3 严重项修复）。
--
-- 动机：W3 之后才转 hypertable 会有"线上 copy 到 chunk"的窗口写入停摆风险。
-- alert_event 主键已按 hypertable 写（PRIMARY KEY (id, occurred_at)），
-- 现状是空表（main 未启动 Host.Run，无 ingest 路径），转 hypertable 零成本。
--
-- 策略（M1 规模：百台主机、分钟级窗口查询，30 天足够）：
--   - chunk 时间粒度：1 天（便于 7 天压缩窗口 + 30 天保留窗口内整 chunk 调度）
--   - 压缩策略：7 天前的 chunk 自动压缩（按 tenant_id, fingerprint 分段）
--   - 保留策略：30 天前的 chunk 自动删除
--   - 连续聚合：M1 暂不启用（"每 (job, instance) 每 1m 计数"等下个迭代）
--
-- 降级路径（若运行时 TimescaleDB 扩展不可用）：本迁移会在 create_hypertable
-- 失败时整体回滚，由 ADR-006 的 downgrade 流程接管（Apache-2 PG 分区路径）。

-- 1) 转 hypertable（idempotent：if_not_exists 让重复执行不再报错）
SELECT create_hypertable(
    'alert_event', 'occurred_at',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE,
    migrate_data => TRUE
);

-- 2) 启用压缩并配置分段键
--    DO 块判断"是否已配置压缩选项"，避免重复 SET 报错。
DO $do$
BEGIN
    IF NOT EXISTS (
        SELECT 1
          FROM timescaledb_information.compression_settings
         WHERE hypertable_name = 'alert_event'
    ) THEN
        ALTER TABLE alert_event SET (
            timescaledb.compress,
            timescaledb.compress_segmentby = 'tenant_id, fingerprint',
            timescaledb.compress_orderby = 'occurred_at DESC'
        );
    END IF;
END $do$;

-- 3) 压缩策略：超过 7 天的 chunk 自动压缩
SELECT add_compression_policy(
    'alert_event',
    INTERVAL '7 days',
    if_not_exists => TRUE
);

-- 4) 保留策略：超过 30 天的 chunk 自动删除
SELECT add_retention_policy(
    'alert_event',
    INTERVAL '30 days',
    if_not_exists => TRUE
);
