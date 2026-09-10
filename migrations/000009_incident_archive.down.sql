-- 回滚 000009：删除归档表（注意：已归档数据随之丢失，回滚前先确认）。
DROP TABLE IF EXISTS incident_archive;
