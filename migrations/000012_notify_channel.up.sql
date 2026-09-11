-- 000012 通知渠道配置（W9-2，F-11 转正后第一个可运维件）。
--
-- 设计：
--   1) 渠道按租户隔离（(tenant_id, name) 唯一）——name 是装配期注册进
--      Registry 的键，也是前端路由参数；
--   2) url 存明文（webhook 地址本身即凭据，与 OPS_WEBHOOK_TOKEN 同级别；
--      后续接 secret 轮换时再加密，属 M3 安全加固）；
--   3) enabled 软开关：禁用不清数据，转正灰度可逐渠道开关；
--   4) 不做渠道级路由规则（severity → 渠道），那属 W9-3 值班升级规则。
CREATE TABLE IF NOT EXISTS notify_channel (
  tenant_id  TEXT NOT NULL,
  name       TEXT NOT NULL,
  kind       TEXT NOT NULL,          -- generic | feishu | wecom
  url        TEXT NOT NULL,
  enabled    BOOLEAN NOT NULL DEFAULT true,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, name)
);

CREATE INDEX IF NOT EXISTS idx_notify_channel_enabled
  ON notify_channel (tenant_id, enabled);
