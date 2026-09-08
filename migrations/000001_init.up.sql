-- 000001_init.up.sql
-- OpsCopilot M1 初始 schema
-- 依据：v1.2 C15（时态拓扑）、ADR-001（告警簇幂等落库）、ADR-005（审计独立，此处仅留索引）

-- 启用 TimescaleDB 扩展（自建 TSL 社区版；Apache-2 降级时本行会失败，需走 PG 原生分区路径）
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- 租户
CREATE TABLE IF NOT EXISTS tenant (
    id           TEXT PRIMARY KEY,
    name         TEXT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 拓扑节点（时态：资源存在区间）
CREATE TABLE IF NOT EXISTS topo_node (
    id           BIGSERIAL PRIMARY KEY,
    tenant_id    TEXT NOT NULL REFERENCES tenant(id),
    node_key     TEXT NOT NULL,                 -- 云资源 ID / 服务名，租户内唯一
    node_type    TEXT NOT NULL,                 -- host/service/db/container...
    valid_from   TIMESTAMPTZ NOT NULL DEFAULT now(),
    valid_to     TIMESTAMPTZ,                   -- NULL = 当前有效
    confidence   TEXT NOT NULL DEFAULT 'low',   -- low/medium/high（v1.1 C1）
    source       TEXT NOT NULL DEFAULT 'inferred',
    last_confirmed_at TIMESTAMPTZ,
    props        JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_topo_node_key_time ON topo_node (tenant_id, node_key, valid_from DESC);

-- 拓扑边（时态 + 置信度，供 RCA 时点查询 as_of 使用）
CREATE TABLE IF NOT EXISTS topo_edge (
    id           BIGSERIAL PRIMARY KEY,
    tenant_id    TEXT NOT NULL REFERENCES tenant(id),
    src_key      TEXT NOT NULL,
    dst_key      TEXT NOT NULL,
    relation     TEXT NOT NULL,                 -- calls/depends/runs_on...
    valid_from   TIMESTAMPTZ NOT NULL DEFAULT now(),
    valid_to     TIMESTAMPTZ,
    confidence   TEXT NOT NULL DEFAULT 'low',
    source       TEXT NOT NULL DEFAULT 'inferred',
    last_confirmed_at TIMESTAMPTZ
);
-- v1.2 C15：支持 as_of 时点查询的关键索引
CREATE INDEX IF NOT EXISTS idx_topo_edge_time ON topo_edge (tenant_id, src_key, valid_from DESC);
CREATE INDEX IF NOT EXISTS idx_topo_edge_dst ON topo_edge (tenant_id, dst_key, valid_from DESC);

-- 变更记录（RCA 证据：故障窗口内的变更）
CREATE TABLE IF NOT EXISTS change_record (
    id           BIGSERIAL PRIMARY KEY,
    tenant_id    TEXT NOT NULL REFERENCES tenant(id),
    node_key     TEXT NOT NULL,
    change_type  TEXT NOT NULL,                 -- deploy/config/scale/restart...
    occurred_at  TIMESTAMPTZ NOT NULL,
    actor        TEXT,
    detail       JSONB NOT NULL DEFAULT '{}'::jsonb,
    source       TEXT NOT NULL DEFAULT 'manual' -- git/jenkins/manual/cloud-event
);
CREATE INDEX IF NOT EXISTS idx_change_node_time ON change_record (tenant_id, node_key, occurred_at DESC);

-- 告警簇（ADR-001：幂等落库，Redis 丢失后可从本表重建）
CREATE TABLE IF NOT EXISTS alert_cluster (
    id            BIGSERIAL PRIMARY KEY,
    tenant_id     TEXT NOT NULL REFERENCES tenant(id),
    cluster_key   TEXT NOT NULL,                -- 幂等键：指纹 + 时间窗
    first_seen_at TIMESTAMPTZ NOT NULL,
    last_seen_at  TIMESTAMPTZ NOT NULL,
    state         TEXT NOT NULL DEFAULT 'open', -- open/acked/resolved
    severity      TEXT,
    summary       TEXT,
    evidence      JSONB NOT NULL DEFAULT '{}'::jsonb,
    direct_passthrough BOOLEAN NOT NULL DEFAULT FALSE, -- v1.2 C17 直通模式标记
    UNIQUE (tenant_id, cluster_key)
);
CREATE INDEX IF NOT EXISTS idx_cluster_state ON alert_cluster (tenant_id, state, last_seen_at DESC);

-- 原始告警事件（hypertable 由 downgrade/upgrade 脚本单独处理，此处先建普通表便于单机联调）
CREATE TABLE IF NOT EXISTS alert_event (
    id           BIGSERIAL,
    tenant_id    TEXT NOT NULL,
    cluster_key  TEXT,
    fingerprint  TEXT NOT NULL,
    source       TEXT NOT NULL,                 -- prometheus/cloudwatch/...
    occurred_at  TIMESTAMPTZ NOT NULL,
    payload      JSONB NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (id, occurred_at)
);
CREATE INDEX IF NOT EXISTS idx_alert_event_time ON alert_event (tenant_id, occurred_at DESC);

-- 审计索引（ADR-005：审计正文在独立存储，本表只留索引供关联查询）
CREATE TABLE IF NOT EXISTS audit_index (
    id           BIGSERIAL PRIMARY KEY,
    tenant_id    TEXT,
    occurred_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    actor        TEXT,
    action       TEXT NOT NULL,
    ref_id       TEXT,       -- 指向独立审计存储中的记录 ID
    digest       TEXT        -- 摘要/哈希
);
CREATE INDEX IF NOT EXISTS idx_audit_time ON audit_index (occurred_at DESC);
