-- 000020 W11-4（F-12）Runbook 记录版：处置手册库 + 事件挂载 + append-only 执行记录。
--
-- 口径（排期"只记录与展示、不自动执行"）：本迁移只承载三张数据表——手册正文
-- 是给人看的 markdown，执行记录是人事后手填的运维事实；系统**不执行任何步骤**、
-- 不做审批闭环（runbook-engine 归二期池，v1.1 §2.9 P1-4 同源判断）。
--
-- 执行记录**不算审计证据**（对齐《sessionstore消费方与接线》拍板②同款口径）：
--   * runbook_execution_log 是独立表，incident_audit 哈希链不随挂载/解挂/执行记录
--     变动、不扩 action 封闭集合（000006/000017 纪律：扩枚举须同步扩 CHECK——
--     这里刻意不扩）；
--   * result 是自由文本、refs 是自由 JSONB 数组，无防篡改保证（无哈希链/无前链），
--     只支撑"处置过程展示"这一 UI 语义；合规取证面仍以 incident_audit 为准。
--
-- 表设计：
--   * 全表带 tenant_id（M1 单租户但键空间不省，与 notify_channel/rca_session 同纪）；
--   * runbook.id 为调用方生成/传入的业务键（同 incident id 字符集口径），
--     PK = (tenant_id, id)；scope_severity/scope_service 是**适用性标签**
--     （空串 = 不限），severity 白名单用 CHECK 封闭（'' ∪ 事件域三级）；
--   * incident_runbook 纯挂载表：PK 对 (tenant_id, incident_id, runbook_id)；
--     对 runbook 建 FK ON DELETE RESTRICT——记录版没有删库入口，悬挂挂载
--     必须被 schema 层挡死；对 incident **不建外键**（000014 先例：事件可被
--     retention 归档删除，挂载/执行记录不该阻塞归档，随 (tenant, incident_id)
--     逻辑关联即可）；
--   * 挂载幂等 = PK 天然去重（ON CONFLICT DO NOTHING），重复挂载返回既有行；
--   * 解挂只删挂载行：执行记录**不随解挂消失**（runbook_execution_log 对
--     incident_runbook 无外键——CASCADE 会毁历史，RESTRICT 会挡解挂，都不对），
--     重挂后历史继续可见；
--   * runbook_execution_log.seq 用列级 IDENTITY 发号（跨挂载全局单调、挂载内
--     严格递增）：比 per-mount MAX+1（写偏斜竞态）与 per-mount 计数器列（解挂
--     重挂重置撞 PK）都稳；append-only 由代码纪律保证（store 零 UPDATE/DELETE
--     语句），不加触发器——单机版读多写少，触发器是过度设计。
--
-- 幂等：CREATE TABLE/INDEX IF NOT EXISTS，可重复执行。

CREATE TABLE IF NOT EXISTS runbook (
  tenant_id      TEXT NOT NULL,
  id             TEXT NOT NULL,
  title          TEXT NOT NULL,
  content        TEXT NOT NULL DEFAULT '',            -- markdown 手册正文（给人看，机器不解析）
  scope_severity TEXT NOT NULL DEFAULT ''
                 CHECK (scope_severity IN ('','critical','warning','info')), -- '' = 不限级别
  scope_service  TEXT NOT NULL DEFAULT '',            -- 适用服务标签（'' = 不限）
  created_by     TEXT NOT NULL,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, id)
);

-- 手册库列表：按租户取最新（库 CRUD 从简，M2 规模整表返回）。
CREATE INDEX IF NOT EXISTS idx_runbook_tenant_created
  ON runbook (tenant_id, created_at DESC);

CREATE TABLE IF NOT EXISTS incident_runbook (
  tenant_id   TEXT NOT NULL,
  incident_id TEXT NOT NULL,
  runbook_id  TEXT NOT NULL,
  mounted_by  TEXT NOT NULL,
  mounted_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, incident_id, runbook_id),
  FOREIGN KEY (tenant_id, runbook_id)
    REFERENCES runbook (tenant_id, id) ON DELETE RESTRICT
);

-- 反向查询：这本手册被哪些事件挂着（挂载治理视角）；PK 只覆盖 incident 优先前缀。
CREATE INDEX IF NOT EXISTS idx_incident_runbook_runbook
  ON incident_runbook (tenant_id, runbook_id);

CREATE TABLE IF NOT EXISTS runbook_execution_log (
  tenant_id   TEXT NOT NULL,
  incident_id TEXT NOT NULL,
  runbook_id  TEXT NOT NULL,
  seq         BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY, -- 挂载内严格递增（全局发号）
  executed_by TEXT NOT NULL,                                -- 执行人（人事后自报，同 created_by 身份钩子）
  executed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  result      TEXT NOT NULL,                                -- 自由文本：人写"执行了什么、结果如何"
  refs        JSONB NOT NULL DEFAULT '[]'::jsonb,           -- 自由 JSON 数组：链接/截图 URL/单号等佐证
  PRIMARY KEY (tenant_id, incident_id, runbook_id, seq)
);

COMMENT ON TABLE runbook_execution_log IS
  'Runbook 执行记录（W11-4 记录版）：append-only，只记不执行；**不算审计证据**'
  '（对齐 session 拍板②口径：incident_audit 哈希链不随本表变动，result/refs 无防篡改）。'
  '解挂不删本表——处置历史是事件的时间性事实，不随挂载关系存续';
COMMENT ON COLUMN runbook_execution_log.seq IS
  '列级 IDENTITY：跨挂载全局单调、挂载内严格递增；解挂重挂不重置（历史连续）';
COMMENT ON COLUMN runbook.scope_severity IS
  '''=不限 | critical|warning|info（CHECK 封闭集合，与事件域三级同口径）——适用性标签，非强制路由';
