-- 000018 RCA 复盘会话真相表（二期池 #7 / 设计文档《sessionstore消费方与接线》S1）。
--
-- 定位（对齐 ADR-001"加速层非真相源"与拍板记录 §5）：
--   * Redis（internal/sessionstore 热态袋）只做 TTL 2h 内的热态缓冲，会话真相在 PG；
--     TTL 蒸发后 GET 懒恢复——从本表重建轮次并回填 Redis（拍板①，不改 2h 默认）；
--   * 轮次正文**不算审计证据**（拍板②）：正文落独立表 rca_session_turn，
--     incident_audit 哈希链不动；llm_meta 只存哈希/长度/模型名，绝不存正文外泄面。
--
-- 键与范式（拍板③：incident 级共享，非 per-user）：
--   * rca_session 主键 = (tenant_id, incident_id)，与 incident 域 UNIQUE(tenant_id,
--     incident_id) 幂等范式、incident_escalation（000014）复合自然键范式对齐；
--     对 incident 不建外键——000014 先例：事件已归档删除（retention）时复盘会话
--     不该阻塞归档，也不留悬挂报错，会话随 (tenant, incident_id) 逻辑关联即可。
--   * rca_session_turn 对 rca_session 建复合外键 + ON DELETE CASCADE：轮次是会话的
--     从属数据，物理删除会话必须连带清轮次（无孤儿）。
--   * next_seq：轮次序号发号器。追加轮次用「UPDATE ... SET next_seq = next_seq + 1
--     RETURNING next_seq - 1」借行锁串行化——双实例同写恰一序（无空洞无重复），
--     比序列（跨会话共享）与 MAX(seq)+1（写偏斜竞态）都更贴"会话内连续序号"语义。
--
-- 幂等：CREATE TABLE/INDEX IF NOT EXISTS，可重复执行。

CREATE TABLE IF NOT EXISTS rca_session (
  tenant_id    TEXT NOT NULL,
  incident_id  TEXT NOT NULL,
  created_by   TEXT NOT NULL DEFAULT '',          -- 首发会话者（拍板⑤身份钩子）
  participants JSONB NOT NULL DEFAULT '[]'::jsonb, -- 参与者列表（追加去重，incident 级共享）
  next_seq     BIGINT NOT NULL DEFAULT 1,          -- 轮次发号器（行锁串行化）
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, incident_id)
);

-- 会话列表侧查询：按租户取"最近活跃的复盘会话"（会话跟 incident 走）。
CREATE INDEX IF NOT EXISTS idx_rca_session_tenant_updated
  ON rca_session (tenant_id, updated_at DESC);

CREATE TABLE IF NOT EXISTS rca_session_turn (
  tenant_id   TEXT NOT NULL,
  incident_id TEXT NOT NULL,
  seq         BIGINT NOT NULL,                    -- 会话内连续序号（rca_session.next_seq 发号）
  role        TEXT NOT NULL
              CHECK (role IN ('user','assistant')),
  content     TEXT NOT NULL,                      -- 轮次正文（真相在此，不入审计哈希链）
  llm_meta    JSONB NOT NULL DEFAULT '{}'::jsonb, -- 仅 assistant：模型/提示哈希/长度（脱敏纪律）
  created_by  TEXT NOT NULL DEFAULT '',           -- 轮次作者（user=提问人；assistant=system:llm）
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, incident_id, seq),
  FOREIGN KEY (tenant_id, incident_id)
    REFERENCES rca_session (tenant_id, incident_id) ON DELETE CASCADE
);

COMMENT ON TABLE rca_session IS
  'RCA 复盘会话真相源（二期池 #7）：incident 级共享（拍板③），Redis 热态蒸发后懒恢复重建；'
  'next_seq 行锁发号保证双实例同写恰一序';
COMMENT ON COLUMN rca_session.participants IS
  '参与者列表（jsonb 字符串数组，追加去重）；created_by=首发者，身份钩子留鉴权升级（拍板⑤）';
COMMENT ON COLUMN rca_session.next_seq IS
  '下一条轮次的 seq 发号器；追加语句 UPDATE...RETURNING next_seq-1 借行锁串行化';
COMMENT ON COLUMN rca_session_turn.llm_meta IS
  '仅存哈希/长度/模型名（prompt SHA-256、字符/字节数，对齐 llmgw.Stats 脱敏口径，拍板②）；'
  '正文在 content 列，本表不是审计证据，incident_audit 哈希链不随轮次变动';
COMMENT ON COLUMN rca_session_turn.role IS
  'user | assistant —— 封闭集合（CHECK）；assistant 轮仅由 llmgw 真实产出写入，'
  'LLM 失败不落轮（fail-open pending，宁 pending 不假答）';
