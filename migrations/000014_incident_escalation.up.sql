-- 000014 事件升级台账（W9-3 未 ack 超时重发一次）。
--
-- 设计：
--   1) 台账 = "该事件已经升级过"的幂等凭证。只发一次由**主键**保证：
--      Claim 走 INSERT ... ON CONFLICT (tenant_id, incident_id) DO NOTHING，
--      只有真正插入成功（RowsAffected>0）的那一次才发通知——多实例部署下
--      也不会重复发；
--   2) 单独成表而非给 incident 加列：升级是"通知侧的观测"，不是事件本体
--      属性。加列会把 notify 关注点渗进事件域（internal/incident 不改，见
--      项目纪律"接线留在 cmd/"）；
--   3) 不做 TTL/清理：一条事件一行，量级与事件同阶，随事件归档一并处置
--      （M3 保留策略覆盖）。
CREATE TABLE IF NOT EXISTS incident_escalation (
  tenant_id    TEXT NOT NULL,
  incident_id  TEXT NOT NULL,
  escalated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, incident_id)
);

CREATE INDEX IF NOT EXISTS idx_incident_escalation_time
  ON incident_escalation (tenant_id, escalated_at DESC);
