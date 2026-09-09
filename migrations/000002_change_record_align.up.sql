-- 000002_change_record_align.up.sql
-- 对齐 change_record 与内存 ChangeEvent（N2 严重项修复）。
--
-- 三方差异收敛（本迁移只动 DB，内存模型不变）：
--   - 内存 ChangeEvent.ID 是 string（commit SHA / build URL / UUID）—— 幂等键；
--   - 原 DB change_record 用 BIGSERIAL id，无幂等键列 —— 重复写入无法拒识；
--   - 原 DB change_type 注释说 "deploy/config/scale/restart..." 是开放集，
--     而 ChangeType 封闭为 deploy/config_change/rollback —— 两边值域不一致。
--
-- 本迁移加：
--   1) event_id TEXT —— 幂等键列，承载 ChangeEvent.ID（idempotency key）；
--   2) UNIQUE(tenant_id, event_id) —— DB 层防重复，对应 ErrDuplicateChange；
--   3) CHECK(change_type IN ('deploy','config_change','rollback'))
--      —— 强制与 ChangeType 封闭集合一致；扩展 ChangeType 须同步扩此 CHECK。

-- 1) 新增 event_id 列（可空 → 回填 → NOT NULL，便于"已有数据可平滑升级"）
ALTER TABLE change_record
    ADD COLUMN IF NOT EXISTS event_id TEXT;

-- 回填：对历史行（若有）派生一个稳定的事件键；正常部署下 change_record 为空，
-- 本 UPDATE 是 0-row 安全幂等。
UPDATE change_record
   SET event_id = 'legacy-' || id::TEXT
 WHERE event_id IS NULL;

-- 提升为 NOT NULL
ALTER TABLE change_record
    ALTER COLUMN event_id SET NOT NULL;

-- 2) 幂等键唯一索引（DB 层防重复 —— 跟 ChangeStore.Record 的内存判重语义一致）
CREATE UNIQUE INDEX IF NOT EXISTS uq_change_record_tenant_event
    ON change_record (tenant_id, event_id);

-- 3) change_type 枚举约束与 ChangeType 封闭集合对齐
--    用 DO 块实现幂等（PG 的 ADD CONSTRAINT 没有 IF NOT EXISTS）。
DO $do$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'chk_change_record_type'
    ) THEN
        ALTER TABLE change_record
            ADD CONSTRAINT chk_change_record_type
            CHECK (change_type IN ('deploy', 'config_change', 'rollback'));
    END IF;
END $do$;

-- 同步：把字段注释校准为封闭集合（影响 \d+ change_record 的可读性）
COMMENT ON COLUMN change_record.change_type IS
    'deploy | config_change | rollback —— 与 internal/topology.ChangeType 封闭集合一致（迁移 000002）';
COMMENT ON COLUMN change_record.event_id IS
    '幂等键（commit SHA / build URL / UUID），与 ChangeEvent.ID 对齐（迁移 000002）';

-- 4) 补齐内存 ChangeEvent 已有而 DB 缺失的证据列（第三轮复审 R1）：
--    pb.ChangeRecord 侧同批缺口（C5 留痕），落库前先补齐 schema，
--    避免 W4/W5 接线时再走一次 ALTER。
ALTER TABLE change_record ADD COLUMN IF NOT EXISTS ref        TEXT; -- 分支名/流水线名
ALTER TABLE change_record ADD COLUMN IF NOT EXISTS revision   TEXT; -- commit sha/build number
ALTER TABLE change_record ADD COLUMN IF NOT EXISTS summary    TEXT; -- 一句话变更说明（RCA 证据展示）
ALTER TABLE change_record ADD COLUMN IF NOT EXISTS confidence TEXT NOT NULL DEFAULT 'medium'; -- ADR-007
