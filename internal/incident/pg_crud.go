// pg_crud.go PGStore 的单事件写读路径：Create/Get/Transition/AttachCluster/List 与人工合并归档。
package incident

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// Create 新建事件（UNIQUE(tenant_id, incident_id) 冲突 → 报错，与 MemStore
// 重复 ID 语义一致）。初始状态恒 open（DB 默认值保证，不信任调用方）。
func (s *PGStore) Create(id, title, severity, createdBy string) (*Incident, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return nil, errors.New("incident: id is required")
	}
	if strings.TrimSpace(title) == "" {
		return nil, errors.New("incident: title is required")
	}
	severity = normalizeSeverity(severity) // D2 入口收口（与 MemStore 同一函数）
	if err := s.ensureTenant(context.Background()); err != nil {
		return nil, err
	}
	ctx, cancel := s.ctx()
	defer cancel()
	// 列均 NOT NULL DEFAULT ''：直接写入归一化值即可。此前对 severity/
	// created_by 用 NULLIF(x,'')——显式 NULL 不走 DEFAULT，空串反而撞
	// NOT NULL 报错（D2/D3 的 PG 侧根因）。
	var inc Incident
	err := scanIncident(s.pool.QueryRow(ctx, `
INSERT INTO incident (tenant_id, incident_id, title, severity, state, origin, created_by, auto_close_policy)
VALUES ($1, $2, $3, $4, 'open', 'manual', $5, 'manual_only')
RETURNING `+pgIncidentCols,
		s.tenantID, id, title, severity, createdBy), &inc)
	if err != nil {
		if isUniqueViolation(err) {
			return nil, fmt.Errorf("incident: duplicate id %q", id)
		}
		return nil, fmt.Errorf("incident pg: create: %w", err)
	}
	return &inc, nil
}

// Get 读取事件（含簇关联聚合）。
func (s *PGStore) Get(id string) (Incident, error) {
	ctx, cancel := s.ctx()
	defer cancel()
	inc, err := s.queryOne(ctx, `incident_id = $1`, id, s.tenantID)
	if err != nil {
		return Incident{}, err
	}
	return inc, nil
}

// Transition 状态推进：事务内 SELECT FOR UPDATE 读当前态 → 状态机校验 →
// UPDATE（并发下仍保持状态机正确性）。合法性判定与副作用计划走两 Store 共用
// 的 planTransition（statemachine.go 状态机单一实现）——SQL 的 CASE 只消费计划
// 参数，不再各自表达语义。RETURNING 含 ack_by（D1）。
func (s *PGStore) Transition(id string, to State, actor string) (Incident, error) {
	ctx, cancel := s.ctx()
	defer cancel()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Incident{}, fmt.Errorf("incident pg: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var cur State
	err = tx.QueryRow(ctx, `
SELECT state FROM incident WHERE tenant_id=$1 AND incident_id=$2 FOR UPDATE`,
		s.tenantID, id).Scan(&cur)
	if errors.Is(err, pgx.ErrNoRows) {
		return Incident{}, ErrNotFound
	}
	if err != nil {
		return Incident{}, fmt.Errorf("incident pg: select state: %w", err)
	}
	plan, perr := planTransition(cur, to, actor)
	if perr != nil {
		return Incident{}, perr
	}
	var inc Incident
	err = scanIncident(tx.QueryRow(ctx, `
UPDATE incident SET state=$3, updated_at=now(),
  resolved_at = CASE WHEN $4::bool THEN now() ELSE resolved_at END,
  ack_by      = CASE WHEN $5::text <> '' THEN $5 ELSE ack_by END,
  auto_close_policy = CASE WHEN $6::bool THEN 'manual_only' ELSE auto_close_policy END,
  acked_at    = CASE WHEN $7::bool AND acked_at IS NULL THEN now() ELSE acked_at END
WHERE tenant_id=$1 AND incident_id=$2
RETURNING `+pgIncidentCols,
		s.tenantID, id, to, plan.StampResolved, plan.AckBy, plan.FlipManualOnly, plan.StampAckedAt), &inc)
	if err != nil {
		return Incident{}, fmt.Errorf("incident pg: update state: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Incident{}, fmt.Errorf("incident pg: commit: %w", err)
	}
	return inc, nil
}

// AttachCluster 簇→事件关联：一簇最多一事件（DB 唯一索引兜底）；重复
// 挂同一事件幂等；簇已被其他事件占用报错（与 MemStore 语义一致）。
func (s *PGStore) AttachCluster(id, clusterKey string) error {
	if strings.TrimSpace(clusterKey) == "" {
		return errors.New("incident: empty cluster key")
	}
	ctx, cancel := s.ctx()
	defer cancel()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("incident pg: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var rowID int64
	err = tx.QueryRow(ctx, `
SELECT id FROM incident WHERE tenant_id=$1 AND incident_id=$2`,
		s.tenantID, id).Scan(&rowID)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("incident pg: select id: %w", err)
	}
	// 簇已被占用？同事件幂等，异事件拒绝。
	var prevIncidentID *string
	err = tx.QueryRow(ctx, `
SELECT i.incident_id FROM incident_cluster ic JOIN incident i ON i.id = ic.incident_row_id
WHERE ic.cluster_key=$1`, clusterKey).Scan(&prevIncidentID)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("incident pg: check cluster owner: %w", err)
	}
	if prevIncidentID != nil {
		if *prevIncidentID == id {
			return nil // 幂等
		}
		return &ClusterTakenError{ClusterKey: clusterKey, Owner: *prevIncidentID}
	}
	if _, err = tx.Exec(ctx, `
INSERT INTO incident_cluster (incident_row_id, cluster_key) VALUES ($1, $2)`,
		rowID, clusterKey); err != nil {
		return fmt.Errorf("incident pg: attach: %w", err)
	}
	if _, err = tx.Exec(ctx, `
UPDATE incident SET updated_at=now() WHERE id=$1`, rowID); err != nil {
		return fmt.Errorf("incident pg: touch: %w", err)
	}
	return tx.Commit(ctx)
}

// List 按创建序列出（state 空串=全部；含簇关联聚合）。
// 错误如实上抛（D5 决策 A）——DB 故障不得伪装成"空列表"。
func (s *PGStore) List(state State) ([]Incident, error) {
	ctx, cancel := s.ctx()
	defer cancel()
	// state 走绑定参数（非字符串拼接）：incident.Store 是公开接口，
	// 防线不能只靠调用方白名单。`$2='' OR state=$2` 一次表达"空串=全部"。
	rows, err := s.pool.Query(ctx, `
SELECT `+pgIncidentCols+`
FROM incident
WHERE tenant_id=$1 AND ($2 = '' OR state = $2)
ORDER BY created_at`, s.tenantID, string(state))
	if err != nil {
		return nil, fmt.Errorf("incident pg: list: %w", err)
	}
	defer rows.Close()
	out := []Incident{}
	for rows.Next() {
		var inc Incident
		if err := scanIncident(rows, &inc); err != nil {
			return nil, fmt.Errorf("incident pg: list scan: %w", err)
		}
		out = append(out, inc)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("incident pg: list rows: %w", err)
	}
	// 簇关联批量聚合（一条 ANY(...) 查询取全，见 fillClusters——早期误写成
	// 逐事件一查的 N+1，已修）。
	s.fillClusters(ctx, out)
	return out, nil
}

// MergeInto 人工合并：事务内把被合并单置 resolved + merged_into，
// 簇关联转移给主单（一簇一事件唯一索引约束下跳过主单已占用的簇）。
// 本 SQL 是状态机单一实现 closeAsMerged（statemachine.go）落库侧的镜像：
// resolved_at/updated_at 同语句双 now() ⇒ 恒相等；D7 裁决以此处
// "真实簇转移"语义为准，MemStore 原 no-op 实现已修齐。
func (s *PGStore) MergeInto(id, targetID string) error {
	if id == targetID {
		return errors.New("incident: cannot merge into itself")
	}
	ctx, cancel := s.ctx()
	defer cancel()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("incident pg: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var srcRow, tgtRow int64
	var srcMerged string
	var srcState State
	if err := tx.QueryRow(ctx, `
SELECT id, merged_into, state FROM incident WHERE tenant_id=$1 AND incident_id=$2`,
		s.tenantID, id).Scan(&srcRow, &srcMerged, &srcState); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		return fmt.Errorf("incident pg: select src: %w", err)
	}
	if err := tx.QueryRow(ctx, `
SELECT id FROM incident WHERE tenant_id=$1 AND incident_id=$2`,
		s.tenantID, targetID).Scan(&tgtRow); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		return fmt.Errorf("incident pg: select tgt: %w", err)
	}
	if srcMerged == targetID && srcState == StateResolved {
		return nil // 幂等
	}
	// 簇关联转移：目标已占用该簇则跳过（一簇一事件约束）。
	if _, err := tx.Exec(ctx, `
UPDATE incident_cluster SET incident_row_id=$2
WHERE incident_row_id=$1 AND cluster_key NOT IN (
  SELECT cluster_key FROM incident_cluster WHERE incident_row_id=$2)`, srcRow, tgtRow); err != nil {
		return fmt.Errorf("incident pg: move clusters: %w", err)
	}
	if _, err := tx.Exec(ctx, `
UPDATE incident SET merged_into=$3, state='resolved', resolved_at=now(), updated_at=now()
WHERE id=$1 AND tenant_id=$2`, srcRow, s.tenantID, targetID); err != nil {
		return fmt.Errorf("incident pg: close src: %w", err)
	}
	if _, err := tx.Exec(ctx, `
UPDATE incident SET updated_at=now() WHERE id=$1`, tgtRow); err != nil {
		return fmt.Errorf("incident pg: touch tgt: %w", err)
	}
	return tx.Commit(ctx)
}
