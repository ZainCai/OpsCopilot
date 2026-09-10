// PGStore：事件的 TimescaleDB 持久化实现（W9，功能点 F-01/F-02 落库）。
// 表对齐 migrations/000004（incident + incident_cluster，一簇一事件唯一索引）。
//
// 治理（R6-5 消除）：全部语句经 ctxWithTimeout(5s)——DB 慢不拖垮调用方。
package incident

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// writeTimeout 单语句超时（R6-5：pgxpool 默认无 statement timeout）。
const writeTimeout = 5 * time.Second

// PGStore TimescaleDB 实现。
type PGStore struct {
	pool     *pgxpool.Pool
	tenantID string
}

// NewPGStore 建连接池并确保租户行（FK 要求）。
func NewPGStore(ctx context.Context, dsn, tenantID string) (*PGStore, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("incident pg: %w", err)
	}
	s, err := NewPGStoreWithPool(ctx, pool, tenantID)
	if err != nil {
		pool.Close() // 构造失败由本函数负责释放自己建的池
		return nil, err
	}
	return s, nil
}

// NewPGStoreWithPool 用调用方提供的连接池构造（装配期共享池：事件 Store、
// 导入队列、审计共用一条池，避免多池各占连接）。池的生命周期归调用方。
func NewPGStoreWithPool(ctx context.Context, pool *pgxpool.Pool, tenantID string) (*PGStore, error) {
	s := &PGStore{pool: pool, tenantID: tenantID}
	if err := s.ensureTenant(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

// Close 释放连接池。
func (s *PGStore) Close() { s.pool.Close() }

// Persistence 落库形态标识。
func (s *PGStore) Persistence() string { return "timescaledb" }

// ensureTenant 幂等确保租户行（alert/alert_cluster 同惯例）。
func (s *PGStore) ensureTenant(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()
	_, err := s.pool.Exec(ctx,
		`INSERT INTO tenant (id, name) VALUES ($1, $1) ON CONFLICT (id) DO NOTHING`,
		s.tenantID)
	return err
}

func (s *PGStore) ctx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), writeTimeout)
}

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
	if err := s.ensureTenant(context.Background()); err != nil {
		return nil, err
	}
	ctx, cancel := s.ctx()
	defer cancel()
	var inc Incident
	err := s.pool.QueryRow(ctx, `
INSERT INTO incident (tenant_id, incident_id, title, severity, state, origin, created_by, auto_close_policy)
VALUES ($1, $2, $3, NULLIF($4,''), 'open', 'manual', NULLIF($5,''), 'manual_only')
RETURNING incident_id, title, severity, state, created_at, updated_at, resolved_at,
          origin, source_ref, source_meta, created_by, merged_into, auto_close_policy`,
		s.tenantID, id, title, severity, createdBy).Scan(
		&inc.ID, &inc.Title, &inc.Severity, &inc.State, &inc.CreatedAt, &inc.UpdatedAt,
		&nullTime{t: &inc.ResolvedAt}, &inc.Origin, &inc.SourceRef, &inc.SourceMeta,
		&inc.CreatedBy, &inc.MergedInto, &inc.AutoClosePolicy)
	if err != nil {
		if strings.Contains(err.Error(), "duplicate key") {
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
// UPDATE（并发下仍保持状态机正确性）。
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
	if !CanTransition(cur, to) {
		return Incident{}, ErrInvalidTransition{From: cur, To: to}
	}
	var inc Incident
	err = tx.QueryRow(ctx, `
UPDATE incident SET state=$3, updated_at=now(),
  resolved_at = CASE WHEN $3='resolved' THEN now() ELSE resolved_at END,
  ack_by = CASE WHEN $3='resolved' AND $4<>'' THEN $4 ELSE ack_by END,
  auto_close_policy = CASE WHEN $3='acked' THEN 'manual_only' ELSE auto_close_policy END
WHERE tenant_id=$1 AND incident_id=$2
RETURNING incident_id, title, severity, state, created_at, updated_at, resolved_at,
          origin, source_ref, source_meta, created_by, merged_into, auto_close_policy`,
		s.tenantID, id, to, actor).Scan(
		&inc.ID, &inc.Title, &inc.Severity, &inc.State, &inc.CreatedAt, &inc.UpdatedAt,
		&nullTime{t: &inc.ResolvedAt}, &inc.Origin, &inc.SourceRef, &inc.SourceMeta,
		&inc.CreatedBy, &inc.MergedInto, &inc.AutoClosePolicy)
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
		return fmt.Errorf("incident: cluster %q already attached to %q", clusterKey, *prevIncidentID)
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
func (s *PGStore) List(state State) []Incident {
	ctx, cancel := s.ctx()
	defer cancel()
	where := "TRUE"
	if state != "" {
		where = "state = '" + string(state) + "'" // 白名单枚举内插（调用方经 REST 已限枚举）
	}
	rows, err := s.pool.Query(ctx, `
SELECT incident_id, title, severity, state, created_at, updated_at, resolved_at,
       origin, source_ref, source_meta, created_by, merged_into, auto_close_policy
FROM incident WHERE tenant_id=$1 AND `+where+` ORDER BY created_at`, s.tenantID)
	if err != nil {
		return nil
	}
	defer rows.Close()
	out := []Incident{}
	for rows.Next() {
		var inc Incident
		if err := rows.Scan(&inc.ID, &inc.Title, &inc.Severity, &inc.State,
			&inc.CreatedAt, &inc.UpdatedAt, &nullTime{t: &inc.ResolvedAt},
			&inc.Origin, &inc.SourceRef, &inc.SourceMeta, &inc.CreatedBy,
			&inc.MergedInto, &inc.AutoClosePolicy); err != nil {
			return out
		}
		out = append(out, inc)
	}
	// 簇关联批量聚合（避免 N+1；M2 规模下每事件一查亦可，这里一次取全）。
	s.fillClusters(ctx, out)
	return out
}

// IncidentForCluster 反查簇所属事件。
func (s *PGStore) IncidentForCluster(clusterKey string) (Incident, bool) {
	ctx, cancel := s.ctx()
	defer cancel()
	inc, err := s.queryOne(ctx, `
incident_id = (SELECT i.incident_id FROM incident_cluster ic
  JOIN incident i ON i.id = ic.incident_row_id
  WHERE i.tenant_id=$2 AND ic.cluster_key=$1)`, clusterKey, s.tenantID)
	if err != nil {
		return Incident{}, false
	}
	return inc, true
}

// queryOne 按 where 片段查单事件 + 簇关联。
func (s *PGStore) queryOne(ctx context.Context, where string, args ...any) (Incident, error) {
	var inc Incident
	full := `SELECT incident_id, title, severity, state, created_at, updated_at, resolved_at,
       origin, source_ref, source_meta, created_by, merged_into, auto_close_policy
FROM incident WHERE tenant_id=$2 AND ` + where
	err := s.pool.QueryRow(ctx, full, args...).Scan(
		&inc.ID, &inc.Title, &inc.Severity, &inc.State, &inc.CreatedAt, &inc.UpdatedAt,
		&nullTime{t: &inc.ResolvedAt}, &inc.Origin, &inc.SourceRef, &inc.SourceMeta,
		&inc.CreatedBy, &inc.MergedInto, &inc.AutoClosePolicy)
	if errors.Is(err, pgx.ErrNoRows) {
		return Incident{}, ErrNotFound
	}
	if err != nil {
		return Incident{}, fmt.Errorf("incident pg: query: %w", err)
	}
	tmp := []Incident{inc}
	s.fillClusters(ctx, tmp) // 切片元素回填（直接传 &inc 会绕过切片语义）
	return tmp[0], nil
}

// fillClusters 批量填簇关联（一次查询，M2 规模内联足够）。
// nullTime 可空时间戳扫描器（resolved_at NULL → 零值 time.Time）。
type nullTime struct{ t *time.Time }

func (n nullTime) Scan(src any) error {
	if src == nil {
		*n.t = time.Time{}
		return nil
	}
	t, ok := src.(time.Time)
	if !ok {
		return fmt.Errorf("incident pg: expected time, got %T", src)
	}
	*n.t = t
	return nil
}

func (s *PGStore) fillClusters(ctx context.Context, list []Incident) {
	for i := range list {
		var keys []string
		rows, err := s.pool.Query(ctx, `
SELECT cluster_key FROM incident_cluster ic
JOIN incident i ON i.id = ic.incident_row_id
WHERE i.tenant_id=$1 AND i.incident_id=$2 ORDER BY cluster_key`,
			s.tenantID, list[i].ID)
		if err != nil {
			continue
		}
		for rows.Next() {
			var k string
			if rows.Scan(&k) == nil {
				keys = append(keys, k)
			}
		}
		rows.Close()
		list[i].ClusterKeys = keys
	}
}

// UpsertExternal 外部链路幂等写入（链路 A，L0 幂等）：
// ON CONFLICT (tenant_id, origin, source_ref) DO UPDATE —— 重推只更新不新建。
// incident_id 用 origin:sourceRef 合成，保证 ID 唯一且可反查来源。
func (s *PGStore) UpsertExternal(origin Origin, sourceRef, title, severity, createdBy, meta string) (*Incident, bool, error) {
	if !ValidOrigin(origin) {
		return nil, false, errors.New("incident: invalid origin " + string(origin))
	}
	if strings.TrimSpace(sourceRef) == "" {
		return nil, false, errors.New("incident: source_ref is required for external upsert")
	}
	if strings.TrimSpace(title) == "" {
		return nil, false, errors.New("incident: title is required")
	}
	if meta == "" {
		meta = "{}"
	}
	if err := s.ensureTenant(context.Background()); err != nil {
		return nil, false, err
	}
	ctx, cancel := s.ctx()
	defer cancel()
	var inc Incident
	var xmax string // 用系统列判断是插入还是更新（更新时 xmax<>0）
	err := s.pool.QueryRow(ctx, `
INSERT INTO incident (tenant_id, incident_id, title, severity, state, origin, source_ref, source_meta, created_by, auto_close_policy)
VALUES ($1, $2, $3, NULLIF($4,''), 'open', $5, $6, $7::jsonb, $8, 'auto')
ON CONFLICT (tenant_id, origin, source_ref) WHERE source_ref <> '' DO UPDATE SET
  title       = EXCLUDED.title,
  severity    = EXCLUDED.severity,
  source_meta = EXCLUDED.source_meta,
  updated_at  = now()
RETURNING incident_id, title, severity, state, created_at, updated_at, resolved_at,
          origin, source_ref, source_meta, created_by, merged_into, auto_close_policy, xmax::text`,
		s.tenantID, string(origin)+":"+sourceRef, title, severity, origin, sourceRef, meta, createdBy,
	).Scan(&inc.ID, &inc.Title, &inc.Severity, &inc.State, &inc.CreatedAt, &inc.UpdatedAt,
		&nullTime{t: &inc.ResolvedAt}, &inc.Origin, &inc.SourceRef, &inc.SourceMeta,
		&inc.CreatedBy, &inc.MergedInto, &inc.AutoClosePolicy, &xmax)
	if err != nil {
		return nil, false, fmt.Errorf("incident pg: upsert external: %w", err)
	}
	return &inc, xmax == "0", nil
}

// MergeInto 人工合并：事务内把被合并单置 resolved + merged_into，
// 簇关联转移给主单（一簇一事件唯一索引约束下用 ON CONFLICT 忽略冲突）。
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
