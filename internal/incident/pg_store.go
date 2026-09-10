// PGStore：事件的 TimescaleDB 持久化实现（W9，功能点 F-01/F-02 落库）。
// 表对齐 migrations/000004（incident + incident_cluster，一簇一事件唯一索引）。
//
// 治理（R6-5 消除）：全部语句经 ctxWithTimeout(5s)——DB 慢不拖垮调用方。
package incident

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// writeTimeout 单语句超时（R6-5：pgxpool 默认无 statement timeout）。
const writeTimeout = 5 * time.Second

// pgUniqueViolation SQLSTATE 唯一约束冲突。
const pgUniqueViolation = "23505"

// isUniqueViolation 判定唯一约束冲突。
// 用 SQLSTATE 而非匹配错误文案——PG 的错误文本会随语言/版本变化
// （"duplicate key value violates unique constraint" 不是稳定契约）。
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation
}

// PGStore TimescaleDB 实现。
type PGStore struct {
	pool     *pgxpool.Pool
	tenantID string

	// 租户存在性的进程内记忆：tenant 行只建一次（幂等 INSERT），
	// 每次都去查一遍纯属多余往返（写路径上每单一次）。
	tenantMu   sync.Mutex
	tenantDone bool
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
// ensureTenant 幂等确保租户行（FK 要求）。
//
// 进程内记忆 + 双检：tenant 行只建一次，此前每次写入都先插一遍（幂等但
// 纯属多余往返，建单热路径上每单多一次 DB 往返）。若首查失败不记为完成，
// 下次写入会重试。若进程运行期间租户行被外部删掉，后续写入会以 FK 错误
// 显式失败——宁可响亮报错，不静默。
func (s *PGStore) ensureTenant(ctx context.Context) error {
	s.tenantMu.Lock()
	done := s.tenantDone
	s.tenantMu.Unlock()
	if done {
		return nil
	}
	ectx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()
	if _, err := s.pool.Exec(ectx,
		`INSERT INTO tenant (id, name) VALUES ($1, $1) ON CONFLICT (id) DO NOTHING`,
		s.tenantID); err != nil {
		return err
	}
	s.tenantMu.Lock()
	s.tenantDone = true
	s.tenantMu.Unlock()
	return nil
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
// 错误如实上抛（D5 决策 A）——DB 故障不得伪装成"空列表"。
func (s *PGStore) List(state State) ([]Incident, error) {
	ctx, cancel := s.ctx()
	defer cancel()
	// state 走绑定参数（非字符串拼接）：incident.Store 是公开接口，
	// 防线不能只靠调用方白名单。`$2='' OR state=$2` 一次表达"空串=全部"。
	rows, err := s.pool.Query(ctx, `
SELECT incident_id, title, severity, state, created_at, updated_at, resolved_at,
       origin, source_ref, source_meta, created_by, merged_into, auto_close_policy
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
		if err := rows.Scan(&inc.ID, &inc.Title, &inc.Severity, &inc.State,
			&inc.CreatedAt, &inc.UpdatedAt, &nullTime{t: &inc.ResolvedAt},
			&inc.Origin, &inc.SourceRef, &inc.SourceMeta, &inc.CreatedBy,
			&inc.MergedInto, &inc.AutoClosePolicy); err != nil {
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

// ListPage 分页读取（D4 决策 A：游标分页，最新优先；服务端过滤 state/origin）。
// 游标 = 上一页最后一行的 (created_at, incident_id)，行值比较一次定位，
// 无 OFFSET 的"翻深页越翻越慢"问题。Stats 为全量聚合（不受过滤影响）。
func (s *PGStore) ListPage(q PageQuery) (Page, error) {
	ctx, cancel := s.ctx()
	defer cancel()
	if err := normalizePageQuery(&q); err != nil {
		return Page{}, err
	}
	limit := pageLimit(q.Limit)

	// 全量聚合（KPI 数据源）：一条带 FILTER 的聚合，代价可忽略。
	var st PageStats
	if err := s.pool.QueryRow(ctx, `
SELECT count(*) FILTER (WHERE state <> 'resolved'),
       count(*) FILTER (WHERE state = 'resolved'),
       count(*) FILTER (WHERE origin = 'manual'),
       count(*) FILTER (WHERE origin <> 'manual')
FROM incident WHERE tenant_id=$1`, s.tenantID).Scan(
		&st.Active, &st.Resolved, &st.Manual, &st.External); err != nil {
		return Page{}, fmt.Errorf("incident pg: page stats: %w", err)
	}

	args := []any{s.tenantID, string(q.State), q.Origin}
	cond := ""
	if q.Cursor != "" {
		at, id, err := decodeCursor(q.Cursor)
		if err != nil {
			return Page{}, err
		}
		args = append(args, at, id)
		cond = fmt.Sprintf(" AND (created_at, incident_id) < ($%d, $%d)", len(args)-1, len(args))
	}
	args = append(args, limit+1) // 多取一行判断是否还有下一页
	rows, err := s.pool.Query(ctx, `
SELECT incident_id, title, severity, state, created_at, updated_at, resolved_at,
       origin, source_ref, source_meta, created_by, merged_into, auto_close_policy
FROM incident
WHERE tenant_id=$1
  AND ($2 = '' OR ($2 = 'active' AND state <> 'resolved') OR state = $2)
  AND ($3 = '' OR origin = $3)`+cond+`
ORDER BY created_at DESC, incident_id DESC
LIMIT $`+strconv.Itoa(len(args)), args...)
	if err != nil {
		return Page{}, fmt.Errorf("incident pg: page query: %w", err)
	}
	defer rows.Close()

	items := []Incident{}
	for rows.Next() {
		var inc Incident
		if err := rows.Scan(&inc.ID, &inc.Title, &inc.Severity, &inc.State,
			&inc.CreatedAt, &inc.UpdatedAt, &nullTime{t: &inc.ResolvedAt},
			&inc.Origin, &inc.SourceRef, &inc.SourceMeta, &inc.CreatedBy,
			&inc.MergedInto, &inc.AutoClosePolicy); err != nil {
			return Page{}, fmt.Errorf("incident pg: page scan: %w", err)
		}
		items = append(items, inc)
	}
	if err := rows.Err(); err != nil {
		return Page{}, fmt.Errorf("incident pg: page rows: %w", err)
	}
	s.fillClusters(ctx, items)

	next := ""
	if len(items) > limit {
		last := items[limit-1]
		next = encodeCursor(last.CreatedAt, last.ID)
		items = items[:limit]
	}
	return Page{Items: items, NextCursor: next, Stats: st}, nil
}

// IncidentForCluster 反查簇所属事件。
//
// ⚠️ 返回值语义：`(zero, false)` 同时覆盖"该簇没有事件"与"DB 故障"两种情况
// ——接口签名无 error，二者不可区分。当前生产代码**没有**调用本方法
// （仅测试），故无实际影响；若未来接入业务，请优先用 `Get()`（带 error）
// 而不是把 false 当成"无事件"去新建，否则 DB 不可用时会建出重复单。
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

// fillClusters 批量填充簇关联（**单次查询**，非 N+1）。
//
// 早期实现是"遍历 list 逐个查 incident_cluster"——注释写着"避免 N+1"但
// 实现就是 N+1：一次 GET /api/v1/incidents 会串行发 N 条查询。表里上千行时
// 单请求要数秒，并发下（100 并发压测实测 P95 3.7s）彻底打爆。改为一条
// `incident_id = ANY($2)` 批量取回再按 ID 分桶，查询数与事件数解耦。
func (s *PGStore) fillClusters(ctx context.Context, list []Incident) {
	if len(list) == 0 {
		return
	}
	ids := make([]string, len(list))
	idx := make(map[string]int, len(list))
	for i := range list {
		ids[i] = list[i].ID
		idx[list[i].ID] = i
	}
	rows, err := s.pool.Query(ctx, `
SELECT i.incident_id, ic.cluster_key FROM incident_cluster ic
JOIN incident i ON i.id = ic.incident_row_id
WHERE i.tenant_id=$1 AND i.incident_id = ANY($2)
ORDER BY i.incident_id, ic.cluster_key`, s.tenantID, ids)
	if err != nil {
		return // 关联属补充信息：查不到不阻断主列表（与旧实现容错口径一致）
	}
	defer rows.Close()
	for rows.Next() {
		var id, k string
		if rows.Scan(&id, &k) == nil {
			if i, ok := idx[id]; ok {
				list[i].ClusterKeys = append(list[i].ClusterKeys, k)
			}
		}
	}
}

// externalGenRetries 分配新代时的重试上限：并发 worker 同号插入会撞唯一索引，
// 撞了就重读当前代再试下一代（有界，防止异常数据下打转）。
// externalIncidentID 见 incident.go（与 MemStore 同一规则，必须一致）。
const externalGenRetries = 8

// UpsertExternal 外部链路写入（链路 A）。幂等语义（M9：复发即新建）：
//
//   - 命中未解决（open/acked/mitigated）的当前代 → 刷新（重推不新建）；
//   - 命中已解决的当前代 → 视为告警复发，**新开一代**（generation+1，
//     incident_id 追加 #N）。否则新故障只会去刷新一条已关闭的旧单，
//     运维看不见——这正是 M9 要修的。
//   - 第 1 代 incident_id 保持裸 origin:sourceRef（兼容历史数据与反查习惯）。
//
// 幂等键：migrations/000008 把唯一索引从 (tenant,origin,source_ref) 换成
// (…, generation)。并发 worker 同号插入会撞索引，此处以"重读当前代 →
// 重试下一代"收敛（有界）。
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
	base := string(origin) + ":" + sourceRef

	// 当前代 = 该 (origin, source_ref) 下 generation 最大的那一行。
	var cur Incident
	var curGen int
	err := s.pool.QueryRow(ctx, `
SELECT incident_id, title, severity, state, created_at, updated_at, resolved_at,
       origin, source_ref, source_meta, created_by, merged_into, auto_close_policy, generation
FROM incident
WHERE tenant_id=$1 AND origin=$2 AND source_ref=$3
ORDER BY generation DESC LIMIT 1`,
		s.tenantID, string(origin), sourceRef).Scan(
		&cur.ID, &cur.Title, &cur.Severity, &cur.State, &cur.CreatedAt, &cur.UpdatedAt,
		&nullTime{t: &cur.ResolvedAt}, &cur.Origin, &cur.SourceRef, &cur.SourceMeta,
		&cur.CreatedBy, &cur.MergedInto, &cur.AutoClosePolicy, &curGen)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		inc, ierr := s.insertExternalGen(ctx, origin, sourceRef, 1, title, severity, createdBy, meta)
		if ierr != nil {
			return nil, false, ierr
		}
		return inc, true, nil
	case err != nil:
		return nil, false, fmt.Errorf("incident pg: upsert external lookup: %w", err)
	}

	if cur.State != StateResolved {
		// 未解决：就地刷新当前代（重推不新建，与历史行为一致）。
		var inc Incident
		err := s.pool.QueryRow(ctx, `
UPDATE incident SET title=$3, severity=$4, source_meta=$5::jsonb, updated_at=now()
WHERE tenant_id=$1 AND incident_id=$2
RETURNING incident_id, title, severity, state, created_at, updated_at, resolved_at,
          origin, source_ref, source_meta, created_by, merged_into, auto_close_policy`,
			s.tenantID, cur.ID, title, severity, meta).Scan(
			&inc.ID, &inc.Title, &inc.Severity, &inc.State, &inc.CreatedAt, &inc.UpdatedAt,
			&nullTime{t: &inc.ResolvedAt}, &inc.Origin, &inc.SourceRef, &inc.SourceMeta,
			&inc.CreatedBy, &inc.MergedInto, &inc.AutoClosePolicy)
		if err != nil {
			return nil, false, fmt.Errorf("incident pg: upsert external refresh: %w", err)
		}
		return &inc, false, nil
	}

	// 已解决：复发，新开一代。
	for gen := curGen + 1; gen <= curGen+externalGenRetries; gen++ {
		inc, ierr := s.insertExternalGen(ctx, origin, sourceRef, gen, title, severity, createdBy, meta)
		if ierr == nil {
			return inc, true, nil
		}
		if !isUniqueViolation(ierr) {
			return nil, false, ierr
		}
		// 撞号：另一 worker 抢先建了这一代。重读它——未解决就按"刷新"返回。
		var winner Incident
		e := s.pool.QueryRow(ctx, `
SELECT incident_id, title, severity, state, created_at, updated_at, resolved_at,
       origin, source_ref, source_meta, created_by, merged_into, auto_close_policy
FROM incident WHERE tenant_id=$1 AND incident_id=$2`,
			s.tenantID, externalIncidentID(base, gen)).Scan(
			&winner.ID, &winner.Title, &winner.Severity, &winner.State, &winner.CreatedAt,
			&winner.UpdatedAt, &nullTime{t: &winner.ResolvedAt}, &winner.Origin,
			&winner.SourceRef, &winner.SourceMeta, &winner.CreatedBy, &winner.MergedInto,
			&winner.AutoClosePolicy)
		if e != nil {
			continue // 读不到（异常）→ 试下一代
		}
		if winner.State != StateResolved {
			return &winner, false, nil
		}
		// 抢到的这代也已经解决了 → 继续下一代
	}
	return nil, false, fmt.Errorf("incident pg: upsert external: cannot allocate generation for %q", base)
}

// insertExternalGen 写入外部事件的第 gen 代。
func (s *PGStore) insertExternalGen(ctx context.Context, origin Origin, sourceRef string, gen int, title, severity, createdBy, meta string) (*Incident, error) {
	var inc Incident
	id := externalIncidentID(string(origin)+":"+sourceRef, gen)
	err := s.pool.QueryRow(ctx, `
INSERT INTO incident (tenant_id, incident_id, title, severity, state, origin, source_ref, source_meta, created_by, auto_close_policy, generation)
VALUES ($1, $2, $3, NULLIF($4,''), 'open', $5, $6, $7::jsonb, $8, 'auto', $9)
RETURNING incident_id, title, severity, state, created_at, updated_at, resolved_at,
          origin, source_ref, source_meta, created_by, merged_into, auto_close_policy`,
		s.tenantID, id, title, severity, origin, sourceRef, meta, createdBy, gen).Scan(
		&inc.ID, &inc.Title, &inc.Severity, &inc.State, &inc.CreatedAt, &inc.UpdatedAt,
		&nullTime{t: &inc.ResolvedAt}, &inc.Origin, &inc.SourceRef, &inc.SourceMeta,
		&inc.CreatedBy, &inc.MergedInto, &inc.AutoClosePolicy)
	if err != nil {
		return nil, fmt.Errorf("incident pg: insert external gen %d: %w", gen, err)
	}
	return &inc, nil
}

// ExternalActive 报告该 (origin, source_ref) 是否存在未解决的事件。
func (s *PGStore) ExternalActive(origin Origin, sourceRef string) (bool, error) {
	ctx, cancel := s.ctx()
	defer cancel()
	var active bool
	err := s.pool.QueryRow(ctx, `
SELECT EXISTS(SELECT 1 FROM incident
  WHERE tenant_id=$1 AND origin=$2 AND source_ref=$3 AND state <> 'resolved')`,
		s.tenantID, string(origin), sourceRef).Scan(&active)
	if err != nil {
		return false, fmt.Errorf("incident pg: external active: %w", err)
	}
	return active, nil
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
