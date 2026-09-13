// Package runbook W11-4（F-12）Runbook 记录版的纯 CRUD store（PG）。
//
// 定位与口径（对齐 migration 000020 头注释）：
//   - 只记不执行：本包只提供手册库/挂载关系/执行记录三张表的存取，没有任何
//     "执行引擎"语义；执行记录 append-only（零 UPDATE/DELETE 语句），
//     且**不算审计证据**（session 拍板②口径：incident_audit 哈希链不随本表变动）；
//   - 独立模块纪律：不 import 任何其它 internal 模块（边界脚本守护），
//     DTO/校验/编排上沉到 cmd 装配层（与 SLA 派生在 cmd 同先例）；
//   - PG-only（无内存兜底）：与 ChannelStore 同款——无 DSN 时端点 503
//     （对齐 RCA 端点降级口径），不做"重启即丢"的内存假象。
package runbook

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// 哨兵错误（REST 映射用类型判断，不做文案匹配——第八轮审核反模式纪律）。
var (
	// ErrRunbookNotFound 手册库中不存在该 id（挂载前置检查 / FK 23503 归一）。
	ErrRunbookNotFound = errors.New("runbook: not found")
	// ErrDuplicateID 同租户下手册 id 重复（REST 409）。
	ErrDuplicateID = errors.New("runbook: duplicate id")
	// ErrNotMounted 挂载对不存在（解挂/记录执行的前置；REST 404）。
	ErrNotMounted = errors.New("runbook: not mounted")
)

// Runbook 手册库行（DTO 与 API 响应同构，snake_case）。
type Runbook struct {
	ID            string    `json:"id"`
	Title         string    `json:"title"`
	Content       string    `json:"content"` // markdown 正文（展示用，系统不解析不执行）
	ScopeSeverity string    `json:"scope_severity"`
	ScopeService  string    `json:"scope_service"`
	CreatedBy     string    `json:"created_by"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// MountView 挂载聚合行：手册展示字段 + 挂载元数据 + 执行计数，平铺单层
// （前端事件详情页"挂载手册"卡片直接渲染，无需二跳）。刻意不嵌入 Runbook：
// 嵌入会把库主键 "id" 和平铺挂载键 "runbook_id" 同时带进 JSON——契约面上
// 一个键就够（runbook_id），重复键是两个真相源的 JSON 版。
type MountView struct {
	RunbookID      string    `json:"runbook_id"`
	Title          string    `json:"title"`
	Content        string    `json:"content"`
	ScopeSeverity  string    `json:"scope_severity"`
	ScopeService   string    `json:"scope_service"`
	CreatedBy      string    `json:"created_by"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
	MountedBy      string    `json:"mounted_by"`
	MountedAt      time.Time `json:"mounted_at"`
	ExecutionCount int64     `json:"execution_count"`
}

// Execution 执行记录行（append-only；refs 为自由 JSON 数组原样透传）。
type Execution struct {
	Seq        int64           `json:"seq"`
	ExecutedBy string          `json:"executed_by"`
	ExecutedAt time.Time       `json:"executed_at"`
	Result     string          `json:"result"`
	Refs       json.RawMessage `json:"refs"`
}

// Store 三表的 PG 存取（共享事件域连接池，R9；租户构造期固定）。
type Store struct {
	pool   *pgxpool.Pool
	tenant string
}

// NewStore 构造。pool 为 nil 时不得调用任何方法（装配层负责不接线 → 端点 503）。
func NewStore(pool *pgxpool.Pool, tenant string) *Store {
	return &Store{pool: pool, tenant: tenant}
}

// Persistence 落库形态透出（R6-4 口径；本 store 只在 DB 装配时存在）。
func (s *Store) Persistence() string { return "timescaledb" }

// queryCtx 统一 5s 超时（ChannelStore 同纪：读面小查询不该挂住请求线程）。
func queryCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, 5*time.Second)
}

// Create 新建手册（记录版库 CRUD 从简：无更新/删除入口）。成功回写时间戳。
func (s *Store) Create(ctx context.Context, rb *Runbook) error {
	cctx, cancel := queryCtx(ctx)
	defer cancel()
	err := s.pool.QueryRow(cctx, `
INSERT INTO runbook (tenant_id, id, title, content, scope_severity, scope_service, created_by)
VALUES ($1, $2, $3, $4, $5, $6, $7)
RETURNING created_at, updated_at`,
		s.tenant, rb.ID, rb.Title, rb.Content, rb.ScopeSeverity, rb.ScopeService, rb.CreatedBy).
		Scan(&rb.CreatedAt, &rb.UpdatedAt)
	if isUniqueViolation(err) {
		return ErrDuplicateID
	}
	if err != nil {
		return fmt.Errorf("runbook store: create %s: %w", rb.ID, err)
	}
	return nil
}

// List 全库列表（新→旧；M2 规模整表返回，分页留给真实需求出现时）。
func (s *Store) List(ctx context.Context) ([]Runbook, error) {
	cctx, cancel := queryCtx(ctx)
	defer cancel()
	rows, err := s.pool.Query(cctx, `
SELECT id, title, content, scope_severity, scope_service, created_by, created_at, updated_at
FROM runbook WHERE tenant_id = $1 ORDER BY created_at DESC, id`, s.tenant)
	if err != nil {
		return nil, fmt.Errorf("runbook store: list: %w", err)
	}
	defer rows.Close()
	var out []Runbook
	for rows.Next() {
		var r Runbook
		if err := rows.Scan(&r.ID, &r.Title, &r.Content, &r.ScopeSeverity, &r.ScopeService,
			&r.CreatedBy, &r.CreatedAt, &r.UpdatedAt); err != nil {
			return nil, fmt.Errorf("runbook store: list scan: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// Get 单本手册（挂载前置存在性检查复用；不存在 → ErrRunbookNotFound）。
func (s *Store) Get(ctx context.Context, id string) (Runbook, error) {
	cctx, cancel := queryCtx(ctx)
	defer cancel()
	var r Runbook
	err := s.pool.QueryRow(cctx, `
SELECT id, title, content, scope_severity, scope_service, created_by, created_at, updated_at
FROM runbook WHERE tenant_id = $1 AND id = $2`, s.tenant, id).
		Scan(&r.ID, &r.Title, &r.Content, &r.ScopeSeverity, &r.ScopeService,
			&r.CreatedBy, &r.CreatedAt, &r.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Runbook{}, ErrRunbookNotFound
	}
	if err != nil {
		return Runbook{}, fmt.Errorf("runbook store: get %s: %w", id, err)
	}
	return r, nil
}

// Mount 挂载（幂等：PK 冲突即既有挂载，ON CONFLICT DO NOTHING 吞掉——
// 重复挂载恰一挂载，不刷新 mounted_at/by，既有留痕不被后写覆盖）。
// runbook 不存在 → FK 23503 归一为 ErrRunbookNotFound。
func (s *Store) Mount(ctx context.Context, incidentID, runbookID, mountedBy string) error {
	cctx, cancel := queryCtx(ctx)
	defer cancel()
	_, err := s.pool.Exec(cctx, `
INSERT INTO incident_runbook (tenant_id, incident_id, runbook_id, mounted_by, mounted_at)
VALUES ($1, $2, $3, $4, now())
ON CONFLICT (tenant_id, incident_id, runbook_id) DO NOTHING`,
		s.tenant, incidentID, runbookID, mountedBy)
	if isForeignKeyViolation(err) {
		return ErrRunbookNotFound
	}
	if err != nil {
		return fmt.Errorf("runbook store: mount %s->%s: %w", incidentID, runbookID, err)
	}
	return nil
}

// Unmount 解挂（只删挂载关系；执行记录不随解挂消失——见 migration 头注释）。
// 挂载不存在 → ErrNotMounted。
func (s *Store) Unmount(ctx context.Context, incidentID, runbookID string) error {
	cctx, cancel := queryCtx(ctx)
	defer cancel()
	tag, err := s.pool.Exec(cctx, `
DELETE FROM incident_runbook WHERE tenant_id = $1 AND incident_id = $2 AND runbook_id = $3`,
		s.tenant, incidentID, runbookID)
	if err != nil {
		return fmt.Errorf("runbook store: unmount %s->%s: %w", incidentID, runbookID, err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotMounted
	}
	return nil
}

// ListMounts 事件挂载列表（runbook join + 执行计数聚合，新挂→旧挂）。
func (s *Store) ListMounts(ctx context.Context, incidentID string) ([]MountView, error) {
	cctx, cancel := queryCtx(ctx)
	defer cancel()
	rows, err := s.pool.Query(cctx, `
SELECT m.runbook_id, m.mounted_by, m.mounted_at,
       r.title, r.content, r.scope_severity, r.scope_service, r.created_by, r.created_at, r.updated_at,
       (SELECT count(*) FROM runbook_execution_log l
         WHERE l.tenant_id = m.tenant_id AND l.incident_id = m.incident_id AND l.runbook_id = m.runbook_id)
FROM incident_runbook m
JOIN runbook r ON r.tenant_id = m.tenant_id AND r.id = m.runbook_id
WHERE m.tenant_id = $1 AND m.incident_id = $2
ORDER BY m.mounted_at DESC, m.runbook_id`, s.tenant, incidentID)
	if err != nil {
		return nil, fmt.Errorf("runbook store: list mounts %s: %w", incidentID, err)
	}
	defer rows.Close()
	var out []MountView
	for rows.Next() {
		var v MountView
		if err := rows.Scan(&v.RunbookID, &v.MountedBy, &v.MountedAt,
			&v.Title, &v.Content, &v.ScopeSeverity, &v.ScopeService, &v.CreatedBy, &v.CreatedAt, &v.UpdatedAt,
			&v.ExecutionCount); err != nil {
			return nil, fmt.Errorf("runbook store: list mounts scan: %w", err)
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// Mounted 挂载对是否存在（记录执行的前置检查）。
func (s *Store) Mounted(ctx context.Context, incidentID, runbookID string) (bool, error) {
	cctx, cancel := queryCtx(ctx)
	defer cancel()
	var one int
	err := s.pool.QueryRow(cctx, `
SELECT 1 FROM incident_runbook WHERE tenant_id = $1 AND incident_id = $2 AND runbook_id = $3`,
		s.tenant, incidentID, runbookID).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("runbook store: mounted %s->%s: %w", incidentID, runbookID, err)
	}
	return true, nil
}

// AppendExecution 追加一条执行记录（**append-only：本包对该表零
// UPDATE/DELETE 语句**）。seq 由列级 IDENTITY 发号，挂载内严格递增。
// 挂载不存在 → ErrNotMounted（执行必须挂在既有挂载点上，"记无所记"如实拒绝）。
// 竞态纪律：挂载检查与插入同事务，且对挂载行 **FOR UPDATE**——并行的解挂会
// 等本事务提交再删（历史落在保留下来的日志里），或先删成功、本处检查落空
// 如实 404；不给"边解挂边写入"留下第三种既非也不是的中间态。
// （本表对挂载**不建外键**是刻意的：CASCADE 会毁历史、RESTRICT 会挡解挂，
// 都不对——见 migration 000020 头注释；所以约束兜不住，必须行锁。）
// refs 为空时落 '[]'；合法性由 cmd 入口校验（本层只保证 JSONB 可存）。
func (s *Store) AppendExecution(ctx context.Context, incidentID, runbookID, executedBy, result string, refs json.RawMessage) (Execution, error) {
	if len(refs) == 0 {
		refs = json.RawMessage("[]")
	}
	cctx, cancel := queryCtx(ctx)
	defer cancel()
	tx, err := s.pool.Begin(cctx)
	if err != nil {
		return Execution{}, fmt.Errorf("runbook store: begin append %s->%s: %w", incidentID, runbookID, err)
	}
	defer tx.Rollback(cctx) //nolint:errcheck // Commit 后为 no-op；失败路径负责回滚
	var one int
	err = tx.QueryRow(cctx, `
SELECT 1 FROM incident_runbook
WHERE tenant_id = $1 AND incident_id = $2 AND runbook_id = $3 FOR UPDATE`,
		s.tenant, incidentID, runbookID).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		return Execution{}, ErrNotMounted
	}
	if err != nil {
		return Execution{}, fmt.Errorf("runbook store: lock mount %s->%s: %w", incidentID, runbookID, err)
	}
	var e Execution
	err = tx.QueryRow(cctx, `
INSERT INTO runbook_execution_log (tenant_id, incident_id, runbook_id, executed_by, result, refs)
VALUES ($1, $2, $3, $4, $5, $6::jsonb)
RETURNING seq, executed_at`,
		s.tenant, incidentID, runbookID, executedBy, result, string(refs)).
		Scan(&e.Seq, &e.ExecutedAt)
	if err != nil {
		return Execution{}, fmt.Errorf("runbook store: append execution %s->%s: %w", incidentID, runbookID, err)
	}
	if err := tx.Commit(cctx); err != nil {
		return Execution{}, fmt.Errorf("runbook store: commit append %s->%s: %w", incidentID, runbookID, err)
	}
	e.ExecutedBy, e.Result, e.Refs = executedBy, result, refs
	return e, nil
}

// ListExecutions 某挂载点的执行记录（旧→新，seq 递增序）。
// 不加挂载存在性前置：解挂后历史仍可查（GET 语义），从未执行回空列表。
func (s *Store) ListExecutions(ctx context.Context, incidentID, runbookID string) ([]Execution, error) {
	cctx, cancel := queryCtx(ctx)
	defer cancel()
	rows, err := s.pool.Query(cctx, `
SELECT seq, executed_by, executed_at, result, refs
FROM runbook_execution_log
WHERE tenant_id = $1 AND incident_id = $2 AND runbook_id = $3
ORDER BY seq`, s.tenant, incidentID, runbookID)
	if err != nil {
		return nil, fmt.Errorf("runbook store: list executions %s->%s: %w", incidentID, runbookID, err)
	}
	defer rows.Close()
	var out []Execution
	for rows.Next() {
		var e Execution
		var refs []byte
		if err := rows.Scan(&e.Seq, &e.ExecutedBy, &e.ExecutedAt, &e.Result, &refs); err != nil {
			return nil, fmt.Errorf("runbook store: executions scan: %w", err)
		}
		e.Refs = json.RawMessage(refs)
		out = append(out, e)
	}
	return out, rows.Err()
}

// isUniqueViolation / isForeignKeyViolation PG 约束错误分型（23505/23503）：
// 用 PgError 码而非文案匹配（第八轮纪律）。
func isUniqueViolation(err error) bool {
	var pe *pgconn.PgError
	return errors.As(err, &pe) && pe.Code == "23505"
}

func isForeignKeyViolation(err error) bool {
	var pe *pgconn.PgError
	return errors.As(err, &pe) && pe.Code == "23503"
}
