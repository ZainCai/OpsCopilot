// PGStore：事件的 TimescaleDB 持久化实现（W9，功能点 F-01/F-02 落库）。
// 本文件承载连接与扫描辅助层：构造/超时 ctx/列清单与扫描器/单行查询与簇聚合共用件。
// 表对齐 migrations/000004（incident + incident_cluster，一簇一事件唯一索引）。
//
// 治理（R6-5 消除）：全部语句经 ctxWithTimeout(5s)——DB 慢不拖垮调用方。

package incident

import (
	"context"
	"errors"
	"fmt"
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

// pgIncidentCols Incident 全量业务列（含 ack_by——D1 教训：所有查询与
// RETURNING 必须共用这一份清单。此前 ack_by 有写无读，正是因为读路径各写
// 各的列清单、漏了它；统一后新增字段只改一处）。行内 SELECT 需要附加列的
// 地方在其后拼接（如 generation）。
const pgIncidentCols = `incident_id, title, severity, state, created_at, updated_at, resolved_at,
       origin, source_ref, source_meta, created_by, merged_into, auto_close_policy, ack_by`

// rowScanner pgx Rows/Row 的公共扫描面。
type rowScanner interface {
	Scan(dest ...any) error
}

// scanIncident 按 pgIncidentCols 顺序扫描一行（与列清单的对应关系仅此一处）。
func scanIncident(r rowScanner, inc *Incident) error {
	return r.Scan(&inc.ID, &inc.Title, &inc.Severity, &inc.State, &inc.CreatedAt,
		&inc.UpdatedAt, &nullTime{t: &inc.ResolvedAt}, &inc.Origin, &inc.SourceRef,
		&inc.SourceMeta, &inc.CreatedBy, &inc.MergedInto, &inc.AutoClosePolicy,
		&inc.AckBy)
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

// queryOne 按 where 片段查单事件 + 簇关联。
func (s *PGStore) queryOne(ctx context.Context, where string, args ...any) (Incident, error) {
	var inc Incident
	full := `SELECT ` + pgIncidentCols + `
FROM incident WHERE tenant_id=$2 AND ` + where
	err := scanIncident(s.pool.QueryRow(ctx, full, args...), &inc)
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
