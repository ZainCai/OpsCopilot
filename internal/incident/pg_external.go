// pg_external.go PGStore 外部链路（链路 A）：复发代际幂等 upsert（M9）与活跃态查询。
package incident

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// externalGenRetries 分配新代时的重试上限：并发 worker 同号插入会撞唯一索引，
// 撞了就重读当前代再试下一代（有界，防止异常数据下打转）。
// externalIncidentID 见 incident.go（与 MemStore 同一规则，必须一致）。
const externalGenRetries = 8

// rowQuerier 事务与连接池共用的单行查询能力（insertExternalGen 复用）。
type rowQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// UpsertExternal 外部链路写入（链路 A）。幂等语义（M9：复发即新建）：
//
//   - 命中未解决（open/acked/mitigated）的当前代 → 刷新（重推不新建）；
//   - 命中已解决的当前代 → 视为告警复发，**新开一代**（generation+1，
//     incident_id 追加 #N）。否则新故障只会去刷新一条已关闭的旧单，
//     运维看不见——这正是 M9 要修的。
//   - 第 1 代 incident_id 保持裸 origin:sourceRef（兼容历史数据与反查习惯）。
//
// 并发（第七轮 M-1 修正）：**整段"锁定当前代 → 分支处理"在一个事务内，
// 且对该行 FOR UPDATE** —— 此前"读当前代（无锁）"与"刷新 UPDATE"之间，
// 另一 worker 可能为同一 ref 建了下一代，本次刷新会落到已 resolved 的旧代
// 上（本次告警丢失、恢复语错挂）。复发分支撞唯一索引则整事务回滚、
// 上层有界重试（重读时会看到抢先者并按刷新处理）。
func (s *PGStore) UpsertExternal(origin Origin, sourceRef, title, severity, createdBy, meta string) (*Incident, bool, error) {
	if !ValidOrigin(origin) {
		return nil, false, errors.New("incident: invalid origin " + string(origin))
	}
	if strings.TrimSpace(sourceRef) == "" {
		return nil, false, errors.New("incident: source_ref is required for external upsert")
	}
	if strings.Contains(sourceRef, "#") {
		// '#' 是 M9 复发代际后缀的分隔符（externalIncidentID）：含 '#' 的
		// sourceRef 会让"ref=a 的 gen2"与"ref=a#2 的 gen1"得到同一 incident_id，
		// 造成跨告警串单（第七轮 H2）。存量数据已查证为 0 行，可直接拒绝。
		return nil, false, errors.New("incident: source_ref must not contain '#'")
	}
	if strings.TrimSpace(title) == "" {
		return nil, false, errors.New("incident: title is required")
	}
	severity = normalizeSeverity(severity) // D2/D4 入口收口：新建与刷新分支都写不到空串
	if meta == "" {
		meta = "{}"
	}
	if err := s.ensureTenant(context.Background()); err != nil {
		return nil, false, err
	}
	ctx, cancel := s.ctx()
	defer cancel()
	base := string(origin) + ":" + sourceRef

	for attempt := 0; attempt < externalGenRetries; attempt++ {
		inc, isNew, err := s.upsertExternalAttempt(ctx, base, origin, sourceRef, title, severity, createdBy, meta)
		if err == nil {
			return inc, isNew, nil
		}
		if !isUniqueViolation(err) {
			return nil, false, err
		}
		// 撞唯一索引：另一 worker 抢先建了这一代。整事务已回滚，重读重试——
		// 下一次 attempt 会锁到抢先者的单：未解决则按"刷新"返回，已解决则继续开新一代。
	}
	return nil, false, fmt.Errorf("incident pg: upsert external: cannot allocate generation for %q", base)
}

// upsertExternalAttempt 单次尝试：事务内 锁定当前代 → 分支（新建/刷新/复发）。
func (s *PGStore) upsertExternalAttempt(ctx context.Context, base string, origin Origin, sourceRef, title, severity, createdBy, meta string) (*Incident, bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("incident pg: upsert begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // 提交后为 no-op

	var cur Incident
	var curGen int
	err = tx.QueryRow(ctx, `
SELECT `+pgIncidentCols+`, generation
FROM incident
WHERE tenant_id=$1 AND origin=$2 AND source_ref=$3
ORDER BY generation DESC LIMIT 1
FOR UPDATE`, s.tenantID, string(origin), sourceRef).Scan(
		&cur.ID, &cur.Title, &cur.Severity, &cur.State, &cur.CreatedAt, &cur.UpdatedAt,
		&nullTime{t: &cur.ResolvedAt}, &cur.Origin, &cur.SourceRef, &cur.SourceMeta,
		&cur.CreatedBy, &cur.MergedInto, &cur.AutoClosePolicy, &cur.AckBy, &curGen)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		inc, ierr := insertExternalGen(ctx, tx, s.tenantID, base, origin, sourceRef, 1, title, severity, createdBy, meta)
		if ierr != nil {
			return nil, false, ierr
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, false, fmt.Errorf("incident pg: upsert commit: %w", err)
		}
		return inc, true, nil
	case err != nil:
		return nil, false, fmt.Errorf("incident pg: upsert external lookup: %w", err)
	}

	if cur.State != StateResolved {
		// 未解决：就地刷新当前代（行已被 FOR UPDATE 锁定，无 TOCTOU）。
		var inc Incident
		err = scanIncident(tx.QueryRow(ctx, `
UPDATE incident SET title=$3, severity=$4, source_meta=$5::jsonb, updated_at=now()
WHERE tenant_id=$1 AND incident_id=$2
RETURNING `+pgIncidentCols,
			s.tenantID, cur.ID, title, severity, meta), &inc)
		if err != nil {
			return nil, false, fmt.Errorf("incident pg: upsert external refresh: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, false, fmt.Errorf("incident pg: upsert commit: %w", err)
		}
		// D6：刷新返回值回填 ClusterKeys，与 Get/List 一致（提交后读，
		// 不与本事务的行锁互踩）。
		tmp := []Incident{inc}
		s.fillClusters(ctx, tmp)
		return &tmp[0], false, nil
	}

	// 已解决：复发，新开一代（同事务内；撞唯一索引→回滚→上层重试）。
	inc, ierr := insertExternalGen(ctx, tx, s.tenantID, base, origin, sourceRef, curGen+1, title, severity, createdBy, meta)
	if ierr != nil {
		return nil, false, ierr
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, false, fmt.Errorf("incident pg: upsert commit: %w", err)
	}
	return inc, true, nil
}

// insertExternalGen 写入外部事件的第 gen 代（db 可为连接池或事务）。
// severity 已由 UpsertExternal 入口归一化（D2/D4），不再 NULLIF。
func insertExternalGen(ctx context.Context, db rowQuerier, tenantID, base string, origin Origin, sourceRef string, gen int, title, severity, createdBy, meta string) (*Incident, error) {
	var inc Incident
	id := externalIncidentID(base, gen)
	err := scanIncident(db.QueryRow(ctx, `
INSERT INTO incident (tenant_id, incident_id, title, severity, state, origin, source_ref, source_meta, created_by, auto_close_policy, generation)
VALUES ($1, $2, $3, $4, 'open', $5, $6, $7::jsonb, $8, 'auto', $9)
RETURNING `+pgIncidentCols,
		tenantID, id, title, severity, origin, sourceRef, meta, createdBy, gen), &inc)
	if err != nil {
		return nil, fmt.Errorf("incident pg: insert external gen %d: %w", gen, err)
	}
	return &inc, nil
}

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
