// W9 双链路二期：事件审计（人工操作与外部自动动作统一留痕）。
//
// 双链路的信任基础：一条链路由人操作、另一条由系统自动动作，
// 没有审计就无法回答"这一单为什么被关了 / 谁合的 / 限流丢了什么"。
package main

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// AuditAction 审计动作（对齐 incident_audit.action CHECK）。
type AuditAction string

const (
	AuditCreate                  AuditAction = "create"
	AuditTransition              AuditAction = "transition"
	AuditAttachCluster           AuditAction = "attach_cluster"
	AuditMerge                   AuditAction = "merge"
	AuditExternalRecoveryIgnored AuditAction = "external_recovery_ignored"
	AuditRateLimited             AuditAction = "rate_limited"
	AuditIngestFailed            AuditAction = "ingest_failed"
)

// AuditEntry 一条审计记录。
type AuditEntry struct {
	IncidentID string
	Action     AuditAction
	Actor      string
	Detail     map[string]any
	OccurredAt time.Time
}

// AuditLog 审计写入口（内存/PG 双实现）。
type AuditLog interface {
	Append(e AuditEntry)
	List(incidentID string) []AuditEntry
}

// MemAuditLog 内存审计（无 DB 场景；进程重启即丢，与 MemStore 同口径）。
type MemAuditLog struct {
	mu      sync.Mutex
	entries []AuditEntry
}

// NewMemAuditLog 构造。
func NewMemAuditLog() *MemAuditLog { return &MemAuditLog{} }

// Append 追加。
func (l *MemAuditLog) Append(e AuditEntry) {
	l.mu.Lock()
	defer l.mu.Unlock()
	e.OccurredAt = time.Now()
	l.entries = append(l.entries, e)
}

// List 按事件过滤（倒序）。
func (l *MemAuditLog) List(incidentID string) []AuditEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := []AuditEntry{}
	for i := len(l.entries) - 1; i >= 0; i-- {
		if incidentID == "" || l.entries[i].IncidentID == incidentID {
			out = append(out, l.entries[i])
		}
	}
	return out
}

// PGAuditLog TimescaleDB 审计（持久化）。写入失败只记日志——审计失败
// 不该阻断业务动作（与降噪落库同一取舍）。
type PGAuditLog struct {
	pool   *pgxpool.Pool
	tenant string
	logf   func(string, ...any)
}

// NewPGAuditLog 构造。
func NewPGAuditLog(pool *pgxpool.Pool, tenant string, logf func(string, ...any)) *PGAuditLog {
	return &PGAuditLog{pool: pool, tenant: tenant, logf: logf}
}

// Append 写入审计行。
func (l *PGAuditLog) Append(e AuditEntry) {
	detail, err := json.Marshal(e.Detail)
	if err != nil {
		detail = []byte("{}")
	}
	ctx, cancel := context.WithTimeout(context.Background(), ingestTimeout)
	defer cancel()
	if _, err := l.pool.Exec(ctx, `
INSERT INTO incident_audit (tenant_id, incident_id, action, actor, detail)
VALUES ($1, $2, $3, $4, $5::jsonb)`,
		l.tenant, e.IncidentID, e.Action, e.Actor, string(detail)); err != nil && l.logf != nil {
		l.logf("WARNING: audit append failed (%s/%s): %v", e.IncidentID, e.Action, err)
	}
}

// List 按事件过滤（倒序，最多 200 条）。
func (l *PGAuditLog) List(incidentID string) []AuditEntry {
	ctx, cancel := context.WithTimeout(context.Background(), ingestTimeout)
	defer cancel()
	rows, err := l.pool.Query(ctx, `
SELECT incident_id, action, actor, detail, occurred_at FROM incident_audit
WHERE tenant_id=$1 AND ($2='' OR incident_id=$2)
ORDER BY occurred_at DESC LIMIT 200`, l.tenant, incidentID)
	if err != nil {
		return nil
	}
	defer rows.Close()
	out := []AuditEntry{}
	for rows.Next() {
		var e AuditEntry
		var detail string
		if err := rows.Scan(&e.IncidentID, &e.Action, &e.Actor, &detail, &e.OccurredAt); err != nil {
			return out
		}
		_ = json.Unmarshal([]byte(detail), &e.Detail)
		out = append(out, e)
	}
	return out
}
