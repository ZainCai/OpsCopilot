// W9 双链路二期：事件审计（人工操作与外部自动动作统一留痕）。
//
// 双链路的信任基础：一条链路由人操作、另一条由系统自动动作，
// 没有审计就无法回答"这一单为什么被关了 / 谁合的 / 限流丢了什么"。
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"opscopilot/pkg/memguard"
)

// ErrAuditUnavailable 审计查询失败的类型化根因（第八轮审核 D1）。
//
// 为什么必须类型化：调用方要区分"审计后端故障"与"该事件没有审计记录"——
// 前者必须回 5xx（D5 决策 A：DB 故障如实暴露），后者是正常的空列表。
// 此前 PGAuditLog.List 查库失败直接 `return nil`，把**故障静默降级成"没有
// 记录"**：排障时会得出"这一步没人操作过"这种完全相反的结论，且 200 空响应
// 掩盖了后端已经不可用的事实。
//
// 错误里保留底层原因（%w/%v）供日志排查，客户端只看到脱敏后的 general 500。
var ErrAuditUnavailable = errors.New("audit: backend unavailable")

// AuditAction 审计动作（对齐 incident_audit.action CHECK）。
type AuditAction string

const (
	AuditCreate     AuditAction = "create"
	AuditTransition AuditAction = "transition"
	// AuditAttachCluster 预留：生产代码尚无调用 AttachCluster 的路径
	// （簇→事件关联是 F-02 的预留读侧/写侧），故本动作当前不会被写入。
	// 保留它与 incident_audit.action 的 CHECK 约束对齐，接线时无需迁移。
	AuditAttachCluster           AuditAction = "attach_cluster"
	AuditMerge                   AuditAction = "merge"
	AuditExternalRecoveryIgnored AuditAction = "external_recovery_ignored"
	AuditRateLimited             AuditAction = "rate_limited"
	AuditIngestFailed            AuditAction = "ingest_failed"
)

// AuditEntry 一条审计记录（json tag 与 Incident 同口径：REST 契约不暴露
// Go 字段名）。
type AuditEntry struct {
	IncidentID string         `json:"incident_id"`
	Action     AuditAction    `json:"action"`
	Actor      string         `json:"actor"`
	Detail     map[string]any `json:"detail"`
	OccurredAt time.Time      `json:"occurred_at"`
}

// AuditLog 审计写入口（内存/PG 双实现）。
//
// Append 刻意不返回错误（审计失败不阻断业务动作，见 PGAuditLog 注释）；
// List 则**必须**返回错误——读路径要把"后端不可用"与"没有记录"分开，
// 否则故障会被静默当成空结果（D1）。
type AuditLog interface {
	Append(e AuditEntry)
	List(incidentID string) ([]AuditEntry, error)
}

// MemAuditLog 内存审计（无 DB 场景；进程重启即丢，与 MemStore 同口径）。
//
// 内存有界化（优化方案 #6）：entries 只 append 不回收，DB 缺席时是无界
// slice。装护栏后**超限丢最旧**（追加序即活跃度序，队首 = 最久未活跃），
// 默认 5 万条上限下正常规模永不触发。
type MemAuditLog struct {
	mu      sync.Mutex
	entries []AuditEntry
	guard   *memguard.Guard
}

// NewMemAuditLog 构造（无容量上限，行为与历史一致）。
func NewMemAuditLog() *MemAuditLog { return NewMemAuditLogWithLimits(nil) }

// NewMemAuditLogWithLimits 构造并装配容量护栏（nil = 不设限）。
func NewMemAuditLogWithLimits(guard *memguard.Guard) *MemAuditLog {
	l := &MemAuditLog{guard: guard}
	guard.SetSize(l.Size)
	return l
}

// MemGuards 返回装配的护栏（供装配层注册指标）。
func (l *MemAuditLog) MemGuards() []*memguard.Guard {
	if l.guard == nil {
		return nil
	}
	return []*memguard.Guard{l.guard}
}

// Size 当前条数（锁内读；gauge 回调）。
func (l *MemAuditLog) Size() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.entries)
}

// Append 追加。
func (l *MemAuditLog) Append(e AuditEntry) {
	l.mu.Lock()
	defer l.mu.Unlock()
	e.OccurredAt = time.Now()
	l.entries = append(l.entries, e)
	if excess := l.guard.Over(len(l.entries)); excess > 0 {
		if excess > len(l.entries) {
			excess = len(l.entries)
		}
		l.entries = append([]AuditEntry(nil), l.entries[excess:]...)
		l.guard.Evicted(excess)
	}
}

// List 按事件过滤（倒序）。
//
// 返回**深拷贝**：AuditEntry.Detail 是 map（引用类型），浅拷贝会让调用方的
// 改动写回库内状态，也与并发读构成竞争。
//
// 内存实现不会失败，error 恒为 nil（接口统一形态）。
func (l *MemAuditLog) List(incidentID string) ([]AuditEntry, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := []AuditEntry{}
	for i := len(l.entries) - 1; i >= 0; i-- {
		if incidentID == "" || l.entries[i].IncidentID == incidentID {
			out = append(out, cloneAuditEntry(l.entries[i]))
		}
	}
	return out, nil
}

// cloneAuditEntry 深拷贝一条审计（只 Detail 是引用类型）。
func cloneAuditEntry(e AuditEntry) AuditEntry {
	if e.Detail == nil {
		return e
	}
	d := make(map[string]any, len(e.Detail))
	for k, v := range e.Detail {
		d[k] = v
	}
	e.Detail = d
	return e
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
//
// 失败一律返回以 ErrAuditUnavailable 为根因的错误（D1）——**不返回半截列表**：
// 部分数据 + 错误会让调用方在两难中做选择，而"审计轨迹缺几行"在排障场景里
// 与"没有这几行"无法区分，宁可整体失败让上层回 5xx。
func (l *PGAuditLog) List(incidentID string) ([]AuditEntry, error) {
	if l == nil || l.pool == nil {
		return nil, fmt.Errorf("%w: no database pool", ErrAuditUnavailable)
	}
	ctx, cancel := context.WithTimeout(context.Background(), ingestTimeout)
	defer cancel()
	rows, err := l.pool.Query(ctx, `
SELECT incident_id, action, actor, detail, occurred_at FROM incident_audit
WHERE tenant_id=$1 AND ($2='' OR incident_id=$2)
ORDER BY occurred_at DESC LIMIT 200`, l.tenant, incidentID)
	if err != nil {
		return nil, fmt.Errorf("%w: query: %v", ErrAuditUnavailable, err)
	}
	defer rows.Close()
	out := []AuditEntry{}
	for rows.Next() {
		var e AuditEntry
		var detail string
		if err := rows.Scan(&e.IncidentID, &e.Action, &e.Actor, &detail, &e.OccurredAt); err != nil {
			return nil, fmt.Errorf("%w: scan: %v", ErrAuditUnavailable, err)
		}
		_ = json.Unmarshal([]byte(detail), &e.Detail)
		out = append(out, e)
	}
	// 迭代错误此前也从未检查（rows.Err()）——与吞错是同一类问题：
	// 中途断连时 rows.Next() 返回 false，循环正常结束，返回一个"看起来
	// 完整"的短列表。一并修。
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: iterate: %v", ErrAuditUnavailable, err)
	}
	return out, nil
}
