// W9 双链路一期：链路 A 的异步通道——导入队列 + 消费 worker。
//
// 队列落 DB（ingest_queue）：接收即落盘、可积压、可重放、可审计。
// worker 单 goroutine 轮询（SKIP LOCKED），与人工建单的同步 REST 路径
// 物理分离——外部源再突发也不占人工路径的连接与事务（互不阻塞保证①/②）。
//
// 自动建单开关（决策 2 / R8）：OPS_INCIDENT_AUTOCREATE=off（默认，影子期）
// 时 worker **不消费**，只让消息堆积——转正后开启即可回放历史消息建单。
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"opscopilot/internal/incident"
)

// ingestTimeout 单条处理超时（与 PGStore 同口径，5s）。
const ingestTimeout = 5 * time.Second

// PGIngestQueue 基于 ingest_queue 表的导入队列。
type PGIngestQueue struct {
	pool   *pgxpool.Pool
	tenant string
}

// NewPGIngestQueue 构造。
func NewPGIngestQueue(pool *pgxpool.Pool, tenant string) *PGIngestQueue {
	return &PGIngestQueue{pool: pool, tenant: tenant}
}

// Enqueue 入队：待处理态 (tenant, origin, source_ref) 唯一索引兜底，
// 重复推送返回 queued=false（不堆积、不报错）。
func (q *PGIngestQueue) Enqueue(origin incident.Origin, sourceRef, payload string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), ingestTimeout)
	defer cancel()
	res, err := q.pool.Exec(ctx, `
INSERT INTO ingest_queue (tenant_id, origin, source_ref, payload)
VALUES ($1, $2, $3, $4::jsonb)
ON CONFLICT (tenant_id, origin, source_ref) WHERE source_ref <> '' AND processed_at IS NULL
DO NOTHING`, q.tenant, origin, sourceRef, payload)
	if err != nil {
		return false, fmt.Errorf("ingest: enqueue: %w", err)
	}
	return res.RowsAffected() > 0, nil
}

// Item 待处理消息。
type Item struct {
	ID        int64
	Origin    incident.Origin
	SourceRef string
	Payload   string
	Attempts  int
}

// Claim 取一批待处理消息（FOR UPDATE SKIP LOCKED：多 worker 安全）。
func (q *PGIngestQueue) Claim(limit int) ([]Item, error) {
	ctx, cancel := context.WithTimeout(context.Background(), ingestTimeout)
	defer cancel()
	rows, err := q.pool.Query(ctx, `
SELECT id, origin, source_ref, payload, attempts FROM ingest_queue
WHERE processed_at IS NULL AND tenant_id=$1
ORDER BY received_at FOR UPDATE SKIP LOCKED LIMIT $2`, q.tenant, limit)
	if err != nil {
		return nil, fmt.Errorf("ingest: claim: %w", err)
	}
	defer rows.Close()
	var out []Item
	for rows.Next() {
		var it Item
		if err := rows.Scan(&it.ID, &it.Origin, &it.SourceRef, &it.Payload, &it.Attempts); err != nil {
			return out, fmt.Errorf("ingest: scan: %w", err)
		}
		out = append(out, it)
	}
	return out, rows.Err()
}

// Done 处理完成（成功置 processed_at；失败累加 attempts 与错误）。
func (q *PGIngestQueue) Done(id int64, err error) error {
	ctx, cancel := context.WithTimeout(context.Background(), ingestTimeout)
	defer cancel()
	if err == nil {
		_, e := q.pool.Exec(ctx, `UPDATE ingest_queue SET processed_at=now() WHERE id=$1`, id)
		return e
	}
	_, e := q.pool.Exec(ctx, `
UPDATE ingest_queue SET attempts=attempts+1, last_error=$2 WHERE id=$1`, id, err.Error())
	return e
}

// Pending 待处理条数（运维观察与测试断言）。
func (q *PGIngestQueue) Pending() (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), ingestTimeout)
	defer cancel()
	var n int
	if err := q.pool.QueryRow(ctx, `
SELECT count(*) FROM ingest_queue WHERE processed_at IS NULL AND tenant_id=$1`,
		q.tenant).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

// IngestWorker 队列消费者：把外部消息落成事件（UpsertExternal 幂等）。
type IngestWorker struct {
	queue      *PGIngestQueue
	store      incident.Store
	audit      AuditLog
	interval   time.Duration
	batch      int
	autoCreate bool // 开关：false = 只入队不建单（影子期默认）
	// rateLimit 建单限流（R1 建单风暴应对）：窗口内新建上限；超限不新建，
	// 改更新"群体事件"聚合单（source_ref=burst:<窗口起点>），事后可回放。
	rateLimit  int
	rateWindow time.Duration
	created    map[int64]int // 窗口起点(unix) → 已建单数
	mu         sync.Mutex
	logf       func(string, ...any)
}

// NewIngestWorker 构造。autoCreate=false 时 Run 立即返回（不消费）。
func NewIngestWorker(q *PGIngestQueue, s incident.Store, audit AuditLog, interval time.Duration, batch int, autoCreate bool, rateLimit int, rateWindow time.Duration, logf func(string, ...any)) *IngestWorker {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	if batch <= 0 {
		batch = 20
	}
	if rateLimit <= 0 {
		rateLimit = 50 // 默认：5 分钟内最多建 50 单，其余进聚合单
	}
	if rateWindow <= 0 {
		rateWindow = 5 * time.Minute
	}
	if logf == nil { // nil logger 容忍（测试/嵌入式场景不再空指针）
		logf = func(string, ...any) {}
	}
	return &IngestWorker{queue: q, store: s, audit: audit, interval: interval, batch: batch,
		autoCreate: autoCreate, rateLimit: rateLimit, rateWindow: rateWindow,
		created: map[int64]int{}, logf: logf}
}

// Run 轮询消费，ctx 取消或开关关闭即退出（优雅停机）。
func (w *IngestWorker) Run(ctx context.Context) {
	if !w.autoCreate {
		w.logf("ingest worker: auto-create OFF (OPS_INCIDENT_AUTOCREATE=off) — messages queue up, no incidents created")
		return
	}
	w.logf("ingest worker: started (interval %v, batch %d)", w.interval, w.batch)
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			w.logf("ingest worker: stopped")
			return
		case <-ticker.C:
			w.drain()
		}
	}
}

// drain 处理一批（单条失败记 attempts 由下轮重试，不阻塞其余）。
func (w *IngestWorker) drain() {
	items, err := w.queue.Claim(w.batch)
	if err != nil {
		w.logf("WARNING: ingest claim: %v", err)
		return
	}
	for _, it := range items {
		perr := w.process(it)
		if perr != nil {
			w.logf("WARNING: ingest item %d failed: %v", it.ID, perr)
		}
		if err := w.queue.Done(it.ID, perr); err != nil {
			w.logf("WARNING: ingest done: %v", err)
		}
	}
}

// process 单条：按 origin 解析载荷 → 幂等建/更新事件。
// push（alertmanager/webhook）与 pull（prometheus，见 pull_alerts.go）载荷
// 同形态（amAlert），走同一解析路径——两条进入方式在 worker 处不分叉。
func (w *IngestWorker) process(it Item) error {
	switch it.Origin {
	case incident.OriginAlertmanager, incident.OriginWebhook, incident.OriginPrometheus:
		return w.processAlertmanager(it)
	default:
		return fmt.Errorf("unsupported origin %q", it.Origin)
	}
}

// processAlertmanager 解析 AM 告警 → UpsertExternal。
// 恢复语（R2）：若告警已恢复且事件被人工接手（auto_close_policy=
// manual_only），**不自动关单**，只把恢复事实写回 source_meta 待人确认。
func (w *IngestWorker) processAlertmanager(it Item) error {
	var a amAlert
	if err := json.Unmarshal([]byte(it.Payload), &a); err != nil {
		return fmt.Errorf("decode payload: %w", err)
	}
	title := amTitle(a)
	sev := amSeverity(a)
	origin := it.Origin
	actor := "system:" + string(origin)

	// R1 建单风暴：窗口内超限 → 不新建，更新聚合单（保留可回放）。
	ref, burst := w.applyRateLimit(it.SourceRef)
	created, isNew, err := w.store.UpsertExternal(origin, ref, title, sev, actor, it.Payload)
	if err != nil {
		return err
	}
	if burst && isNew {
		w.auditAppend(AuditEntry{IncidentID: created.ID, Action: AuditRateLimited,
			Actor: actor, Detail: map[string]any{"source_ref": it.SourceRef, "bucket": ref}})
		w.logf("rate limited: %s folded into burst incident %s", it.SourceRef, created.ID)
	}
	if isNew && !burst {
		w.auditAppend(AuditEntry{IncidentID: created.ID, Action: AuditCreate,
			Actor: actor, Detail: map[string]any{"origin": origin, "source_ref": it.SourceRef, "title": title}})
		w.logf("incident created from %s: %s (%s)", origin, created.ID, title)
	}
	if !amResolved(a) {
		return nil
	}
	// 已恢复：受 auto_close_policy 约束。
	if strings.TrimSpace(created.AutoClosePolicy) == "manual_only" {
		w.auditAppend(AuditEntry{IncidentID: created.ID, Action: AuditExternalRecoveryIgnored,
			Actor: actor, Detail: map[string]any{"source_ref": it.SourceRef}})
		w.logf("external recovery ignored (manual_only): %s — awaiting human confirm", created.ID)
		return nil
	}
	if _, err := w.store.Transition(created.ID, incident.StateResolved, actor); err != nil &&
		!errors.Is(err, incident.ErrNotFound) && !errors.Is(err, pgx.ErrNoRows) {
		var invalid incident.ErrInvalidTransition
		if !errors.As(err, &invalid) { // 已 resolved 不是错误（重复恢复通知）
			return fmt.Errorf("auto resolve %s: %w", created.ID, err)
		}
	}
	return nil
}

// applyRateLimit 建单限流（R1）：窗口内新建数达上限后，后续消息折叠进
// "群体事件"聚合单（source_ref=burst:<窗口起点>）——不丢消息、可回放，
// 也不让看板被上千单淹没。返回实际使用的 source_ref 与是否命中限流。
func (w *IngestWorker) applyRateLimit(sourceRef string) (string, bool) {
	if w.rateLimit <= 0 {
		return sourceRef, false
	}
	bucket := time.Now().Unix() / int64(w.rateWindow.Seconds())
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.created[bucket] < w.rateLimit {
		w.created[bucket]++
		return sourceRef, false
	}
	w.created[bucket]++
	return "burst:" + strconv.FormatInt(bucket, 10), true
}

// auditAppend 审计写入（审计未接线时静默跳过，不阻断业务）。
func (w *IngestWorker) auditAppend(e AuditEntry) {
	if w.audit != nil {
		w.audit.Append(e)
	}
}
