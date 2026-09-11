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

	"opscopilot/internal/config"
	"opscopilot/internal/incident"
)

// ingestTimeout 单条 SQL 超时。
const ingestTimeout = 5 * time.Second

// batchTimeoutFor 一批"领取→处理→落状态"的整体超时，与批量规模对齐。
//
// 第七轮 M-2：固定 30s 会在 DB 慢时让 Commit 失败整批回滚，而批内 store 写
// （UpsertExternal 等）走独立连接**已提交、不会回滚** → 下轮重放。幂等挡住了
// 重复建单，但重放会重复走 ExternalActive/applyRateLimit，极端下把重放计成
// 新建、提前折叠进 burst 单。单条最坏路径 ≈ ExternalActive + UpsertExternal
// （+ 恢复语 Transition）三次 5s 语句超时 ≈ 15s，故超时随批量线性放大。
// perItem（15s/条）与批量下限/封顶都收敛为 config 的默认值（#10 去魔法数字），
// 装配层经 OPS_INGEST_BATCH_TIMEOUT_PER_ITEM 可调。
//
// 注意：整批一个事务的前提是**单 worker**（多 worker 也安全——FOR UPDATE
// SKIP LOCKED 各领各的——但"行锁覆盖处理全程"的互斥语义只在单连接池内成立）。
// 多实例部署时需改为按条短事务（列入 M2）。
func batchTimeoutFor(batch int, perItem time.Duration) time.Duration {
	if batch <= 0 {
		batch = config.DefaultIngestBatch
	}
	if perItem <= 0 {
		perItem = config.DefaultIngestBatchPerItem
	}
	d := time.Duration(batch) * perItem
	if d < 30*time.Second {
		d = 30 * time.Second
	}
	if d > 10*time.Minute {
		d = 10 * time.Minute
	}
	return d
}

// maxIngestAttempts 单条消息的最大处理次数。达上限后不再被领取（死信），
// 否则一条永久坏消息会每轮占用批量名额、每轮失败、刷爆日志。
const maxIngestAttempts = 5

// PGIngestQueue 基于 ingest_queue 表的导入队列。
type PGIngestQueue struct {
	pool   *pgxpool.Pool
	tenant string
	// batchPerItem 批超时随批量线性放大的单条系数（config.Ingest.BatchTimeoutPerItem）。
	batchPerItem time.Duration
}

// NewPGIngestQueue 构造。batchPerItem<=0 时回退 config 默认（15s/条）。
func NewPGIngestQueue(pool *pgxpool.Pool, tenant string, batchPerItem time.Duration) *PGIngestQueue {
	if batchPerItem <= 0 {
		batchPerItem = config.DefaultIngestBatchPerItem
	}
	return &PGIngestQueue{pool: pool, tenant: tenant, batchPerItem: batchPerItem}
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

// processBatch 在一个**事务内**完成"领取 → 逐条处理 → 落状态"。
//
// 为什么必须是一个事务：`FOR UPDATE SKIP LOCKED` 的行锁只在事务存续期间有效。
// 此前用 pool.Query 领取，隐式事务在 rows 读完（Close）时就已提交，行锁随即
// 释放 → 另一个 worker 能重复领到同一批消息，注释宣称的"多 worker 安全"
// 并不成立。把 claim 与 done 放进同一事务后，行锁覆盖到处理结束。
//
// 失败语义：单条失败只累加 attempts（事务照常提交），不因一条坏消息回滚整批；
// 达 maxIngestAttempts 后该行不再被领取（死信，attempts/last_error 留存可查）。
func (q *PGIngestQueue) processBatch(limit int, process func(Item) error) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), batchTimeoutFor(limit, q.batchPerItem))
	defer cancel()
	tx, err := q.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("ingest: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // 提交后为 no-op

	rows, err := tx.Query(ctx, `
SELECT id, origin, source_ref, payload, attempts FROM ingest_queue
WHERE processed_at IS NULL AND attempts < $3 AND tenant_id=$1
ORDER BY received_at FOR UPDATE SKIP LOCKED LIMIT $2`,
		q.tenant, limit, maxIngestAttempts)
	if err != nil {
		return 0, fmt.Errorf("ingest: claim: %w", err)
	}
	var items []Item
	for rows.Next() {
		var it Item
		if err := rows.Scan(&it.ID, &it.Origin, &it.SourceRef, &it.Payload, &it.Attempts); err != nil {
			rows.Close()
			return 0, fmt.Errorf("ingest: scan: %w", err)
		}
		items = append(items, it)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("ingest: rows: %w", err)
	}

	for _, it := range items {
		if perr := process(it); perr != nil {
			if _, err := tx.Exec(ctx, `
UPDATE ingest_queue SET attempts=attempts+1, last_error=$2 WHERE id=$1`,
				it.ID, perr.Error()); err != nil {
				return 0, fmt.Errorf("ingest: record failure: %w", err)
			}
			continue
		}
		if _, err := tx.Exec(ctx,
			`UPDATE ingest_queue SET processed_at=now() WHERE id=$1`, it.ID); err != nil {
			return 0, fmt.Errorf("ingest: done: %w", err)
		}
	}
	return len(items), tx.Commit(ctx)
}

// Pending 待处理条数（运维观察与测试断言）。达死信上限的行不计入。
func (q *PGIngestQueue) Pending() (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), ingestTimeout)
	defer cancel()
	var n int
	if err := q.pool.QueryRow(ctx, `
SELECT count(*) FROM ingest_queue
WHERE processed_at IS NULL AND attempts < $2 AND tenant_id=$1`,
		q.tenant, maxIngestAttempts).Scan(&n); err != nil {
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
	// rateLimit 建单限流（R1 建单风暴应对）：窗口内**新建**上限；超限不新建，
	// 改更新"群体事件"聚合单（source_ref=burst:<窗口起点>），事后可回放。
	// 只统计真正新建的单：去重刷新不占额度（否则重复推送的老告警会吃光额度，
	// 把真正的新告警误折叠进聚合单）。
	rateLimit  int
	rateWindow time.Duration
	curBucket  int64 // 当前窗口桶号；-1 = 尚未开始
	created    int   // 当前窗口内已新建单数（只保留当前窗口，不随窗口数增长）
	mu         sync.Mutex
	logf       func(string, ...any)
}

// NewIngestWorker 构造。autoCreate=false 时 Run 立即返回（不消费）。
// interval/batch/rateLimit/rateWindow 传零值时回退 config 默认（与
// OPS_INGEST_* 缺省值同源，#10 单一定义）。
func NewIngestWorker(q *PGIngestQueue, s incident.Store, audit AuditLog, interval time.Duration, batch int, autoCreate bool, rateLimit int, rateWindow time.Duration, logf func(string, ...any)) *IngestWorker {
	if interval <= 0 {
		interval = config.DefaultIngestInterval
	}
	if batch <= 0 {
		batch = config.DefaultIngestBatch
	}
	if rateLimit <= 0 {
		// 默认：5 分钟内最多建 50 单，其余进聚合单（R1 建单风暴）。
		rateLimit = config.DefaultIngestRateLimit
	}
	if rateWindow <= 0 {
		rateWindow = config.DefaultIngestRateWindow
	}
	if logf == nil { // nil logger 容忍（测试/嵌入式场景不再空指针）
		logf = func(string, ...any) {}
	}
	return &IngestWorker{queue: q, store: s, audit: audit, interval: interval, batch: batch,
		autoCreate: autoCreate, rateLimit: rateLimit, rateWindow: rateWindow,
		curBucket: -1, logf: logf}
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

// drain 处理一批：领取与落状态在同一事务内（行锁覆盖处理全程）。
// 单条失败只累加 attempts 由下轮重试，不阻塞其余；单条 panic 被隔离为本条失败，
// 不能让一个坏输入把整个消费循环带走（此前 process panic 会让 goroutine 静默
// 死亡、队列永久停摆）。
func (w *IngestWorker) drain() {
	n, err := w.queue.processBatch(w.batch, func(it Item) error {
		perr := w.processSafely(it)
		if perr != nil {
			if it.Attempts+1 >= maxIngestAttempts {
				// 死信可观测：达上限后该行不再被领取（attempts/last_error 留存）。
				w.logf("ERROR: ingest item %d dead-lettered after %d attempts (origin=%s ref=%s): %v",
					it.ID, it.Attempts+1, it.Origin, it.SourceRef, perr)
				w.auditAppend(AuditEntry{IncidentID: string(it.Origin) + ":" + it.SourceRef,
					Action: AuditIngestFailed, Actor: "system:" + string(it.Origin),
					Detail: map[string]any{"queue_id": it.ID, "attempts": it.Attempts + 1, "error": perr.Error()}})
			} else {
				w.logf("WARNING: ingest item %d failed (attempt %d/%d): %v",
					it.ID, it.Attempts+1, maxIngestAttempts, perr)
			}
		}
		return perr
	})
	if err != nil {
		w.logf("WARNING: ingest batch: %v", err)
		return
	}
	if n > 0 {
		w.logf("ingest: processed %d message(s)", n)
	}
}

// sanitizeLog 去除控制字符，防日志/审计注入（第七轮 L3）：
// SourceRef 来自外部告警载荷，内容不可控，含 \n/\r 可伪造日志行。
func sanitizeLog(s string) string {
	return strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return '?'
		}
		return r
	}, s)
}

// processSafely 执行 process，把 panic 转换为本条失败（隔离坏输入）。
func (w *IngestWorker) processSafely(it Item) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic while processing: %v", r)
		}
	}()
	return w.process(it)
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

	// 先判"这会新建一单，还是刷新既有单"：刷新不占建单额度，避免重复推送的
	// 老告警吃光额度、把新告警误折叠。M9 之后同一 (origin, source_ref) 会按代
	// 演进（复发改建），所以判据是"有没有**未解决**的单"而不是"有没有单"——
	// 已解决的旧单不代表没有，复发会新开一代。
	active, aerr := w.store.ExternalActive(origin, strings.TrimSpace(it.SourceRef))
	willCreate := aerr != nil || !active
	if aerr != nil {
		// 查询失败按"会新建"处理：宁可多计额度，也不让风暴防护失效。
		w.logf("WARNING: ingest external-active check (%s): %v — assume new", it.SourceRef, aerr)
	}

	// R1 建单风暴：窗口内新建超限 → 不新建，更新聚合单（保留可回放）。
	ref, burst := w.applyRateLimit(it.SourceRef, willCreate)
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

// applyRateLimit 建单限流（R1）：窗口内**新建**数达上限后，后续消息折叠进
// "群体事件"聚合单（source_ref=burst:<窗口起点>）——不丢消息、可回放，
// 也不让看板被上千单淹没。
//
// count=false 表示本条只是刷新既有事件（去重命中），**不占额度**：此前把刷新
// 也算进去，重复推送的老告警会把额度吃光，让真正的新告警被误折叠。
// 桶只保留当前窗口（旧窗口直接丢弃），不随运行时长增长。
func (w *IngestWorker) applyRateLimit(sourceRef string, count bool) (string, bool) {
	// rateWindow < 1s 时整除为 0（除零 panic），视为不限流。
	if w.rateLimit <= 0 || w.rateWindow < time.Second {
		return sourceRef, false
	}
	bucket := time.Now().Unix() / int64(w.rateWindow/time.Second)
	w.mu.Lock()
	defer w.mu.Unlock()
	if bucket != w.curBucket { // 窗口推进：重置计数
		w.curBucket, w.created = bucket, 0
	}
	if !count {
		return sourceRef, false
	}
	w.created++
	if w.created <= w.rateLimit {
		return sourceRef, false
	}
	return "burst:" + strconv.FormatInt(bucket, 10), true
}

// auditAppend 审计写入（审计未接线时静默跳过，不阻断业务）。
func (w *IngestWorker) auditAppend(e AuditEntry) {
	if w.audit != nil {
		w.audit.Append(e)
	}
}
