// W9 双链路二期：事件审计（人工操作与外部自动动作统一留痕）。
//
// 双链路的信任基础：一条链路由人操作、另一条由系统自动动作，
// 没有审计就无法回答"这一单为什么被关了 / 谁合的 / 限流丢了什么"。
package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"opscopilot/internal/incident"
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
	// AuditAttachCluster W10-6 起是活代码：OPS_AUTOATTACH 的 enforce 判决
	// new-incident 联动挂簇成功时落一条（actor=system:autoattach，detail 带
	// cluster_key/fingerprint/source_ref，见 noise_process.go
	// autoAttachClusters）。共簇冲突（一簇一事件）只计指标不落本动作——
	// 挂簇动作没发生就不写审计。与 incident_audit.action 的 CHECK 对齐。
	AuditAttachCluster           AuditAction = "attach_cluster"
	AuditMerge                   AuditAction = "merge"
	AuditExternalRecoveryIgnored AuditAction = "external_recovery_ignored"
	AuditRateLimited             AuditAction = "rate_limited"
	AuditIngestFailed            AuditAction = "ingest_failed"
	// AuditRCA 按需根因分析留痕（#12/ADR-014）：谁在什么时候对哪一单跑了
	// 一次 RCA、证据链体检结果进 Detail。incident_audit.action 的 CHECK
	// 由 migration 000017 扩入本值。
	AuditRCA AuditAction = "rca"
)

// AuditEntry 一条审计记录（json tag 与 Incident 同口径：REST 契约不暴露
// Go 字段名）。
type AuditEntry struct {
	IncidentID string         `json:"incident_id"`
	Action     AuditAction    `json:"action"`
	Actor      string         `json:"actor"`
	Detail     map[string]any `json:"detail"`
	OccurredAt time.Time      `json:"occurred_at"`

	// seq 全局排序的 id 侧 tiebreaker（ListPage 游标键 (occurred_at DESC, seq
	// DESC)）：PG 实现填 incident_audit.id（BIGSERIAL），内存实现填追加序号。
	// 不导出、不进 JSON——REST 契约里没有它；调用方构造的条目 seq 无意义，
	// 只有 Append/List/ListPage 的实现侧负责赋值。
	seq int64
}

// AuditQuery 全局审计分页查询（GET /api/v1/audit 的服务端形态）。
// 语义（Mem/PG 双实现逐字一致，契约测试锁死）：
//   - Actor/Action 空串 = 不过滤；Action 必须落在封闭集合内（未知值返回
//     ErrBadAuditAction，REST 映射 400——本期不扩 CHECK，动作集合不变）；
//   - Since 含（occurred_at >= since）、Until 不含（occurred_at < until），
//     半开区间 [Since, Until)；零值 = 该侧无界；
//   - Cursor 为上一页 next_cursor（空 = 第一页）；坏游标 ErrBadAuditCursor；
//   - Limit <=0 取默认、超上限截断（与 incident 分页同源常量）。
type AuditQuery struct {
	Actor  string
	Action string
	Since  time.Time
	Until  time.Time
	Cursor string
	Limit  int
}

// AuditPage 分页结果。NextCursor 为空 = 已到末尾（与 incident.Page 同形态）。
type AuditPage struct {
	Items      []AuditEntry `json:"entries"`
	NextCursor string       `json:"next_cursor"`
}

// 游标与参数校验的错误（REST 层用 errors.Is 映射 400，文案不外泄细节）。
var (
	ErrBadAuditAction = errors.New("audit: unknown action filter")
	ErrBadAuditCursor = errors.New("audit: bad cursor")
)

// validAuditAction 动作过滤值的封闭集合校验（与 incident_audit.action CHECK
// 及上方 AuditAction 常量同源；口径"本期不扩 action CHECK"）。
func validAuditAction(s string) bool {
	switch AuditAction(s) {
	case AuditCreate, AuditTransition, AuditAttachCluster, AuditMerge,
		AuditExternalRecoveryIgnored, AuditRateLimited, AuditIngestFailed, AuditRCA:
		return true
	}
	return false
}

// normalizeAuditQuery 校验/归一化（两实现共用，语义必须一致——对齐
// incident.normalizePageQuery 范式）。
func normalizeAuditQuery(q *AuditQuery) error {
	if q.Action != "" && !validAuditAction(q.Action) {
		return fmt.Errorf("%w: %q", ErrBadAuditAction, q.Action)
	}
	return nil
}

// auditPageLimit 归一化 limit：默认/上限直接取 incident 分页同源常量
// （#10 去魔法数字——200/1000 一处定义，两个域不漂移）。
func auditPageLimit(n int) int {
	if n <= 0 {
		return incident.PageLimitDefault
	}
	if n > incident.PageLimitMax {
		return incident.PageLimitMax
	}
	return n
}

// encodeAuditCursor / decodeAuditCursor keyset 游标：
// base64url( RFC3339Nano(occurred_at) \x1f seq )，与 incident 包同款编码。
//
// 游标方案取舍（与 incident ListPage 对齐选 keyset，注释理由）：
//   - base64 offset 翻页在高频写入表上"翻页期间来了新行"会整页错位重/漏，
//     审计恰是全局最新优先的写面——运维第一屏就是"刚刚发生了什么"；
//   - keyset (occurred_at, id) 行值比较一次 seek 定位，深翻页成本恒定，
//     且与 000021 的两条索引逐列对齐；
//   - 代价是排序键必须决定性（id 唯一收尾）——audit 行有 BIGSERIAL/追加序，
//     满足。
func encodeAuditCursor(at time.Time, seq int64) string {
	return base64.RawURLEncoding.EncodeToString(
		[]byte(at.Format(time.RFC3339Nano) + "\x1f" + strconv.FormatInt(seq, 10)))
}

func decodeAuditCursor(s string) (time.Time, int64, error) {
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return time.Time{}, 0, fmt.Errorf("%w: %v", ErrBadAuditCursor, err)
	}
	i := strings.IndexByte(string(raw), 0x1f)
	if i < 0 {
		return time.Time{}, 0, ErrBadAuditCursor
	}
	at, err := time.Parse(time.RFC3339Nano, string(raw[:i]))
	if err != nil {
		return time.Time{}, 0, fmt.Errorf("%w: %v", ErrBadAuditCursor, err)
	}
	seq, err := strconv.ParseInt(string(raw[i+1:]), 10, 64)
	if err != nil {
		return time.Time{}, 0, fmt.Errorf("%w: %v", ErrBadAuditCursor, err)
	}
	return at, seq, nil
}

// AuditLog 审计写入口（内存/PG 双实现）。
//
// Append 刻意不返回错误（审计失败不阻断业务动作，见 PGAuditLog 注释）；
// List 则**必须**返回错误——读路径要把"后端不可用"与"没有记录"分开，
// 否则故障会被静默当成空结果（D1）。ListPage 同守该口径。
type AuditLog interface {
	Append(e AuditEntry)
	List(incidentID string) ([]AuditEntry, error)
	// ListPage 全局分页读取（W12 审计解锁包：GET /api/v1/audit 的存储面）。
	// 排序 (occurred_at DESC, seq DESC) 决定性全序；游标 keyset（理由见
	// encodeAuditCursor 注释）；过滤/归一化语义 Mem/PG 逐字一致。
	// 后端故障必须返回以 ErrAuditUnavailable 为根因的错误（D1），
	// 参数错返回 ErrBadAuditAction / ErrBadAuditCursor（REST 映射 400）。
	ListPage(q AuditQuery) (AuditPage, error)
}

// MemAuditLog 内存审计（无 DB 场景；进程重启即丢，与 MemStore 同口径）。
//
// 内存有界化（优化方案 #6）：entries 只 append 不回收，DB 缺席时是无界
// slice。装护栏后**超限丢最旧**（追加序即活跃度序，队首 = 最久未活跃），
// 默认 5 万条上限下正常规模永不触发。
type MemAuditLog struct {
	mu      sync.Mutex
	entries []AuditEntry
	// seq 追加发号（ListPage 游标的 id 侧；与 incident_audit.BIGSERIAL 同角色）。
	// 独立于 len(entries)：容量护栏丢最旧后 slice 下标会漂移，seq 不回退。
	seq   int64
	guard *memguard.Guard
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
	l.seq++
	e.seq = l.seq
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

// Persistence 落库形态（R6-4 透出：内存审计重启即丢，REST 响应如实标
// memory——与 MemStore 同口径）。
func (l *MemAuditLog) Persistence() string { return "memory" }

// ListPage 全局分页读取（内存镜像）。过滤/排序/游标语义与 PGAuditLog.ListPage
// 逐字一致（契约测试 TestAuditListPageContract 锁死）：
// 排序 (occurred_at DESC, seq DESC) 决定性全序，seq 为追加发号。
//
// 内存实现不失败（error 恒 nil 的接口统一形态）；参数错仍按同一口径返回
// ErrBadAuditAction / ErrBadAuditCursor——两实现在"参数错"上也必须一致，
// 否则 REST 的 400 会随部署形态漂移。
func (l *MemAuditLog) ListPage(q AuditQuery) (AuditPage, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := normalizeAuditQuery(&q); err != nil {
		return AuditPage{}, err
	}
	limit := auditPageLimit(q.Limit)
	var curT time.Time
	var curSeq int64
	if q.Cursor != "" {
		t, seq, err := decodeAuditCursor(q.Cursor)
		if err != nil {
			return AuditPage{}, err
		}
		curT, curSeq = t, seq
	}
	out := make([]AuditEntry, 0, len(l.entries))
	for _, e := range l.entries {
		if !auditRowMatch(q, e, curT, curSeq, q.Cursor != "") {
			continue
		}
		out = append(out, cloneAuditEntry(e))
	}
	// 与 PG 同一排序契约（occurred_at DESC, seq DESC）：内存里追加序通常
	// 即时序，但墙钟回拨可造成 occurred_at 与 seq 逆序——显式排序兜底，
	// 保证两实现的输出序列逐条一致。
	sortAuditEntriesDESC(out)
	next := ""
	if len(out) > limit {
		last := out[limit-1]
		next = encodeAuditCursor(last.OccurredAt, last.seq)
		out = out[:limit]
	}
	return AuditPage{Items: out, NextCursor: next}, nil
}

// auditRowMatch 判定一条内存审计行是否进入本页（过滤 + keyset 游标谓词），
// 与 PG WHERE 子句逐项对应（含 Since 闭 / Until 开半开区间口径）。
func auditRowMatch(q AuditQuery, e AuditEntry, curT time.Time, curSeq int64, hasCursor bool) bool {
	if q.Actor != "" && e.Actor != q.Actor {
		return false
	}
	if q.Action != "" && string(e.Action) != q.Action {
		return false
	}
	if !q.Since.IsZero() && e.OccurredAt.Before(q.Since) {
		return false
	}
	if !q.Until.IsZero() && !e.OccurredAt.Before(q.Until) {
		return false
	}
	if hasCursor && !auditAfterCursor(e, curT, curSeq) {
		return false
	}
	return true
}

// auditAfterCursor keyset 谓词：行 (occurred_at, seq) 必须严格排在游标行之后
// （DESC 序 = 行值 < 游标）。与 PG 的 (occurred_at, id) < ($n,$n) 同语义。
func auditAfterCursor(e AuditEntry, curT time.Time, curSeq int64) bool {
	if !e.OccurredAt.Equal(curT) {
		return e.OccurredAt.Before(curT)
	}
	return e.seq < curSeq
}

// sortAuditEntriesDESC 按 (occurred_at DESC, seq DESC) 决定性全序排列
// （stdlib sort：匹配集在护栏上限 5 万条内，O(n log n) 足够）。
func sortAuditEntriesDESC(entries []AuditEntry) {
	sort.SliceStable(entries, func(i, j int) bool {
		return auditEntryLessDESC(entries[i], entries[j])
	})
}

func auditEntryLessDESC(a, b AuditEntry) bool { // a 应排在 b 前（更"新"）
	if !a.OccurredAt.Equal(b.OccurredAt) {
		return a.OccurredAt.After(b.OccurredAt)
	}
	return a.seq > b.seq
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

// Persistence 落库形态（R6-4 透出：REST 响应据此声明持久化承诺）。
func (l *PGAuditLog) Persistence() string { return "timescaledb" }

// ListPage 全局分页读取（GET /api/v1/audit 的存储面）。排序
// (occurred_at DESC, id DESC) 决定性全序，游标 = 上一页最后一行的
// (occurred_at, id) 行值比较（keyset，选型理由见 encodeAuditCursor 注释）。
// 索引依赖 migration 000021 的 (tenant_id, occurred_at DESC) 与
// (tenant_id, actor, occurred_at DESC)。
//
// 错误语义与 List 同款（D1）：后端故障一律 ErrAuditUnavailable 根因、
// 不返回半截列表；参数错 ErrBadAuditAction / ErrBadAuditCursor 在触库前返回。
// Since 闭 / Until 开的半开区间、actor/action 精确匹配——与 MemAuditLog
// 逐字一致（契约测试锁死）。既有单事件 List 零改动。
func (l *PGAuditLog) ListPage(q AuditQuery) (AuditPage, error) {
	if l == nil || l.pool == nil {
		return AuditPage{}, fmt.Errorf("%w: no database pool", ErrAuditUnavailable)
	}
	if err := normalizeAuditQuery(&q); err != nil {
		return AuditPage{}, err
	}
	limit := auditPageLimit(q.Limit)
	ctx, cancel := context.WithTimeout(context.Background(), ingestTimeout)
	defer cancel()

	args := []any{l.tenant, q.Actor, q.Action}
	// 时间窗：*time.Time 的 nil 透传为 SQL NULL，IS NULL = 该侧无界
	// （与 incident pg_query 的 '' 哨兵同风格的"NULL 哨兵"变体）。
	var sincePtr, untilPtr any
	if !q.Since.IsZero() {
		sincePtr = q.Since
	}
	if !q.Until.IsZero() {
		untilPtr = q.Until
	}
	args = append(args, sincePtr, untilPtr)
	cond := `
  AND ($4::timestamptz IS NULL OR occurred_at >= $4)
  AND ($5::timestamptz IS NULL OR occurred_at < $5)`
	if q.Cursor != "" {
		at, seq, err := decodeAuditCursor(q.Cursor)
		if err != nil {
			return AuditPage{}, err
		}
		args = append(args, at, seq)
		cond += fmt.Sprintf(" AND (occurred_at, id) < ($%d, $%d)", len(args)-1, len(args))
	}
	args = append(args, limit+1) // 多取一行判断是否还有下一页
	rows, err := l.pool.Query(ctx, `
SELECT id, incident_id, action, actor, detail, occurred_at FROM incident_audit
WHERE tenant_id=$1
  AND ($2='' OR actor=$2)
  AND ($3='' OR action=$3)`+cond+`
ORDER BY occurred_at DESC, id DESC
LIMIT $`+strconv.Itoa(len(args)), args...)
	if err != nil {
		return AuditPage{}, fmt.Errorf("%w: page query: %v", ErrAuditUnavailable, err)
	}
	defer rows.Close()
	out := []AuditEntry{}
	for rows.Next() {
		var e AuditEntry
		var detail string
		if err := rows.Scan(&e.seq, &e.IncidentID, &e.Action, &e.Actor, &detail, &e.OccurredAt); err != nil {
			return AuditPage{}, fmt.Errorf("%w: page scan: %v", ErrAuditUnavailable, err)
		}
		_ = json.Unmarshal([]byte(detail), &e.Detail)
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return AuditPage{}, fmt.Errorf("%w: page iterate: %v", ErrAuditUnavailable, err)
	}
	next := ""
	if len(out) > limit {
		last := out[limit-1]
		next = encodeAuditCursor(last.OccurredAt, last.seq)
		out = out[:limit]
	}
	return AuditPage{Items: out, NextCursor: next}, nil
}
