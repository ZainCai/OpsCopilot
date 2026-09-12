// rest_timeline.go W10-1（F-03）事件混合时间线：告警进出 ∥ 变更记录 ∥
// 人工处置动作，三源按时间归并（alert_event / change_record 后端 /
// incident_audit）。
//
// 为什么在 cmd/（package main）：三源分属 noise、topology、incident 三个
// 模块，internal 包之间不得互相 import（v1.3 §5.2）——跨模块汇合只能发生在
// 编排层，本文件即时间线的汇合点。取数路径全部复用既有单一出口：
//   - 告警流：alert_event（Timescale hypertable）按 tenant+cluster_key 过滤
//     （incident→簇经 incident_cluster/AttachCluster 关联，W10-6 生产自动挂簇
//     保证外部单有簇）；
//   - 变更：SemanticModelServer.GetRecentChanges（内存读缓存按保留窗，
//     RCA 同款直调，一套语义两个门面）；故障域 = 簇 NodeKeys 并集，与
//     RCAOrchestrator.faultDomain 同源（无域 ⇒ 无可关联变更，不把全库变更
//     泼上事件时间线——时间线是"这一单的叙事"，不是变更流水账）；
//   - 处置：AuditLog.List（内存/PG 双实现同一接口）。
//
// 同刻稳定序（total order，注释理由）：rank 取 change(0) < action(1) <
// alert_in(2) < alert_out(3)，再按 source_id 升序收尾。理由：同一时间戳下
// 按因果链阅读——变更是潜在上游动因，处置是人的响应，告警判决是下游结果；
// alert_out（降噪收敛判决）信息量最低，排同族 alert_in 之后。source_id 带
// 源前缀且逐条唯一（见各 fetch），(ts, rank, source_id) 即无并列的全序，
// 分页因此确定。
//
// 分页方案：offset 游标（与 /incidents 同为不透明 base64url next_cursor、
// 空=到末尾、坏游标 400——"游标风格"一致，但键取 offset）。选择理由：
// 三源本就是每次请求全量重算后归并（内存态小集合），offset 在此是精确的；
// 且排序固定为**时间升序**，新事件只会追加在尾部，不会移动已读页的偏移——
// 翻页期间来了新告警也不重不漏。keyset 游标要携带 (ts,kind,id) 三元组并在
// 每次重算里做二元组比较，收益（免 OFFSET）在百条级合并集上是零。
//
// 降级语义（partial）：三源各自独立尽力——
//   - 依赖未接线（无 DSN ⇒ 告警源缺席；降噪关闭/无 sem ⇒ 变更源无法归因、
//     记 not wired；audit nil ⇒ 处置源缺席）或已接线但查询失败：该源计入
//     missing（原因透出）并置 partial=true，**响应仍是 200 + 可用源归并**
//     （时间线是汇合视图，一个源塌了不该让整页不可读；专用端点如
//     /audit 仍按 D5"故障如实 500"）；
//   - 单源达到抓取上限（timelineSourceCap）同样记 missing=truncated——
//     "不完整"的原因集合是一个口径；
//   - incident 不存在 → 404（口径同 GET /incidents/{id}）；事件域未接线
//     → 503（连存在性都无法判定，无从降级）。
//   - 源可用但没有记录（如无簇的手工单 ⇒ 告警流为空）不算缺席——
//     partial 只表达"有源没能进归并"。
package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"opscopilot/internal/config"
	pb "opscopilot/internal/contracts/pb"
	"opscopilot/internal/incident"
)

// TimelineItem 统一时间线条目 DTO（REST 契约；snake_case 与其余 handler 同口径）。
// Severity 只对告警源有意义（告警严重级）；Confidence 只对变更源有意义
// （ADR-007 证据置信度）——其余留空即 omitempty 缺席，前端按 kind 取用。
type TimelineItem struct {
	TS         time.Time `json:"ts"`
	Kind       string    `json:"kind"` // alert_in|alert_out|change|action
	SourceID   string    `json:"source_id"`
	Summary    string    `json:"summary"`
	Severity   string    `json:"severity,omitempty"`
	Confidence string    `json:"confidence,omitempty"`
}

// 时间线 kind 封闭集合（对齐前端 TimelineKind）。
const (
	TimelineKindAlertIn  = "alert_in"  // 告警进场：判决放行/新事件（未被降噪收敛）
	TimelineKindAlertOut = "alert_out" // 告警出场：判决收敛（窗口去重/并簇，真降噪时不单独通知）
	TimelineKindChange   = "change"    // 变更事件（故障域节点、事件生命周期窗口内）
	TimelineKindAction   = "action"    // 人工/系统处置动作（incident_audit）
)

// timelineKindRank 同刻稳定序的 kind 优先级（理由见文件头"同刻稳定序"）。
func timelineKindRank(kind string) int {
	switch kind {
	case TimelineKindChange:
		return 0
	case TimelineKindAction:
		return 1
	case TimelineKindAlertIn:
		return 2
	default: // alert_out 与防御性兜底（未知 kind 不可能来自本文件的构造路径）
		return 3
	}
}

// timelineLess 时间线全序：ts 升序 → kind rank → source_id 升序。
// source_id 逐条唯一，因此不存在第三种并列——排序是决定性的。
func timelineLess(a, b TimelineItem) bool {
	if !a.TS.Equal(b.TS) {
		return a.TS.Before(b.TS)
	}
	ra, rb := timelineKindRank(a.Kind), timelineKindRank(b.Kind)
	if ra != rb {
		return ra < rb
	}
	return a.SourceID < b.SourceID
}

// timelineSourceCap 单源抓取上限（hypertable 查询护栏：簇被长事件复用时
// 判决行可无限增长；截断即记 missing=truncated，不静默）。
const timelineSourceCap = 2000

// timelineSource 一路数据源。fetch 返回条目 + missing 原因：
// missing=="" 表示源可用（空列表 = 可用但无记录）；非空表示该源缺席/不完整
// 的原因（"not wired …"/"query failed …"/"truncated …"），聚合层据此置
// partial。fetch 内部自行消化可降级错误（不外抛 panic 级错误），error 仅
// 为聚合契约保留（纯测试源可恒返回 nil）。
type timelineSource struct {
	name  string // "alert" | "change" | "action"
	fetch func(ctx context.Context, inc incident.Incident) (items []TimelineItem, missing string, err error)
}

// ErrTimelineBadCursor 游标不合法（REST 映射 400，固定文案——不透出
// 编解码细节，对齐 incident.ErrBadCursor 的口径）。
var ErrTimelineBadCursor = errors.New("timeline: bad cursor")

// encodeTimelineCursor offset → 不透明游标（base64url 十进制偏移）。
func encodeTimelineCursor(offset int) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.Itoa(offset)))
}

// decodeTimelineCursor 游标解码（负数/非数字/非法 base64 一律 ErrTimelineBadCursor）。
func decodeTimelineCursor(cur string) (int, error) {
	raw, err := base64.RawURLEncoding.DecodeString(cur)
	if err != nil {
		return 0, ErrTimelineBadCursor
	}
	n, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil || n < 0 {
		return 0, ErrTimelineBadCursor
	}
	return n, nil
}

// timelinePage 聚合结果（REST 响应的原料）。
type timelinePage struct {
	Items   []TimelineItem
	Total   int
	Next    string
	Partial bool
	Missing map[string]string // 源名 → 缺席/不完整原因（partial 的明细）
}

// paginateTimeline 在全量归并排序后的列表上按 offset 切页。升序 + 尾部追加
// 稳定 ⇒ 翻页不重不漏（理由见文件头）。offset 越界钳到末尾（返回空页，
// 不报错——游标只可能由本端自己发出，钳位是防御而非语义）。
func paginateTimeline(full []TimelineItem, limit, offset int) timelinePage {
	if offset > len(full) {
		offset = len(full)
	}
	end := offset + limit
	if end > len(full) {
		end = len(full)
	}
	page := timelinePage{
		Items:   make([]TimelineItem, end-offset),
		Total:   len(full),
		Missing: map[string]string{},
	}
	copy(page.Items, full[offset:end]) // 拷贝而非切片别名：调用方改动不得写回归并全量
	if end < len(full) {
		page.Next = encodeTimelineCursor(end)
	}
	return page
}

// assembleTimeline 汇合三源：全量拉取 → 归并排序 → offset 切页。
// 任一源 missing 非空即 partial（响应 200 不 5xx，见文件头降级语义）。
func assembleTimeline(ctx context.Context, inc incident.Incident,
	sources []timelineSource, limit, offset int) (timelinePage, error) {
	full := make([]TimelineItem, 0, 64)
	missing := map[string]string{}
	for _, s := range sources {
		items, ms, err := s.fetch(ctx, inc)
		if err != nil {
			ms = "query failed: " + err.Error() // error 通道兜底（当前三源都走 missing）
		}
		full = append(full, items...)
		if ms != "" {
			missing[s.name] = ms
		}
	}
	sort.SliceStable(full, func(i, j int) bool { return timelineLess(full[i], full[j]) })
	page := paginateTimeline(full, limit, offset)
	page.Missing = missing
	page.Partial = len(missing) > 0
	return page, nil
}

// handleIncidentTimeline GET /api/v1/incidents/{id}/timeline?limit=&cursor=
// 读路径鉴权口径同现有 GET（S1 loopback-only 默认无 Token，见 rest_gateway.go
// 文件头）；limit 默认/上限复用事件列表口径（200/1000，同源常量）。
func (g *RESTGateway) handleIncidentTimeline(w http.ResponseWriter, r *http.Request) {
	if g.incidents == nil {
		writeErr(w, http.StatusServiceUnavailable, "incident store not wired")
		return
	}
	id := r.PathValue("id")
	inc, err := g.incidents.Get(id)
	if err != nil {
		writeErr(w, http.StatusNotFound, "incident not found: "+id)
		return
	}
	q := r.URL.Query()
	limit := 0
	if raw := q.Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 0 {
			writeErr(w, http.StatusBadRequest, "limit must be a non-negative integer")
			return
		}
		limit = n
	}
	if limit <= 0 {
		limit = incident.PageLimitDefault
	}
	if limit > incident.PageLimitMax {
		limit = incident.PageLimitMax
	}
	offset := 0
	if cur := q.Get("cursor"); cur != "" {
		offset, err = decodeTimelineCursor(cur)
		if err != nil {
			writeErr(w, http.StatusBadRequest, "bad cursor")
			return
		}
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	page, err := assembleTimeline(ctx, inc, []timelineSource{
		{name: "alert", fetch: g.timelineAlertSource},
		{name: "change", fetch: g.timelineChangeSource},
		{name: "action", fetch: g.timelineActionSource},
	}, limit, offset)
	if err != nil {
		g.logf("WARNING: timeline assemble (%s): %v", id, err)
		writeErr(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"incident_id": id, "items": page.Items, "count": len(page.Items),
		"total": page.Total, "next_cursor": page.Next,
		"partial": page.Partial, "missing": page.Missing,
	})
}

// timelineAlertSource 告警进出源：alert_event（hypertable，source='shadow'
// 影子判决流——与告警中心/评估同表同口径）按本事件关联簇过滤。
// 分类依据持久化字段而非猜测：would_suppress/would_converge 为真 ⇒
// alert_out（降噪收敛=告警出场），否则 alert_in（新事件/直通=告警进场）。
func (g *RESTGateway) timelineAlertSource(ctx context.Context, inc incident.Incident) ([]TimelineItem, string, error) {
	if g.db == nil {
		return nil, "not wired (set OPS_DB_DSN)", nil
	}
	if len(inc.ClusterKeys) == 0 {
		return nil, "", nil // 无关联簇 ⇒ 没有可归因的告警流（源可用、无记录，不算缺席）
	}
	rows, err := g.db.Query(ctx, `
SELECT occurred_at, fingerprint, COALESCE(cluster_key,''), payload
FROM alert_event
WHERE tenant_id=$1 AND source='shadow' AND cluster_key = ANY($2)
ORDER BY occurred_at ASC, id ASC
LIMIT $3`, g.tenant, inc.ClusterKeys, timelineSourceCap)
	if err != nil {
		g.logf("WARNING: timeline alert_event query (%s): %v", inc.ID, err)
		return nil, "query failed (see server log)", nil
	}
	defer rows.Close()
	type payload struct {
		NodeKey  string `json:"node_key"`
		Severity string `json:"severity"`
		Summary  string `json:"summary"`
		Suppress bool   `json:"would_suppress"`
		Converge bool   `json:"would_converge"`
		Reason   string `json:"reason"`
	}
	out := []TimelineItem{}
	truncated := false
	seq := 0
	for rows.Next() {
		var (
			at     time.Time
			fp, ck string
			blob   []byte
		)
		if err := rows.Scan(&at, &fp, &ck, &blob); err != nil {
			g.logf("WARNING: timeline alert_event scan (%s): %v", inc.ID, err)
			return nil, "query failed (see server log)", nil
		}
		var pl payload
		_ = json.Unmarshal(blob, &pl) // payload 损坏不阻断归并，字段留零值（告警中心同款）
		kind := TimelineKindAlertIn
		if pl.Suppress || pl.Converge {
			kind = TimelineKindAlertOut
		}
		summary := pl.Summary
		if summary == "" {
			summary = "告警 " + fp
		}
		if pl.Reason != "" {
			summary += "（" + pl.Reason + "）"
		}
		if ck != "" {
			summary += " @" + ck
		}
		out = append(out, TimelineItem{
			TS: at, Kind: kind,
			SourceID: fmt.Sprintf("alert:%s:%d:%d", fp, at.UnixNano(), seq),
			Summary:  summary, Severity: pl.Severity,
		})
		seq++
	}
	if err := rows.Err(); err != nil {
		g.logf("WARNING: timeline alert_event rows (%s): %v", inc.ID, err)
		return nil, "query failed (see server log)", nil
	}
	if seq >= timelineSourceCap {
		truncated = true
	}
	if truncated {
		return out, fmt.Sprintf("truncated at %d rows", timelineSourceCap), nil
	}
	return out, "", nil
}

// timelineChangeSource 变更源：故障域（簇 NodeKeys 并集，RCA faultDomain
// 同源读法）× 事件生命周期窗口（[created - RCA 窗口回看, resolved|now]）
// 内的变更。回看下界用 config.DefaultRCAWindow：变更常发生在故障**之前**
// （部署引入故障），窗口起点只按 created_at 切会把诱因全部截掉——与 RCA
// 的取证窗口同一口径，两处不再是隐形差异。
// 结束口径：已解决单取 resolved_at（闭环后的部署与本单叙事无关），
// 未解决取 now（GetRecentChanges 空 window_end 即默认 now，不显式传）。
func (g *RESTGateway) timelineChangeSource(ctx context.Context, inc incident.Incident) ([]TimelineItem, string, error) {
	if g.sem == nil {
		return nil, "not wired (semantic model absent)", nil
	}
	if g.noise == nil {
		return nil, "noise engine disabled (OPS_NOISE_SHADOW=off) — fault domain unknown", nil
	}
	domain := map[string]struct{}{}
	missingClusters := 0
	for _, key := range inc.ClusterKeys {
		cl := g.noise.shadow.Clusterer().Get(key)
		if cl == nil {
			missingClusters++
			continue
		}
		for k := range cl.NodeKeys {
			if k != "" {
				domain[k] = struct{}{}
			}
		}
	}
	if missingClusters > 0 {
		// 簇不在内存视图（重启未 Restore / 已淘汰）：变更关联面变小，
		// 但源本身可用——只记日志不置 partial（RCA 同款"宁缺毋滥"，
		// 时间线不为半截证据谎报缺席）。
		g.logf("timeline: incident %s has %d cluster(s) not in memory view — change correlation may be partial",
			inc.ID, missingClusters)
	}
	if len(domain) == 0 {
		return nil, "", nil // 无故障域 ⇒ 无可关联变更（不算缺席，见文件头）
	}
	start := inc.CreatedAt
	if start.IsZero() {
		start = time.Now()
	}
	start = start.Add(-config.DefaultRCAWindow)
	req := &pb.GetRecentChangesRequest{
		TenantId:    g.tenant,
		WindowStart: start.Format(time.RFC3339Nano),
	}
	if !inc.ResolvedAt.IsZero() {
		req.WindowEnd = inc.ResolvedAt.Format(time.RFC3339Nano)
	}
	resp, err := g.sem.GetRecentChanges(ctx, req)
	if err != nil {
		g.logf("WARNING: timeline change window (%s): %v", inc.ID, err)
		return nil, "query failed (see server log)", nil
	}
	out := []TimelineItem{}
	for _, c := range resp.GetChanges() {
		if _, ok := domain[c.GetNodeKey()]; !ok {
			continue
		}
		at, perr := time.Parse(time.RFC3339, c.GetOccurredAt())
		if perr != nil {
			// 不可解析的时间戳不进时间线（脏数据不参与归并），但必须留日志
			// ——静默丢条目是最难查的坑（RCA 同款纪律）。
			g.logf("WARNING: timeline skip change %s: bad occurred_at %q: %v", c.GetId(), c.GetOccurredAt(), perr)
			continue
		}
		summary := c.GetChangeType() + " on " + c.GetNodeKey()
		if c.GetActor() != "" {
			summary += " by " + c.GetActor()
		}
		if s := c.GetSummary(); s != "" {
			summary += " — " + s
		}
		out = append(out, TimelineItem{
			TS: at, Kind: TimelineKindChange,
			SourceID:   "change:" + c.GetId(),
			Summary:    summary,
			Confidence: c.GetConfidence(),
		})
	}
	return out, "", nil
}

// timelineActionSource 处置动作源：incident_audit（内存/PG 双实现同一接口，
// 与 GET /{id}/audit 同源）。create/transition/merge/attach_cluster/rca/…
// 全部进时间线——"人工处置动作"在审计面前不挑肥拣瘦，自动动作同样留痕。
func (g *RESTGateway) timelineActionSource(_ context.Context, inc incident.Incident) ([]TimelineItem, string, error) {
	if g.audit == nil {
		return nil, "not wired (audit absent)", nil
	}
	entries, err := g.audit.List(inc.ID)
	if err != nil {
		// 审计后端故障在专用 /audit 端点回 500（D5/D1），在汇合视图里降级为
		// "该源缺席 + partial"——另外两源照常可读（文件头降级语义）。
		g.logf("WARNING: timeline audit list (%s): %v", inc.ID, err)
		return nil, "query failed (see server log)", nil
	}
	out := make([]TimelineItem, 0, len(entries))
	for i, e := range entries {
		summary := string(e.Action)
		if e.Actor != "" {
			summary += " by " + e.Actor
		}
		if d := compactAuditDetail(e.Detail); d != "" {
			summary += " " + d
		}
		out = append(out, TimelineItem{
			TS: e.OccurredAt, Kind: TimelineKindAction,
			// source_id 唯一性：审计行无自然键，(action, ts, 本次快照序号)
			// 足够定序；序号在 List 稳定（PG 按 occurred_at DESC、内存按
			// 追加序）时跨页一致——翻页新条目只会追加在 ts 尾部。
			SourceID: fmt.Sprintf("action:%s:%d:%d", e.Action, e.OccurredAt.UnixNano(), i),
			Summary:  summary,
		})
	}
	return out, "", nil
}

// compactAuditDetail 审计 Detail 压成 "k=v k=v"（键排序保决定性）。
func compactAuditDetail(detail map[string]any) string {
	if len(detail) == 0 {
		return ""
	}
	keys := make([]string, 0, len(detail))
	for k := range detail {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%v", k, detail[k]))
	}
	return strings.Join(parts, " ")
}
