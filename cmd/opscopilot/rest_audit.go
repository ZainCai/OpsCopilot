// rest_audit.go GET /api/v1/audit —— 全局审计检索（W12 审计解锁包）。
//
// 口径（拍板已定）：
//   - 鉴权：与既有 GET /api/v1/incidents/{id}/audit 后缀处理完全一致——
//     读路径无 Token（S1 bind loopback-only；非回环暴露必须前置鉴权反代，
//     main 启动警告覆盖）。RCA 读端点要 Token 是因为它会**写**审计并读全量
//     证据链（见 rest_rca.go 头注释）；本端点纯读不掺混口径。
//   - 动作过滤封闭集合 = cmd/audit.go AuditAction 常量（本期不扩 action
//     CHECK）；未知 action → ErrBadAuditAction → 400（两 store 同源校验）。
//   - "查审计"计数：opscopilot_audit_reads_total{source=global|incident}
//     作为本期留痕替代——独立 audit_read 动作与哈希链审计属 M3（ADR-005）。
//   - 错误包络同族：参数错 400 固定文案（不透出解码细节）；后端故障
//     ErrAuditUnavailable → 500 脱敏（细节进日志，D5/D1）；未接线 503。
//   - persistence 透出（R6-4）：无 DSN 走 Mem 镜像如实标 memory，并附
//     partial_hint 声明"重启即丢/护栏丢最旧"——不伪装持久化承诺。
package main

import (
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// auditEntryView 全局审计列表的行视图（REST 契约只回显人需要的五列；
// detail 全文仍可从单事件端点 GET /incidents/{id}/audit 取）。
type auditEntryView struct {
	IncidentID string    `json:"incident_id"`
	Action     string    `json:"action"`
	Actor      string    `json:"actor"`
	OccurredAt time.Time `json:"occurred_at"`
	Summary    string    `json:"summary"`
}

// auditSummaryMaxRunes summary 缺键回退时的 detail 截断长度（口径：首 120
// 字符，rune 计数防多字节切断）。
const auditSummaryMaxRunes = 120

// auditDetailStr 从 detail 取一个键的可读值（缺失/空串回 ""；非字符串值用
// %v，JSON 数字经 map[string]any 是 float64——用整数形态显示避免
// "attempts=3" 变 "attempts=+3.000"）。
func auditDetailStr(detail map[string]any, key string) string {
	v, ok := detail[key]
	if !ok || v == nil {
		return ""
	}
	switch t := v.(type) {
	case string:
		return t
	case float64:
		if t == float64(int64(t)) {
			return strconv.FormatInt(int64(t), 10)
		}
		return fmt.Sprintf("%v", t)
	default:
		return fmt.Sprintf("%v", t)
	}
}

// auditSummary 各 action 的一行摘要。**摘要键与代码里 Append 调用点的
// detail 结构逐一对应**（读代码定，缺键回退 detail JSON 首 120 字符）：
//
//	create                    → detail.title（rest_incidents.go / ingest_queue.go）
//	transition                → detail.to（rest_incidents.go）
//	merge                     → detail.merged_into（rest_incidents.go）
//	attach_cluster            → detail.cluster_key（noise_process.go autoAttachClusters）
//	external_recovery_ignored → detail.source_ref（ingest_queue.go）
//	rate_limited              → detail.source_ref（ingest_queue.go）
//	ingest_failed             → detail.error（ingest_queue.go）
//	rca                       → detail.root_causes/findings/duration_ms（rca_orchestrator.go）
//
// 缺键（历史数据/测试直写/未来新动作）→ 回退 detail 的 "k=v" 压缩形态
// （与时间线 compactAuditDetail 同风格）截断首 120 字符；空 detail → 空串。
func auditSummary(e AuditEntry) string {
	switch e.Action {
	case AuditCreate:
		if s := auditDetailStr(e.Detail, "title"); s != "" {
			return "建单：" + s
		}
	case AuditTransition:
		if s := auditDetailStr(e.Detail, "to"); s != "" {
			return "流转到 " + s
		}
	case AuditMerge:
		if s := auditDetailStr(e.Detail, "merged_into"); s != "" {
			return "合并到 " + s
		}
	case AuditAttachCluster:
		if s := auditDetailStr(e.Detail, "cluster_key"); s != "" {
			return "挂簇 " + s
		}
	case AuditExternalRecoveryIgnored:
		if s := auditDetailStr(e.Detail, "source_ref"); s != "" {
			return "忽略外部恢复 " + s
		}
	case AuditRateLimited:
		if s := auditDetailStr(e.Detail, "source_ref"); s != "" {
			return "限流折叠 " + s
		}
	case AuditIngestFailed:
		if s := auditDetailStr(e.Detail, "error"); s != "" {
			return "摄取失败：" + s
		}
	case AuditRCA:
		return fmt.Sprintf("根因 %s 条 · findings %s · 用时 %sms",
			orDash(auditDetailStr(e.Detail, "root_causes")),
			orDash(auditDetailStr(e.Detail, "findings")),
			orDash(auditDetailStr(e.Detail, "duration_ms")))
	}
	return compactDetailTruncated(e.Detail)
}

func orDash(s string) string {
	if s == "" {
		return "—"
	}
	return s
}

// compactDetailTruncated 缺键回退：detail 压成 "k=v"（键排序保决定性，
// 同 rest_timeline.go compactAuditDetail 口径），截断首 120 字符。
func compactDetailTruncated(detail map[string]any) string {
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
	s := strings.Join(parts, " ")
	if r := []rune(s); len(r) > auditSummaryMaxRunes {
		return string(r[:auditSummaryMaxRunes]) + "…"
	}
	return s
}

// auditPersistence 落库形态透出（可选接口断言，不扩 AuditLog 主接口——
// 测试替身无需实现；生产两实现 Mem/PG 都带）。
func auditPersistence(l AuditLog) string {
	if p, ok := l.(interface{ Persistence() string }); ok {
		return p.Persistence()
	}
	return "unknown"
}

// handleAuditGlobal GET /api/v1/audit?actor=&action=&since=&until=&limit=&cursor=
//
// 参数：actor/action 精确过滤（action 封闭集合，未知 400）；since/until
// RFC3339 时间窗（[since, until) 半开）；limit 默认 200 上限 1000
// （incident 分页同源常量）；cursor 上一页 next_cursor（坏 → 400）。
// 响应：{entries, count, next_cursor, persistence, partial_hint?}；
// next_cursor 空 = 已到末尾。排序 (occurred_at DESC, id DESC) 最新优先。
func (g *RESTGateway) handleAuditGlobal(w http.ResponseWriter, r *http.Request) {
	if g.audit == nil {
		writeErr(w, http.StatusServiceUnavailable, "audit not wired")
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
	var since, until time.Time
	if raw := q.Get("since"); raw != "" {
		t, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			writeErr(w, http.StatusBadRequest, "since must be RFC3339")
			return
		}
		since = t
	}
	if raw := q.Get("until"); raw != "" {
		t, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			writeErr(w, http.StatusBadRequest, "until must be RFC3339")
			return
		}
		until = t
	}

	aq := AuditQuery{
		Actor:  q.Get("actor"),
		Action: q.Get("action"),
		Since:  since, Until: until,
		Cursor: q.Get("cursor"),
		Limit:  limit,
	}
	page, err := g.audit.ListPage(aq)
	// 打点口径：一次"查审计"访问 = 一次对审计后端的读取尝试——参数错
	// （未知 action / 坏游标）在 store 归一层即返回、从未触库，不计；
	// 后端故障仍计入——降级面恰恰需要被数出来。
	if !errors.Is(err, ErrBadAuditAction) && !errors.Is(err, ErrBadAuditCursor) {
		g.countAuditRead(auditSourceGlobal)
	}
	if err != nil {
		// 与 /incidents 列表同款三分：客户端参数错 400 固定文案；后端故障
		// 500 脱敏（D5/D1：绝不把故障伪装成空列表）。
		switch {
		case errors.Is(err, ErrBadAuditAction):
			writeErr(w, http.StatusBadRequest, "action must be one of "+strings.Join(auditActionList(), "|"))
		case errors.Is(err, ErrBadAuditCursor):
			writeErr(w, http.StatusBadRequest, "bad cursor")
		default:
			g.logf("WARNING: audit global list: %v", err)
			writeErr(w, http.StatusInternalServerError, "internal error")
			return
		}
		return
	}

	entries := make([]auditEntryView, 0, len(page.Items))
	for _, e := range page.Items {
		entries = append(entries, auditEntryView{
			IncidentID: e.IncidentID, Action: string(e.Action), Actor: e.Actor,
			OccurredAt: e.OccurredAt, Summary: auditSummary(e),
		})
	}
	resp := map[string]any{
		"entries":     entries,
		"count":       len(entries),
		"next_cursor": page.NextCursor,
		"persistence": auditPersistence(g.audit),
	}
	if resp["persistence"] == "memory" {
		// 内存镜像如实提示易失面（R6-4 纪律：不伪装持久化承诺）。
		resp["partial_hint"] = "in-memory mirror: lost on restart; oldest dropped beyond capacity guard"
	}
	writeJSON(w, http.StatusOK, resp)
}

// auditActionList 封闭集合的文档化枚举（400 文案用；顺序与 audit.go 常量块
// 一致——新增动作常量时这里必须同步，validAuditAction 与 CHECK 的同步由
// "本期不扩 action CHECK" 口径冻结）。
func auditActionList() []string {
	return []string{
		string(AuditCreate), string(AuditTransition), string(AuditAttachCluster),
		string(AuditMerge), string(AuditExternalRecoveryIgnored),
		string(AuditRateLimited), string(AuditIngestFailed), string(AuditRCA),
	}
}
