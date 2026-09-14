// 告警中心 handler（D12=B 配套；对齐原型 pages/alerts.jsx 的"告警中心"页）。
// 数据 = alert_event(source='shadow')，与 W6-3 评估（evaluate.py）同表同口径；
// 运维在控制台看到的告警流，就是降噪引擎影子判决的原始流。
package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// ErrBadAlertCursor 游标损坏/非法 → REST 400（与 audit/timeline 同契约）。
var ErrBadAlertCursor = errors.New("alerts: bad cursor")

// defaultAlertsLimit 未传 limit 时的页大小（对齐 console 200 条口径）。
const defaultAlertsLimit = 200

// maxAlertsLimit 单页上限（对齐 incident/audit 分页同源常量）。
const maxAlertsLimit = 1000

// alertCursorEnvelope 游标内容：(occurred_at, id) 字典序 = 排序键，与
// ORDER BY occurred_at DESC, id DESC 一致，翻页期间新行/压缩不影响已读页。
// 编码：base64url( RFC3339Nano(occurred_at) \x1f id )，与 audit 包同款编码。
func encodeAlertCursor(at time.Time, id int64) string {
	return base64.RawURLEncoding.EncodeToString(
		[]byte(fmt.Sprintf("%s\x1f%d", at.UTC().Format(time.RFC3339Nano), id)))
}

func decodeAlertCursor(s string) (time.Time, int64, error) {
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return time.Time{}, 0, fmt.Errorf("%w: invalid base64", ErrBadAlertCursor)
	}
	parts := strings.SplitN(string(raw), "\x1f", 2)
	if len(parts) != 2 {
		return time.Time{}, 0, fmt.Errorf("%w: malformed cursor", ErrBadAlertCursor)
	}
	t, err := time.Parse(time.RFC3339Nano, parts[0])
	if err != nil {
		return time.Time{}, 0, fmt.Errorf("%w: bad timestamp", ErrBadAlertCursor)
	}
	id, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return time.Time{}, 0, fmt.Errorf("%w: bad id", ErrBadAlertCursor)
	}
	return t, id, nil
}

// handleAlerts GET /api/v1/alerts —— 影子判决流（最新优先，keyset 游标分页）。
// 参数：
//   - limit（默认 200，上限 1000）
//   - since / until（RFC3339 时间过滤，可选；since 必须早于 until）
//   - cursor（上一页 next_cursor，空 = 第一页；坏游标 → 400）
//
// 未接 DB → 503（R6-4 口径）。
// 响应：{"alerts":[...], "count":N, "next_cursor":"..."}；next_cursor 空 = 已到末尾。
func (g *RESTGateway) handleAlerts(w http.ResponseWriter, r *http.Request) {
	if g.db == nil {
		writeErr(w, http.StatusServiceUnavailable, "alert persistence not wired (set OPS_DB_DSN)")
		return
	}
	q := r.URL.Query()

	limit := defaultAlertsLimit
	if raw := q.Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			writeErr(w, http.StatusBadRequest, "limit must be a positive integer")
			return
		}
		if n > maxAlertsLimit {
			n = maxAlertsLimit
		}
		limit = n
	}

	where := "tenant_id=$1 AND source='shadow'"
	args := []any{g.tenant}
	argn := 2

	var since, until *time.Time
	if raw := q.Get("since"); raw != "" {
		t, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			writeErr(w, http.StatusBadRequest, "since must be RFC3339 (e.g. 2026-09-14T08:00:00Z)")
			return
		}
		since = &t
	}
	if raw := q.Get("until"); raw != "" {
		t, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			writeErr(w, http.StatusBadRequest, "until must be RFC3339 (e.g. 2026-09-14T08:00:00Z)")
			return
		}
		until = &t
	}
	if since != nil && until != nil && !since.Before(*until) {
		writeErr(w, http.StatusBadRequest, "since must be earlier than until")
		return
	}
	if since != nil {
		where += fmt.Sprintf(" AND occurred_at >= $%d", argn)
		args = append(args, *since)
		argn++
	}
	if until != nil {
		where += fmt.Sprintf(" AND occurred_at <= $%d", argn)
		args = append(args, *until)
		argn++
	}

	var curT time.Time
	var curID int64
	if raw := q.Get("cursor"); raw != "" {
		t, id, err := decodeAlertCursor(raw)
		if err != nil {
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		}
		curT, curID = t, id
		where += fmt.Sprintf(" AND (occurred_at, id) < ($%d, $%d)", argn, argn+1)
		args = append(args, curT, curID)
		argn += 2
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	// 多取 1 行探测是否还有下一页（keyset 分页标准做法：翻页不重不漏，
	// 不受翻页期间新写入影响——游标锚定的是上一页末行的排序键）。
	query := fmt.Sprintf(`
SELECT occurred_at, id, fingerprint, COALESCE(cluster_key,''), payload
FROM alert_event
WHERE %s
ORDER BY occurred_at DESC, id DESC
LIMIT $%d`, where, argn)
	args = append(args, limit+1)

	rows, err := g.db.Query(ctx, query, args...)
	if err != nil {
		g.logf("WARNING: alerts query: %v", err)
		writeErr(w, http.StatusInternalServerError, "internal error")
		return
	}
	defer rows.Close()

	type alertView struct {
		// id 排序键（keyset 游标锚点；不序列化——内部卫生字段）。
		id          int64
		OccurredAt  time.Time `json:"occurred_at"`
		Fingerprint string    `json:"fingerprint"`
		ClusterKey  string    `json:"cluster_key"`
		Reason      string    `json:"reason"`
		// 知识库富化字段（第七轮后新增，D12=B 告警中心可操作性）。
		Title     string   `json:"title"`
		Level     string   `json:"level"`
		ErrorCode string   `json:"error_code"`
		Service   string   `json:"service"`
		Summary   string   `json:"summary"`
		Metric    string   `json:"metric"`
		Threshold string   `json:"threshold"`
		Impact    string   `json:"impact"`
		Steps     []string `json:"steps"`
	}
	list := []alertView{}
	more := false
	for rows.Next() {
		var (
			a       alertView
			fp      string
			ck      string
			payload []byte
		)
		if err := rows.Scan(&a.OccurredAt, &a.id, &fp, &ck, &payload); err != nil {
			g.logf("WARNING: alerts scan: %v", err)
			writeErr(w, http.StatusInternalServerError, "internal error")
			return
		}
		var pl struct {
			Summary  string `json:"summary"`
			Severity string `json:"severity"`
			NodeKey  string `json:"node_key"`
			Reason   string `json:"reason"`
			Suppress bool   `json:"would_suppress"`
			Converge bool   `json:"would_converge"`
		}
		_ = json.Unmarshal(payload, &pl) // payload 损坏不阻断整页，字段留零值
		kb := knowledgeFor(pl.Summary)
		a.Fingerprint, a.ClusterKey, a.Reason = fp, ck, pl.Reason
		a.Title = enrichTitle(kb, pl.NodeKey)
		a.Level = levelOf(kb.Level, pl.Severity)
		a.ErrorCode = kb.ErrorCode
		a.Service = pl.NodeKey
		a.Summary = pl.Summary
		a.Metric, a.Threshold = kb.Metric, kb.Threshold
		a.Impact = kb.Impact
		a.Steps = kb.Steps
		list = append(list, a)
	}
	if err := rows.Err(); err != nil {
		g.logf("WARNING: alerts rows: %v", err)
		writeErr(w, http.StatusInternalServerError, "internal error")
		return
	}
	// 探测行（limit+1）只用于判"还有下一页"，不进响应；游标必须锚定
	// **返回页的末行**（探测行被截掉后若以其为锚，下一页会丢一行）。
	if len(list) > limit {
		list = list[:limit]
		more = true
	}
	next := ""
	if more && len(list) > 0 {
		last := list[len(list)-1]
		next = encodeAlertCursor(last.OccurredAt, last.id)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"alerts": list, "count": len(list), "next_cursor": next,
	})
}
