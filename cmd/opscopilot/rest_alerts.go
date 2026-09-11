// 告警中心 handler（D12=B 配套；对齐原型 pages/alerts.jsx 的"告警中心"页）。
// 数据 = alert_event(source='shadow')，与 W6-3 评估（evaluate.py）同表同口径：
// 运维在控制台看到的告警流，就是降噪引擎影子判决的原始流。
package main

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"
)

// handleAlerts GET /api/v1/alerts —— 影子判决流（最新优先）。
// 参数：limit（默认 200，上限 1000）。未接 DB → 503（R6-4 口径）。
func (g *RESTGateway) handleAlerts(w http.ResponseWriter, r *http.Request) {
	if g.db == nil {
		writeErr(w, http.StatusServiceUnavailable, "alert persistence not wired (set OPS_DB_DSN)")
		return
	}
	limit := 200
	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			writeErr(w, http.StatusBadRequest, "limit must be a positive integer")
			return
		}
		if n > 1000 {
			n = 1000
		}
		limit = n
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	rows, err := g.db.Query(ctx, `
SELECT occurred_at, fingerprint, COALESCE(cluster_key,''), payload
FROM alert_event
WHERE tenant_id=$1 AND source='shadow'
ORDER BY occurred_at DESC LIMIT $2`, g.tenant, limit)
	if err != nil {
		g.logf("WARNING: alerts query: %v", err)
		writeErr(w, http.StatusInternalServerError, "internal error")
		return
	}
	defer rows.Close()
	type alertView struct {
		OccurredAt  time.Time `json:"occurred_at"`
		Fingerprint string    `json:"fingerprint"`
		ClusterKey  string    `json:"cluster_key"`
		Reason      string    `json:"reason"`
		// 知识库富化字段（第七轮后新增：D12=B 告警中心可操作性）。
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
	for rows.Next() {
		var (
			a       alertView
			fp      string
			ck      string
			payload []byte
		)
		if err := rows.Scan(&a.OccurredAt, &fp, &ck, &payload); err != nil {
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
	writeJSON(w, http.StatusOK, map[string]any{
		"alerts": list, "count": len(list),
	})
}
