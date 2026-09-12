// rest_kpi.go W10-3（F-07）运维 KPI 聚合端点 GET /api/v1/kpis。
//
// 口径唯一来源 internal/incident/kpi.go（KPIStats 注释）：队列 = created_at
// 入窗（默认 OPS_KPI_WINDOW=168h，?window= 覆盖），MTTA=avg(acked−created)、
// MTTR=avg(resolved−created)（**平均闭环时长与 MTTR 同口径，合并为一个字段
// 报告**，不做两个数字的伪双口径）、吞吐计数 created/resolved；空窗返回零值
// + 样本数（samples=0 即"无数据"，消费方按 samples 判空）。
//
// 数据源 = incident 表本身：KPI 是聚合问题，走聚合 SQL（PG 一条带 FILTER
// 的 SELECT，Mem 线性扫描同口径）；timeline 端点是"单条事件的叙事"三源汇
// 合，属展示源不是计算源。"与手工按时间线计算一致"（排期验收）由
// kpi_test.go 的手算对账测试兑现：同一批事件的 (created/acked/resolved)
// 时间戳逐条手算均值，与端点读数比对。
//
// 无分页（聚合结果一行）；读路径鉴权口径同其余 GET。
package main

import (
	"net/http"
	"strings"
	"time"
)

// handleKPIs GET /api/v1/kpis?window=168h&severity=critical
// window 缺失/空 → 装配默认（OPS_KPI_WINDOW）；非法（不可解析/非正）→ 400。
// severity 缺失/空 → 全部；白名单外 → 400（与建单入口同一 validSeverity）。
func (g *RESTGateway) handleKPIs(w http.ResponseWriter, r *http.Request) {
	if g.incidents == nil {
		writeErr(w, http.StatusServiceUnavailable, "incident store not wired")
		return
	}
	q := r.URL.Query()
	window := g.limits.KPIWindow
	if raw := strings.TrimSpace(q.Get("window")); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil || d <= 0 {
			writeErr(w, http.StatusBadRequest, "window must be a positive duration (e.g. 168h, 30m)")
			return
		}
		window = d
	}
	severity := strings.ToLower(strings.TrimSpace(q.Get("severity")))
	if severity != "" && !validSeverity(severity) {
		writeErr(w, http.StatusBadRequest, "severity must be one of critical|warning|info")
		return
	}
	now := time.Now()
	since := now.Add(-window)
	stats, err := g.incidents.KPI(since, severity)
	if err != nil {
		// 细节只进日志（第七轮 M1 口径），客户端只见固定文案。
		g.logf("WARNING: kpi aggregate (window=%s severity=%q): %v", window, severity, err)
		writeErr(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"window":       window.String(),
		"window_start": since,
		"generated_at": now,
		"severity":     severity, // 空串 = 未过滤
		"stats":        stats,
	})
}
