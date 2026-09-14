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
//
// 时钟口径（P2-D5）：window_start 由本进程 time.Now() 计算（进程钟），
// created_at / resolved_at / acked_at 由存储侧时钟落戳（PG now()；Mem
// time.Now()）。**两钟必须同步**：API 与 DB 同主机或已 NTP 校准时窗口边界
// 精确；跨主机且未校准时，窗口以 API 进程钟为界，DB 钟偏移会造成窗口整体
// 平移（漏掉刚创建的 incident 或把未来时间戳算进窗）。同仓库默认部署
// （compose 同机）即满足；跨主机部署须保证 NTP 同步，否则 KPI 口径失真。
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
