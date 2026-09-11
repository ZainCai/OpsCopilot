// metrics.go W9-4 告警链路延迟打点 + /metrics 暴露（M1 出口标准 3 的直接证据）。
//
// 背景：M1 出口报告把"告警链路端到端延迟 P95<30s"判为**间接达标**——当时只有
// "判决 occurred_at 与注入器节拍逐条同秒对齐"的间接观察，建议 M2 补一个
// `verdict_written_at - alert_fired_at` 的字段级专项。本文件就是那个专项。
//
// 两个直方图（都是"越小越好"的秒级延迟）：
//
//	opscopilot_alert_fired_to_verdict_seconds —— 告警发射 → 判决**落库完成**
//	    口径 = verdict_written_at − alert_fired_at（M1 报告点名的那个）
//	opscopilot_alert_fired_to_notify_seconds  —— 告警发射 → 通知**送达完成**
//	    只在 enforce 模式、且该判决被放行时观测（影子模式无通知，自然无样本）
//
// 为什么"落库完成"而不是"内存判决"：M1 报告要的是可对账的落库时刻；
// 且落库是链路里最慢的一环（同步 IO），用内存时刻会系统性低估。
//
// 诚实边界（写在这里也写进报告）：
//   - `alert_fired_at` 取上游 alert 的 startsAt/activeAt。若源侧告警已积压
//     （activeAt 远早于我们采集时刻），该差值里就含"告警年龄"，其量级由
//     采集节拍决定（默认 30s 轮询 → 上界 30s）。这与 opscopilot 自身处理
//     耗时是两件事，报告里分开说。
//   - startsAt 缺失的告警不观测（用合成时间打点等于自欺）。这类条数记进
//     `opscopilot_alert_latency_skipped_total{stage}`，让"处理数 − 观测数"
//     的任何差值都能自解释（另一个差值是快照非原子：processed 在锁内
//     累加、延迟观测在锁外 IO 之后，抓取瞬间可能读到中间态）。
package main

import (
	"net/http"
	"time"

	"opscopilot/internal/noise"
	"opscopilot/pkg/metrics"
)

// latencyBuckets 延迟桶上界（秒）。覆盖 5ms~30s：下限对应系统内处理，
// 上限对齐 M1 标准 3 的 30s 红线（+Inf 桶兜住超出部分）。
var latencyBuckets = []float64{
	0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 20, 30,
}

// latencyWindow 精确分位的滑窗容量。运维指标看的是"最近一段时间的分布"，
// 而不是开机以来的全量均值——10000 个样本在 30s 采集节拍下≈足够长的近期窗口。
const latencyWindow = 10000

// AppMetrics 应用指标集 + /metrics 处理器。
// nil 安全：NoiseEngine 侧对 nil 做判空（指标缺失不该影响告警链路）。
type AppMetrics struct {
	reg *metrics.Registry

	// AlertsProcessed 进入降噪引擎的告警条数（含被去重/并簇的）。
	AlertsProcessed *metrics.Counter
	// AlertsToVerdict 告警发射 → 判决落库完成的延迟。
	AlertsToVerdict *metrics.Histogram
	// AlertsToNotify 告警发射 → 通知送达完成的延迟（仅 enforce + 放行）。
	AlertsToNotify *metrics.Histogram
	// Verdicts 按判决原因分桶（标签固定，编译期确定维度集合）。
	Verdicts map[string]*metrics.Counter
	// LatencySkipped 因上游未给发射时刻（startsAt/activeAt 皆空）而未参与
	// 延迟观测的条数，按阶段（verdict / notify）分桶。
	//
	// 为什么需要它：AlertsProcessed / Verdicts 在引擎锁内累加，而延迟观测
	// 发生在锁外（落库读完 / 渠道 Send 之后）——抓取瞬间可能读到"处理了
	// N 条却只观测到 M 条"的瞬时不一致。有了 skipped 就可自解释：
	// M + skipped == N 即稳定态，差值只是快照非原子。
	// 它同时是"源侧没给发射时刻"的唯一可观察信号（该情形下延迟指标
	// 分母会系统性偏小，报告须声明）。
	LatencySkipped map[string]*metrics.Counter
}

// latencyStages 延迟观测的两种阶段（跳过计数器的固定维度集合）。
var latencyStages = []string{"verdict", "notify"}

// verdictReasons 判决原因的全部取值（+ 未参与判定的空串）。
// 固定集合 → 计数器在构造期建好，观测路径只做 map 查表。
var verdictReasons = []string{
	noise.ReasonNewIncident,
	noise.ReasonDedupWindow,
	noise.ReasonClusterMerge,
	"unjudged", // Reason == ""：无指纹/未参与判定
	"other",    // 兜底：新增原因但忘了在这里登记时，不会静默丢样本
}

// NewAppMetrics 构造并注册全部指标。
func NewAppMetrics() *AppMetrics {
	reg := metrics.New()
	m := &AppMetrics{
		reg:             reg,
		AlertsProcessed: reg.Counter("opscopilot_alerts_processed_total", "Alerts passed through the shadow noise engine", nil),
		AlertsToVerdict: reg.Histogram("opscopilot_alert_fired_to_verdict_seconds",
			"Latency from alert fired (startsAt) to verdict persisted (M1 exit criterion 3)", nil, latencyBuckets, latencyWindow),
		AlertsToNotify: reg.Histogram("opscopilot_alert_fired_to_notify_seconds",
			"Latency from alert fired (startsAt) to notification delivered (enforce mode, admitted only)", nil, latencyBuckets, latencyWindow),
		Verdicts:       make(map[string]*metrics.Counter, len(verdictReasons)),
		LatencySkipped: make(map[string]*metrics.Counter, len(latencyStages)),
	}
	for _, r := range verdictReasons {
		m.Verdicts[r] = reg.Counter("opscopilot_noise_verdicts_total",
			"Verdicts by convergence reason", metrics.LabelSet{"reason": r})
	}
	for _, s := range latencyStages {
		m.LatencySkipped[s] = reg.Counter("opscopilot_alert_latency_skipped_total",
			"Alerts excluded from latency observation because the source provided no fired time", metrics.LabelSet{"stage": s})
	}
	return m
}

// Registry 暴露底层注册表（装配层挂 GaugeFunc 用）。
func (m *AppMetrics) Registry() *metrics.Registry {
	if m == nil {
		return nil
	}
	return m.reg
}

// CountAlert 计一条告警。
func (m *AppMetrics) CountAlert() {
	if m == nil {
		return
	}
	m.AlertsProcessed.Inc()
}

// CountVerdict 计一条判决（按原因分桶）。
func (m *AppMetrics) CountVerdict(reason string) {
	if m == nil {
		return
	}
	c, ok := m.Verdicts[reason]
	if !ok {
		if reason == "" {
			c = m.Verdicts["unjudged"]
		} else {
			c = m.Verdicts["other"]
		}
	}
	c.Inc()
}

// CountLatencySkipped 计一条"因缺发射时刻而未观测延迟"。
// stage 取 "verdict" / "notify"；未知阶段归 notify 之外的兜底无意义——
// 两处调用点都是字面量，这里直接查表，缺键说明代码与 latencyStages 脱节，
// 静默忽略即可（不 panic，指标不该拖垮告警链路）。
func (m *AppMetrics) CountLatencySkipped(stage string) {
	if m == nil {
		return
	}
	if c, ok := m.LatencySkipped[stage]; ok {
		c.Inc()
	}
}

// ObserveVerdictLatency 记录"发射 → 判决落库"延迟。
func (m *AppMetrics) ObserveVerdictLatency(d time.Duration) {
	if m == nil {
		return
	}
	m.AlertsToVerdict.Observe(d.Seconds())
}

// ObserveNotifyLatency 记录"发射 → 通知送达"延迟。
func (m *AppMetrics) ObserveNotifyLatency(d time.Duration) {
	if m == nil {
		return
	}
	m.AlertsToNotify.Observe(d.Seconds())
}

// Handler /metrics 暴露（Prometheus 文本格式）。
//
// 读路径无鉴权：与既有 GET 读端点一致（只暴露聚合数字，不含簇键/URL 等
// 敏感原文）。Cache-Control: no-store——抓取端与浏览器都不该缓存指标。
func (m *AppMetrics) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		if m == nil || m.reg == nil {
			writeErr(w, http.StatusServiceUnavailable, "metrics not wired")
			return
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store, must-revalidate")
		w.WriteHeader(http.StatusOK)
		if err := m.reg.WritePrometheus(w); err != nil {
			// 响应头已发出，只能记日志（写一半的指标比不写更危险，
			// 但已无补救手段——错误只会来自底层 writer）。
			return
		}
	}
}
