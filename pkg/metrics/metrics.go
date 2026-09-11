// Package metrics 极简指标原语 + Prometheus 文本暴露（M2 W9-4）。
//
// 定位：本项目只需要"观测 + 文本暴露"两件事，不引第三方依赖。
// prometheus/client_golang 会带进一整套注册表/采集模型/标签向量机制，
// 对当前规模（十来个指标）是净负担。
//
// 设计取舍：
//   - **标签在构造期固定**（不是 WithLabelValues）：本项目的维度集合
//     （渠道名、判决原因）在编译期就确定，固定标签省掉了向量查表与
//     标签集合一致性检查，观测路径零分配。
//   - **直方图同时保留桶计数与滑窗原始样本**：桶用于标准 Prometheus
//     聚合（rate/histogram_quantile）；滑窗用于直接给出**精确分位**
//     （p50/p95/p99 作为 gauge 暴露）。Prometheus 官方直方图只能给
//     桶内插值的近似分位，本项目要拿 P95 当验收证据，宁可要精确值。
//   - 并发安全：Counter 用原子；Histogram 用互斥（观测不在热循环里，
//     每告警一次）。
package metrics

import (
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// LabelSet 静态标签（渲染时按 key 排序，保证输出稳定可比对）。
type LabelSet map[string]string

// Counter 单调递增计数器。
type Counter struct {
	name   string
	help   string
	labels LabelSet
	v      atomic.Uint64
}

// Inc 加一。
func (c *Counter) Inc() { c.v.Add(1) }

// Add 加 n。
func (c *Counter) Add(n uint64) { c.v.Add(n) }

// Value 当前值。
func (c *Counter) Value() uint64 { return c.v.Load() }

// Histogram 直方图（桶计数 + 滑窗精确分位）。
//
// 桶语义与 Prometheus 一致：`upper` 升序，每个桶的计数是**累计**的
// （le 语义），最后一个隐含桶是 +Inf。
type Histogram struct {
	name   string
	help   string
	labels LabelSet
	upper  []float64
	window int

	mu      sync.Mutex
	counts  []uint64 // len(upper)+1；末位为 +Inf
	sum     float64
	samples []float64 // 滑窗原始样本（精确分位用）
	cursor  int       // 滑窗写指针：样本满窗后按环形覆写
}

// Observe 观测一个值。NaN/Inf 被丢弃：它们会污染 sum 与分位计算，
// 而这类值只可能来自调用方的计时错误，不该静默进入指标。
func (h *Histogram) Observe(v float64) {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return
	}
	if v < 0 {
		v = 0 // 时钟回拨等原因造成的负延迟归零，不产生负桶
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.sum += v
	idx := len(h.upper) // 默认落 +Inf 桶
	for i, ub := range h.upper {
		if v <= ub {
			idx = i
			break
		}
	}
	for i := idx; i < len(h.counts); i++ { // le 累计语义：所有 >= 该桶的上界桶都 +1
		h.counts[i]++
	}
	if h.window > 0 {
		if len(h.samples) < h.window {
			h.samples = append(h.samples, v)
		} else {
			h.samples[h.cursor] = v
			h.cursor = (h.cursor + 1) % h.window
		}
	}
}

// Count 观测总数。
func (h *Histogram) Count() uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.counts[len(h.counts)-1]
}

// Sum 观测值总和。
func (h *Histogram) Sum() float64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.sum
}

// Quantile 精确分位（在滑窗原始样本上计算；无样本返回 NaN）。
// q 取 [0,1]；越界收敛到端点。采用最近秩法（nearest-rank）：结果必定
// 是真实观测值，不做插值——验收要的是"实测到的 P95"。
func (h *Histogram) Quantile(q float64) float64 {
	h.mu.Lock()
	n := len(h.samples)
	if n == 0 {
		h.mu.Unlock()
		return math.NaN()
	}
	cp := make([]float64, n)
	copy(cp, h.samples)
	h.mu.Unlock()

	sort.Float64s(cp)
	if q <= 0 {
		return cp[0]
	}
	if q >= 1 {
		return cp[n-1]
	}
	// 最近秩：rank = ceil(q*n)，1-based
	rank := int(math.Ceil(q * float64(n)))
	if rank < 1 {
		rank = 1
	}
	if rank > n {
		rank = n
	}
	return cp[rank-1]
}

// Window 滑窗容量（分位基于最近 Window 个样本）。
func (h *Histogram) Window() int { return h.window }

// gaugeFunc 回调式 gauge（每次暴露时求值——用于读运行时状态，
// 如闸门累计计数，避免在两条路径上重复记账）。
type gaugeFunc struct {
	name   string
	help   string
	labels LabelSet
	f      func() float64
}

// Registry 指标注册表（持有输出顺序 = 注册顺序，保证文本稳定）。
type Registry struct {
	mu       sync.Mutex
	counters []*Counter
	hists    []*Histogram
	gauges   []*gaugeFunc
}

// New 构造空注册表。
func New() *Registry { return &Registry{} }

// Counter 注册并返回计数器（标签可为 nil）。
func (r *Registry) Counter(name, help string, labels LabelSet) *Counter {
	c := &Counter{name: name, help: help, labels: labels}
	r.mu.Lock()
	r.counters = append(r.counters, c)
	r.mu.Unlock()
	return c
}

// Histogram 注册并返回直方图。upper 必须升序；window <=0 时退化为
// 不保留原始样本（此时 Quantile 恒 NaN，桶仍然可用）。
func (r *Registry) Histogram(name, help string, labels LabelSet, upper []float64, window int) *Histogram {
	h := &Histogram{
		name: name, help: help, labels: labels,
		upper:   append([]float64(nil), upper...),
		window:  window,
		counts:  make([]uint64, len(upper)+1),
		samples: make([]float64, 0, window),
	}
	r.mu.Lock()
	r.hists = append(r.hists, h)
	r.mu.Unlock()
	return h
}

// GaugeFunc 注册回调 gauge（每次暴露求值一次）。
func (r *Registry) GaugeFunc(name, help string, labels LabelSet, f func() float64) {
	r.mu.Lock()
	r.gauges = append(r.gauges, &gaugeFunc{name: name, help: help, labels: labels, f: f})
	r.mu.Unlock()
}

// WritePrometheus 输出 Prometheus 文本格式（version 0.0.4 的无时间戳子集）。
// 输出顺序 = 注册顺序，同一进程多次暴露逐字节稳定（便于 diff 与工具解析）。
func (r *Registry) WritePrometheus(w io.Writer) error {
	r.mu.Lock()
	counters := append([]*Counter(nil), r.counters...)
	hists := append([]*Histogram(nil), r.hists...)
	gauges := append([]*gaugeFunc(nil), r.gauges...)
	r.mu.Unlock()

	for _, c := range counters {
		if err := writeHeader(w, c.name, c.help, "counter"); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(w, "%s%s %d\n", c.name, renderLabels(c.labels), c.Value()); err != nil {
			return err
		}
	}
	for _, h := range hists {
		if err := writeHistogram(w, h); err != nil {
			return err
		}
	}
	for _, g := range gauges {
		if err := writeHeader(w, g.name, g.help, "gauge"); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(w, "%s%s %s\n", g.name, renderLabels(g.labels),
			formatFloat(g.f())); err != nil {
			return err
		}
	}
	return nil
}

// writeHistogram 桶（le 累计）+ _sum/_count + 精确分位 gauge。
//
// 分位 gauge（name_p50/p95/p99）是**本包扩展**，不是 Prometheus 标准：
// HELP 里显式写明窗口容量，避免使用者误以为它是全量分位。
func writeHistogram(w io.Writer, h *Histogram) error {
	help := h.help
	if h.window > 0 {
		help += fmt.Sprintf(" (exact p50/p95/p99 gauges computed over the last %d samples)", h.window)
	}
	if err := writeHeader(w, h.name, help, "histogram"); err != nil {
		return err
	}
	h.mu.Lock()
	counts := append([]uint64(nil), h.counts...)
	sum := h.sum
	h.mu.Unlock()

	for i, ub := range h.upper {
		if _, err := fmt.Fprintf(w, "%s_bucket%s %d\n",
			h.name, renderLabels(withLE(h.labels, formatFloat(ub))), counts[i]); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintf(w, "%s_bucket%s %d\n",
		h.name, renderLabels(withLE(h.labels, "+Inf")), counts[len(counts)-1]); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "%s_sum%s %s\n", h.name, renderLabels(h.labels), formatFloat(sum)); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "%s_count%s %d\n", h.name, renderLabels(h.labels), counts[len(counts)-1]); err != nil {
		return err
	}
	if h.window <= 0 {
		return nil
	}
	for _, q := range []struct {
		suffix string
		q      float64
	}{{"p50", 0.50}, {"p95", 0.95}, {"p99", 0.99}} {
		name := h.name + "_" + q.suffix
		if err := writeHeader(w, name, helpQuantile(h, q.q), "gauge"); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(w, "%s%s %s\n", name, renderLabels(h.labels),
			formatFloat(h.Quantile(q.q))); err != nil {
			return err
		}
	}
	return nil
}

func helpQuantile(h *Histogram, q float64) string {
	return fmt.Sprintf("%s quantile %g over the last %d samples (exact, nearest-rank)",
		h.name, q, h.window)
}

func writeHeader(w io.Writer, name, help, typ string) error {
	if _, err := fmt.Fprintf(w, "# HELP %s %s\n", name, sanitizeHelp(help)); err != nil {
		return err
	}
	_, err := fmt.Fprintf(w, "# TYPE %s %s\n", name, typ)
	return err
}

// renderLabels 渲染 {k="v",...}（key 排序；空标签集返回空串）。
func renderLabels(labels LabelSet) string {
	if len(labels) == 0 {
		return ""
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(k)
		b.WriteString(`="`)
		b.WriteString(escapeLabelValue(labels[k]))
		b.WriteByte('"')
	}
	b.WriteByte('}')
	return b.String()
}

// withLE 复制标签集并加上 le（Prometheus 直方图桶约定）。
func withLE(labels LabelSet, le string) LabelSet {
	out := make(LabelSet, len(labels)+1)
	for k, v := range labels {
		out[k] = v
	}
	out["le"] = le
	return out
}

// escapeLabelValue 转义标签值中的反斜杠、双引号与换行（Prometheus 文本格式要求）。
func escapeLabelValue(v string) string {
	if !strings.ContainsAny(v, "\\\"\n") {
		return v
	}
	var b strings.Builder
	for _, r := range v {
		switch r {
		case '\\':
			b.WriteString(`\\`)
		case '"':
			b.WriteString(`\"`)
		case '\n':
			b.WriteString(`\n`)
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

// sanitizeHelp HELP 文本不能含换行（会破坏行格式）。
func sanitizeHelp(s string) string {
	return strings.ReplaceAll(strings.ReplaceAll(s, "\r", " "), "\n", " ")
}

// formatFloat 统一浮点渲染（g 格式，最短可往返表示）。
// NaN 渲染为 NaN（Prometheus 接受该字面量）。
func formatFloat(v float64) string {
	if math.IsNaN(v) {
		return "NaN"
	}
	if math.IsInf(v, 1) {
		return "+Inf"
	}
	if math.IsInf(v, -1) {
		return "-Inf"
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}
