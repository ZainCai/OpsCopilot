// W9-4 指标原语测试：桶累计语义、精确分位、滑窗、文本暴露格式。
package metrics

import (
	"math"
	"strings"
	"testing"
)

func TestCounterIncAddValue(t *testing.T) {
	r := New()
	c := r.Counter("x_total", "help", nil)
	c.Inc()
	c.Inc()
	c.Add(3)
	if got := c.Value(); got != 5 {
		t.Fatalf("value = %d, want 5", got)
	}
}

func TestHistogramCumulativeBuckets(t *testing.T) {
	h := New().Histogram("h", "help", nil, []float64{1, 2, 5}, 100)
	for _, v := range []float64{0.5, 1.5, 2.0, 3.0, 9.0} {
		h.Observe(v)
	}
	// le=1 → {0.5} =1；le=2 → {0.5,1.5,2.0} =3；le=5 → {…,3.0} =4；+Inf → 5
	if h.counts[0] != 1 || h.counts[1] != 3 || h.counts[2] != 4 || h.counts[3] != 5 {
		t.Fatalf("bucket counts = %v, want [1 3 4 5]", h.counts)
	}
	if h.Count() != 5 {
		t.Fatalf("count = %d, want 5", h.Count())
	}
	if h.Sum() != 16.0 {
		t.Fatalf("sum = %v, want 16", h.Sum())
	}
}

func TestHistogramQuantileNearestRank(t *testing.T) {
	h := New().Histogram("h", "help", nil, []float64{10}, 1000)
	// 1..100 → p95 应为 95（最近秩，取真实观测值）
	for i := 1; i <= 100; i++ {
		h.Observe(float64(i))
	}
	if got := h.Quantile(0.95); got != 95 {
		t.Fatalf("p95 = %v, want 95", got)
	}
	if got := h.Quantile(0.50); got != 50 {
		t.Fatalf("p50 = %v, want 50", got)
	}
	if got := h.Quantile(1); got != 100 {
		t.Fatalf("p100 = %v, want 100", got)
	}
	if got := h.Quantile(0); got != 1 {
		t.Fatalf("p0 = %v, want 1", got)
	}
}

func TestHistogramEmptyQuantileIsNaN(t *testing.T) {
	h := New().Histogram("h", "help", nil, []float64{1}, 10)
	if got := h.Quantile(0.95); !math.IsNaN(got) {
		t.Fatalf("empty quantile = %v, want NaN", got)
	}
}

func TestHistogramSlidingWindow(t *testing.T) {
	h := New().Histogram("h", "help", nil, []float64{1000}, 3)
	// 窗口 3：先塞 1,2,3，再塞 100,100,100 → 分位只看最近 3 个
	for _, v := range []float64{1, 2, 3} {
		h.Observe(v)
	}
	for i := 0; i < 3; i++ {
		h.Observe(100)
	}
	if got := h.Quantile(0.5); got != 100 {
		t.Fatalf("p50 after window rotation = %v, want 100", got)
	}
	if got := h.Count(); got != 6 {
		t.Fatalf("count = %d, want 6 (累计不受窗口影响)", got)
	}
	if got := h.Sum(); got != 306 {
		t.Fatalf("sum = %v, want 306", got)
	}
}

func TestHistogramIgnoresInvalidSamples(t *testing.T) {
	h := New().Histogram("h", "help", nil, []float64{1}, 10)
	h.Observe(math.NaN())
	h.Observe(math.Inf(1))
	h.Observe(-5) // 负值归零
	if got := h.Count(); got != 1 {
		t.Fatalf("count = %d, want 1 (NaN/Inf 丢弃，负值归零后仍计入)", got)
	}
	if got := h.Sum(); got != 0 {
		t.Fatalf("sum = %v, want 0", got)
	}
}

func TestWritePrometheusFormat(t *testing.T) {
	r := New()
	r.Counter("alerts_total", "processed alerts", nil)
	c := r.Counter("verdicts_total", "verdicts", LabelSet{"reason": "new-incident"})
	c.Add(7)
	h := r.Histogram("lat_seconds", "latency", nil, []float64{0.5, 1}, 10)
	h.Observe(0.25)
	h.Observe(0.75)
	h.Observe(3)
	r.GaugeFunc("gate_suppressed_total", "suppressed", nil, func() float64 { return 42 })

	var sb strings.Builder
	if err := r.WritePrometheus(&sb); err != nil {
		t.Fatalf("write: %v", err)
	}
	out := sb.String()

	wants := []string{
		"# TYPE alerts_total counter",
		"alerts_total 0",
		`verdicts_total{reason="new-incident"} 7`,
		"# TYPE lat_seconds histogram",
		`lat_seconds_bucket{le="0.5"} 1`,
		`lat_seconds_bucket{le="1"} 2`,
		`lat_seconds_bucket{le="+Inf"} 3`,
		"lat_seconds_sum 4",
		"lat_seconds_count 3",
		`lat_seconds_p95 `,
		"# TYPE gate_suppressed_total gauge",
		"gate_suppressed_total 42",
	}
	for _, w := range wants {
		if !strings.Contains(out, w) {
			t.Fatalf("exposition missing %q\n---\n%s", w, out)
		}
	}
	// 空的 p95 不应渲染成 0（无样本时是 NaN）
	if strings.Contains(out, "lat_seconds_p99 0\n") {
		t.Fatalf("empty p99 rendered as 0:\n%s", out)
	}
}

func TestWritePrometheusLabelEscaping(t *testing.T) {
	r := New()
	c := r.Counter("m_total", "h", LabelSet{"path": `a"b\c`})
	c.Inc()
	var sb strings.Builder
	if err := r.WritePrometheus(&sb); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sb.String(), `path="a\"b\\c"`) {
		t.Fatalf("label not escaped:\n%s", sb.String())
	}
}

func TestWritePrometheusStableOrder(t *testing.T) {
	r := New()
	r.Counter("b_total", "h", LabelSet{"z": "1", "a": "2"})
	var sb1, sb2 strings.Builder
	_ = r.WritePrometheus(&sb1)
	_ = r.WritePrometheus(&sb2)
	if sb1.String() != sb2.String() {
		t.Fatal("exposition not deterministic")
	}
	// 标签按 key 排序：a 在 z 前
	if !strings.Contains(sb1.String(), `{a="2",z="1"}`) {
		t.Fatalf("labels not sorted:\n%s", sb1.String())
	}
}
