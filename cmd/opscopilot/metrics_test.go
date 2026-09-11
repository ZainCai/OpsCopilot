// metrics_test.go W9-4：告警链路延迟打点与 /metrics 暴露。
//
// 覆盖口径：① 打点只观测"有可靠发射时刻"的告警（startsAt 为空不观测）；
// ② fired→verdict 与 fired→notify 两个直方图都随真实链路增长；
// ③ /metrics 文本含桶/分位/计数行；④ AppMetrics 零值安全。
package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"opscopilot/internal/config"
	"opscopilot/internal/connector"
	"opscopilot/internal/notify"
)

// metricsSink 假渠道：记录收到的通知条数（并发安全）。
type metricsSink struct {
	mu  sync.Mutex
	got int
	srv *httptest.Server
}

func newMetricsSink(t *testing.T) *metricsSink {
	t.Helper()
	s := &metricsSink{}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		s.mu.Lock()
		s.got++
		s.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(s.srv.Close)
	return s
}

func (s *metricsSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.got
}

// TestMetricsLatencyInstrumentation 真链路打点：5 条告警（5 个独立节点 →
// 5 个独立故障域 → 5 条 new-incident → 5 次通知）。
func TestMetricsLatencyInstrumentation(t *testing.T) {
	dsn := os.Getenv("OPS_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("OPS_TEST_PG_DSN not set")
	}
	cfg := testAssemblyConfig("tok")
	cfg.DB.DSN = dsn
	cfg.Noise.Mode = ModeEnforce
	cfg.Noise.Window = config.DefaultNoiseWindow

	sink := newMetricsSink(t)
	pgSink := pgSinkForTest(t)
	cleanup := func() {
		pgExec(t, pgSink, `DELETE FROM notify_channel WHERE tenant_id = $1`, testTenant)
		pgExec(t, pgSink, `UPDATE notify_gate_stats SET suppressed=0, dispatched=0 WHERE tenant_id = $1`, testTenant)
	}
	cleanup()
	t.Cleanup(cleanup)

	store := NewChannelStore(pgSink.pool, testTenant)
	if err := store.Upsert(context.Background(), "m-sink", notify.KindGeneric, sink.srv.URL, "info", true); err != nil {
		t.Fatalf("seed channel: %v", err)
	}

	asm, err := NewAssembly(newQuietLogger(), cfg)
	if err != nil {
		t.Fatalf("assembly: %v", err)
	}
	defer asm.pool.Close()
	// 判决落库出口在 main 里接线（main.go: asm.Noise.SetVerdictSink(pgSink)）——
	// 本测试复现同一接线，否则"fired → verdict"链路不执行。
	// 先接线再登记 drain：t.Cleanup 逆序执行，保证 writer 在 pgSink 池关闭前退出。
	asm.Noise.SetVerdictSink(pgSink)
	t.Cleanup(asm.Noise.StopVerdictWriter) // 优化方案 #8：异步落库先 drain

	now := time.Now()
	// 5 个互不相邻的节点 → 5 个独立故障域（reachable 只在 a==b 或图上可达时为真）。
	nodes := make([]connector.ResourceNode, 0, 5)
	for i := 1; i <= 5; i++ {
		inst := "mnode" + string(rune('0'+i))
		nodes = append(nodes, connector.ResourceNode{
			Key: "prometheus://nodes/" + inst, Type: "node",
			Labels: map[string]string{"instance": inst}, ObservedAt: now,
		})
	}
	if err := asm.Sink.IngestDiscover(context.Background(), &connector.DiscoverResult{Nodes: nodes}); err != nil {
		t.Fatalf("IngestDiscover: %v", err)
	}

	// 每条告警带不同的"发射时刻偏移"，延迟应≈该偏移 + 处理耗时。
	offsets := []time.Duration{30 * time.Millisecond, 80 * time.Millisecond, 150 * time.Millisecond, 300 * time.Millisecond, 600 * time.Millisecond}
	alerts := make([]connector.Alert, 0, len(offsets))
	for i, off := range offsets {
		inst := "mnode" + string(rune('0'+i+1))
		alerts = append(alerts, connector.Alert{
			Fingerprint: "fp-m-" + inst,
			Labels:      map[string]string{"alertname": "HighDisk", "instance": inst},
			StartsAt:    now.Add(-off), // 告警在这之前就发射了
			Severity:    "critical",
		})
	}
	if err := asm.Sink.IngestCollect(context.Background(), &connector.CollectResult{Alerts: alerts}); err != nil {
		t.Fatalf("IngestCollect: %v", err)
	}

	m := asm.Metrics
	if m == nil {
		t.Fatal("assembly metrics not wired")
	}
	if got := m.AlertsProcessed.Value(); got != 5 {
		t.Fatalf("alerts_processed = %d, want 5", got)
	}
	if got := m.Verdicts["new-incident"].Value(); got != 5 {
		t.Fatalf(`verdicts{reason="new-incident"} = %d, want 5`, got)
	}
	if got := m.AlertsToVerdict.Count(); got != 5 {
		t.Fatalf("fired_to_verdict count = %d, want 5", got)
	}
	if got := m.AlertsToNotify.Count(); got != 5 {
		t.Fatalf("fired_to_notify count = %d, want 5", got)
	}
	if sink.count() != 5 {
		t.Fatalf("sink notifications = %d, want 5", sink.count())
	}
	// 发射时刻齐全 → 不该有任何跳过（恒等式：观测数 + skipped == 处理数）。
	for _, stage := range latencyStages {
		if got := m.LatencySkipped[stage].Value(); got != 0 {
			t.Fatalf(`latency_skipped{stage=%q} = %d, want 0`, stage, got)
		}
	}
	// P95 应 ≥ 最大偏移（600ms），且远小于 30s 红线。
	p95 := m.AlertsToVerdict.Quantile(0.95)
	if p95 < 0.6 || p95 > 30 {
		t.Fatalf("fired_to_verdict p95 = %v s, want in [0.6, 30]", p95)
	}
	if np95 := m.AlertsToNotify.Quantile(0.95); np95 < 0.6 || np95 > 30 {
		t.Fatalf("fired_to_notify p95 = %v s, want in [0.6, 30]", np95)
	}

	// /metrics 文本暴露
	rec := httptest.NewRecorder()
	asm.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/metrics = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{
		"opscopilot_alerts_processed_total 5",
		`opscopilot_noise_verdicts_total{reason="new-incident"} 5`,
		"# TYPE opscopilot_alert_fired_to_verdict_seconds histogram",
		"opscopilot_alert_fired_to_verdict_seconds_count 5",
		"opscopilot_alert_fired_to_verdict_seconds_p95 ",
		"opscopilot_alert_fired_to_notify_seconds_count 5",
		"opscopilot_alert_fired_to_notify_seconds_p95 ",
		"opscopilot_noise_gate_dispatched_total 5",
		`opscopilot_alert_fired_to_verdict_seconds_bucket{le="30"}`,
		`opscopilot_alert_latency_skipped_total{stage="verdict"} 0`,
		`opscopilot_alert_latency_skipped_total{stage="notify"} 0`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("/metrics missing %q\n---\n%s", want, body)
		}
	}
}

// TestMetricsSkipsAlertsWithoutStartsAt 无 startsAt 的告警照常计数，
// 但不进延迟直方图（用合成时间打点等于自欺），且**跳过必须留痕**——
// 否则"processed 1 / 观测 0"会被误读成丢样本。
// 配了渠道（否则 Gate 返回 ErrNoChannel → 走异常分支，notify 跳过路径测不到）。
func TestMetricsSkipsAlertsWithoutStartsAt(t *testing.T) {
	dsn := os.Getenv("OPS_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("OPS_TEST_PG_DSN not set")
	}
	cfg := testAssemblyConfig("tok")
	cfg.DB.DSN = dsn
	cfg.Noise.Mode = ModeEnforce

	sink := newMetricsSink(t)
	pgSink := pgSinkForTest(t)
	cleanup := func() {
		pgExec(t, pgSink, `DELETE FROM notify_channel WHERE tenant_id = $1`, testTenant)
		pgExec(t, pgSink, `UPDATE notify_gate_stats SET suppressed=0, dispatched=0 WHERE tenant_id = $1`, testTenant)
	}
	cleanup()
	t.Cleanup(cleanup)

	store := NewChannelStore(pgSink.pool, testTenant)
	if err := store.Upsert(context.Background(), "m-sink", notify.KindGeneric, sink.srv.URL, "info", true); err != nil {
		t.Fatalf("seed channel: %v", err)
	}

	asm, err := NewAssembly(newQuietLogger(), cfg)
	if err != nil {
		t.Fatalf("assembly: %v", err)
	}
	defer asm.pool.Close()
	// 必须接线判决落库出口：否则判决分发（submitVerdicts）整个不执行，
	// "观测数为 0" 会因**没跑落库**而空过——测不到 startsAt 缺失这条路径。
	// 先接线再登记 drain（Cleanup 逆序：writer 先退、池后关）。
	asm.Noise.SetVerdictSink(pgSink)
	t.Cleanup(asm.Noise.StopVerdictWriter) // 优化方案 #8：异步落库先 drain

	now := time.Now()
	if err := asm.Sink.IngestDiscover(context.Background(), &connector.DiscoverResult{Nodes: []connector.ResourceNode{
		{Key: "prometheus://nodes/nostart", Type: "node", Labels: map[string]string{"instance": "nostart"}, ObservedAt: now},
	}}); err != nil {
		t.Fatalf("IngestDiscover: %v", err)
	}
	// StartsAt 零值
	if err := asm.Sink.IngestCollect(context.Background(), &connector.CollectResult{Alerts: []connector.Alert{
		{Fingerprint: "fp-nostart", Labels: map[string]string{"alertname": "X", "instance": "nostart"}},
	}}); err != nil {
		t.Fatalf("IngestCollect: %v", err)
	}

	m := asm.Metrics
	if got := m.AlertsProcessed.Value(); got != 1 {
		t.Fatalf("alerts_processed = %d, want 1", got)
	}
	if got := m.AlertsToVerdict.Count(); got != 0 {
		t.Fatalf("fired_to_verdict count = %d, want 0 (no startsAt → no latency sample)", got)
	}
	// 跳过必须留痕：否则"processed 1 / 观测 0"无法自解释。
	if got := m.LatencySkipped["verdict"].Value(); got != 1 {
		t.Fatalf(`latency_skipped{stage="verdict"} = %d, want 1`, got)
	}
	if got := m.LatencySkipped["notify"].Value(); got != 1 {
		t.Fatalf(`latency_skipped{stage="notify"} = %d, want 1 (admitted but no fired time)`, got)
	}
}

// TestAppMetricsNilSafe 零值接收者不 panic（指标缺失不得影响告警链路）。
func TestAppMetricsNilSafe(t *testing.T) {
	var m *AppMetrics
	m.CountAlert()
	m.CountVerdict("new-incident")
	m.ObserveVerdictLatency(time.Second)
	m.ObserveNotifyLatency(time.Second)
	m.CountLatencySkipped("verdict")
	if m.Registry() != nil {
		t.Fatal("nil metrics registry must be nil")
	}
	rec := httptest.NewRecorder()
	m.Handler()(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("nil metrics /metrics = %d, want 503", rec.Code)
	}
}
