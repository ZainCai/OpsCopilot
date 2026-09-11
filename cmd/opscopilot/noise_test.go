// W4-1.4 cmd 接线测试：配置注入 + 拓扑域函数端到端 + Sink 挂载。
// （#2/#10：env 解析与非法值 fail-fast 收敛进 internal/config；本文件改为
// 直接构造 config.NoiseSection 参数，不再 t.Setenv。）
package main

import (
	"strings"
	"testing"
	"time"

	"opscopilot/internal/config"
	"opscopilot/internal/connector"
	"opscopilot/internal/noise"
	"opscopilot/internal/topology"
)

func noiseTestSink(t *testing.T) *TopologySink {
	t.Helper()
	b := topology.NewBuilder()
	t0 := time.Now()
	// n1 - n2 连通（medium 因果边），n3 孤立；labels.instance 供反查。
	if err := b.AddDiscovery([]topology.NodeInput{
		{Key: "prom://n1", Type: "node", Labels: map[string]string{"instance": "i1"}, ObservedAt: t0},
		{Key: "prom://n2", Type: "node", Labels: map[string]string{"instance": "i2"}, ObservedAt: t0},
		{Key: "prom://n3", Type: "node", Labels: map[string]string{"instance": "i3"}, ObservedAt: t0},
	}); err != nil {
		t.Fatalf("AddDiscovery: %v", err)
	}
	if err := b.AddEdge(topology.EdgeInput{SrcKey: "prom://n1", DstKey: "prom://n2",
		Relation: "depends_on", ObservedAt: t0, Confidence: topology.ConfidenceMedium}); err != nil {
		t.Fatalf("AddEdge: %v", err)
	}
	sink, err := NewTopologySink(b, newQuietLogger())
	if err != nil {
		t.Fatalf("NewTopologySink: %v", err)
	}
	return sink
}

// noiseSpec 降噪配置基线（默认开、10m 窗、shadow）+ 按需覆写。
func noiseSpec(mut func(*config.NoiseSection)) config.NoiseSection {
	s := config.Defaults().Noise
	if mut != nil {
		mut(&s)
	}
	return s
}

// newTestNoiseEngine 构造默认启用的引擎（窗口 10m，与 config 默认同源）。
func newTestNoiseEngine(t *testing.T, sink *TopologySink) *NoiseEngine {
	t.Helper()
	ne := NewNoiseEngine(sink, newQuietLogger(), testTenant, noiseSpec(nil), config.MemLimitSection{})
	if ne == nil {
		t.Fatal("expected enabled engine")
	}
	return ne
}

func TestNewNoiseEngineOff(t *testing.T) {
	ne := NewNoiseEngine(noiseTestSink(t), newQuietLogger(), testTenant,
		noiseSpec(func(s *config.NoiseSection) { s.Enabled = false }), config.MemLimitSection{})
	if ne != nil {
		t.Fatal("Enabled=false must yield nil engine")
	}
}

// TestNewNoiseEngineDefaultWindow 默认构造：启用、shadow、10m 窗。
func TestNewNoiseEngineDefaultWindow(t *testing.T) {
	ne := newTestNoiseEngine(t, noiseTestSink(t))
	if !ne.Enabled() {
		t.Fatal("engine must be enabled by default")
	}
	if ne.Mode() != ModeShadow {
		t.Fatalf("mode = %q, want shadow", ne.Mode())
	}
}

// TestNoiseInvalidValuesFailAtLoad 非法窗口/模式（原 NewNoiseEngine 的
// 构造期错误，含 F1 "0s 不得 panic" 回归）现由 config.Load 聚合 fail-fast。
func TestNoiseInvalidValuesFailAtLoad(t *testing.T) {
	env := func(m map[string]string) config.LookupFunc {
		return func(k string) (string, bool) { v, ok := m[k]; return v, ok }
	}
	base := map[string]string{
		config.EnvRedisAlertAddr: "127.0.0.1:6380",
		config.EnvRedisCacheAddr: "127.0.0.1:6381",
	}
	for _, c := range []struct {
		key, val, want string
	}{
		{config.EnvNoiseWindow, "not-a-duration", "OPS_NOISE_WINDOW"},
		{config.EnvNoiseWindow, "0s", "OPS_NOISE_WINDOW"}, // F1：零值同样干净拒绝，不 panic
		{config.EnvNoiseMode, "yolo", "OPS_NOISE_MODE"},
	} {
		m := map[string]string{}
		for k, v := range base {
			m[k] = v
		}
		m[c.key] = c.val
		_, err := config.LoadFrom(env(m))
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Fatalf("%s=%q: want error mentioning %s, got %v", c.key, c.val, c.want, err)
		}
	}
}

func TestProcessAlertsDomainMergeEndToEnd(t *testing.T) {
	sink := noiseTestSink(t)
	ne := newTestNoiseEngine(t, sink)
	now := time.Now()
	alerts := []connector.Alert{
		{Fingerprint: "fpA", Labels: map[string]string{"instance": "i1", "alertname": "HighCPU"}, Severity: "warning", StartsAt: now},
		{Fingerprint: "fpB", Labels: map[string]string{"instance": "i2", "alertname": "HighLoad"}, Severity: "critical", StartsAt: now.Add(time.Second)},
		{Fingerprint: "fpC", Labels: map[string]string{"instance": "i3", "alertname": "DiskFull"}, Severity: "warning", StartsAt: now.Add(2 * time.Second)},
	}
	ne.ProcessAlerts(alerts)

	total, converged := ne.Stats()
	if total != 3 {
		t.Fatalf("total = %d, want 3", total)
	}
	// fpA 与 fpB 经 n1-n2 因果边并簇 → 1 次收敛；fpC 孤立 → 新事件。
	if converged != 1 {
		t.Fatalf("converged = %d, want 1", converged)
	}
	// 簇结构验证：fpA 簇含 n1/n2 两节点与两个指纹。
	clusters := ne.shadow.Clusterer().Clusters()
	if len(clusters) != 2 {
		t.Fatalf("cluster count = %d, want 2 (merged pair + isolated)", len(clusters))
	}
	for _, cl := range clusters {
		if len(cl.Fingerprints) == 2 {
			if len(cl.NodeKeys) != 2 {
				t.Fatalf("merged cluster NodeKeys = %d, want 2", len(cl.NodeKeys))
			}
			if cl.Severity != "critical" {
				t.Fatalf("merged cluster severity = %s, want critical", cl.Severity)
			}
			return
		}
	}
	t.Fatal("no merged cluster found")
}

func TestProcessAlertsEmptyFingerprintUsesFallback(t *testing.T) {
	ne := newTestNoiseEngine(t, noiseTestSink(t))
	now := time.Now()
	ne.ProcessAlerts([]connector.Alert{
		{Labels: map[string]string{"instance": "i1", "alertname": "X"}, StartsAt: now},
		{Labels: map[string]string{"instance": "i1", "alertname": "X"}, StartsAt: now.Add(time.Second)},
	})
	total, converged := ne.Stats()
	if total != 2 || converged != 1 {
		t.Fatalf("fallback fingerprint: total=%d converged=%d, want 2/1", total, converged)
	}
}

func TestProcessAlertsUnknownInstanceNoNodeKey(t *testing.T) {
	ne := newTestNoiseEngine(t, noiseTestSink(t))
	now := time.Now()
	// instance 不在拓扑里 → NodeKey 空 → 只能靠指纹/不聚类，不 panic。
	ne.ProcessAlerts([]connector.Alert{
		{Fingerprint: "fpGhost", Labels: map[string]string{"instance": "ghost"}, StartsAt: now},
	})
	total, converged := ne.Stats()
	if total != 1 || converged != 0 {
		t.Fatalf("ghost instance: total=%d converged=%d, want 1/0", total, converged)
	}
}

// TestSinkIngestCollectTriggersNoise 端到端：Host 投递路径
// IngestCollect → NoiseEngine 自动转交（挂载生效）。
func TestSinkIngestCollectTriggersNoise(t *testing.T) {
	sink := noiseTestSink(t)
	ne := newTestNoiseEngine(t, sink)
	sink.AttachNoise(ne)

	now := time.Now()
	err := sink.IngestCollect(nil, &connector.CollectResult{
		Alerts: []connector.Alert{
			{Fingerprint: "fp1", Labels: map[string]string{"instance": "i1"}, StartsAt: now},
			{Fingerprint: "fp1", Labels: map[string]string{"instance": "i1"}, StartsAt: now.Add(time.Minute)},
		},
	})
	if err != nil {
		t.Fatalf("IngestCollect: %v", err)
	}
	total, converged := ne.Stats()
	if total != 2 || converged != 1 {
		t.Fatalf("via sink: total=%d converged=%d, want 2/1", total, converged)
	}
	if sink.AlertCount() != 2 {
		t.Fatalf("sink alert count = %d, want 2 (计数与影子并存)", sink.AlertCount())
	}
}

// TestNilEngineTolerated 未挂引擎（或 off）时 Sink 正常工作——W3 行为回归。
func TestNilEngineTolerated(t *testing.T) {
	sink := noiseTestSink(t)
	sink.AttachNoise(nil) // 显式卸载
	err := sink.IngestCollect(nil, &connector.CollectResult{
		Alerts: []connector.Alert{{Fingerprint: "fp1"}},
	})
	if err != nil {
		t.Fatalf("IngestCollect with nil engine: %v", err)
	}
	if sink.AlertCount() != 1 {
		t.Fatalf("alert count = %d, want 1", sink.AlertCount())
	}
	// nil 引擎的方法调用安全（指针接收者 nil 检查）。
	if total, _ := (*NoiseEngine)(nil).Stats(); total != 0 {
		t.Fatal("nil engine Stats must be zero")
	}
	if (*NoiseEngine)(nil).Enabled() {
		t.Fatal("nil engine must not be enabled")
	}
	(*NoiseEngine)(nil).ProcessAlerts([]connector.Alert{{Fingerprint: "x"}}) // 不得 panic
}

// ---- 第四轮扫描修复回归 ----

// countingSink 记录 SaveCluster 调用次数（F3 签名去重验证）。
type countingSink struct {
	calls int
}

func (c *countingSink) SaveCluster(noise.ClusterRecord) error { c.calls++; return nil }

// TestPersistSignatureDedup F3 回归：状态未变的簇不重复落库；
// 状态变化（新告警）后才再次落库。
func TestPersistSignatureDedup(t *testing.T) {
	engine := newTestNoiseEngine(t, noiseTestSink(t))
	cs := &countingSink{}
	engine.SetRecordSink(cs)

	now := time.Now()
	engine.ProcessAlerts([]connector.Alert{
		{Fingerprint: "fp1", Labels: map[string]string{"instance": "i1"}, StartsAt: now},
	})
	first := cs.calls
	if first != 1 {
		t.Fatalf("first batch saves = %d, want 1", first)
	}
	// 第二批：窗口内重复指纹（LastSeen/AlertCount 更新 → 签名变化 → 仍落一次）。
	engine.ProcessAlerts([]connector.Alert{
		{Fingerprint: "fp1", Labels: map[string]string{"instance": "i1"}, StartsAt: now.Add(time.Minute)},
	})
	if cs.calls != 2 {
		t.Fatalf("second batch saves = %d, want 2 (AlertCount is a persisted field)", cs.calls)
	}
	// 第三批：无关告警（独立指纹+未知 instance 自成簇）——只落新簇，
	// fp1 簇状态未变不重写（F3 写放大修复的核心断言）。
	engine.ProcessAlerts([]connector.Alert{
		{Fingerprint: "fp2", Labels: map[string]string{"instance": "ghost"}, StartsAt: now.Add(2 * time.Minute)},
	})
	if cs.calls != 3 {
		t.Fatalf("unrelated batch must persist only the new cluster, saves = %d, want 3", cs.calls)
	}
}

// ---- 第五轮审核修复回归 ----

// TestPersistFailureRollsBackSignature G1 回归：Save 失败回滚签名，
// 下一批必须重试补写（修复前签名已标记成功，失败写入永不补齐）。
func TestPersistFailureRollsBackSignature(t *testing.T) {
	engine := newTestNoiseEngine(t, noiseTestSink(t))

	// 先让一批失败：签名应被回滚。
	flaky := &flakySink{fail: true}
	engine.SetRecordSink(flaky)
	now := time.Now()
	engine.ProcessAlerts([]connector.Alert{
		{Fingerprint: "fpR", Labels: map[string]string{"instance": "i1"}, StartsAt: now},
	})
	if flaky.calls != 1 {
		t.Fatalf("failing sink calls = %d, want 1", flaky.calls)
	}

	// 恢复成功：下一批（无新告警也要触发）必须补写。
	flaky.fail = false
	engine.ProcessAlerts([]connector.Alert{
		{Fingerprint: "fpR2", Labels: map[string]string{"instance": "i3"}, StartsAt: now.Add(time.Second)},
	})
	// fpR 簇补写 + fpR2 新簇 = 2 次成功调用。
	if flaky.okCalls != 2 {
		t.Fatalf("after recovery okCalls = %d, want 2 (failed write must be retried)", flaky.okCalls)
	}
}

type flakySink struct {
	fail    bool
	calls   int // 总调用次数
	okCalls int // 成功次数
}

func (f *flakySink) SaveCluster(rec noise.ClusterRecord) error {
	f.calls++
	if f.fail {
		return errPersistFailed
	}
	f.okCalls++
	return nil
}

// TestZeroStartsAtFallback G2 回归：startsAt 缺失（零值）的告警兜底为
// 当前时刻——不进 1970 桶、不被去重吞掉。
func TestZeroStartsAtFallback(t *testing.T) {
	engine := newTestNoiseEngine(t, noiseTestSink(t))

	// 两条同指纹、均零值 StartsAt 的告警：若不兜底，第二条会被判窗口内重复。
	engine.ProcessAlerts([]connector.Alert{
		{Fingerprint: "fpZ", Labels: map[string]string{"instance": "i1"}, StartsAt: time.Time{}},
	})
	// 零值兜底后 anchor=各批的 now，两批间隔 <窗口仍会 dedup——这里断言
	// 簇的 LastSeen 是真实当前时间（不是 0001 年），即兜底生效。
	clusters := engine.shadow.Clusterer().ActiveClusters()
	if len(clusters) != 1 {
		t.Fatalf("clusters = %d, want 1", len(clusters))
	}
	if age := time.Since(clusters[0].LastSeen); age > time.Minute {
		t.Fatalf("zero StartsAt leaked into cluster time: LastSeen age = %v", age)
	}
}
