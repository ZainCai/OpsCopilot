// W4-1.4 cmd 接线测试：env 解析 + 拓扑域函数端到端 + Sink 挂载。
package main

import (
	"testing"
	"time"

	"opscopilot/internal/connector"
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

func TestNewNoiseEngineOff(t *testing.T) {
	t.Setenv("OPS_NOISE_SHADOW", "off")
	ne, err := NewNoiseEngine(noiseTestSink(t), newQuietLogger())
	if err != nil {
		t.Fatalf("off mode must not error: %v", err)
	}
	if ne != nil {
		t.Fatal("OPS_NOISE_SHADOW=off must yield nil engine")
	}
}

func TestNewNoiseEngineInvalidWindow(t *testing.T) {
	t.Setenv("OPS_NOISE_WINDOW", "not-a-duration")
	if _, err := NewNoiseEngine(noiseTestSink(t), newQuietLogger()); err == nil {
		t.Fatal("invalid window must fail loudly, not silently default")
	}
}

func TestNewNoiseEngineDefaultWindow(t *testing.T) {
	ne, err := NewNoiseEngine(noiseTestSink(t), newQuietLogger())
	if err != nil || ne == nil {
		t.Fatalf("default construction: (%v, %v)", ne, err)
	}
	if !ne.Enabled() {
		t.Fatal("engine must be enabled by default")
	}
}

func TestProcessAlertsDomainMergeEndToEnd(t *testing.T) {
	t.Setenv("OPS_NOISE_WINDOW", "10m")
	sink := noiseTestSink(t)
	ne, err := NewNoiseEngine(sink, newQuietLogger())
	if err != nil {
		t.Fatalf("engine: %v", err)
	}
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
	t.Setenv("OPS_NOISE_WINDOW", "10m")
	ne, _ := NewNoiseEngine(noiseTestSink(t), newQuietLogger())
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
	t.Setenv("OPS_NOISE_WINDOW", "10m")
	ne, _ := NewNoiseEngine(noiseTestSink(t), newQuietLogger())
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
	t.Setenv("OPS_NOISE_WINDOW", "10m")
	sink := noiseTestSink(t)
	ne, err := NewNoiseEngine(sink, newQuietLogger())
	if err != nil {
		t.Fatalf("engine: %v", err)
	}
	sink.AttachNoise(ne)

	now := time.Now()
	err = sink.IngestCollect(nil, &connector.CollectResult{
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
