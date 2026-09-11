// noise_gate_test.go W9-1 转正模式（ADR-011）：模式解析、enforce 闸门
// 执行、shadow 不碰闸门、计数持久化恢复。
package main

import (
	"os"
	"testing"
	"time"

	"opscopilot/internal/connector"
	"opscopilot/internal/notify"
)

// setNoiseEnv 设置 OPS_NOISE_MODE 并返回清理函数（测试互不污染）。
func setNoiseEnv(t *testing.T, mode string) {
	t.Helper()
	old, had := os.LookupEnv("OPS_NOISE_MODE")
	if mode == "" {
		os.Unsetenv("OPS_NOISE_MODE")
	} else {
		os.Setenv("OPS_NOISE_MODE", mode)
	}
	t.Cleanup(func() {
		if had {
			os.Setenv("OPS_NOISE_MODE", old)
		} else {
			os.Unsetenv("OPS_NOISE_MODE")
		}
	})
}

// recorderChannel 记录型渠道：断言通知真的发出来（且发了什么）。
type recorderChannel struct {
	msgs []notify.Message
}

func (c *recorderChannel) Name() string { return "recorder" }
func (c *recorderChannel) Send(m notify.Message) error {
	c.msgs = append(c.msgs, m)
	return nil
}

func TestNoiseModeDefaultShadow(t *testing.T) {
	setNoiseEnv(t, "")
	ne, err := NewNoiseEngine(noiseTestSink(t), newQuietLogger())
	if err != nil {
		t.Fatalf("default mode must parse: %v", err)
	}
	if ne.Mode() != ModeShadow {
		t.Fatalf("default mode = %q, want shadow", ne.Mode())
	}
}

func TestNoiseModeEnforceAndInvalid(t *testing.T) {
	setNoiseEnv(t, "enforce")
	ne, err := NewNoiseEngine(noiseTestSink(t), newQuietLogger())
	if err != nil {
		t.Fatalf("enforce must parse: %v", err)
	}
	if ne.Mode() != ModeEnforce || !ne.enforce {
		t.Fatalf("mode = %q enforce=%v, want enforce/true", ne.Mode(), ne.enforce)
	}

	setNoiseEnv(t, "bogus")
	if _, err := NewNoiseEngine(noiseTestSink(t), newQuietLogger()); err == nil {
		t.Fatal("invalid mode must fail-fast")
	}
}

// TestEnforceGateAdmitsNewSuppressesDedup enforce 模式：新指纹放行并
// 发通知，窗口内重复指纹被闸门拦截；shadow 模式闸门零调用。
func TestEnforceGateAdmitsNewSuppressesDedup(t *testing.T) {
	setNoiseEnv(t, "enforce")
	ne, err := NewNoiseEngine(noiseTestSink(t), newQuietLogger())
	if err != nil {
		t.Fatalf("engine: %v", err)
	}
	if err := os.Setenv("OPS_NOISE_WINDOW", "10m"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Unsetenv("OPS_NOISE_WINDOW") })
	ne, err = NewNoiseEngine(noiseTestSink(t), newQuietLogger()) // 重建带窗口
	if err != nil {
		t.Fatalf("engine rebuild: %v", err)
	}

	rec := &recorderChannel{}
	reg := notify.NewRegistry()
	reg.Register(rec)
	gate := notify.NewGate(reg)
	ne.SetGate(gate, nil)

	now := time.Now()
	ne.ProcessAlerts([]connector.Alert{
		{Fingerprint: "fp-gate-1", Labels: map[string]string{"alertname": "HighDisk"}, StartsAt: now},
	})
	if len(rec.msgs) != 1 {
		t.Fatalf("new fingerprint must dispatch 1 notification, got %d", len(rec.msgs))
	}
	ne.ProcessAlerts([]connector.Alert{
		{Fingerprint: "fp-gate-1", Labels: map[string]string{"alertname": "HighDisk"}, StartsAt: now.Add(30 * time.Second)},
	})
	if len(rec.msgs) != 1 {
		t.Fatalf("dedup fingerprint must be suppressed (still 1), got %d", len(rec.msgs))
	}
	st := gate.Stats()
	if st.Suppressed != 1 || st.Dispatched != 1 {
		t.Fatalf("gate stats = %+v, want suppressed=1 dispatched=1", st)
	}
}

// TestShadowNeverTouchesGate 影子纪律：影子模式下挂了闸门也绝不调用
// （告警全量放行是 W4-1.4 以来的硬约定）。
func TestShadowNeverTouchesGate(t *testing.T) {
	setNoiseEnv(t, "")
	ne, err := NewNoiseEngine(noiseTestSink(t), newQuietLogger())
	if err != nil {
		t.Fatalf("engine: %v", err)
	}
	rec := &recorderChannel{}
	reg := notify.NewRegistry()
	reg.Register(rec)
	ne.SetGate(notify.NewGate(reg), nil)

	ne.ProcessAlerts([]connector.Alert{
		{Fingerprint: "fp-shadow-1", Labels: map[string]string{"alertname": "X"}, StartsAt: time.Now()},
		{Fingerprint: "fp-shadow-1", Labels: map[string]string{"alertname": "X"}, StartsAt: time.Now().Add(time.Second)},
	})
	if len(rec.msgs) != 0 {
		t.Fatalf("shadow mode must never dispatch, got %d", len(rec.msgs))
	}
	if st := ne.gate.Stats(); st.Suppressed != 0 || st.Dispatched != 0 {
		t.Fatalf("shadow mode must not touch gate stats, got %+v", st)
	}
}

// TestGateStatsRestore 启动期恢复累计计数（R6-6：重启不清零）。
func TestGateStatsRestore(t *testing.T) {
	gate := notify.NewGate(notify.NewRegistry())
	gate.SetStats(notify.Stats{Suppressed: 7, Dispatched: 3})
	if _, err := gate.Admit(notify.Decision{ClusterKey: "c:1", WouldSuppress: true}); err != nil {
		t.Fatalf("admit suppress: %v", err)
	}
	st := gate.Stats()
	if st.Suppressed != 8 || st.Dispatched != 3 {
		t.Fatalf("stats after restore+admit = %+v, want suppressed=8 dispatched=3", st)
	}
}
