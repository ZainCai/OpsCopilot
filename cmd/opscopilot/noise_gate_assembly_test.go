// noise_gate_assembly_test.go W9-1 运行态验证：装配级 enforce 闸门
// ——告警经 TopologySink.IngestCollect 真实链路进闸门（"组件写了+
// 单测过了 ≠ 运行态生效"纪律：接线必须有测试）。
// （#2/#10：模式/窗口经 Config 参数直注，测试不动全局 env。）
package main

import (
	"os"
	"testing"
	"time"

	"opscopilot/internal/connector"
	"opscopilot/internal/notify"
)

// TestAssemblyEnforceGateWiring enforce 装配：真实链路（IngestCollect →
// NoiseEngine → Gate）新指纹放行、窗口重复拦截；shadow 装配闸门为零。
// 无 DB（计数内存态）——持久化语义由 noise_gate_pg_test 单独覆盖。
func TestAssemblyEnforceGateWiring(t *testing.T) {
	cfg := testAssemblyConfig("tok")
	cfg.Noise.Mode = ModeEnforce
	cfg.Noise.Window = 10 * time.Minute // 与原 OPS_NOISE_WINDOW=10m 等价
	asm, err := NewAssembly(newQuietLogger(), cfg)
	if err != nil {
		t.Fatalf("assembly: %v", err)
	}
	if asm.Noise == nil || asm.Noise.Mode() != ModeEnforce {
		t.Fatalf("mode = %q, want enforce", asm.Noise.Mode())
	}
	if asm.Noise.gate == nil {
		t.Fatal("enforce assembly must have gate attached")
	}

	now := time.Now()
	if err := asm.Sink.IngestCollect(nil, &connector.CollectResult{Alerts: []connector.Alert{
		{Fingerprint: "fp-asm-gate", Labels: map[string]string{"alertname": "HighDisk"}, StartsAt: now},
	}}); err != nil {
		t.Fatalf("IngestCollect: %v", err)
	}
	// 等锁外闸门执行完成（ProcessAlerts 同步，IngestCollect 返回即完成）。
	st := asm.Noise.gate.Stats()
	if st.Dispatched != 1 || st.Suppressed != 0 {
		t.Fatalf("after new alert: %+v, want dispatched=1", st)
	}
	if err := asm.Sink.IngestCollect(nil, &connector.CollectResult{Alerts: []connector.Alert{
		{Fingerprint: "fp-asm-gate", Labels: map[string]string{"alertname": "HighDisk"}, StartsAt: now.Add(30 * time.Second)},
	}}); err != nil {
		t.Fatalf("IngestCollect 2: %v", err)
	}
	st = asm.Noise.gate.Stats()
	if st.Dispatched != 1 || st.Suppressed != 1 {
		t.Fatalf("after dedup alert: %+v, want dispatched=1 suppressed=1", st)
	}
}

// TestAssemblyShadowNoGate 影子装配：闸门不挂（默认模式行为与 M1 一致）。
func TestAssemblyShadowNoGate(t *testing.T) {
	asm, err := NewAssembly(newQuietLogger(), testAssemblyConfig("tok")) // 默认 shadow
	if err != nil {
		t.Fatalf("assembly: %v", err)
	}
	if asm.Noise == nil || asm.Noise.Mode() != ModeShadow {
		t.Fatalf("mode = %v, want shadow", asm.Noise.Mode())
	}
	if asm.Noise.gate != nil {
		t.Fatal("shadow assembly must not attach gate")
	}
}

// TestAssemblyEnforceGatePGPersistence enforce + DB：闸门计数经共享池
// 真实落库（migrations/000011），装配断言无 gap。
//
// 密闭性：租户由 Config 注入（#10 去 DefaultTenant 全局）——本测试以
// default 租户写库，**必须先清零再装配**（NewAssembly 的 SetGate 会恢复
// 历史累计，装配后清零已经晚了）。
func TestAssemblyEnforceGatePGPersistence(t *testing.T) {
	dsn := os.Getenv("OPS_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("OPS_TEST_PG_DSN not set")
	}
	cfg := testAssemblyConfig("tok")
	cfg.DB.DSN = dsn
	cfg.Noise.Mode = ModeEnforce
	cfg.Noise.Window = 10 * time.Minute

	// 装配前清零历史累计（default 租户行可能由上次运行写入）。
	pre := pgSinkForTest(t)
	gsPre := &pgPoolGateStats{pool: pre.pool}
	tenant := testTenant
	if err := gsPre.SaveGateStats(tenant, notify.Stats{}); err != nil {
		t.Fatalf("pre-clean: %v", err)
	}
	t.Cleanup(func() { _ = gsPre.SaveGateStats(tenant, notify.Stats{}) })

	asm, err := NewAssembly(newQuietLogger(), cfg)
	if err != nil {
		t.Fatalf("assembly: %v", err)
	}
	defer asm.pool.Close()

	if err := asm.Sink.IngestCollect(nil, &connector.CollectResult{Alerts: []connector.Alert{
		{Fingerprint: "fp-asm-pg", Labels: map[string]string{"alertname": "X"}, StartsAt: time.Now()},
	}}); err != nil {
		t.Fatalf("IngestCollect: %v", err)
	}
	st := asm.Noise.gate.Stats()
	if st.Dispatched != 1 || st.Suppressed != 0 {
		t.Fatalf("memory stats = %+v, want dispatched=1 suppressed=0", st)
	}
	saved, err := gsPre.LoadGateStats(tenant)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if saved.Dispatched != 1 || saved.Suppressed != 0 {
		t.Fatalf("db row = %+v, want dispatched=1 suppressed=0", saved)
	}
}
