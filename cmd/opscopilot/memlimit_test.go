// memlimit_test.go 优化方案 #6（cmd 侧）：DB 缺席降级路径的内存有界化——
// 升级台账 / 内存审计 / 簇落库签名缓存的触限淘汰 + 装配层 /metrics 文本。
package main

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"opscopilot/internal/config"
	"opscopilot/internal/connector"
	"opscopilot/internal/noise"
	"opscopilot/pkg/memguard"
	"opscopilot/pkg/metrics"
)

// TestMemEscalationLedgerBounded 台账超上限：最久未活跃（认领最早）出局；
// 被逐单再被扫到允许重新认领（幂等窗有限化，重启语义的延伸，文档已声明）。
func TestMemEscalationLedgerBounded(t *testing.T) {
	guard := memguard.New("escalation_ledger", 2, 0)
	l := newMemEscalationLedgerWithLimits(guard)
	ctx := context.Background()
	for i, id := range []string{"i-1", "i-2", "i-3"} {
		ok, err := l.Claim(ctx, id)
		if err != nil || !ok {
			t.Fatalf("claim %s (#%d) = %v/%v, want true/nil", id, i, ok, err)
		}
		time.Sleep(2 * time.Millisecond) // 保证认领时刻严格有序
	}
	if l.Size() != 2 {
		t.Fatalf("size = %d, want 2 (cap)", l.Size())
	}
	ok, _ := l.Claim(ctx, "i-1")
	if !ok {
		t.Fatal("被逐的 i-1 再认领应成功（淘汰语义）")
	} // 新认领再次触限 → i-2（此刻最久未活跃）出局；剩 {i-1, i-3}
	if got := guard.Evictions(); got != 2 {
		t.Fatalf("evictions = %d, want 2（两轮各逐一条）", got)
	}
	if l.Size() != 2 {
		t.Fatalf("size = %d, want 2", l.Size())
	}
	// Release 已出局键：幂等 no-op，不误伤计数路径。
	if err := l.Release(ctx, "i-2"); err != nil {
		t.Fatal(err)
	}
	if l.Size() != 2 {
		t.Fatalf("size = %d, want 2", l.Size())
	}

	reg := metrics.New()
	for _, g := range l.MemGuards() {
		g.RegisterTo(reg)
	}
	var buf bytes.Buffer
	if err := reg.WritePrometheus(&buf); err != nil {
		t.Fatal(err)
	}
	body := buf.String()
	for _, want := range []string{
		`opscopilot_mem_entries{store="escalation_ledger"} 2`,
		`opscopilot_mem_evictions_total{store="escalation_ledger"} 2`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics 文本缺 %q\n---\n%s", want, body)
		}
	}
}

// TestMemAuditLogBoundedDropsOldest 无库审计触限时丢最旧（追加序=活跃度序），
// 最新留痕绝不先丢。
func TestMemAuditLogBoundedDropsOldest(t *testing.T) {
	guard := memguard.New("audit", 3, 0)
	l := NewMemAuditLogWithLimits(guard)
	for i := 0; i < 5; i++ {
		l.Append(AuditEntry{IncidentID: string(rune('a' + i)), Action: AuditCreate, Actor: "ops"})
	}
	if l.Size() != 3 {
		t.Fatalf("size = %d, want 3 (cap)", l.Size())
	}
	all, err := l.List("")
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 3 || all[0].IncidentID != string(rune('a'+4)) {
		ids := make([]string, len(all))
		for i, e := range all {
			ids[i] = e.IncidentID
		}
		t.Fatalf("应保留最新 3 条（倒序首条 e），got %v", ids)
	}
	if got := guard.Evictions(); got != 2 {
		t.Fatalf("evictions = %d, want 2", got)
	}
	// 无护栏构造不受限（默认行为不变）。
	un := NewMemAuditLog()
	for i := 0; i < 10; i++ {
		un.Append(AuditEntry{IncidentID: "x", Action: AuditCreate})
	}
	if un.Size() != 10 || len(un.MemGuards()) != 0 {
		t.Fatal("unbounded audit log must keep everything")
	}
}

// capturingRecordSink 记录簇快照落库调用（只验证签名 GC 被批处理驱动）。
type capturingRecordSink struct{ saved int }

func (s *capturingRecordSink) SaveCluster(noise.ClusterRecord) error { s.saved++; return nil }

// TestNoiseEngineSigCacheGC 签名缓存护栏：簇被 Clusterer 容量淘汰后，
// 其孤儿签名在下一批 collectDirtyClusters 时被 GC 并计数。
func TestNoiseEngineSigCacheGC(t *testing.T) {
	sink := noiseTestSink(t)
	// 簇上限 1：第二条新簇挤掉第一条；签名缓存上限 1：孤儿签名被 GC。
	mem := config.MemLimitSection{NoiseClusters: 1, NoiseSigCache: 1}
	ne := NewNoiseEngine(sink, newQuietLogger(), testTenant, noiseSpec(nil), mem)
	if ne == nil {
		t.Fatal("engine expected")
	}
	rs := &capturingRecordSink{}
	ne.SetRecordSink(rs)

	fire := func(fp string) {
		ne.ProcessAlerts([]connector.Alert{{
			Fingerprint: fp,
			Labels:      map[string]string{"alertname": "T", "instance": "i-" + fp},
			StartsAt:    time.Now(),
			Severity:    "warning",
		}})
	}
	fire("fpa") // 簇 A 进签名缓存
	fire("fpb") // 簇 B 建立 + 簇 A 被 Clusterer 逐出；签名缓存超限 → GC 孤儿 A

	ne.mu.Lock()
	sigLen := len(ne.persistedSig)
	ne.mu.Unlock()
	if sigLen > 1 {
		t.Fatalf("签名缓存应被压回上限内, got %d", sigLen)
	}
	var sigGuard *memguard.Guard
	for _, g := range ne.MemGuards() {
		if g.Store() == "noise_sigcache" {
			sigGuard = g
		}
	}
	if sigGuard == nil || sigGuard.Evictions() == 0 {
		t.Fatalf("孤儿签名必须计淘汰（sigGuard=%v）", sigGuard)
	}
}

// TestAssemblyExposesMemMetrics 装配层：/metrics 文本必须出现全部有界结构
// 的规模 gauge 与淘汰 counter（store 标签区分；正常规模下值为 0/未触发）。
func TestAssemblyExposesMemMetrics(t *testing.T) {
	cfg := testAssemblyConfig("tok")
	cfg.Notify.EscalationEnabled = true // 内存台账降级路径打开（无 DB）
	asm, err := NewAssembly(newQuietLogger(), cfg)
	if err != nil {
		t.Fatalf("assembly: %v", err)
	}
	defer asm.Close()

	rec := httptest.NewRecorder()
	asm.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/metrics = %d", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{
		`opscopilot_mem_entries{store="builder"} 0`,
		`opscopilot_mem_entries{store="builder_edges"} 0`,
		`opscopilot_mem_entries{store="incidents"} 0`,
		`opscopilot_mem_entries{store="audit"} 0`,
		`opscopilot_mem_entries{store="noise_dedup"} 0`,
		`opscopilot_mem_entries{store="noise_clusters"} 0`,
		`opscopilot_mem_entries{store="noise_sigcache"} 0`,
		`opscopilot_mem_entries{store="escalation_ledger"} 0`,
		`opscopilot_mem_evictions_total{store="builder"} 0`,
		`opscopilot_mem_evictions_total{store="incidents"} 0`,
		"# TYPE opscopilot_mem_entries gauge",
		"# TYPE opscopilot_mem_evictions_total counter",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("/metrics 缺 %q", want)
		}
	}
}
