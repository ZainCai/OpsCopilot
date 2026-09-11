package memguard

import (
	"bytes"
	"strings"
	"testing"

	"opscopilot/pkg/metrics"
)

// TestNilGuardIsUnlimited maxEntries<=0 → nil 护栏，所有方法 no-op：
// "默认规模永不触发"的实现底座。
func TestNilGuardIsUnlimited(t *testing.T) {
	var g *Guard
	if g.Over(1<<30) != 0 {
		t.Fatal("nil guard must never report excess")
	}
	g.Evicted(5)
	if g.Evictions() != 0 || g.MaxEntries() != 0 || g.Store() != "" {
		t.Fatal("nil guard accessors must be zero")
	}
	g.RegisterTo(metrics.New()) // 不得 panic
}

// TestOverAndEvictAccounting 超限判定、淘汰计数、水位联动的基础语义。
func TestOverAndEvictAccounting(t *testing.T) {
	g := New("demo", 10, 0.8)
	if g == nil {
		t.Fatal("guard expected")
	}
	if g.Over(10) != 0 {
		t.Fatal("at cap is not over cap")
	}
	if got := g.Over(13); got != 3 {
		t.Fatalf("Over(13) = %d, want 3", got)
	}
	g.Evicted(3)
	if got := g.Evictions(); got != 3 {
		t.Fatalf("Evictions = %d, want 3", got)
	}
	g.Evicted(0) // 非法入参静默
	if got := g.Evictions(); got != 3 {
		t.Fatalf("Evictions changed on n=0: %d", got)
	}
}

// TestRegisterToExportsMetrics 注册后 /metrics 文本必须同时出现
// 规模 gauge（含 store 标签）与淘汰 counter，且 gauge 走回调实时求值。
func TestRegisterToExportsMetrics(t *testing.T) {
	reg := metrics.New()
	size := 7
	g := New("demo", 10, 0)
	g.SetSize(func() int { return size })
	g.Evicted(2) // 注册前的存量不得漏账
	g.RegisterTo(reg)
	g.RegisterTo(reg) // 幂等

	var buf bytes.Buffer
	if err := reg.WritePrometheus(&buf); err != nil {
		t.Fatalf("write: %v", err)
	}
	body := buf.String()
	for _, want := range []string{
		`opscopilot_mem_evictions_total{store="demo"} 2`,
		`opscopilot_mem_entries{store="demo"} 7`,
		"# TYPE opscopilot_mem_entries gauge",
		"# TYPE opscopilot_mem_evictions_total counter",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics text missing %q\n---\n%s", want, body)
		}
	}
	// gauge 回调实时性：规模变化后再次暴露读到新值。
	size = 3
	buf.Reset()
	if err := reg.WritePrometheus(&buf); err != nil {
		t.Fatalf("write2: %v", err)
	}
	if !strings.Contains(buf.String(), `opscopilot_mem_entries{store="demo"} 3`) {
		t.Fatalf("gauge must read live size\n---\n%s", buf.String())
	}
	// 注册后的新增淘汰也进 counter。
	g.Evicted(1)
	buf.Reset()
	if err := reg.WritePrometheus(&buf); err != nil {
		t.Fatalf("write3: %v", err)
	}
	if !strings.Contains(buf.String(), `opscopilot_mem_evictions_total{store="demo"} 3`) {
		t.Fatalf("registered counter must see new evictions\n---\n%s", buf.String())
	}
}
