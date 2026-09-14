package memguard

import (
	"bytes"
	"strings"
	"sync"
	"sync/atomic"
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

// TestConcurrentEvictionAndGauge P2-A5（round10）：淘汰进行中并发 gauge
// 求值——多 goroutine 同时 Over/Evicted/SetSize/WritePrometheus，不得有
// 数据竞争（-race 兜底），且淘汰计数最终一致（每次 Evicted(1) 恰计 1）。
// Guard 自身互斥（Over/Evicted/SetSize 均持 mu），gauge 回调经原子计数器
// 求值——两者并发不交叠任何裸共享写。
func TestConcurrentEvictionAndGauge(t *testing.T) {
	g := New("demo", 1000, 0.9)
	reg := metrics.New()
	g.RegisterTo(reg)

	var val atomic.Int64
	g.SetSize(func() int { return int(val.Load()) })

	const workers = 8
	const rounds = 200
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < rounds; j++ {
				g.Over(1100) // 恒超 100 → 水位 WARN 限流路径同样并发
				g.Evicted(1)
				val.Add(1)
				var buf bytes.Buffer
				_ = reg.WritePrometheus(&buf) // gauge 实时求值（读 size 回调）
			}
		}()
	}
	wg.Wait()
	if got := g.Evictions(); got != workers*rounds {
		t.Fatalf("evictions = %d, want %d", got, workers*rounds)
	}
}
