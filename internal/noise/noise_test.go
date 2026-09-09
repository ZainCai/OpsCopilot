// W4-1.2 噪声原语测试：锁定指纹确定性契约与固定窗口去重语义。
package noise

import (
	"sync"
	"testing"
	"time"
)

func TestFingerprintDeterministicAcrossInsertionOrder(t *testing.T) {
	a := Fingerprint(map[string]string{"alertname": "HighCPU", "instance": "n1", "severity": "critical"})
	b := Fingerprint(map[string]string{"severity": "critical", "instance": "n1", "alertname": "HighCPU"})
	if a != b {
		t.Fatalf("insertion order changed fingerprint: %s vs %s", a, b)
	}
	if len(a) != 16 {
		t.Fatalf("fingerprint length = %d, want 16 hex chars", len(a))
	}
}

func TestFingerprintDistinguishesLabelSets(t *testing.T) {
	base := map[string]string{"alertname": "HighCPU", "instance": "n1"}
	cases := []map[string]string{
		{"alertname": "HighCPU", "instance": "n2"},          // 值不同
		{"alertname": "HighMem", "instance": "n1"},          // 名不同
		{"alertname": "HighCPU", "instance": "n1", "x": ""}, // 缺 key vs 空 key
	}
	baseFp := Fingerprint(base)
	for i, c := range cases {
		if Fingerprint(c) == baseFp {
			t.Fatalf("case %d: different label sets collided", i)
		}
	}
}

func TestFingerprintEmptyAndNil(t *testing.T) {
	if Fingerprint(nil) != Fingerprint(map[string]string{}) {
		t.Fatal("nil and empty map must produce the same (deterministic) fingerprint")
	}
	if CanonicalLabels(map[string]string{"b": "2", "a": "1"}) != "a=1\x1fb=2" {
		t.Fatalf("canonical form wrong: %q", CanonicalLabels(map[string]string{"b": "2", "a": "1"}))
	}
}

func TestDedupFixedWindow(t *testing.T) {
	d := NewDedup(5 * time.Minute)
	now := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)
	fp := "abc"

	if !d.Allow(fp, now) {
		t.Fatal("first occurrence must pass")
	}
	if d.Allow(fp, now.Add(4*time.Minute)) {
		t.Fatal("within window must be suppressed")
	}
	if !d.Allow(fp, now.Add(5*time.Minute)) {
		t.Fatal("at window boundary must pass again")
	}
	// 放行即重置锚点：新的 5 分钟窗口从 now+5min 起。
	if d.Allow(fp, now.Add(9*time.Minute)) {
		t.Fatal("second window starts at reset point; 9min is inside it")
	}
}

func TestDedupIndependentFingerprints(t *testing.T) {
	d := NewDedup(time.Hour)
	now := time.Now()
	if !d.Allow("f1", now) || !d.Allow("f2", now) {
		t.Fatal("different fingerprints must not suppress each other")
	}
	if d.Allow("f1", now.Add(time.Minute)) {
		t.Fatal("f1 second occurrence within window must be suppressed")
	}
}

func TestDedupEmptyFingerprintAlwaysAllows(t *testing.T) {
	d := NewDedup(time.Hour)
	now := time.Now()
	for i := 0; i < 3; i++ {
		if !d.Allow("", now) {
			t.Fatal("empty fingerprint must always pass (fail-open, 降噪宁漏勿杀)")
		}
	}
	if d.Len() != 0 {
		t.Fatalf("empty fingerprint must not be recorded, Len = %d", d.Len())
	}
}

func TestDedupZeroWindowDisables(t *testing.T) {
	d := NewDedup(0)
	now := time.Now()
	for i := 0; i < 3; i++ {
		if !d.Allow("f", now) {
			t.Fatal("zero window = dedup disabled, must always pass")
		}
	}
}

func TestDedupSweepReclaimsExpired(t *testing.T) {
	d := NewDedup(time.Minute)
	base := time.Date(2026, 9, 9, 0, 0, 0, 0, time.UTC)
	d.Allow("f1", base)
	d.Allow("f2", base.Add(10*time.Second))
	if d.Len() != 2 {
		t.Fatalf("Len = %d, want 2", d.Len())
	}
	if n := d.Sweep(base.Add(30 * time.Second)); n != 0 {
		t.Fatalf("sweep inside window reclaimed %d, want 0", n)
	}
	if n := d.Sweep(base.Add(2 * time.Minute)); n != 2 {
		t.Fatalf("sweep after window reclaimed %d, want 2", n)
	}
	if d.Len() != 0 {
		t.Fatalf("Len after sweep = %d, want 0", d.Len())
	}
	// 清扫后同指纹重新放行。
	if !d.Allow("f1", base.Add(3*time.Minute)) {
		t.Fatal("after expiry, fingerprint must pass again")
	}
}

// TestDedupConcurrent 并发压测：多 goroutine 同时 Allow 不死锁不 panic
// （本机无 cgo 跑不了 -race，并发正确性靠互斥锁纪律 + CI race job 兜底）。
func TestDedupConcurrent(t *testing.T) {
	d := NewDedup(time.Hour)
	start := time.Date(2026, 9, 9, 0, 0, 0, 0, time.UTC)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				d.Allow(string(rune('a'+g%26))+string(rune('0'+i%10)), start.Add(time.Duration(i)*time.Second))
			}
		}(g)
	}
	wg.Wait()
	if d.Len() == 0 {
		t.Fatal("expected recorded entries after concurrent allows")
	}
}
