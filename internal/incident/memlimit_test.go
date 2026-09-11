// incident MemStore 容量护栏（优化方案 #6，DB 缺席降级路径）。
package incident

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"opscopilot/pkg/memguard"
	"opscopilot/pkg/metrics"
)

func TestMemStoreBoundedEvictsLeastRecentlyActive(t *testing.T) {
	guard := memguard.New("incidents", 2, 0)
	s := NewMemStoreWithLimits(guard)
	// 时钟每秒推进一格，保证 UpdatedAt 严格有序、淘汰决定性。
	base := time.Now()
	tick := 0
	s.SetClock(func() time.Time {
		tick++
		return base.Add(time.Duration(tick) * time.Minute)
	})

	if _, err := s.Create("a", "A", "warning", "u"); err != nil {
		t.Fatal(err)
	}
	if err := s.AttachCluster("a", "ck-a"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Create("b", "B", "warning", "u"); err != nil {
		t.Fatal(err)
	}
	// 第 3 单进账（cap=2）→ 全 open，淘汰 UpdatedAt 最早的 "a"（最久未活跃）。
	if _, err := s.Create("c", "C", "warning", "u"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get("a"); err != ErrNotFound {
		t.Fatalf("a 应已被淘汰, got err=%v", err)
	}
	if _, err := s.Get("b"); err != nil {
		t.Fatalf("b 不该被误伤: %v", err)
	}
	// 淘汰必须同步清索引：a 的簇关联反查不到。
	if _, ok := s.IncidentForCluster("ck-a"); ok {
		t.Fatal("byCluster 索引残留：ck-a 仍指向被逐单")
	}
	// order 一致性：List 不再吐出被逐单，且计数吻合。
	all, _ := s.List("")
	if len(all) != 2 {
		t.Fatalf("List = %d 条, want 2", len(all))
	}

	// resolved 优先淘汰：把 b 关掉再建 d → 先走最旧 resolved（b），
	// 而不是活跃但更旧的 c。
	if _, err := s.Transition("b", StateResolved, "u"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Create("d", "D", "warning", "u"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get("b"); err != ErrNotFound {
		t.Fatalf("已 resolved 的 b 应优先被逐, got err=%v", err)
	}
	if _, err := s.Get("c"); err != nil {
		t.Fatalf("活跃单 c 最后才动: %v", err)
	}
	if got := guard.Evictions(); got != 2 {
		t.Fatalf("evictions = %d, want 2", got)
	}

	reg := metrics.New()
	for _, g := range s.MemGuards() {
		g.RegisterTo(reg)
	}
	var buf bytes.Buffer
	if err := reg.WritePrometheus(&buf); err != nil {
		t.Fatal(err)
	}
	body := buf.String()
	for _, want := range []string{
		`opscopilot_mem_entries{store="incidents"} 2`,
		`opscopilot_mem_evictions_total{store="incidents"} 2`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics 文本缺 %q\n---\n%s", want, body)
		}
	}
}

// TestMemStoreExternalIndexEvicted 外部链路索引（byExternal）在被逐时同步清除，
// 不留"ExternalActive 说谎"的残影。
func TestMemStoreExternalIndexEvicted(t *testing.T) {
	guard := memguard.New("incidents", 1, 0)
	s := NewMemStoreWithLimits(guard)
	base := time.Now()
	tick := 0
	s.SetClock(func() time.Time { tick++; return base.Add(time.Duration(tick) * time.Minute) })

	if _, _, err := s.UpsertExternal(OriginWebhook, "ref-1", "T1", "warning", "sys", "{}"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.UpsertExternal(OriginWebhook, "ref-2", "T2", "warning", "sys", "{}"); err != nil {
		t.Fatal(err) // ref-1 被逐
	}
	if active, err := s.ExternalActive(OriginWebhook, "ref-1"); err != nil || active {
		t.Fatalf("ref-1 的 byExternal 索引必须随单清除: active=%v err=%v", active, err)
	}
	if guard.Evictions() != 1 {
		t.Fatalf("evictions = %d, want 1", guard.Evictions())
	}
}

// TestMemStoreUnboundedKeepsOldBehavior NewMemStore（无护栏）行为不变。
func TestMemStoreUnboundedKeepsOldBehavior(t *testing.T) {
	s := NewMemStore()
	for i := 0; i < 10; i++ {
		if _, err := s.Create(string(rune('a'+i)), "t", "warning", "u"); err != nil {
			t.Fatal(err)
		}
	}
	if s.Size() != 10 || len(s.MemGuards()) != 0 {
		t.Fatal("unbounded MemStore must keep everything and expose no guards")
	}
}
