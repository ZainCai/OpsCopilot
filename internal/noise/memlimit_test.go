// memlimit_test.go 优化方案 #6：去重指纹表与内存簇的容量护栏。
package noise

import (
	"bytes"
	"strconv"
	"strings"
	"testing"
	"time"

	"opscopilot/pkg/memguard"
	"opscopilot/pkg/metrics"
)

// boundedClusterKey 复刻 newClusterLocked 的键公式（window >= 1s 时的窗口桶）。
func boundedClusterKey(fp string, at time.Time, window time.Duration) string {
	secs := int64(window / time.Second)
	bucket := at.UnixNano()
	if secs > 0 {
		bucket = at.Unix() / secs
	}
	return "c:" + fp + "@" + strconv.FormatInt(bucket, 10)
}

// TestDedupBoundedEvictsOldestAnchor 超上限按"最久未活跃"（锚点最早）回收——
// 被逐指纹再出现会被放行一次（fail-open，与"宁漏勿杀"同向）。
func TestDedupBoundedEvictsOldestAnchor(t *testing.T) {
	guard := memguard.New("noise_dedup", 2, 0)
	d := NewDedupWithLimits(time.Hour, guard)
	t0 := time.Now()
	if !d.Allow("fp1", t0) || !d.Allow("fp2", t0.Add(time.Minute)) {
		t.Fatal("first sightings must pass")
	}
	if !d.Allow("fp3", t0.Add(2*time.Minute)) {
		t.Fatal("fp3 first sighting must pass")
	}
	if d.Len() != 2 {
		t.Fatalf("len = %d, want 2 (cap)", d.Len())
	}
	// fp3 进账时淘汰锚点最旧的 fp1；此刻 fp2 仍在表内 → 窗口内应被抑制。
	if d.Allow("fp2", t0.Add(3*time.Minute)) {
		t.Fatal("fp2 仍在窗口内，应被抑制")
	}
	// 被淘汰的 fp1 已被遗忘 → 再出现重新放行（fail-open，宁漏勿杀）。
	if !d.Allow("fp1", t0.Add(3*time.Minute)) {
		t.Fatal("fp1 应已被淘汰、再次出现时放行")
	}
	if got := guard.Evictions(); got < 1 {
		t.Fatalf("evictions = %d, want >=1", got)
	}

	reg := metrics.New()
	for _, g := range d.MemGuards() {
		g.RegisterTo(reg)
	}
	var buf bytes.Buffer
	if err := reg.WritePrometheus(&buf); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), `opscopilot_mem_entries{store="noise_dedup"} 2`) {
		t.Fatalf("gauge 缺失\n---\n%s", buf.String())
	}
}

// TestClustererBoundedEvictsLeastActive 活跃区洪峰（resolved 为空）时，
// active 里最久未活跃者出局，新簇保留。
func TestClustererBoundedEvictsLeastActive(t *testing.T) {
	window := 10 * time.Minute
	guard := memguard.New("noise_clusters", 2, 0)
	c := NewClustererWithLimits(window, nil, guard)
	t0 := time.Now()
	ingest := func(fp string, off time.Duration) {
		t.Helper()
		if _, ok := c.Ingest(Event{Fingerprint: fp, NodeKey: "n-" + fp, OccurredAt: t0.Add(off)}); !ok {
			t.Fatalf("ingest %s", fp)
		}
	}
	ingest("a", 0)
	ingest("b", time.Minute)
	ingest("c", 2*time.Minute) // 第三条新簇（cap=2）→ 淘汰最久未活跃的 a 簇
	if got := guard.Evictions(); got != 1 {
		t.Fatalf("evictions = %d, want 1", got)
	}
	if c.Get(boundedClusterKey("a", t0, window)) != nil {
		t.Fatal("a 簇应已被逐")
	}
	if c.Get(boundedClusterKey("c", t0.Add(2*time.Minute), window)) == nil {
		t.Fatal("c 簇不该被误伤")
	}
	if c.ActiveCount()+c.ResolvedCount() != 2 {
		t.Fatalf("总量应压回 cap: active=%d resolved=%d", c.ActiveCount(), c.ResolvedCount())
	}
}

// TestClustererPrefersResolvedVictim resolved 区最先出局（历史可自存储侧重建），
// 活跃簇承载故障域聚合现场、最后才动。
func TestClustererPrefersResolvedVictim(t *testing.T) {
	window := time.Minute
	guard := memguard.New("noise_clusters", 2, 0)
	c := NewClustererWithLimits(window, nil, guard)
	t0 := time.Now()
	c.Ingest(Event{Fingerprint: "x", OccurredAt: t0})
	c.Ingest(Event{Fingerprint: "y", OccurredAt: t0.Add(30 * time.Second)})
	if n := c.Sweep(t0.Add(10 * time.Minute)); n != 2 {
		t.Fatalf("sweep = %d, want 2", n) // 双簇 resolve，总量仍 2（未超限）
	}
	c.Ingest(Event{Fingerprint: "z", OccurredAt: t0.Add(11 * time.Minute)}) // 3 > 2 → 先走 resolved 最旧 x
	if c.Get(boundedClusterKey("x", t0, window)) != nil {
		t.Fatal("resolved 区最旧的 x 簇应最先被淘汰")
	}
	if c.Get(boundedClusterKey("y", t0.Add(30*time.Second), window)) == nil {
		t.Fatal("resolved 区较新的 y 不该陪葬")
	}
	if c.Get(boundedClusterKey("z", t0.Add(11*time.Minute), window)) == nil {
		t.Fatal("活跃 z 簇最后才动")
	}
	if got := guard.Evictions(); got != 1 {
		t.Fatalf("evictions = %d, want 1", got)
	}
}

// TestRestoreEnforcesCap 从真相源重建同样受容量护栏约束（#6 注释口径）。
func TestRestoreEnforcesCap(t *testing.T) {
	guard := memguard.New("noise_clusters", 2, 0)
	c := NewClustererWithLimits(10*time.Minute, nil, guard)
	now := time.Now()
	recs := make([]ClusterRecord, 0, 5)
	for i := 0; i < 5; i++ {
		recs = append(recs, ClusterRecord{
			ClusterKey: "k" + strconv.Itoa(i),
			State:      StateOpen,
			FirstSeen:  now.Add(time.Duration(i) * time.Minute),
			LastSeen:   now.Add(time.Duration(i) * time.Minute),
		})
	}
	if err := c.Restore(recs); err != nil {
		t.Fatal(err)
	}
	if c.ActiveCount() != 2 {
		t.Fatalf("active = %d, want 2 (cap)", c.ActiveCount())
	}
	if got := guard.Evictions(); got != 3 {
		t.Fatalf("evictions = %d, want 3", got)
	}
	// 留下的恰是最活跃的两条（k3/k4）。
	if c.Get("k0") != nil || c.Get("k4") == nil {
		t.Fatal("Restore 淘汰应保留最活跃尾部")
	}
}

// TestShadowMemGuards shadow 聚合暴露两把护栏；无护栏构造不暴露任何 guard。
func TestShadowMemGuards(t *testing.T) {
	dg := memguard.New("noise_dedup", 5, 0)
	cg := memguard.New("noise_clusters", 5, 0)
	s := NewShadowWithLimits(time.Minute, nil, dg, cg)
	if got := s.MemGuards(); len(got) != 2 {
		t.Fatalf("MemGuards = %d, want 2", len(got))
	}
	if got := NewShadow(time.Minute, nil).MemGuards(); len(got) != 0 {
		t.Fatalf("unbounded shadow must expose 0 guards, got %d", len(got))
	}
}
