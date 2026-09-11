// W6-0 故障注入器测试：端点契约 + 剧本推进 + 预热语义。
package main

import (
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"
)

func TestTargetsShape(t *testing.T) {
	inj := newInjector(1.0, 0)
	mux := newMux(inj)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest("GET", "/api/v1/targets", nil))
	var resp struct {
		Status string `json:"status"`
		Data   struct {
			ActiveTargets []struct {
				Labels     map[string]string `json:"labels"`
				ScrapePool string            `json:"scrapePool"`
			} `json:"activeTargets"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Status != "success" || len(resp.Data.ActiveTargets) != 5 {
		t.Fatalf("targets: status=%s count=%d, want success/5", resp.Status, len(resp.Data.ActiveTargets))
	}
	if got := resp.Data.ActiveTargets[0].Labels["instance"]; got != "n1" {
		t.Fatalf("first instance = %q, want n1", got)
	}
}

// TestWarmupSuppressesAlerts 预热期无告警（给 opscopilot 留发现建图时间）。
func TestWarmupSuppressesAlerts(t *testing.T) {
	inj := newInjector(1.0, 3600) // 预热 1h——测试进程内必处预热期
	mux := newMux(inj)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest("GET", "/api/v1/alerts", nil))
	var resp struct {
		Data struct {
			Alerts []json.RawMessage `json:"alerts"`
		} `json:"data"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	if len(resp.Data.Alerts) != 0 {
		t.Fatalf("warmup alerts = %d, want 0", len(resp.Data.Alerts))
	}
	// status 显示 WARMUP 场景。
	rec2 := httptest.NewRecorder()
	mux.ServeHTTP(rec2, httptest.NewRequest("GET", "/api/v1/status", nil))
	if !bytesContains(rec2.Body.Bytes(), "WARMUP") {
		t.Fatal("status during warmup must report WARMUP scenario")
	}
}

func bytesContains(b []byte, sub string) bool {
	return len(b) >= len(sub) && (string(b) == sub || indexOf(string(b), sub) >= 0)
}
func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

// TestAnswerbookStartsAfterWarmup 答案簿从剧本起点展开，预热段不入簿。
func TestAnswerbookStartsAfterWarmup(t *testing.T) {
	inj := newInjector(0.001, 60)
	// 缩放 0.001 → 每场景 ~0.3s，cycle ~1.8s：等几个周期再查。
	time.Sleep(3 * time.Second)
	mux := newMux(inj)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest("GET", "/answerbook", nil))
	var resp struct {
		PlaybookStart time.Time `json:"playbook_start"`
		Segments      []Segment `json:"segments"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Segments) == 0 {
		t.Fatal("answerbook must contain segments")
	}
	if !resp.PlaybookStart.Equal(inj.startedAt.Add(60 * time.Second)) {
		t.Fatalf("playbook start = %v, want startedAt+60s", resp.PlaybookStart)
	}
	// 段连续：后一段起点 = 前一段终点。
	for i := 1; i < len(resp.Segments); i++ {
		if !resp.Segments[i].Start.Equal(resp.Segments[i-1].End) {
			t.Fatalf("segments not contiguous at %d", i)
		}
	}
}

// TestScenarioSequencing 剧本顺序：A → C → B → C → D → C（缩放后按时钟推进）。
func TestScenarioSequencing(t *testing.T) {
	inj := newInjector(0.001, 0) // 无预热，每场景 ~0.3s，cycle ~1.8s
	seen := map[string]bool{}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		sc, _ := inj.current(time.Now())
		seen[sc.Code] = true
		if seen["A"] && seen["B"] && seen["C"] && seen["D"] {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("playbook did not cover all scenarios in time: seen=%v", seen)
}

// TestCurrentWrapsAcrossCycles 第七轮 W6-3 二轮评估实锤的回归：current()
// 原实现缺周期回绕，走完一个 totalCycle 后永远钉在场景 A——首个周期
// 全 PASS、其后静默段持续收到 A 的告警（20.6% 假象的根因）。
func TestCurrentWrapsAcrossCycles(t *testing.T) {
	inj := newInjector(1.0, 60) // 6 段 × 300s = 1800s 周期
	base := inj.startedAt.Add(time.Duration(inj.warmupSec) * time.Second)
	// 周期回绕边界前后：周期 1 末段是 D，周期 2 首段应回到 A。
	cases := []struct {
		afterPlayback time.Duration // 剧本播放起点后多久
		want          string
	}{
		// 剧本顺序（半开区间 [start,start+dur)，边界瞬间归后一段）：
		//   A[0,300) C[300,600) B[600,900) C[900,1200) D[1200,1500) C[1500,1800)
		{0 * time.Second, "A"}, {299 * time.Second, "A"}, {300 * time.Second, "C"},
		{899 * time.Second, "B"}, {900 * time.Second, "C"}, {1200 * time.Second, "D"},
		{1499 * time.Second, "D"}, {1500 * time.Second, "C"}, {1799 * time.Second, "C"},
		{1800 * time.Second, "A"}, {2400 * time.Second, "B"}, {3600 * time.Second, "A"},
		{5400 * time.Second, "A"},
	}
	for _, c := range cases {
		sc, segStart := inj.current(base.Add(c.afterPlayback)) // afterPlayback 已是 Duration
		if testing.Verbose() {
			t.Logf("t=+%v -> %s (segStart %v, base %v, startedAt %v)", c.afterPlayback, sc.Code, segStart, base, inj.startedAt)
		}
		if sc.Code != c.want {
			t.Fatalf("t=+%ds: scenario = %s, want %s", c.afterPlayback, sc.Code, c.want)
		}
		// 段起点必须落在当前周期的时间线上（不能是 startedAt 原点）。
		cycle := 1800 * time.Second
		cycleOff := (c.afterPlayback / cycle) * cycle
		wantStart := base.Add(cycleOff)
		rem := c.afterPlayback - cycleOff
		for _, s := range inj.scenarios {
			d := time.Duration(s.Duration) * time.Second
			if rem < d {
				break
			}
			rem -= d
			wantStart = wantStart.Add(d)
		}
		if segStart.Sub(wantStart).Abs() > time.Second {
			t.Fatalf("t=+%ds: segStart = %v, want ~%v", c.afterPlayback, segStart, wantStart)
		}
	}
}
