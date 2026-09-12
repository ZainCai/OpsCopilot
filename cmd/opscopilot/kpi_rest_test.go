// kpi_rest_test.go W10-3（F-07）KPI 端点 REST 面测试（内存 Store，无 DB 依赖）：
// **与手工手算对账**（排期 W10-3 验收"与手工按时间线计算一致"）——手算输入
// 取 GET /incidents 视图行（与时间线端点同一批事件、同一组 created/acked/
// resolved 时间戳），逐条算差值取均值，再与 /kpis 读数比对；window/severity
// 校验与空窗零值同样在此锁死。harness 复用 sla_rest_test.go 的
// newSLAKPIGateway/doJSON。
package main

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"opscopilot/internal/incident"
)

func near(a, b float64) bool {
	d := a - b
	if d < 0 {
		d = -d
	}
	return d < 0.001
}

func TestKPIEndpointMatchesManualReckoning(t *testing.T) {
	store, srv := newSLAKPIGateway(t)

	// 造混合队列：全链 ×2、只 ack ×1、跳级直解 ×1、滞留 open ×1。
	mk := func(id, sev string, ack, resolve bool) {
		t.Helper()
		if _, err := store.Create(id, id, sev, "ops"); err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
		if ack {
			if _, err := store.Transition(id, incident.StateAcked, "bob"); err != nil {
				t.Fatalf("ack %s: %v", id, err)
			}
		}
		if resolve {
			if ack {
				if _, err := store.Transition(id, incident.StateMitigated, "bob"); err != nil {
					t.Fatalf("mitigate %s: %v", id, err)
				}
			}
			if _, err := store.Transition(id, incident.StateResolved, "bob"); err != nil {
				t.Fatalf("resolve %s: %v", id, err)
			}
		}
		time.Sleep(15 * time.Millisecond) // 拉开可测的时间差（真实时钟）
	}
	mk("K-1", "critical", true, true)
	mk("K-2", "critical", true, true)
	mk("K-3", "critical", true, false)
	mk("K-4", "warning", false, true)
	mk("K-5", "warning", false, false)

	// 手算源：列表视图行（limit 给足，单页）。
	code, raw := doJSON(t, "GET", srv.URL+"/api/v1/incidents?limit=100", nil)
	if code != http.StatusOK {
		t.Fatalf("list code = %d: %s", code, raw)
	}
	type kpiRow struct {
		Severity   string    `json:"severity"`
		CreatedAt  time.Time `json:"created_at"`
		AckedAt    time.Time `json:"acked_at"`
		ResolvedAt time.Time `json:"resolved_at"`
	}
	var lr struct {
		Incidents []kpiRow `json:"incidents"`
	}
	if err := json.Unmarshal(raw, &lr); err != nil {
		t.Fatalf("list decode: %v", err)
	}
	// 手算（口径 = kpi.go：队列全量、均值只对非零样本；逐条差值、逐条累加，
	// 独立于端点的聚合实现——这正是"与手工按时间线计算一致"的字面兑现）。
	var c, a, r int64
	var as, rs, mtta, mttr float64
	for _, row := range lr.Incidents {
		c++
		if !row.AckedAt.IsZero() {
			a++
			as += row.AckedAt.Sub(row.CreatedAt).Seconds()
		}
		if !row.ResolvedAt.IsZero() {
			r++
			rs += row.ResolvedAt.Sub(row.CreatedAt).Seconds()
		}
	}
	if a > 0 {
		mtta = as / float64(a)
	}
	if r > 0 {
		mttr = rs / float64(r)
	}

	code, raw = doJSON(t, "GET", srv.URL+"/api/v1/kpis", nil)
	if code != http.StatusOK {
		t.Fatalf("kpis code = %d: %s", code, raw)
	}
	var kr struct {
		Window      string `json:"window"`
		WindowStart string `json:"window_start"`
		Severity    string `json:"severity"`
		Stats       struct {
			CreatedCount    int64   `json:"created_count"`
			ResolvedCount   int64   `json:"resolved_count"`
			AckedSamples    int64   `json:"acked_samples"`
			ResolvedSamples int64   `json:"resolved_samples"`
			MTTASeconds     float64 `json:"mtta_seconds"`
			MTTRSeconds     float64 `json:"mttr_seconds"`
		} `json:"stats"`
	}
	if err := json.Unmarshal(raw, &kr); err != nil {
		t.Fatalf("kpis decode: %v", err)
	}
	if kr.Window != "168h0m0s" {
		t.Fatalf("default window = %q, want 168h0m0s (OPS_KPI_WINDOW)", kr.Window)
	}
	if kr.Stats.CreatedCount != c || kr.Stats.AckedSamples != a || kr.Stats.ResolvedSamples != r || kr.Stats.ResolvedCount != r {
		t.Fatalf("counts mismatch: endpoint %+v vs manual created=%d acked=%d resolved=%d", kr.Stats, c, a, r)
	}
	if !near(kr.Stats.MTTASeconds, mtta) || !near(kr.Stats.MTTRSeconds, mttr) {
		t.Fatalf("means mismatch: endpoint MTTA=%v MTTR=%v vs manual MTTA=%v MTTR=%v",
			kr.Stats.MTTASeconds, kr.Stats.MTTRSeconds, mtta, mttr)
	}

	// severity 过滤：warning 队（K-4/K-5）：acked_samples=0、resolved=1。
	code, raw = doJSON(t, "GET", srv.URL+"/api/v1/kpis?severity=warning", nil)
	if code != http.StatusOK {
		t.Fatalf("kpis severity code = %d: %s", code, raw)
	}
	if err := json.Unmarshal(raw, &kr); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if kr.Stats.CreatedCount != 2 || kr.Stats.AckedSamples != 0 || kr.Stats.ResolvedSamples != 1 {
		t.Fatalf("warning cohort = %+v, want {2 created, 0 acked, 1 resolved}", kr.Stats)
	}
	if kr.Stats.MTTASeconds != 0 {
		t.Fatalf("zero-sample MTTA must be 0, got %v", kr.Stats.MTTASeconds)
	}

	// 空队列（severity 无行）：零值 + 样本数 0。
	if _, err := store.Create("K-6", "info row", "info", "ops"); err != nil {
		t.Fatalf("create K-6: %v", err)
	}
	code, raw = doJSON(t, "GET", srv.URL+"/api/v1/kpis?severity=info&window=24h", nil)
	if code != http.StatusOK {
		t.Fatalf("kpis info code = %d: %s", code, raw)
	}
	if err := json.Unmarshal(raw, &kr); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if kr.Stats.AckedSamples != 0 || kr.Stats.ResolvedSamples != 0 ||
		kr.Stats.MTTASeconds != 0 || kr.Stats.MTTRSeconds != 0 || kr.Stats.CreatedCount != 1 {
		t.Fatalf("sparse cohort = %+v, want zero means + created 1", kr.Stats)
	}

	// 参数校验：非法 window/severity → 400（细节不外泄）。
	for _, bad := range []string{"?window=abc", "?window=-5m", "?window=0s", "?severity=bogus"} {
		if code, raw := doJSON(t, "GET", srv.URL+"/api/v1/kpis"+bad, nil); code != http.StatusBadRequest {
			t.Fatalf("kpis %s: code = %d (%s), want 400", bad, code, raw)
		}
	}
}
