// rest_timeline_e2e_pg_test.go W10-1（F-03）事件混合时间线端到端（PG 真库，
// OPS_TEST_PG_DSN 门控，仓库惯例同 autoattach_e2e_pg_test.go）：
//
//	W10-6 生产路径（enforce+autoattach 建单挂簇）→ 注入变更 → 人工 ack →
//	GET /api/v1/incidents/{id}/timeline 断言三类（alert_in/change/action）
//	都有且时间单调、partial=false；limit=1 翻页不重不漏；404。
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"opscopilot/internal/config"
	"opscopilot/internal/connector"
	"opscopilot/internal/topology"
)

type tlItem struct {
	TS         time.Time `json:"ts"`
	Kind       string    `json:"kind"`
	SourceID   string    `json:"source_id"`
	Summary    string    `json:"summary"`
	Severity   string    `json:"severity"`
	Confidence string    `json:"confidence"`
}

type tlResponse struct {
	IncidentID string            `json:"incident_id"`
	Items      []tlItem          `json:"items"`
	Count      int               `json:"count"`
	Total      int               `json:"total"`
	NextCursor string            `json:"next_cursor"`
	Partial    bool              `json:"partial"`
	Missing    map[string]string `json:"missing"`
}

func timelineFetch(t *testing.T, h http.Handler, path string) (int, tlResponse) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	var body tlResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	return rec.Code, body
}

func TestTimelineEndToEndWithPG(t *testing.T) {
	dsn := os.Getenv("OPS_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("OPS_TEST_PG_DSN not set — timeline pg end-to-end skipped")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Skipf("pg unreachable (create pool): %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("OPS_TEST_PG_DSN unreachable — skipped: %v（先跑 bash scripts/reset_test_pg.sh）", err)
	}
	pool.Close()

	suffix := time.Now().UnixNano()
	token := fmt.Sprintf("tl-e2e-token-%d", suffix)
	nodeKey := fmt.Sprintf("prometheus://nodes/tl-e2e-%d", suffix)
	instLabel := fmt.Sprintf("tl-e2e-%d", suffix)
	chgID := fmt.Sprintf("chg-tl-e2e-%d", suffix)

	cfg := testAssemblyConfig(token)
	cfg.DB.DSN = dsn
	cfg.Tenant = fmt.Sprintf("tl-e2e-%d", suffix)
	cfg.Noise.Mode = config.NoiseModeEnforce
	cfg.Noise.AutoAttach = true // W10-6 生产自动挂簇：本用例的建单+挂簇路径

	asm, err := NewAssembly(newQuietLogger(), cfg)
	if err != nil {
		t.Fatalf("assembly: %v", err)
	}
	defer asm.Close()
	h := asm.Handler()

	cleanup := func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer ccancel()
		p, perr := pgxpool.New(cctx, dsn)
		if perr != nil {
			return
		}
		defer p.Close()
		_, _ = p.Exec(cctx, `DELETE FROM incident_cluster WHERE incident_row_id IN
			(SELECT id FROM incident WHERE tenant_id=$1)`, cfg.Tenant)
		_, _ = p.Exec(cctx, `DELETE FROM incident_audit WHERE tenant_id=$1`, cfg.Tenant)
		_, _ = p.Exec(cctx, `DELETE FROM incident WHERE tenant_id=$1`, cfg.Tenant)
		_, _ = p.Exec(cctx, `DELETE FROM alert_event WHERE tenant_id=$1`, cfg.Tenant)
		_, _ = p.Exec(cctx, `DELETE FROM change_record WHERE tenant_id=$1`, cfg.Tenant)
	}
	cleanup()
	defer cleanup()

	// ① 发现入图（变更 nodeCheck 要求节点先在拓扑）。
	if err := asm.Sink.IngestDiscover(ctx, &connector.DiscoverResult{
		Nodes: []connector.ResourceNode{{
			Key: nodeKey, Type: "host",
			Labels: map[string]string{"instance": instLabel},
		}}, TenantID: cfg.Tenant,
	}); err != nil {
		t.Fatalf("discover: %v", err)
	}

	// ② 变更入库（故障发生**前** 2 分钟的部署——变更回看窗口的靶心）。
	if _, err := asm.Changes.Record(topology.ChangeEvent{
		ID: chgID, NodeKey: nodeKey, Type: topology.ChangeDeploy,
		Source: "jenkins", Author: "e2e-bot", Summary: "timeline e2e deploy",
		OccurredAt: time.Now().Add(-2 * time.Minute),
		Confidence: topology.ConfidenceHigh,
	}); err != nil {
		t.Fatalf("record change: %v", err)
	}

	// ②′ 判决落库出口（镜像 main.go 的 DB 部署形态：NewAssembly 不挂 sink，
	// 真进程由 main 挂 PGClusterSink——不挂则影子判决永不进 alert_event，
	// W6-1 起的装配事实，见 assembly.go"队列…sink 后挂"注释）。
	psink, err := NewPGClusterSink(ctx, dsn, cfg.Tenant)
	if err != nil {
		t.Fatalf("pg verdict sink: %v", err)
	}
	defer psink.Close()
	asm.Noise.SetVerdictSink(psink)

	// ③ W10-6 生产路径：告警 → enforce new-incident → 自动建单+挂簇+审计。
	asm.Noise.ProcessAlerts([]connector.Alert{{
		Fingerprint: fmt.Sprintf("fp-tl-%d", suffix),
		Labels:      map[string]string{"alertname": "DiskFull", "instance": instLabel, "severity": "critical"},
		Severity:    "critical", StartsAt: time.Now(),
	}})
	deadline := time.Now().Add(15 * time.Second)
	var incID string
	for {
		list, lerr := asm.Incidents.List("")
		if lerr != nil {
			t.Fatalf("list: %v", lerr)
		}
		if len(list) == 1 && len(list[0].ClusterKeys) == 1 {
			incID = list[0].ID
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("autoattach 建单挂簇未出现，got %+v", list)
		}
		time.Sleep(50 * time.Millisecond)
	}

	// ④ 人工处置：POST transition ack（写路径鉴权 + requireJSON）。
	req := httptest.NewRequest(http.MethodPost, "/api/v1/incidents/"+incID+"/transition",
		strings.NewReader(`{"to":"acked","actor":"ops-e2e"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(AuthHeader, token)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("transition: code = %d body = %s", rec.Code, rec.Body.String())
	}

	// ⑤ 时间线：三类都有且时间单调。alert_event 判决走异步落库队列，
	// 轮询到 alert_in 出现为止（其余两类到得更早，不会等）。
	var body tlResponse
	deadline = time.Now().Add(20 * time.Second)
	for {
		code, b := timelineFetch(t, h, "/api/v1/incidents/"+incID+"/timeline?limit=100")
		if code != http.StatusOK {
			t.Fatalf("timeline: code = %d body = %s", code, b.Missing)
		}
		body = b
		var hasAlert bool
		for _, it := range body.Items {
			if strings.HasPrefix(it.Kind, "alert_") {
				hasAlert = true
			}
		}
		if hasAlert {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("alert 判决未落库/未关联，items = %+v missing = %v", body.Items, body.Missing)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if body.Partial || len(body.Missing) != 0 {
		t.Fatalf("三源齐备时 partial/missing = %v/%v, want false/empty", body.Partial, body.Missing)
	}
	kinds := map[string]int{}
	var prev time.Time
	for i, it := range body.Items {
		kinds[it.Kind]++
		if i > 0 && it.TS.Before(prev) {
			t.Fatalf("timeline not time-monotonic at %d: %v after %v (%+v)", i, it.TS, prev, body.Items)
		}
		prev = it.TS
	}
	if kinds["alert_in"] < 1 || kinds["change"] < 1 || kinds["action"] < 1 {
		t.Fatalf("kinds = %v, want alert_in>=1 change>=1 action>=1", kinds)
	}
	// action 至少两条：attach_cluster（system:autoattach）+ transition（ops-e2e）。
	if kinds["action"] < 2 {
		t.Fatalf("action = %d, want >=2 (attach_cluster + transition): %+v", kinds["action"], body.Items)
	}
	// 变更条目带着置信度与摘要进时间线。
	var changeItem tlItem
	for _, it := range body.Items {
		if it.Kind == "change" {
			changeItem = it
		}
	}
	if changeItem.Confidence != "high" || !strings.Contains(changeItem.Summary, "timeline e2e deploy") {
		t.Fatalf("change item = %+v", changeItem)
	}
	if body.NextCursor != "" {
		t.Fatalf("next_cursor = %q, want empty (single page)", body.NextCursor)
	}

	// ⑥ limit=1 翻页：并集=全量、不重不漏、顺序与整页一致。
	var walked []tlItem
	cursor := ""
	for pages := 0; ; pages++ {
		path := "/api/v1/incidents/" + incID + "/timeline?limit=1"
		if cursor != "" {
			path += "&cursor=" + cursor
		}
		code, pageBody := timelineFetch(t, h, path)
		if code != http.StatusOK {
			t.Fatalf("page walk: code = %d", code)
		}
		if len(pageBody.Items) != 1 {
			t.Fatalf("limit=1 page items = %+v", pageBody.Items)
		}
		walked = append(walked, pageBody.Items[0])
		if pageBody.NextCursor == "" {
			break
		}
		cursor = pageBody.NextCursor
		if pages > 20 {
			t.Fatal("cursor walk did not terminate")
		}
	}
	if len(walked) != body.Total {
		t.Fatalf("walked %d items, want total %d", len(walked), body.Total)
	}
	seen := map[string]bool{}
	for i := range walked {
		if seen[walked[i].SourceID] {
			t.Fatalf("duplicate across pages: %s", walked[i].SourceID)
		}
		seen[walked[i].SourceID] = true
		if walked[i].SourceID != body.Items[i].SourceID {
			t.Fatalf("walk differs from full page at %d: %s vs %s", i, walked[i].SourceID, body.Items[i].SourceID)
		}
	}

	// ⑦ 不存在的 incident → 404（口径同 GET /incidents/{id}）。
	if code, _ := timelineFetch(t, h, "/api/v1/incidents/INC-tl-does-not-exist/timeline"); code != http.StatusNotFound {
		t.Fatalf("unknown incident: code = %d, want 404", code)
	}
}
