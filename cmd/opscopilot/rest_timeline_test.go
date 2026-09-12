// rest_timeline.go 的单元测试（W10-1/F-03）：三源混排正确性（表驱动假源）、
// 同刻稳定序、offset 游标翻页不重不漏、坏游标、partial 降级、handler
// （404 / 无 DB 时降级返回可用源）。PG 端到端在 rest_timeline_e2e_pg_test.go。
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"opscopilot/internal/connector"
	"opscopilot/internal/incident"
	"opscopilot/internal/topology"
)

// fakeSource 把固定条目/missing 原因包装成 timelineSource（表驱动假源用）。
func fakeSource(name string, items []TimelineItem, missing string) timelineSource {
	return timelineSource{
		name: name,
		fetch: func(_ context.Context, _ incident.Incident) ([]TimelineItem, string, error) {
			return items, missing, nil
		},
	}
}

func mustItem(ts time.Time, kind, id string) TimelineItem {
	return TimelineItem{TS: ts, Kind: kind, SourceID: id, Summary: id}
}

// timelineSig 条目序列的紧凑指纹（kind|source_id），断言顺序用。
func timelineSig(items []TimelineItem) string {
	parts := make([]string, 0, len(items))
	for _, it := range items {
		parts = append(parts, it.Kind+"|"+it.SourceID)
	}
	return strings.Join(parts, ",")
}

// TestTimelineMergeThreeSources 三源混排：乱序输入 → 按 ts 升序归并，
// severity/confidence 逐条透传。
func TestTimelineMergeThreeSources(t *testing.T) {
	t0 := time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC)
	alerts := []TimelineItem{
		{TS: t0.Add(2 * time.Minute), Kind: TimelineKindAlertOut, SourceID: "alert:fp2", Summary: "dedup", Severity: "warning"},
		{TS: t0.Add(1 * time.Minute), Kind: TimelineKindAlertIn, SourceID: "alert:fp1", Summary: "new", Severity: "critical"},
	}
	changes := []TimelineItem{
		{TS: t0.Add(3 * time.Minute), Kind: TimelineKindChange, SourceID: "change:c1", Summary: "deploy", Confidence: "high"},
	}
	actions := []TimelineItem{
		{TS: t0, Kind: TimelineKindAction, SourceID: "action:a1", Summary: "transition"},
	}
	page, err := assembleTimeline(context.Background(), incident.Incident{ID: "INC-merge"}, []timelineSource{
		fakeSource("alert", alerts, ""),
		fakeSource("change", changes, ""),
		fakeSource("action", actions, ""),
	}, 100, 0)
	if err != nil {
		t.Fatalf("assemble: %v", err)
	}
	if page.Total != 4 || len(page.Items) != 4 || page.Next != "" {
		t.Fatalf("page = total %d items %d next %q, want 4/4/empty", page.Total, len(page.Items), page.Next)
	}
	if want := "action|action:a1,alert_in|alert:fp1,alert_out|alert:fp2,change|change:c1"; timelineSig(page.Items) != want {
		t.Fatalf("merged order = %s, want %s", timelineSig(page.Items), want)
	}
	if page.Partial || len(page.Missing) != 0 {
		t.Fatalf("partial/missing = %v/%v, want false/empty", page.Partial, page.Missing)
	}
	// 字段透传：severity 只在告警、confidence 只在变更。
	if page.Items[1].Severity != "critical" || page.Items[3].Confidence != "high" {
		t.Fatalf("field passthrough lost: %+v", page.Items)
	}
}

// TestTimelineTieStableOrder 同刻稳定序：change < action < alert_in < alert_out，
// 同 kind 内 source_id 升序；归并对源/输入顺序不敏感（决定性）。
func TestTimelineTieStableOrder(t *testing.T) {
	at := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	mk := func(kind, id string) TimelineItem { return mustItem(at, kind, id) }
	inputs := [][]timelineSource{
		{
			fakeSource("alert", []TimelineItem{mk(TimelineKindAlertOut, "alert:z"), mk(TimelineKindAlertIn, "alert:b")}, ""),
			fakeSource("change", []TimelineItem{mk(TimelineKindChange, "change:m")}, ""),
			fakeSource("action", []TimelineItem{mk(TimelineKindAction, "action:a1"), mk(TimelineKindAction, "action:a0")}, ""),
		},
		{ // 源顺序颠倒 + 同 kind 内乱序 → 归并结果必须逐字一致。
			fakeSource("action", []TimelineItem{mk(TimelineKindAction, "action:a0"), mk(TimelineKindAction, "action:a1")}, ""),
			fakeSource("change", []TimelineItem{mk(TimelineKindChange, "change:m")}, ""),
			fakeSource("alert", []TimelineItem{mk(TimelineKindAlertIn, "alert:b"), mk(TimelineKindAlertOut, "alert:z")}, ""),
		},
	}
	const want = "change|change:m,action|action:a0,action|action:a1,alert_in|alert:b,alert_out|alert:z"
	for i, srcs := range inputs {
		page, err := assembleTimeline(context.Background(), incident.Incident{ID: "INC-tie"}, srcs, 100, 0)
		if err != nil {
			t.Fatalf("case %d: %v", i, err)
		}
		if got := timelineSig(page.Items); got != want {
			t.Fatalf("case %d stable order = %s, want %s", i, got, want)
		}
	}
}

// TestTimelinePaginationNoDupNoMiss 25 条 × 每页 7：跨页并集=全量、
// 页内顺序与全量一致、末页 next_cursor 为空、游标由服务端自洽。
func TestTimelinePaginationNoDupNoMiss(t *testing.T) {
	t0 := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	var items []TimelineItem
	for i := 0; i < 25; i++ {
		items = append(items, mustItem(t0.Add(time.Duration(i)*time.Minute),
			TimelineKindAction, fmt.Sprintf("action:%02d", i)))
	}
	srcs := []timelineSource{fakeSource("action", items, "")}

	full, err := assembleTimeline(context.Background(), incident.Incident{ID: "INC-page"}, srcs, 25, 0)
	if err != nil {
		t.Fatal(err)
	}

	var gotIDs []string
	seen := map[string]bool{}
	cursor := ""
	pages := 0
	for {
		offset := 0
		if cursor != "" {
			if offset, err = decodeTimelineCursor(cursor); err != nil {
				t.Fatalf("bad cursor emitted by server: %v", err)
			}
		}
		page, err := assembleTimeline(context.Background(), incident.Incident{ID: "INC-page"}, srcs, 7, offset)
		if err != nil {
			t.Fatal(err)
		}
		pages++
		if pages > 10 {
			t.Fatal("cursor never terminates")
		}
		for _, it := range page.Items {
			if seen[it.SourceID] {
				t.Fatalf("duplicate across pages: %s", it.SourceID)
			}
			seen[it.SourceID] = true
			gotIDs = append(gotIDs, it.SourceID)
		}
		if page.Next == "" {
			break
		}
		cursor = page.Next
	}
	if len(gotIDs) != 25 || pages != 4 {
		t.Fatalf("collected %d items in %d pages, want 25 in 4 (no dup no miss violated)", len(gotIDs), pages)
	}
	for i := range gotIDs {
		if gotIDs[i] != full.Items[i].SourceID {
			t.Fatalf("page walk order differs from full list at %d: %v", i, gotIDs)
		}
	}
}

// TestTimelineCursorCodec 游标编解码与坏游标（400 的根因层）。
func TestTimelineCursorCodec(t *testing.T) {
	for _, n := range []int{0, 7, 12345} {
		got, err := decodeTimelineCursor(encodeTimelineCursor(n))
		if err != nil || got != n {
			t.Fatalf("roundtrip %d: got %d err %v", n, got, err)
		}
	}
	for _, bad := range []string{"!!!not-base64!!!", "abc123", "-1", "notanumber"} {
		if _, err := decodeTimelineCursor(bad); err != ErrTimelineBadCursor {
			t.Fatalf("bad cursor %q: err = %v, want ErrTimelineBadCursor", bad, err)
		}
	}
}

// TestTimelinePartialDegradation 依赖缺席的降级：可用源照常归并、
// 缺席源进 missing、partial=true（HTTP 200 语义在 handler 层测）。
func TestTimelinePartialDegradation(t *testing.T) {
	at := time.Now()
	page, err := assembleTimeline(context.Background(), incident.Incident{ID: "INC-p"}, []timelineSource{
		fakeSource("alert", nil, "not wired (set OPS_DB_DSN)"),
		fakeSource("change", []TimelineItem{mustItem(at, TimelineKindChange, "change:c1")}, ""),
		fakeSource("action", nil, "query failed (see server log)"),
	}, 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !page.Partial {
		t.Fatal("partial = false, want true")
	}
	if len(page.Missing) != 2 || page.Missing["alert"] == "" || page.Missing["action"] == "" {
		t.Fatalf("missing = %v, want alert+action", page.Missing)
	}
	if len(page.Items) != 1 || page.Items[0].Kind != TimelineKindChange {
		t.Fatalf("items = %v, want only available change source", page.Items)
	}
}

// ---------- handler 级（内存装配，无 DB）：404 / 坏参数 / 降级归并 ----------

func TestTimelineHandlerNotFoundAndBadParams(t *testing.T) {
	asm, h := restTest(t)
	if code, _ := getJSON(t, h, "/api/v1/incidents/nope/timeline"); code != http.StatusNotFound {
		t.Fatalf("unknown incident: code = %d, want 404", code)
	}
	if _, err := asm.Incidents.Create("INC-tl-params", "时间线用例", "info", "tester"); err != nil {
		t.Fatalf("seed incident: %v", err)
	}
	base := "/api/v1/incidents/INC-tl-params/timeline"
	if code, _ := getJSON(t, h, base+"?limit=-2"); code != http.StatusBadRequest {
		t.Fatalf("negative limit: code = %d, want 400", code)
	}
	if code, _ := getJSON(t, h, base+"?limit=abc"); code != http.StatusBadRequest {
		t.Fatalf("non-numeric limit: code = %d, want 400", code)
	}
	if code, _ := getJSON(t, h, base+"?cursor=%23%24bad"); code != http.StatusBadRequest {
		t.Fatalf("bad cursor: code = %d, want 400", code)
	}
	// 可用但全空：手工单无簇、无变更、建单走 Store 直连不写审计 → 空页 200。
	code, body := getJSON(t, h, base)
	if code != http.StatusOK {
		t.Fatalf("empty timeline: code = %d body = %v", code, body)
	}
	if body["total"].(float64) != 0 || body["next_cursor"] != "" {
		t.Fatalf("empty timeline body = %v, want total 0 / no cursor", body)
	}
}

// TestTimelineHandlerDegradedNoDB 无 DSN 场景的降级语义端到端（handler 层）：
// 告警源缺席计入 partial+missing，change（内存变更库×故障域）与 action
// （内存审计）照常归并单调；这就是任务口径的"仅 audit+内存 change 可用则
// 返回可用的并带 partial 标记"。
func TestTimelineHandlerDegradedNoDB(t *testing.T) {
	asm, h := restTest(t)
	now := time.Now()

	// ① 节点入图 + 变更（Record 的 nodeCheck 要求节点先在拓扑）。
	if err := asm.Sink.IngestDiscover(context.Background(), &connector.DiscoverResult{
		Nodes: []connector.ResourceNode{{
			Key: "tl-n1", Type: "host", ObservedAt: now,
			Labels: map[string]string{"instance": "i-tl"},
		}},
	}); err != nil {
		t.Fatalf("discover: %v", err)
	}
	if _, err := asm.Changes.Record(topology.ChangeEvent{
		ID: "chg-tl", NodeKey: "tl-n1", Type: topology.ChangeDeploy,
		Source: "jenkins", Author: "bot", Summary: "v2 deploy",
		OccurredAt: now.Add(-5 * time.Minute),
	}); err != nil {
		t.Fatalf("record change: %v", err)
	}

	// ② 告警成簇（instance 反查 → 故障域含 tl-n1）并挂到事件。
	asm.Noise.ProcessAlerts([]connector.Alert{{
		Fingerprint: "fp-tl", Labels: map[string]string{"alertname": "DiskFull", "instance": "i-tl"},
		Severity: "critical", StartsAt: now,
	}})
	active := asm.Noise.shadow.Clusterer().ActiveClusters()
	if len(active) == 0 {
		t.Fatal("no cluster formed")
	}
	if _, err := asm.Incidents.Create("INC-tl-deg", "磁盘打满", "critical", "tester"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := asm.Incidents.AttachCluster("INC-tl-deg", active[0].Key); err != nil {
		t.Fatalf("attach: %v", err)
	}

	// ③ 人工 ack（真 handler 写路径 → MemAuditLog）。
	req := httptest.NewRequest(http.MethodPost, "/api/v1/incidents/INC-tl-deg/transition",
		strings.NewReader(`{"to":"acked","actor":"zhangsan"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("transition: code = %d body = %s", rec.Code, rec.Body.String())
	}

	// ④ 时间线：200 + partial（无 DB ⇒ alert 缺席）+ change/action 归并单调。
	code, body := getJSON(t, h, "/api/v1/incidents/INC-tl-deg/timeline")
	if code != http.StatusOK {
		t.Fatalf("timeline: code = %d body = %v", code, body)
	}
	if body["partial"] != true {
		t.Fatalf("partial = %v, want true (no DB)", body["partial"])
	}
	missing, _ := body["missing"].(map[string]any)
	if missing == nil || missing["alert"] == nil {
		t.Fatalf("missing = %v, want alert entry", body["missing"])
	}
	itemsRaw, _ := body["items"].([]any)
	if len(itemsRaw) < 2 {
		t.Fatalf("items = %v, want change+action merged", body["items"])
	}
	var prev time.Time
	var sawChange, sawAction bool
	for _, raw := range itemsRaw {
		it, _ := raw.(map[string]any)
		tsStr, _ := it["ts"].(string)
		ts, err := time.Parse(time.RFC3339Nano, tsStr)
		if err != nil {
			t.Fatalf("bad ts %q: %v", tsStr, err)
		}
		if ts.Before(prev) {
			t.Fatalf("timeline not monotonic: %v after %v", ts, prev)
		}
		prev = ts
		switch it["kind"] {
		case TimelineKindChange:
			sawChange = true
			if c, _ := it["confidence"].(string); c == "" {
				t.Fatalf("change item lost confidence: %v", it)
			}
			if s, _ := it["summary"].(string); !strings.Contains(s, "v2 deploy") {
				t.Fatalf("change summary = %q, want to carry change summary", s)
			}
		case TimelineKindAction:
			sawAction = true
			if s, _ := it["summary"].(string); !strings.Contains(s, "zhangsan") {
				t.Fatalf("action summary = %q, want actor in it", s)
			}
		}
	}
	if !sawChange || !sawAction {
		t.Fatalf("merged items = %v, want both change and action", body["items"])
	}
}

// TestTimelineItemJSON DTO 的 JSON 契约锁：snake_case、omitempty、ts RFC3339。
func TestTimelineItemJSON(t *testing.T) {
	b, err := json.Marshal(TimelineItem{
		TS: time.Date(2026, 3, 1, 8, 0, 0, 0, time.UTC), Kind: TimelineKindAlertIn,
		SourceID: "alert:fp:1", Summary: "HighCPU", Severity: "critical",
	})
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	if _, ok := m["confidence"]; ok {
		t.Fatalf("confidence should be omitted when empty: %s", b)
	}
	if m["kind"] != "alert_in" || m["source_id"] != "alert:fp:1" || m["severity"] != "critical" {
		t.Fatalf("json contract drift: %s", b)
	}
	if ts, _ := m["ts"].(string); !strings.HasPrefix(ts, "2026-03-01T08:00:00Z") {
		t.Fatalf("ts format = %v, want RFC3339", m["ts"])
	}
}
