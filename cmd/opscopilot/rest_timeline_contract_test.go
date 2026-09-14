// rest_timeline_contract_test.go P2-D3：GET /api/v1/incidents/{id}/timeline
// 的 HTTP 契约体——mem（无 DB 内存装配）与 PG（真库全链路）两实现跑同一
// 套断言，防"双份单侧测"行为漂移（对齐 internal/incident
// store_contract_test.go 的 runContract 打法：契约体只断言两实现都必须
// 成立的行为，实现差异参数化）。
//
// 两实现差异（参数表达）：
//   - alert 源可用性：mem 装配无 DSN ⇒ alert 缺席进 missing + partial=true；
//     PG 生产路径三源齐备 ⇒ partial=false、轮询到 alert_in 出现（判决异步
//     落库）；
//   - action 条目下限：PG 生产路径多一条 attach_cluster 审计（system 自挂）。
//
// 契约体只断言结构（单调、kind 期望、字段非空、翻页不重不漏、partial/
// missing 口径），不绑各驱动器种子的具体文案。
package main

import (
	"net/http"
	"strings"
	"testing"
	"time"
)

// timelineContractEnv 契约测试的装配面（mem/PG 各提供一个驱动器）。
type timelineContractEnv struct {
	name        string
	newHandler  func(t *testing.T) (h http.Handler, incID string)
	expectAlert bool // 该装配下 alert 源是否可用（mem 无 DSN → false）
	minActions  int  // action 条目下限（PG 生产路径含 attach_cluster 审计）
}

func runTimelineHandlerContract(t *testing.T, env timelineContractEnv) {
	t.Helper()
	h, incID := env.newHandler(t)
	base := "/api/v1/incidents/" + incID + "/timeline"

	// ① 不存在的 incident → 404（口径同 GET /incidents/{id}）。
	if code, _ := getJSON(t, h, "/api/v1/incidents/INC-zzz-missing/timeline"); code != http.StatusNotFound {
		t.Fatalf("unknown incident: code = %d, want 404", code)
	}

	// ② 坏参数 → 400（limit 非负整数、游标可解码）。
	for _, q := range []string{"limit=-2", "limit=abc", "cursor=%23%24bad"} {
		if code, _ := getJSON(t, h, base+"?"+q); code != http.StatusBadRequest {
			t.Fatalf("bad param %q: code = %d, want 400", q, code)
		}
	}

	// ③ 整页：契约字段与时间单调。alert 源可用时轮询到 alert_in 出现
	//（PG 判决异步落库队列）；不可用时断言缺席原因进 missing。
	var body map[string]any
	deadline := time.Now().Add(20 * time.Second)
	for {
		code, b := getJSON(t, h, base+"?limit=100")
		if code != http.StatusOK {
			t.Fatalf("timeline: code = %d body = %v", code, b)
		}
		body = b
		if !env.expectAlert || hasTimelineAlertKind(b) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("alert 判决未落库/未关联，items=%v missing=%v", b["items"], b["missing"])
		}
		time.Sleep(100 * time.Millisecond)
	}

	// partial 与 missing 口径：partial = 有源缺席；可用源不留 missing 条目。
	if env.expectAlert {
		if body["partial"] != false {
			t.Fatalf("三源齐备 partial = %v, want false", body["partial"])
		}
		if missing, _ := body["missing"].(map[string]any); len(missing) != 0 {
			t.Fatalf("missing = %v, want empty (all sources available)", body["missing"])
		}
	} else {
		if body["partial"] != true {
			t.Fatalf("无 DB partial = %v, want true", body["partial"])
		}
		missing, _ := body["missing"].(map[string]any)
		if missing == nil || missing["alert"] == nil {
			t.Fatalf("missing = %v, want alert entry (no DSN)", body["missing"])
		}
	}

	// 条目契约：kind 期望、ts 单调非降、change 带 confidence+summary、
	// action 带 actor 段（"by "）、alert 条目数按可用性。
	itemsRaw, _ := body["items"].([]any)
	kinds := map[string]int{}
	var prev time.Time
	for _, raw := range itemsRaw {
		it, _ := raw.(map[string]any)
		tsStr, _ := it["ts"].(string)
		ts, err := time.Parse(time.RFC3339Nano, tsStr)
		if err != nil {
			t.Fatalf("bad ts %q: %v", tsStr, err)
		}
		if ts.Before(prev) {
			t.Fatalf("timeline not time-monotonic: %v after %v (%+v)", ts, prev, itemsRaw)
		}
		prev = ts
		kind, _ := it["kind"].(string)
		kinds[kind]++
		switch kind {
		case TimelineKindChange:
			if c, _ := it["confidence"].(string); c == "" {
				t.Fatalf("change item lost confidence: %v", it)
			}
			if s, _ := it["summary"].(string); s == "" {
				t.Fatalf("change item empty summary: %v", it)
			}
		case TimelineKindAction:
			if s, _ := it["summary"].(string); !strings.Contains(s, "by ") {
				t.Fatalf("action summary = %q, want actor segment (by …)", s)
			}
		}
	}
	if kinds["change"] < 1 {
		t.Fatalf("kinds = %v, want change >= 1", kinds)
	}
	if kinds["action"] < env.minActions {
		t.Fatalf("action = %d, want >= %d: %+v", kinds["action"], env.minActions, body["items"])
	}
	if env.expectAlert {
		if kinds["alert_in"] < 1 {
			t.Fatalf("alert_in = %d, want >= 1: %+v", kinds["alert_in"], body["items"])
		}
	} else if kinds["alert_in"]+kinds["alert_out"] != 0 {
		t.Fatalf("alert 源缺席却出现 alert 条目: %+v", body["items"])
	}

	// ④ limit=1 翻页：并集=全量、不重不漏、顺序与整页一致、游标自洽终止。
	var walked []map[string]any
	seen := map[string]bool{}
	cursor := ""
	pages := 0
	for {
		path := base + "?limit=1"
		if cursor != "" {
			path += "&cursor=" + cursor
		}
		code, pb := getJSON(t, h, path)
		if code != http.StatusOK {
			t.Fatalf("page walk: code = %d body = %v", code, pb)
		}
		pageItems, _ := pb["items"].([]any)
		if len(pageItems) != 1 {
			t.Fatalf("limit=1 page items = %v", pageItems)
		}
		it, _ := pageItems[0].(map[string]any)
		sid, _ := it["source_id"].(string)
		if seen[sid] {
			t.Fatalf("duplicate across pages: %s", sid)
		}
		seen[sid] = true
		walked = append(walked, it)
		if pb["next_cursor"] == "" {
			break
		}
		cursor, _ = pb["next_cursor"].(string)
		pages++
		if pages > 30 {
			t.Fatal("cursor walk did not terminate")
		}
	}
	totalF, _ := body["total"].(float64)
	if len(walked) != int(totalF) {
		t.Fatalf("walked %d items, want total %d", len(walked), int(totalF))
	}
	for i := range walked {
		fullIt, _ := itemsRaw[i].(map[string]any)
		if walked[i]["source_id"] != fullIt["source_id"] {
			t.Fatalf("walk differs from full page at %d: %v vs %v", i, walked[i]["source_id"], fullIt["source_id"])
		}
	}
}

// hasTimelineAlertKind items 里是否已有 alert_* 条目（PG 轮询条件）。
func hasTimelineAlertKind(body map[string]any) bool {
	itemsRaw, _ := body["items"].([]any)
	for _, raw := range itemsRaw {
		it, _ := raw.(map[string]any)
		kind, _ := it["kind"].(string)
		if strings.HasPrefix(kind, "alert_") {
			return true
		}
	}
	return false
}
