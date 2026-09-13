// W12 审计解锁包：GET /api/v1/audit 的 REST 层测试（httptest，内存装配）。
// 覆盖：鉴权口径（读路径无 Token——与 /{id}/audit 同族，锁死不误伤）、
// 200 契约形状（entries/next_cursor/persistence/partial_hint）、summary
// 提取与缺键回退、400 参数族（未知 action/坏游标/坏 limit/坏时间戳）、
// 游标翻页端到端、后端故障 500 脱敏（D5/D1）、audit_reads 计数口径。
package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func getAuditJSON(t *testing.T, h http.Handler, path string) (int, map[string]any) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil && rec.Body.Len() > 0 {
		t.Fatalf("%s: non-JSON body %q: %v", path, rec.Body.String(), err)
	}
	return rec.Code, body
}

func auditEntries(t *testing.T, body map[string]any) []map[string]any {
	t.Helper()
	raw, err := json.Marshal(body["entries"])
	if err != nil {
		t.Fatalf("re-marshal entries: %v", err)
	}
	var out []map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("entries shape: %v", err)
	}
	return out
}

// seedRestAudit 经内存审计追加 5 条（Append 打当下时间戳 → 追加序即时间序，
// 输出按 seq DESC = 倒序回显）。
func seedRestAudit(t *testing.T, asm *Assembly) {
	t.Helper()
	l, ok := asm.audit.(*MemAuditLog)
	if !ok {
		t.Fatalf("expected MemAuditLog in no-DSN assembly, got %T", asm.audit)
	}
	l.Append(AuditEntry{IncidentID: "INC-a", Action: AuditCreate, Actor: "zhang",
		Detail: map[string]any{"title": "磁盘满", "origin": "manual"}})
	l.Append(AuditEntry{IncidentID: "INC-a", Action: AuditTransition, Actor: "li",
		Detail: map[string]any{"to": "acked"}})
	l.Append(AuditEntry{IncidentID: "INC-b", Action: AuditRCA, Actor: "auto",
		Detail: map[string]any{"root_causes": float64(1), "findings": float64(4), "duration_ms": float64(1200)}})
	l.Append(AuditEntry{IncidentID: "INC-b", Action: AuditMerge, Actor: "zhang",
		Detail: map[string]any{"merged_into": "INC-a"}})
	// 未知动作（历史/直写数据）：detail 无既定摘要键 → 回退压缩形态。
	l.Append(AuditEntry{IncidentID: "INC-c", Action: AuditAction("legacy_op"), Actor: "ops",
		Detail: map[string]any{"note": strings.Repeat("长", 200)}})
}

func TestAuditGlobalEndpointOK(t *testing.T) {
	asm, h := restTest(t)
	seedRestAudit(t, asm)

	// 鉴权口径：不带任何 Token 的 GET 必须 200（与 GET /{id}/audit 同款
	// "读路径无鉴权、bind loopback-only" 口径——锁死，防止将来无意识收紧
	// 或本实现无意识放宽到 RCA 那档）。
	code, body := getAuditJSON(t, h, "/api/v1/audit")
	if code != http.StatusOK {
		t.Fatalf("code = %d body=%v", code, body)
	}
	if body["persistence"] != "memory" {
		t.Fatalf("persistence = %v, want memory (R6-4 如实标注)", body["persistence"])
	}
	if ph, _ := body["partial_hint"].(string); !strings.Contains(ph, "in-memory") {
		t.Fatalf("partial_hint = %v, want memory 易失声明", body["partial_hint"])
	}
	entries := auditEntries(t, body)
	if len(entries) != 5 {
		t.Fatalf("count = %d, want 5", len(entries))
	}
	// 最新优先 = 追加序倒排；未知动作行在最前（最后追加）。
	wantIDs := []string{"INC-c", "INC-b", "INC-b", "INC-a", "INC-a"}
	wantActions := []string{"legacy_op", "merge", "rca", "transition", "create"}
	for i, e := range entries {
		if e["incident_id"] != wantIDs[i] || e["action"] != wantActions[i] {
			t.Fatalf("row %d = %v/%v, want %v/%v", i, e["incident_id"], e["action"], wantIDs[i], wantActions[i])
		}
		for _, k := range []string{"actor", "occurred_at", "summary"} {
			if _, ok := e[k]; !ok {
				t.Fatalf("row %d missing key %q", i, k)
			}
		}
	}
	// summary 提取（各 action 的既定键 + 缺键回退）。
	if got := entries[4]["summary"]; got != "建单：磁盘满" {
		t.Fatalf("create summary = %v", got)
	}
	if got := entries[3]["summary"]; got != "流转到 acked" {
		t.Fatalf("transition summary = %v", got)
	}
	if got := entries[2]["summary"]; got != "根因 1 条 · findings 4 · 用时 1200ms" {
		t.Fatalf("rca summary = %v", got)
	}
	if got := entries[1]["summary"]; got != "合并到 INC-a" {
		t.Fatalf("merge summary = %v", got)
	}
	legacy := entries[0]["summary"].(string)
	if !strings.HasPrefix(legacy, "note=") {
		t.Fatalf("fallback summary = %q, want compact k=v detail", legacy)
	}
	if r := []rune(strings.TrimSuffix(legacy, "…")); len(r) > auditSummaryMaxRunes {
		t.Fatalf("fallback summary not truncated to %d runes: %d", auditSummaryMaxRunes, len(r))
	}
	if !strings.HasSuffix(legacy, "…") {
		t.Fatal("truncated summary must carry ellipsis")
	}
}

func TestAuditGlobalFilterAndPaging(t *testing.T) {
	asm, h := restTest(t)
	seedRestAudit(t, asm)

	_, body := getAuditJSON(t, h, "/api/v1/audit?actor=zhang")
	entries := auditEntries(t, body)
	if len(entries) != 2 || entries[0]["action"] != "merge" || entries[1]["action"] != "create" {
		t.Fatalf("actor=zhang → %v", entries)
	}
	_, body = getAuditJSON(t, h, "/api/v1/audit?action=rca")
	entries = auditEntries(t, body)
	if len(entries) != 1 || entries[0]["incident_id"] != "INC-b" {
		t.Fatalf("action=rca → %v", entries)
	}
	// 时间窗：until 在过去 → 空列表 200（"没记录"与"故障"的区分仍成立）。
	// QueryEscape 必须——RFC3339 的 `+08:00` 裸进 query 会被解成空格。
	_, body = getAuditJSON(t, h, "/api/v1/audit?until="+url.QueryEscape(time.Now().Add(-time.Hour).Format(time.RFC3339)))
	if entries := auditEntries(t, body); len(entries) != 0 {
		t.Fatalf("until=-1h should be empty, got %v", entries)
	}
	// 游标翻页不重不漏。
	seen := []string{}
	cur := ""
	for i := 0; i < 10; i++ {
		_, body = getAuditJSON(t, h, fmt.Sprintf("/api/v1/audit?limit=2&cursor=%s", cur))
		entries = auditEntries(t, body)
		for _, e := range entries {
			seen = append(seen, fmt.Sprintf("%v/%v", e["incident_id"], e["action"]))
		}
		next, _ := body["next_cursor"].(string)
		if next == "" {
			break
		}
		cur = next
	}
	want := []string{"INC-c/legacy_op", "INC-b/merge", "INC-b/rca", "INC-a/transition", "INC-a/create"}
	if strings.Join(seen, ",") != strings.Join(want, ",") {
		t.Fatalf("REST walk = %v, want %v", seen, want)
	}
}

func TestAuditGlobalBadParams400(t *testing.T) {
	_, h := restTest(t)
	for path, wantMsg := range map[string]string{
		"/api/v1/audit?action=delete_everything": "action must be one of",
		"/api/v1/audit?cursor=%21%21%21bad":      "bad cursor",
		"/api/v1/audit?limit=-5":                 "limit must be a non-negative integer",
		"/api/v1/audit?since=yesterday":          "since must be RFC3339",
		"/api/v1/audit?until=soon":               "until must be RFC3339",
	} {
		code, body := getAuditJSON(t, h, path)
		if code != http.StatusBadRequest {
			t.Fatalf("%s → %d (%v), want 400", path, code, body)
		}
		if err, _ := body["error"].(string); !strings.Contains(err, wantMsg) {
			t.Fatalf("%s error = %q, want contains %q", path, err, wantMsg)
		}
	}
	// 400 文案必须枚举封闭集合（对齐"未知 action → 明确错误"口径）。
	_, body := getAuditJSON(t, h, "/api/v1/audit?action=nope")
	if err, _ := body["error"].(string); !strings.Contains(err, "rca") || !strings.Contains(err, "attach_cluster") {
		t.Fatalf("400 message should enumerate closed set, got %q", err)
	}
}

func TestAuditGlobalBackendFailure500(t *testing.T) {
	asm, h := restTest(t)
	asm.REST.SetAudit(failingAuditLog{}) // 同接口故障替身（audit_test.go）

	for _, path := range []string{"/api/v1/audit", "/api/v1/audit?action=rca", "/api/v1/incidents/X/audit"} {
		code, body := getAuditJSON(t, h, path)
		if code != http.StatusInternalServerError {
			t.Fatalf("%s → %d, want 500", path, code)
		}
		if err, _ := body["error"].(string); err != "internal error" {
			t.Fatalf("%s leaked: %q", path, err)
		}
	}
	// 后端故障的读取尝试仍计入 audit_reads（降级面要数得出来）。
	if got := asm.Metrics.AuditReads[auditSourceGlobal].Value(); got != 2 {
		t.Fatalf("global reads = %d, want 2 (failures included)", got)
	}
	if got := asm.Metrics.AuditReads[auditSourceIncident].Value(); got != 1 {
		t.Fatalf("incident reads = %d, want 1", got)
	}
}

func TestAuditReadsCounterAccounting(t *testing.T) {
	asm, h := restTest(t)
	seedRestAudit(t, asm)
	g, i := asm.Metrics.AuditReads[auditSourceGlobal], asm.Metrics.AuditReads[auditSourceIncident]

	// 400 族不触库不计。
	if code, _ := getAuditJSON(t, h, "/api/v1/audit?action=nope"); code != 400 {
		t.Fatalf("code = %d", code)
	}
	if g.Value() != 0 || i.Value() != 0 {
		t.Fatalf("bad params must not count: global=%d incident=%d", g.Value(), i.Value())
	}
	// 成功读取各计一次；两个入口分桶不串。
	if code, _ := getAuditJSON(t, h, "/api/v1/audit"); code != 200 {
		t.Fatalf("code = %d", code)
	}
	if code, _ := getAuditJSON(t, h, "/api/v1/incidents/INC-a/audit"); code != 200 {
		t.Fatalf("code = %d", code)
	}
	if g.Value() != 1 || i.Value() != 1 {
		t.Fatalf("counters = global %d incident %d, want 1/1", g.Value(), i.Value())
	}
	// /metrics 文本可见两维标签（暴露面无漂移）。
	text := gatherMetrics(t, asm.Metrics)
	if !strings.Contains(text, `opscopilot_audit_reads_total{source="global"} 1`) ||
		!strings.Contains(text, `opscopilot_audit_reads_total{source="incident"} 1`) {
		t.Fatalf("metrics text missing audit_reads lines:\n%s", text)
	}
}
