// rest_runbook_test.go W11-4 runbook REST 契约（无 DSN 形态，httptest 直调
// Assembly.Handler）：store 未接线一律显式 503（对齐 RCA/渠道降级口径——绝不
// 空 200 伪装"没有手册"）；写路径 Token 门禁先于接线判定（401 不被 503 掩盖，
// 与 createIncident 同序）；非 JSON 写请求 415（CSRF 免预检阻挡）。
// PG 真跑的全链闭环（挂载/记录/查看 + 404/409）在 rest_runbook_e2e_pg_test.go。
package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// doRunbook 通用请求件（同 doSession 形态）。
func doRunbook(t *testing.T, h http.Handler, method, path, token, ctype, body string) (int, map[string]any) {
	t.Helper()
	var r *http.Request
	if body == "" {
		r = httptest.NewRequest(method, path, nil)
	} else {
		r = httptest.NewRequest(method, path, strings.NewReader(body))
	}
	if ctype != "" {
		r.Header.Set("Content-Type", ctype)
	}
	if token != "" {
		r.Header.Set(AuthHeader, token)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	var out map[string]any
	if rec.Body.Len() > 0 {
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatalf("%s %s: non-JSON: %v (%s)", method, path, err, rec.Body.String())
		}
	}
	return rec.Code, out
}

// TestRESTRunbookNoDB503 无 DSN 装配（内存事件域）：七个 runbook 端点全部 503。
func TestRESTRunbookNoDB503(t *testing.T) {
	asm, err := NewAssembly(newQuietLogger(), testAssemblyConfig("tok"))
	if err != nil {
		t.Fatalf("assembly: %v", err)
	}
	defer asm.Close()
	if asm.Runbooks != nil {
		t.Fatal("runbook store must not be wired without DB (PG-only, no memory fallback)")
	}
	h := asm.Handler()
	paths := []struct{ method, path, ctype, body string }{
		{http.MethodGet, "/api/v1/runbooks", "", ""},
		{http.MethodPost, "/api/v1/runbooks", "application/json", `{"title":"t","created_by":"a"}`},
		{http.MethodGet, "/api/v1/incidents/INC-1/runbooks", "", ""},
		{http.MethodPost, "/api/v1/incidents/INC-1/runbooks", "application/json", `{"runbook_id":"rb","mounted_by":"a"}`},
		{http.MethodDelete, "/api/v1/incidents/INC-1/runbooks/rb", "", ""},
		{http.MethodGet, "/api/v1/incidents/INC-1/runbooks/rb/executions", "", ""},
		{http.MethodPost, "/api/v1/incidents/INC-1/runbooks/rb/executions", "application/json", `{"executed_by":"a","result":"done"}`},
	}
	for i, p := range paths {
		// 门禁序：Token → (POST 才要) Content-Type → 接线。DELETE 无 body 不强制
		// JSON（对齐 notify/channels）；GET 无鉴权直查接线。
		code, resp := doRunbook(t, h, p.method, p.path, "tok", p.ctype, p.body)
		if code != http.StatusServiceUnavailable {
			t.Fatalf("path %d %s %s: code = %d body = %v, want 503", i, p.method, p.path, code, resp)
		}
	}
}

// TestRESTRunbookTokenGate 写路径 401 先于 503（接线坏不给鉴权漏风可乘）；
// GET 读路径无 Token（与事件列表同口径）且仍 503。
func TestRESTRunbookTokenGate(t *testing.T) {
	asm, err := NewAssembly(newQuietLogger(), testAssemblyConfig("sekret"))
	if err != nil {
		t.Fatalf("assembly: %v", err)
	}
	defer asm.Close()
	h := asm.Handler()
	writes := []struct{ method, path, body string }{
		{http.MethodPost, "/api/v1/runbooks", `{"title":"t","created_by":"a"}`},
		{http.MethodPost, "/api/v1/incidents/INC-1/runbooks", `{"runbook_id":"rb","mounted_by":"a"}`},
		{http.MethodPost, "/api/v1/incidents/INC-1/runbooks/rb/executions", `{"executed_by":"a","result":"x"}`},
	}
	for _, w := range writes {
		if code, _ := doRunbook(t, h, w.method, w.path, "", "application/json", w.body); code != http.StatusUnauthorized {
			t.Fatalf("%s %s no token: code = %d, want 401", w.method, w.path, code)
		}
		if code, _ := doRunbook(t, h, w.method, w.path, "wrong", "application/json", w.body); code != http.StatusUnauthorized {
			t.Fatalf("%s %s bad token: code = %d, want 401", w.method, w.path, code)
		}
	}
	if code, _ := doRunbook(t, h, http.MethodDelete, "/api/v1/incidents/INC-1/runbooks/rb", "wrong", "application/json", ""); code != http.StatusUnauthorized {
		t.Fatalf("DELETE bad token: code = %d, want 401", code)
	}
}

// TestRESTRunbookWriteRequiresJSON CSRF 口径：写路径非 application/json → 415
// （先于 503——接线状态不泄露给未验 CSRF 的请求没有意义，顺序是 Token → JSON → 接线）。
func TestRESTRunbookWriteRequiresJSON(t *testing.T) {
	asm, err := NewAssembly(newQuietLogger(), testAssemblyConfig("tok"))
	if err != nil {
		t.Fatalf("assembly: %v", err)
	}
	defer asm.Close()
	h := asm.Handler()
	if code, _ := doRunbook(t, h, http.MethodPost, "/api/v1/runbooks", "tok", "text/plain", `x`); code != http.StatusUnsupportedMediaType {
		t.Fatalf("non-json POST: code = %d, want 415", code)
	}
}
