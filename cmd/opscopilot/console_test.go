// W5-2.3 控制台路由测试：可达性 + 内容类型 + / 跳转。
package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestConsoleServed(t *testing.T) {
	_, h := restTest(t)

	req := httptest.NewRequest(http.MethodGet, "/console", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /console: code = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "text/html") {
		t.Fatalf("content-type = %q, want text/html", ct)
	}
	body := rec.Body.String()
	for _, marker := range []string{
		"OpsCopilot 控制台", "api/v1/clusters", "api/v1/topology", "AGG_THRESHOLD",
		// W9 双链路事件页：来源徽标 + 事件 API + 写操作端点。
		"api/v1/incidents", "view-events", "originBadge", "/transition", "/merge",
	} {
		if !strings.Contains(body, marker) {
			t.Fatalf("console.html missing marker %q", marker)
		}
	}
}

func TestRootRedirectsToConsole(t *testing.T) {
	_, h := restTest(t)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusFound {
		t.Fatalf("GET /: code = %d, want 302", rec.Code)
	}
	if loc := rec.Header().Get("Location"); loc != "/console" {
		t.Fatalf("location = %q, want /console", loc)
	}
}
