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
	// 缓存纪律：控制台必须每次回源（旧副本会让新功能"看不见"）。
	if cc := rec.Header().Get("Cache-Control"); !strings.Contains(cc, "no-store") {
		t.Fatalf("cache-control = %q, want no-store", cc)
	}
	body := rec.Body.String()
	// 构建标识占位符必须已被替换（运维据此确认打开的是哪个 build）。
	if strings.Contains(body, "{{BUILD}}") {
		t.Fatal("build stamp placeholder not substituted")
	}
	for _, marker := range []string{
		"OpsCopilot 控制台", "api/v1/clusters", "api/v1/topology", "AGG_THRESHOLD",
		// W9 双链路事件页：来源徽标 + 事件 API + 写操作端点。
		"api/v1/incidents", "view-events", "originBadge", "/transition", "/merge",
		// W11 实时推送：SSE 终结端 + 前端订阅 + 降级轮询。
		"api/v1/events/stream", "EventSource", "streamChip", "sseLive",
		// 安全：动作按钮走 data-* + 事件委托；esc 覆盖单引号。
		`data-act="transition"`, `data-act="merge"`, "&#39;",
	} {
		if !strings.Contains(body, marker) {
			t.Fatalf("console.html missing marker %q", marker)
		}
	}
	// XSS 回归门禁：禁止把服务端数据拼进内联 onclick 的 JS 字符串
	// （事件 id 可含引号，闭合后即可注入脚本、窃取会话写 Token）。
	for _, bad := range []string{`onclick="doTransition(`, `onclick="doMerge(`} {
		if strings.Contains(body, bad) {
			t.Fatalf("console.html must not inline server data into onclick (%q)", bad)
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
