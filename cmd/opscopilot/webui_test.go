package main

// W-UI 换皮控制台挂载测试：dist 有效/无效/禁用三态下的根路由行为，且任何
// 状态都不破坏 /console 与 API 路由（dist 缺失回退历史 302 行为）。

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"opscopilot/internal/config"
)

const webUIMarker = "webui-marker"

// webUIDist 构造临时 dist（index.html + assets/app.js）。
func webUIDist(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "assets"), 0o755); err != nil {
		t.Fatalf("mkdir assets: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "index.html"), []byte("<html>"+webUIMarker+"</html>"), 0o644); err != nil {
		t.Fatalf("write index: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "assets", "app.js"), []byte("console.log('webui')"), 0o644); err != nil {
		t.Fatalf("write asset: %v", err)
	}
	return dir
}

func TestWebUIServedWhenDistValid(t *testing.T) {
	cfg := testAssemblyConfig("")
	cfg.UI.WebDist = webUIDist(t)
	_, h := restTestWith(t, cfg)

	// 首页：返回换皮 index.html（no-store 缓存纪律）。
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /: code = %d, want 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), webUIMarker) {
		t.Fatalf("GET /: body 缺 web 产物标记")
	}
	if cc := rec.Header().Get("Cache-Control"); !strings.Contains(cc, "no-store") {
		t.Fatalf("GET /: Cache-Control = %q, want no-store", cc)
	}

	// 静态产物：/assets/* 正常返回。
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/assets/app.js", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /assets/app.js: code = %d, want 200", rec.Code)
	}
	if body := rec.Body.String(); !strings.Contains(body, "webui") {
		t.Fatalf("GET /assets/app.js: body = %q", body)
	}

	// /console 最小视图仍独立可达（go:embed 冻结版）。
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/console", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /console: code = %d, want 200", rec.Code)
	}

	// API 不受挂载影响。
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/incidents", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/v1/incidents: code = %d, want 200", rec.Code)
	}
}

func TestWebUIFallsBackWhenDistInvalid(t *testing.T) {
	cfg := testAssemblyConfig("")
	cfg.UI.WebDist = t.TempDir() // 存在但无 index.html
	_, h := restTestWith(t, cfg)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusFound {
		t.Fatalf("GET / (dist 无效): code = %d, want 302", rec.Code)
	}
	if loc := rec.Header().Get("Location"); loc != "/console" {
		t.Fatalf("GET /: Location = %q, want /console", loc)
	}
}

func TestWebUIDisabledWhenEmpty(t *testing.T) {
	cfg := testAssemblyConfig("")
	cfg.UI.WebDist = "" // 显式禁用
	_, h := restTestWith(t, cfg)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusFound {
		t.Fatalf("GET / (禁用): code = %d, want 302", rec.Code)
	}
	if loc := rec.Header().Get("Location"); loc != "/console" {
		t.Fatalf("GET /: Location = %q, want /console", loc)
	}
}

// Defaults 基线含默认 web/dist 路径（相对 cwd），确保配置契约存在。
func TestWebUIDefaultPathSet(t *testing.T) {
	if got := config.Defaults().UI.WebDist; got != "web/dist" {
		t.Fatalf("Defaults().UI.WebDist = %q, want web/dist", got)
	}
}
