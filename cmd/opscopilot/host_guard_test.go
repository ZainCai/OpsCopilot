// Host 头校验（第十一轮 P1-1）：回环监听下的 DNS rebinding 防护。
//
// 三组边界：
//   - 伪造 Host（rebinding 域名 / 端口不符 / 大小写变体 / 空 Host）→ 403；
//   - 三个回环形态（含缺端口形态）→ 放行；
//   - 非回环监听配置 → 校验不启用（该形态已有 token 强制门禁）。
//
// 另有一组覆盖面用例：guard 包在整棵 mux 之外，SSE/console/静态/指标/写路径
// 一律同口径 403——留一个豁免路由就是留一条穿透路径。
package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"

	"opscopilot/internal/config"
)

// hostGuardProbe 构造一个"放行即 200"的被包装 handler。
func hostGuardProbe(listenAddr string) http.Handler {
	return hostGuard(listenAddr, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("reached"))
	}))
}

// doWithHost 发一个只改 Host 头的请求（httptest.NewRequest 默认 Host 是
// example.com，本身就是"非本机来源"形态，故一律显式覆盖）。
func doWithHost(h http.Handler, method, target, host string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, target, nil)
	req.Host = host
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// TestHostGuardRejectsRebinding 伪造 Host 拒：rebinding 域名、端口不符、
// 大小写变体、空 Host、通配地址形态一律 403，且响应为固定文案（不回显 Host）。
func TestHostGuardRejectsRebinding(t *testing.T) {
	h := hostGuardProbe("127.0.0.1:8080")
	bad := []string{
		"evil.com",           // rebinding 域名（浏览器会带端口，也覆盖不带的形态）
		"evil.com:8080",      // rebinding 域名 + 配置端口
		"127.0.0.1.evil.com", // 后缀伪装：前缀看着像回环，实际是攻击者域名
		"localhost.evil.com", // 同上
		"127.0.0.1:9999",     // 回环 host 但端口与配置不符（跨实例串台）
		"LOCALHOST:8080",     // 大写形态不做归一，一律拒
		"Localhost:8080",     // 同上
		"[::1]:9999",         // IPv6 回环但端口不符
		"::1",                // 裸 IPv6：Host 头规范形态必须带方括号
		"0.0.0.0:8080",       // 通配地址不是合法的目标 host
		"10.0.0.5:8080",      // 内网网卡地址
		"",                   // 空 Host（HTTP/1.0 无 Host 形态）
		"127.0.0.1:8080 ",    // 尾随空白：字符串全等比对不放宽
		"evil.com:80",        // 常见反代端口伪装
	}
	for _, host := range bad {
		rec := doWithHost(h, http.MethodGet, "/api/v1/audit", host)
		if rec.Code != http.StatusForbidden {
			t.Fatalf("Host %q: code = %d, want 403", host, rec.Code)
		}
		var body map[string]string
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("Host %q: 非 JSON 响应 %q: %v", host, rec.Body.String(), err)
		}
		if body["error"] != hostGuardRejection {
			t.Fatalf("Host %q: error = %q, want 固定文案 %q", host, body["error"], hostGuardRejection)
		}
		// 固定文案不得反射用户可控的 Host（XSS/日志注入面）。
		if host != "" && strings.Contains(rec.Body.String(), host) {
			t.Fatalf("Host %q: 响应体回显了 Host 头", host)
		}
	}
}

// TestHostGuardAllowsLoopbackForms 三形态回环 Host 通（带配置端口 + 缺端口形态）。
func TestHostGuardAllowsLoopbackForms(t *testing.T) {
	h := hostGuardProbe("127.0.0.1:8080")
	good := []string{
		"127.0.0.1:8080", // IPv4 回环 + 配置端口
		"localhost:8080", // 域名回环 + 配置端口
		"[::1]:8080",     // IPv6 回环（方括号形态）+ 配置端口
		"127.0.0.1",      // 缺端口形态：原样比对集合里的裸条目
		"localhost",
		"[::1]",
	}
	for _, host := range good {
		rec := doWithHost(h, http.MethodGet, "/api/v1/audit", host)
		if rec.Code != http.StatusOK {
			t.Fatalf("Host %q: code = %d, want 200（body=%s）", host, rec.Code, rec.Body.String())
		}
	}
}

// TestHostGuardDisabledForNonLoopbackListen 非回环监听配置不启用校验：
// 该形态 ADR-009 已强制 token 门禁（无 token 直接启动失败），且前置反代/
// 自定义域名会让 Host 白名单变成误杀源。
func TestHostGuardDisabledForNonLoopbackListen(t *testing.T) {
	for _, listen := range []string{"0.0.0.0:8080", ":8080", "10.0.0.5:8080", "[::]:8080", ""} {
		h := hostGuardProbe(listen)
		for _, host := range []string{"evil.com", "ops.example.com:443", "127.0.0.1:9999", ""} {
			rec := doWithHost(h, http.MethodGet, "/api/v1/audit", host)
			if rec.Code != http.StatusOK {
				t.Fatalf("listen %q + Host %q: code = %d, want 200（校验不应启用）",
					listen, host, rec.Code)
			}
		}
	}
}

// TestHostGuardUnknownListenPort 配置端口不可知（`:0` 让内核派随机端口、
// 或只写了 host）时退化为按 host 部分判定：回环 host 任意端口放行，
// 非回环 host 照拒——否则合法本机调用会带着随机端口被整段拒掉（静默砖化）。
func TestHostGuardUnknownListenPort(t *testing.T) {
	for _, listen := range []string{"127.0.0.1:0", "localhost", "[::1]"} {
		h := hostGuardProbe(listen)
		for _, host := range []string{"127.0.0.1:54321", "[::1]:54321", "localhost:1", "127.0.0.1"} {
			if rec := doWithHost(h, http.MethodGet, "/api/v1/audit", host); rec.Code != http.StatusOK {
				t.Fatalf("listen %q + Host %q: code = %d, want 200", listen, host, rec.Code)
			}
		}
		for _, host := range []string{"evil.com:54321", "evil.com", "127.0.0.1.evil.com:80"} {
			if rec := doWithHost(h, http.MethodGet, "/api/v1/audit", host); rec.Code != http.StatusForbidden {
				t.Fatalf("listen %q + Host %q: code = %d, want 403", listen, host, rec.Code)
			}
		}
	}
}

// TestHostGuardAllowSetConstruction 集合构造钉死：三个回环形态 × {配置端口,
// 缺端口} = 6 条，多一条少一条都算契约变更。
func TestHostGuardAllowSetConstruction(t *testing.T) {
	allow, portKnown := hostGuardAllowSet("127.0.0.1:8080")
	if !portKnown {
		t.Fatal("portKnown = false, want true（配置带明确端口）")
	}
	got := make([]string, 0, len(allow))
	for k := range allow {
		got = append(got, k)
	}
	sort.Strings(got)
	want := []string{"127.0.0.1", "127.0.0.1:8080", "[::1]", "[::1]:8080", "localhost", "localhost:8080"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("allow set = %v, want %v", got, want)
	}

	// 端口不可知形态：只含裸形态，portKnown=false。
	allow, portKnown = hostGuardAllowSet("[::1]:0")
	if portKnown {
		t.Fatal("`:0` 端口不可知，portKnown 应为 false")
	}
	if len(allow) != len(loopbackHostForms) {
		t.Fatalf("allow set = %v, want 仅 %d 条裸形态", allow, len(loopbackHostForms))
	}

	// 端口取配置而非硬编码：换端口集合跟着换（防"8080 写死在代码里"）。
	allow, _ = hostGuardAllowSet("localhost:9191")
	for _, h := range []string{"localhost:9191", "127.0.0.1:9191", "[::1]:9191"} {
		if _, ok := allow[h]; !ok {
			t.Fatalf("allow set 缺 %q", h)
		}
	}
	if _, ok := allow["localhost:8080"]; ok {
		t.Fatal("allow set 含旧端口 8080（端口未取配置）")
	}
}

// TestHostGuardCoversAllRoutes 覆盖面：guard 包在整棵 mux 之外，故 SSE、
// /console、换皮 UI 静态资源、/healthz、/metrics 与全部写路径同口径 403。
func TestHostGuardCoversAllRoutes(t *testing.T) {
	cfg := testAssemblyConfig("tok")
	cfg.UI.WebDist = webUIDist(t) // 挂上静态资源路由（/assets/）
	asm, mux := restTestWith(t, cfg)
	defer asm.Close()

	h := hostGuard(config.DefaultListenAddr, mux) // 127.0.0.1:8080
	routes := []struct{ method, path string }{
		{http.MethodGet, "/healthz"},
		{http.MethodGet, "/metrics"},
		{http.MethodGet, "/console"},
		{http.MethodGet, "/"},                     // 换皮 UI 首页
		{http.MethodGet, "/assets/app.js"},        // 静态产物
		{http.MethodGet, "/api/v1/audit"},         // 全局审计（高敏、读路径无 Token）
		{http.MethodGet, "/api/v1/events/stream"}, // SSE
		{http.MethodGet, "/api/v1/incidents"},     // 事件列表
		{http.MethodGet, "/api/v1/notify/channels"},
		{http.MethodPost, "/api/v1/incidents"},                  // 写路径（带 token 也必须先过 Host）
		{http.MethodPost, "/api/v1/incidents/INC-1/transition"}, // 写路径
		{http.MethodPost, "/api/v1/changes"},                    // 变更 webhook
		{http.MethodPost, "/api/v1/ingest/alertmanager"},        // 外部导入
	}
	for _, rt := range routes {
		rec := doWithHost(h, rt.method, rt.path, "evil.com:8080")
		if rec.Code != http.StatusForbidden {
			t.Fatalf("%s %s: 伪造 Host 下 code = %d, want 403", rt.method, rt.path, rec.Code)
		}
		if !strings.Contains(rec.Body.String(), hostGuardRejection) {
			t.Fatalf("%s %s: body = %q, want 固定文案", rt.method, rt.path, rec.Body.String())
		}
	}

	// 反向对照：合法回环 Host 下同一批只读路由照常工作（证明 guard 不是
	// 无脑 403）。SSE 与写路径不参与对照——前者会阻塞到超时，后者需要 body。
	for _, path := range []string{"/healthz", "/metrics", "/console", "/", "/assets/app.js",
		"/api/v1/audit", "/api/v1/incidents", "/api/v1/notify/channels"} {
		if rec := doWithHost(h, http.MethodGet, path, "localhost:8080"); rec.Code == http.StatusForbidden {
			t.Fatalf("GET %s: 合法回环 Host 被拒（code=403, body=%s）", path, rec.Body.String())
		}
	}
}

// TestNewHTTPServerWiresHostGuard 接线回归：newHTTPServer 必须包 hostGuard
// （单点接线，删了就等于把 P1-1 整段回退）。用 `:0` 之外的端口断言 403。
func TestNewHTTPServerWiresHostGuard(t *testing.T) {
	srv := newHTTPServer(config.Defaults().Metrics, "127.0.0.1:8080",
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	rec := doWithHost(srv.Handler, http.MethodGet, "/healthz", "evil.com:8080")
	if rec.Code != http.StatusForbidden {
		t.Fatalf("newHTTPServer 未接 hostGuard：伪造 Host code = %d, want 403", rec.Code)
	}
	if rec := doWithHost(srv.Handler, http.MethodGet, "/healthz", "127.0.0.1:8080"); rec.Code != http.StatusOK {
		t.Fatalf("合法回环 Host code = %d, want 200", rec.Code)
	}
}
