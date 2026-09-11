// notify_channels_test.go W9-2：渠道配置校验、存储存取、REST 端点与
// 端到端（装配 + 假渠道：new-incident 送达、dedup/merge 不重复通知）。
package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"opscopilot/internal/connector"
	"opscopilot/internal/notify"
)

func TestValidateChannelRules(t *testing.T) {
	ok := []struct{ name, kind, url, sev string }{
		{"ops-feishu", notify.KindFeishu, "https://open.feishu.cn/open-apis/bot/v2/hook/xxx", "critical"},
		{"wecom-1", notify.KindWecom, "http://127.0.0.1:9999/send", "warning"},
		{"A.b_c-1", notify.KindGeneric, "https://example.com/hook", ""}, // 空 = info
		{"info-sink", notify.KindGeneric, "https://example.com/hook", "info"},
	}
	for _, c := range ok {
		if err := ValidateChannel(c.name, c.kind, c.url, c.sev); err != nil {
			t.Fatalf("valid channel rejected (%s, sev=%q): %v", c.name, c.sev, err)
		}
	}
	bad := []struct{ name, kind, url, sev string }{
		{"", notify.KindGeneric, "http://x", ""},
		{"console", notify.KindGeneric, "http://x", ""},                       // 保留名（兜底渠道）
		{"-lead", notify.KindGeneric, "http://x", ""},                         // 首字符非法
		{"has space", notify.KindGeneric, "http://x", ""},                     // 字符集
		{"ok", "slack", "http://x", ""},                                       // kind 白名单
		{"ok", notify.KindGeneric, "ftp://x", ""},                             // 非 http(s)
		{"ok", notify.KindGeneric, "", ""},                                    // 空 url
		{"ok", notify.KindGeneric, "http://" + strings.Repeat("a", 2100), ""}, // 超长
		{"ok", notify.KindGeneric, "http://x", "fatal"},                       // min_severity 白名单
	}
	for _, c := range bad {
		if err := ValidateChannel(c.name, c.kind, c.url, c.sev); err == nil {
			t.Fatalf("invalid channel accepted: name=%q kind=%q url=%.20q sev=%q", c.name, c.kind, c.url, c.sev)
		}
	}
}

// TestNormalizeMinSeverity 空/空白/大小写归一。
func TestNormalizeMinSeverity(t *testing.T) {
	for in, want := range map[string]string{
		"":         "info",
		"   ":      "info",
		"INFO":     "info",
		" Warning": "warning",
		"critical": "critical",
	} {
		if got := NormalizeMinSeverity(in); got != want {
			t.Fatalf("NormalizeMinSeverity(%q) = %q, want %q", in, got, want)
		}
	}
}

// channelStoreForTest PG 门控构造 + 清理。
func channelStoreForTest(t *testing.T) (*ChannelStore, func()) {
	t.Helper()
	dsn := os.Getenv("OPS_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("OPS_TEST_PG_DSN not set")
	}
	sink := pgSinkForTest(t)
	store := NewChannelStore(sink.pool, "channel-test")
	cleanup := func() {
		pgExec(t, sink, `DELETE FROM notify_channel WHERE tenant_id = $1`, "channel-test")
	}
	cleanup()
	return store, cleanup
}

func TestChannelStoreRoundtrip(t *testing.T) {
	store, cleanup := channelStoreForTest(t)
	defer cleanup()
	ctx := context.Background()

	if err := store.Upsert(ctx, "ch1", notify.KindGeneric, "http://a/hook", "", true); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	// 幂等 upsert：同 name 覆盖（含 min_severity 一并覆盖）
	if err := store.Upsert(ctx, "ch1", notify.KindFeishu, "http://b/hook", "critical", true); err != nil {
		t.Fatalf("upsert 2: %v", err)
	}
	if err := store.Upsert(ctx, "ch2", notify.KindWecom, "http://c/hook", "warning", false); err != nil {
		t.Fatalf("upsert 3: %v", err)
	}

	all, err := store.List(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("list len = %d, want 2", len(all))
	}
	if all[0].Name != "ch1" || all[0].Kind != notify.KindFeishu || all[0].URL != "http://b/hook" || all[0].MinSeverity != "critical" {
		t.Fatalf("ch1 not overwritten: %+v", all[0])
	}
	if all[1].Name != "ch2" || all[1].MinSeverity != "warning" {
		t.Fatalf("ch2 min_severity = %+v, want warning", all[1])
	}
	enabled, err := store.ListEnabled(ctx)
	if err != nil {
		t.Fatalf("list enabled: %v", err)
	}
	if len(enabled) != 1 || enabled[0].Name != "ch1" {
		t.Fatalf("enabled = %+v, want only ch1", enabled)
	}

	if err := store.SetEnabled(ctx, "ch2", true); err != nil {
		t.Fatalf("set enabled: %v", err)
	}
	if err := store.SetEnabled(ctx, "nope", true); err != ErrChannelNotFound {
		t.Fatalf("missing channel toggle err = %v, want ErrChannelNotFound", err)
	}

	if err := store.Delete(ctx, "ch2"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := store.Delete(ctx, "ch2"); err != ErrChannelNotFound {
		t.Fatalf("double delete err = %v, want ErrChannelNotFound", err)
	}
	// 校验失败不得落库
	if err := store.Upsert(ctx, "bad name", notify.KindGeneric, "http://x", "", true); err == nil {
		t.Fatal("invalid upsert must fail")
	}
}

func TestLoadChannelsIntoRegistryAndReload(t *testing.T) {
	store, cleanup := channelStoreForTest(t)
	defer cleanup()
	ctx := context.Background()

	if err := store.Upsert(ctx, "chA", notify.KindGeneric, "http://a/hook", "", true); err != nil {
		t.Fatal(err)
	}
	reg := notify.NewRegistry()
	n, err := loadChannelsIntoRegistry(ctx, store, reg, func(string, ...any) {})
	if err != nil || n != 1 {
		t.Fatalf("load n=%d err=%v, want 1/nil", n, err)
	}
	if got := reg.Names(); len(got) != 1 || got[0] != "chA" {
		t.Fatalf("registry = %v, want [chA]", got)
	}
	// 禁用后重载：先清后注册 → 不留残影
	if err := store.SetEnabled(ctx, "chA", false); err != nil {
		t.Fatal(err)
	}
	reg.Clear()
	if n, _ := loadChannelsIntoRegistry(ctx, store, reg, func(string, ...any) {}); n != 0 {
		t.Fatalf("disabled channel still loaded (n=%d)", n)
	}
	if reg.Len() != 0 {
		t.Fatalf("registry not empty after reload: %v", reg.Names())
	}
}

// TestAssemblyChannelE2E 端到端（装配 + 假渠道）：
// ① 新指纹 → 恰好一条通知；② 同批并入的第二个告警不重复通知；
// ③ 窗口内重复指纹不重复通知；④ dedup/merge 计入 suppressed。
func TestAssemblyChannelE2E(t *testing.T) {
	dsn := os.Getenv("OPS_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("OPS_TEST_PG_DSN not set")
	}
	t.Setenv("OPS_DB_DSN", dsn)
	t.Setenv("OPS_NOISE_MODE", "enforce")
	t.Setenv("OPS_NOISE_WINDOW", "10m")

	// 假渠道：本地 httptest sink
	var got []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		got = append(got, body["title"].(string))
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	sinkStore := pgSinkForTest(t)
	store := NewChannelStore(sinkStore.pool, DefaultTenant)
	cleanup := func() {
		pgExec(t, sinkStore, `DELETE FROM notify_channel WHERE tenant_id = $1`, DefaultTenant)
		pgExec(t, sinkStore, `UPDATE notify_gate_stats SET suppressed=0, dispatched=0 WHERE tenant_id = $1`, DefaultTenant)
	}
	cleanup()
	t.Cleanup(cleanup)
	if err := store.Upsert(context.Background(), "e2e-sink", notify.KindGeneric, srv.URL, "", true); err != nil {
		t.Fatalf("seed channel: %v", err)
	}

	asm, err := NewAssembly(newQuietLogger(), "tok")
	if err != nil {
		t.Fatalf("assembly: %v", err)
	}
	defer asm.pool.Close()

	now := time.Now()
	// 先发现节点：告警要映射到 NodeKey 才参与故障域聚合（空 NodeKey 的
	// 告警按设计不参与域聚合——见 noise.go toEvent 注释）。
	if err := asm.Sink.IngestDiscover(context.Background(), &connector.DiscoverResult{
		Nodes: []connector.ResourceNode{
			{Key: "prometheus://nodes/n1", Type: "node", Labels: map[string]string{"instance": "n1"}, ObservedAt: now},
		},
	}); err != nil {
		t.Fatalf("IngestDiscover: %v", err)
	}
	// ① 新指纹 + ② 同节点不同指纹（同故障域 → 并入既有簇 → 不通知）
	if err := asm.Sink.IngestCollect(nil, &connector.CollectResult{Alerts: []connector.Alert{
		{Fingerprint: "fp-e2e-disk", Labels: map[string]string{"alertname": "HighDisk", "instance": "n1"}, StartsAt: now},
		{Fingerprint: "fp-e2e-inode", Labels: map[string]string{"alertname": "InodeExhaust", "instance": "n1"}, StartsAt: now.Add(time.Second)},
	}}); err != nil {
		t.Fatalf("IngestCollect: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("after batch: %d notifications, want 1 (merge must not notify); got %v", len(got), got)
	}
	// ③ 窗口内重复 → 不通知
	if err := asm.Sink.IngestCollect(nil, &connector.CollectResult{Alerts: []connector.Alert{
		{Fingerprint: "fp-e2e-disk", Labels: map[string]string{"alertname": "HighDisk", "instance": "n1"}, StartsAt: now.Add(30 * time.Second)},
	}}); err != nil {
		t.Fatalf("IngestCollect 2: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("after dedup: %d notifications, want still 1; got %v", len(got), got)
	}
	st := asm.Noise.gate.Stats()
	if st.Dispatched != 1 || st.Suppressed != 2 {
		t.Fatalf("gate stats = %+v, want dispatched=1 suppressed=2", st)
	}
	// 注册表热生效：console 兜底 + 1 个真实渠道
	if names := asm.NotifyReg.Names(); len(names) != 2 {
		t.Fatalf("registry names = %v, want console + e2e-sink", names)
	}
}

// TestNotifyChannelRESTEndpoints 写路径门禁与校验（PG 门控）。
func TestNotifyChannelRESTEndpoints(t *testing.T) {
	dsn := os.Getenv("OPS_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("OPS_TEST_PG_DSN not set")
	}
	t.Setenv("OPS_DB_DSN", dsn)
	t.Setenv("OPS_NOISE_MODE", "enforce")
	sink := pgSinkForTest(t)
	cleanup := func() { pgExec(t, sink, `DELETE FROM notify_channel WHERE tenant_id = $1`, DefaultTenant) }
	cleanup()
	t.Cleanup(cleanup)

	asm, err := NewAssembly(newQuietLogger(), "tok")
	if err != nil {
		t.Fatalf("assembly: %v", err)
	}
	defer asm.pool.Close()
	h := asm.Handler()

	do := func(method, path, ct, body, token string) *httptest.ResponseRecorder {
		var rdr io.Reader
		if body != "" {
			rdr = strings.NewReader(body)
		}
		req := httptest.NewRequest(method, path, rdr)
		if ct != "" {
			req.Header.Set("Content-Type", ct)
		}
		if token != "" {
			req.Header.Set(AuthHeader, token)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}

	// 读路径无鉴权
	if rec := do(http.MethodGet, "/api/v1/notify/channels", "", "", ""); rec.Code != http.StatusOK {
		t.Fatalf("GET list = %d, want 200", rec.Code)
	}
	// 写路径：无 token → 401
	if rec := do(http.MethodPost, "/api/v1/notify/channels", "application/json",
		`{"name":"x","kind":"generic","url":"http://h"}`, ""); rec.Code != http.StatusUnauthorized {
		t.Fatalf("POST without token = %d, want 401", rec.Code)
	}
	// 免预检 CT → 415（CSRF 防护）
	if rec := do(http.MethodPost, "/api/v1/notify/channels", "text/plain",
		`{"name":"x","kind":"generic","url":"http://h"}`, "tok"); rec.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("POST text/plain = %d, want 415", rec.Code)
	}
	// 非法 kind → 400（带原因）
	if rec := do(http.MethodPost, "/api/v1/notify/channels", "application/json",
		`{"name":"x","kind":"slack","url":"http://h"}`, "tok"); rec.Code != http.StatusBadRequest {
		t.Fatalf("POST bad kind = %d, want 400 (body %s)", rec.Code, rec.Body.String())
	}
	// 保留名 → 400
	if rec := do(http.MethodPost, "/api/v1/notify/channels", "application/json",
		`{"name":"console","kind":"generic","url":"http://h"}`, "tok"); rec.Code != http.StatusBadRequest {
		t.Fatalf("POST reserved name = %d, want 400", rec.Code)
	}
	// 正常创建 → 200 且 active_channels 含 console 兜底 + 新渠道
	rec := do(http.MethodPost, "/api/v1/notify/channels", "application/json",
		`{"name":"rest-ch","kind":"wecom","url":"http://127.0.0.1:9/hook"}`, "tok")
	if rec.Code != http.StatusOK {
		t.Fatalf("POST valid = %d, body %s", rec.Code, rec.Body.String())
	}
	var created map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if n, _ := created["active_channels"].(float64); n < 2 {
		t.Fatalf("active_channels = %v, want >=2 (console + rest-ch)", created["active_channels"])
	}
	// 列表含新渠道
	rec = do(http.MethodGet, "/api/v1/notify/channels", "", "", "")
	if !strings.Contains(rec.Body.String(), "rest-ch") {
		t.Fatalf("list missing created channel: %s", rec.Body.String())
	}
	// 软开关
	if rec := do(http.MethodPost, "/api/v1/notify/channels/rest-ch/enabled", "application/json",
		`{"enabled":false}`, "tok"); rec.Code != http.StatusOK {
		t.Fatalf("toggle = %d, body %s", rec.Code, rec.Body.String())
	}
	// 删除 + 二次删除 404
	if rec := do(http.MethodDelete, "/api/v1/notify/channels/rest-ch", "", "", "tok"); rec.Code != http.StatusOK {
		t.Fatalf("delete = %d, body %s", rec.Code, rec.Body.String())
	}
	if rec := do(http.MethodDelete, "/api/v1/notify/channels/rest-ch", "", "", "tok"); rec.Code != http.StatusNotFound {
		t.Fatalf("delete twice = %d, want 404", rec.Code)
	}
}
