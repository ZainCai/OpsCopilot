// sla_rest_test.go W10-2（F-04）SLA 的 REST 面测试（内存 Store，无 DB 依赖）：
//   - GET 事件读视图三字段（detail/list/201 响应）与"覆盖优先、按级默认"；
//   - slaViewOf 口径单测（breached 判定、终态冻结、mitigated 不冻结、未知级兜底）；
//   - create/transition 入参 sla_minutes 校验与 acked_at 的 REST 回读。
//     KPI 端点测试（含手算对账）见 kpi_rest_test.go；本文件的 harness
//     （newSLAKPIGateway/doJSON/decodeMap/mustField）为两者共用。
package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"opscopilot/internal/incident"
)

func newSLAKPIGateway(t *testing.T) (*incident.MemStore, *httptest.Server) {
	t.Helper()
	store := incident.NewMemStore()
	g := NewRESTGateway(nil, nil, store, "", nil, nil, RESTLimits{
		SLACritical: 60 * time.Minute,
		SLAWarning:  240 * time.Minute,
		SLAInfo:     1440 * time.Minute,
		KPIWindow:   168 * time.Hour,
	})
	mux := http.NewServeMux()
	g.Register(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return store, srv
}

func doJSON(t *testing.T, method, url string, body any) (int, []byte) {
	t.Helper()
	var rd io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		rd = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, url, rd)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do %s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	all, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, all
}

// decodeIncidentView 响应 → 通用 map（视图字段断言用）。
func decodeMap(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("decode: %v\n%s", err, raw)
	}
	return m
}

func mustField[T any](t *testing.T, m map[string]any, key string) T {
	t.Helper()
	v, ok := m[key]
	if !ok {
		t.Fatalf("field %q missing in %+v", key, m)
	}
	tv, ok := v.(T)
	if !ok {
		t.Fatalf("field %q = %T, want %T", key, v, tv)
	}
	return tv
}

// TestSLAViewOnIncidentEndpoints 创建/详情/列表三处 GET 视图字段齐备，
// 覆盖优先、无覆盖按级默认。
func TestSLAViewOnIncidentEndpoints(t *testing.T) {
	_, srv := newSLAKPIGateway(t)

	// 带覆盖建单：deadline = created + 30min。
	code, raw := doJSON(t, "POST", srv.URL+"/api/v1/incidents", map[string]any{
		"title": "sla override", "severity": "critical", "created_by": "ops", "sla_minutes": 30,
	})
	if code != http.StatusCreated {
		t.Fatalf("create code = %d: %s", code, raw)
	}
	created := decodeMap(t, raw)
	if got := mustField[float64](t, created, "sla_minutes"); got != 30 {
		t.Fatalf("201 body sla_minutes = %v, want 30", created["sla_minutes"])
	}
	ct, err := time.Parse(time.RFC3339Nano, mustField[string](t, created, "created_at"))
	if err != nil {
		t.Fatalf("created_at: %v", err)
	}
	dl, err := time.Parse(time.RFC3339Nano, mustField[string](t, created, "sla_deadline"))
	if err != nil {
		t.Fatalf("sla_deadline: %v", err)
	}
	if !dl.Equal(ct.Add(30 * time.Minute)) {
		t.Fatalf("override deadline %v != created+30m (%v)", dl, ct.Add(30*time.Minute))
	}
	if mustField[bool](t, created, "sla_breached") {
		t.Fatal("fresh incident must not be breached")
	}
	if mustField[float64](t, created, "sla_remaining_seconds") <= 29*60 {
		t.Fatal("remaining should be ~30min for a fresh 30min SLA")
	}

	// 详情 GET 同样带视图字段。
	code, raw = doJSON(t, "GET", srv.URL+"/api/v1/incidents/"+mustField[string](t, created, "id"), nil)
	if code != http.StatusOK {
		t.Fatalf("detail code = %d: %s", code, raw)
	}
	if _, ok := decodeMap(t, raw)["sla_deadline"]; !ok {
		t.Fatalf("detail view missing sla_deadline: %s", raw)
	}

	// 列表 items 是视图（数组每项含三字段）。
	code, raw = doJSON(t, "GET", srv.URL+"/api/v1/incidents", nil)
	if code != http.StatusOK {
		t.Fatalf("list code = %d: %s", code, raw)
	}
	var lr struct {
		Incidents []map[string]any `json:"incidents"`
	}
	if err := json.Unmarshal(raw, &lr); err != nil {
		t.Fatalf("list decode: %v", err)
	}
	if len(lr.Incidents) != 1 {
		t.Fatalf("list items = %d, want 1", len(lr.Incidents))
	}
	for _, key := range []string{"sla_deadline", "sla_remaining_seconds", "sla_breached"} {
		if _, ok := lr.Incidents[0][key]; !ok {
			t.Fatalf("list item missing %s: %+v", key, lr.Incidents[0])
		}
	}

	// 无覆盖建单：deadline = created + 60min（critical 按级默认）。
	code, raw = doJSON(t, "POST", srv.URL+"/api/v1/incidents", map[string]any{
		"id": "T-DEFAULT60", "title": "sla default", "severity": "critical", "created_by": "ops",
	})
	if code != http.StatusCreated {
		t.Fatalf("create2 code = %d: %s", code, raw)
	}
	created2 := decodeMap(t, raw)
	if got := mustField[float64](t, created2, "sla_minutes"); got != 0 {
		t.Fatalf("sla_minutes = %v, want 0 (no override)", created2["sla_minutes"])
	}
	ct2, _ := time.Parse(time.RFC3339Nano, mustField[string](t, created2, "created_at"))
	dl2, _ := time.Parse(time.RFC3339Nano, mustField[string](t, created2, "sla_deadline"))
	if !dl2.Equal(ct2.Add(60 * time.Minute)) {
		t.Fatalf("default deadline %v != created+60m (%v)", dl2, ct2.Add(60*time.Minute))
	}
	// warning 档默认 240min。
	code, raw = doJSON(t, "POST", srv.URL+"/api/v1/incidents", map[string]any{
		"id": "T-DEFAULT240", "title": "warn default", "severity": "warning", "created_by": "ops",
	})
	if code != http.StatusCreated {
		t.Fatalf("create3 code = %d: %s", code, raw)
	}
	created3 := decodeMap(t, raw)
	ct3, _ := time.Parse(time.RFC3339Nano, mustField[string](t, created3, "created_at"))
	dl3, _ := time.Parse(time.RFC3339Nano, mustField[string](t, created3, "sla_deadline"))
	if !dl3.Equal(ct3.Add(240 * time.Minute)) {
		t.Fatalf("warning default deadline %v != created+240m", dl3)
	}

	// 入参校验：负数/超上限/非法 severity → 400。
	for _, bad := range []map[string]any{
		{"title": "neg", "severity": "info", "created_by": "ops", "sla_minutes": -5},
		{"title": "huge", "severity": "info", "created_by": "ops", "sla_minutes": slaMinutesMax + 1},
	} {
		if code, raw := doJSON(t, "POST", srv.URL+"/api/v1/incidents", bad); code != http.StatusBadRequest {
			t.Fatalf("bad body %v: code = %d (%s), want 400", bad, code, raw)
		}
	}
}

// TestTransitionCarriesSLAAndAckedAt 流转可带 SLA 覆盖；ack 后详情视图回读出
// acked_at（首戳的 REST 面证据；只落一次的硬断言在契约 17）。
func TestTransitionCarriesSLAAndAckedAt(t *testing.T) {
	store, srv := newSLAKPIGateway(t)
	if _, err := store.Create("T-ACK", "t", "critical", "ops"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if code, raw := doJSON(t, "POST", srv.URL+"/api/v1/incidents/T-ACK/transition",
		map[string]any{"to": "acked", "actor": "bob", "sla_minutes": 90}); code != http.StatusOK {
		t.Fatalf("transition code = %d: %s", code, raw)
	}
	_, raw := doJSON(t, "GET", srv.URL+"/api/v1/incidents/T-ACK", nil)
	m := decodeMap(t, raw)
	if got := mustField[float64](t, m, "sla_minutes"); got != 90 {
		t.Fatalf("sla_minutes after transition = %v, want 90", m["sla_minutes"])
	}
	if acked := mustField[string](t, m, "acked_at"); acked == "" || acked == "0001-01-01T00:00:00Z" {
		t.Fatalf("acked_at missing after ack: %s", raw)
	}
	if code, raw := doJSON(t, "POST", srv.URL+"/api/v1/incidents/T-ACK/transition",
		map[string]any{"to": "resolved", "actor": "bob", "sla_minutes": -1}); code != http.StatusBadRequest {
		t.Fatalf("negative sla on transition: code = %d (%s), want 400", code, raw)
	}
	if got, err := store.Get("T-ACK"); err != nil || got.SLAMinutes != 90 {
		t.Fatalf("rejected transition mutated sla_minutes: %+v err=%v", got, err)
	}
}

// TestSLAViewDerivation slaViewOf 口径单测（rest_sla.go 文件头四规则的逐条锁）。
func TestSLAViewDerivation(t *testing.T) {
	g := &RESTGateway{limits: RESTLimits{
		SLACritical: 60 * time.Minute, SLAWarning: 240 * time.Minute, SLAInfo: 1440 * time.Minute,
	}}
	base := time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)

	// 1) 未闭环已过线 → breached，remaining 为负、量等于超时时长。
	v := g.slaViewOf(incident.Incident{Severity: "critical", State: incident.StateOpen, CreatedAt: base},
		base.Add(2*time.Hour))
	if !v.SLABreached || v.SLARemainingSeconds != -3600 {
		t.Fatalf("open overdue: %+v", v)
	}
	// 2) 覆盖优先：sla_minutes=30 压过 critical 默认 60。
	v = g.slaViewOf(incident.Incident{Severity: "critical", State: incident.StateOpen, CreatedAt: base, SLAMinutes: 30},
		base.Add(45*time.Minute))
	if !v.SLABreached {
		t.Fatalf("30min override must breach at +45min: %+v", v)
	}
	// 3) 终态冻结：deadline 前闭环 ⇒ 永远不红（哪怕 now 远过线）。
	v = g.slaViewOf(incident.Incident{Severity: "critical", State: incident.StateResolved, CreatedAt: base,
		ResolvedAt: base.Add(30 * time.Minute)}, base.Add(30*24*time.Hour))
	if v.SLABreached || v.SLARemainingSeconds != 1800 {
		t.Fatalf("terminal freeze wrong: %+v", v)
	}
	// 4) 终态且超线闭环 ⇒ 定格超时事实。
	v = g.slaViewOf(incident.Incident{Severity: "critical", State: incident.StateResolved, CreatedAt: base,
		ResolvedAt: base.Add(90 * time.Minute)}, base)
	if !v.SLABreached || v.SLARemainingSeconds != -1800 {
		t.Fatalf("terminal overdue wrong: %+v", v)
	}
	// 5) mitigated 不是终态：时钟继续走。
	v = g.slaViewOf(incident.Incident{Severity: "critical", State: incident.StateMitigated, CreatedAt: base},
		base.Add(61*time.Minute))
	if !v.SLABreached {
		t.Fatalf("mitigated must keep running: %+v", v)
	}
	// 6) 未知 severity 按 info 档兜底（不给 0 目标）。
	v = g.slaViewOf(incident.Incident{Severity: "catastrophic", State: incident.StateOpen, CreatedAt: base},
		base.Add(23*time.Hour))
	if v.SLABreached {
		t.Fatalf("unknown severity must fall back to info bucket (24h), not 0 target: %+v", v)
	}
	// resolved 但 ResolvedAt 零（理论脏数据）：按 now 继续计，不谎报冻结。
	v = g.slaViewOf(incident.Incident{Severity: "critical", State: incident.StateResolved, CreatedAt: base},
		base.Add(61*time.Minute))
	if !v.SLABreached {
		t.Fatalf("resolved_at zero must not fake a freeze: %+v", v)
	}
}
