// W9 双链路一期测试（不依赖 DB）：外部幂等 upsert / 人工单属性 /
// 人工合并 / 队列入队幂等 / R2 外部恢复不关人工单。
package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"opscopilot/internal/incident"
)

// TestExternalUpsertIdempotent 验收口径①：同一外部事件推 10 次 → 仅 1 单。
func TestExternalUpsertIdempotent(t *testing.T) {
	s := incident.NewMemStore()
	created := 0
	for i := 0; i < 10; i++ {
		inc, isNew, err := s.UpsertExternal(incident.OriginAlertmanager, "fp-123",
			"disk full on n1", "critical", "system:alertmanager", `{"k":1}`)
		if err != nil {
			t.Fatalf("upsert #%d: %v", i, err)
		}
		if isNew {
			created++
		}
		if inc.Origin != incident.OriginAlertmanager || inc.SourceRef != "fp-123" {
			t.Fatalf("origin fields wrong: %+v", inc)
		}
	}
	if created != 1 {
		t.Fatalf("created = %d, want 1 (repeat push must not create)", created)
	}
	if got := len(mustList(s, "")); got != 1 {
		t.Fatalf("list = %d, want 1", got)
	}
	// 空 source_ref / 非法 origin 拒绝。
	if _, _, err := s.UpsertExternal(incident.OriginAlertmanager, "", "t", "critical", "x", "{}"); err == nil {
		t.Fatal("empty source_ref accepted")
	}
	if _, _, err := s.UpsertExternal("bogus", "r1", "t", "critical", "x", "{}"); err == nil {
		t.Fatal("bogus origin accepted")
	}
}

// TestManualIncidentAttributes 人工单来源属性（链路 B）。
func TestManualIncidentAttributes(t *testing.T) {
	s := incident.NewMemStore()
	inc, err := s.Create("INC-M1", "manual ticket", "warning", "ops")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if inc.Origin != incident.OriginManual {
		t.Fatalf("origin = %s, want manual", inc.Origin)
	}
	if inc.AutoClosePolicy != "manual_only" {
		t.Fatalf("auto_close_policy = %s, want manual_only (R2)", inc.AutoClosePolicy)
	}
	// 建单人是审计字段，必须真的落库（此前 Create 丢弃了它）。
	if inc.CreatedBy != "ops" {
		t.Fatalf("created_by = %q, want ops (must persist)", inc.CreatedBy)
	}
}

// TestMergeInto 人工合并（L2 由人触发）：源单关闭 + merged_into + 簇转移。
func TestMergeInto(t *testing.T) {
	s := incident.NewMemStore()
	if _, err := s.Create("A", "a", "critical", "ops"); err != nil {
		t.Fatalf("create A: %v", err)
	}
	if _, err := s.Create("B", "b", "warning", "ops"); err != nil {
		t.Fatalf("create B: %v", err)
	}
	if err := s.AttachCluster("B", "c:x@1"); err != nil {
		t.Fatalf("attach: %v", err)
	}
	if err := s.MergeInto("B", "A"); err != nil {
		t.Fatalf("merge: %v", err)
	}
	got, err := s.Get("B")
	if err != nil {
		t.Fatalf("get B: %v", err)
	}
	if got.State != incident.StateResolved || got.MergedInto != "A" {
		t.Fatalf("merged source wrong: %+v", got)
	}
	// 重复合并幂等。
	if err := s.MergeInto("B", "A"); err != nil {
		t.Fatalf("merge idempotent: %v", err)
	}
	// 自合并 / 不存在的单拒绝。
	if err := s.MergeInto("A", "A"); err == nil {
		t.Fatal("self merge accepted")
	}
	if err := s.MergeInto("A", "ZZZ"); err == nil {
		t.Fatal("merge into missing accepted")
	}
}

// TestAMWebhookEnqueueIdempotent 接收即入队 + 待处理幂等（重推不堆积）。
func TestAMWebhookEnqueueIdempotent(t *testing.T) {
	q := &fakeQueue{}
	owner := &QueueOwner{}
	owner.SetWriter(q)
	h := &AlertmanagerWebhook{Token: "secret", Owner: owner}
	mux := http.NewServeMux()
	h.Register(mux)

	body := `{"status":"firing","alerts":[{"labels":{"alertname":"DiskFull","instance":"n1","severity":"critical"},"annotations":{"summary":"disk full"},"startsAt":"2026-09-10T10:00:00Z","endsAt":"0001-01-01T00:00:00Z","fingerprint":"fp-am-1"}]}`
	do := func() *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/ingest/alertmanager", strings.NewReader(body))
		req.Header.Set(AuthHeader, "secret")
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		return rec
	}
	// 无 Token → 401。
	req := httptest.NewRequest(http.MethodPost, "/api/v1/ingest/alertmanager", strings.NewReader(body))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("no token: code = %d, want 401", rec.Code)
	}
	// 首次 202 + queued=1。
	r1 := do()
	if r1.Code != http.StatusAccepted {
		t.Fatalf("first push: code = %d body=%s", r1.Code, r1.Body.String())
	}
	var resp struct {
		Queued   int `json:"queued"`
		Skipped  int `json:"skipped"`
		Received int `json:"received"`
	}
	_ = json.Unmarshal(r1.Body.Bytes(), &resp)
	if resp.Queued != 1 || resp.Received != 1 {
		t.Fatalf("first push resp = %+v, want queued=1", resp)
	}
	// 重推 → skipped（幂等命中，不建第二单）。
	q.seen = true // 模拟待处理已存在
	r2 := do()
	_ = json.Unmarshal(r2.Body.Bytes(), &resp)
	if resp.Queued != 0 || resp.Skipped != 1 {
		t.Fatalf("repeat push resp = %+v, want skipped=1", resp)
	}
}

// TestWorkerHonorsAutoClosePolicy R2：人工接手（acked）后外部恢复到达
// → 不自动关单（防外部恢复吞掉人工处置）。
func TestWorkerHonorsAutoClosePolicy(t *testing.T) {
	store := incident.NewMemStore()
	if _, _, err := store.UpsertExternal(incident.OriginAlertmanager, "fp-r2",
		"n1 unreachable", "critical", "system:alertmanager", "{}"); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	// 人工确认 → auto_close_policy 自动转 manual_only。
	got, err := store.Transition("alertmanager:fp-r2", incident.StateAcked, "ops")
	if err != nil {
		t.Fatalf("ack: %v", err)
	}
	if got.AutoClosePolicy != "manual_only" {
		t.Fatalf("after ack policy = %s, want manual_only", got.AutoClosePolicy)
	}
	worker := NewIngestWorker(nil, store, NewMemAuditLog(), time.Second, 10, true, 0, 0, func(string, ...any) {})
	// 已恢复的告警（endsAt 已过）不得关闭该单。
	payload := `{"labels":{"severity":"critical"},"annotations":{"summary":"n1 unreachable"},"startsAt":"2026-09-10T10:00:00Z","endsAt":"2020-01-01T00:00:00Z","fingerprint":"fp-r2"}`
	if err := worker.process(Item{ID: 1, Origin: incident.OriginAlertmanager,
		SourceRef: "fp-r2", Payload: payload}); err != nil {
		t.Fatalf("process: %v", err)
	}
	after, err := store.Get("alertmanager:fp-r2")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if after.State != incident.StateAcked {
		t.Fatalf("state after external recovery = %s, want acked (not auto-closed)", after.State)
	}
}

// fakeQueue 队列桩（记录入队次数，模拟幂等命中）。
type fakeQueue struct {
	seen       bool
	calls      int
	lastOrigin incident.Origin
}

func (f *fakeQueue) Enqueue(origin incident.Origin, sourceRef, payload string) (bool, error) {
	f.calls++
	f.lastOrigin = origin
	if f.seen {
		return false, nil
	}
	f.seen = true
	return true, nil
}

// ---- 二期：L2 提示 / 人工合并端点 / 限流聚合 / 审计 ----

// TestDuplicatesEndpointOnlySuggests L2 只提示不合并：返回候选且声明
// auto_merge=false，且被提示的事件状态不变（绝不自动合并）。
func TestDuplicatesEndpointOnlySuggests(t *testing.T) {
	asm, h := restTest(t)
	if _, err := asm.Incidents.Create("INC-D1", "disk full on n1", "critical", "ops"); err != nil {
		t.Fatalf("create D1: %v", err)
	}
	if _, err := asm.Incidents.Create("INC-D2", "disk full on n1", "critical", "ops"); err != nil {
		t.Fatalf("create D2: %v", err)
	}
	code, body := getJSON(t, h, "/api/v1/incidents/INC-D1/duplicates")
	if code != http.StatusOK {
		t.Fatalf("duplicates: code=%d body=%v", code, body)
	}
	if body["auto_merge"] != false {
		t.Fatal("auto_merge must be false (contract: never auto-merge)")
	}
	if body["count"].(float64) < 1 {
		t.Fatalf("candidates = %v, want >=1 (same title)", body["count"])
	}
	// 关键：提示不产生副作用——两单都还在且都是 open。
	for _, id := range []string{"INC-D1", "INC-D2"} {
		inc, err := asm.Incidents.Get(id)
		if err != nil {
			t.Fatalf("get %s: %v", id, err)
		}
		if inc.State != incident.StateOpen || inc.MergedInto != "" {
			t.Fatalf("%s mutated by suggestion: %+v", id, inc)
		}
	}
}

// TestMergeEndpointRequiresAuthAndAudits 人工合并端点：鉴权 + 审计留痕。
func TestMergeEndpointRequiresAuthAndAudits(t *testing.T) {
	asm, h := restTest(t)
	asm.REST.SetAudit(NewMemAuditLog())
	if _, err := asm.Incidents.Create("M1", "a", "warning", "ops"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := asm.Incidents.Create("M2", "b", "warning", "ops"); err != nil {
		t.Fatalf("create: %v", err)
	}
	body := `{"target_id":"M1","actor":"ops"}`
	// 无 Token → 401（restTest 的 token 为空时应当放行——这里显式设 token）。
	asm.REST.token = "tk"
	req := httptest.NewRequest(http.MethodPost, "/api/v1/incidents/M2/merge", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("no token: code = %d, want 401", rec.Code)
	}
	// 带 Token → 200 + 审计。
	req2 := httptest.NewRequest(http.MethodPost, "/api/v1/incidents/M2/merge", strings.NewReader(body))
	req2.Header.Set("Content-Type", "application/json")
	req2.Header.Set(AuthHeader, "tk")
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusOK {
		t.Fatalf("merge: code=%d body=%s", rec2.Code, rec2.Body.String())
	}
	entries := asm.REST.audit.List("M2")
	if len(entries) != 1 || entries[0].Action != AuditMerge || entries[0].Actor != "ops" {
		t.Fatalf("audit = %+v, want 1 merge entry by ops", entries)
	}
	// 审计端点可读。
	code, abody := getJSON(t, h, "/api/v1/incidents/M2/audit")
	if code != http.StatusOK || abody["count"].(float64) != 1 {
		t.Fatalf("audit endpoint: code=%d body=%v", code, abody)
	}
}

// TestRateLimitFoldsIntoBurstIncident R1：窗口内超限 → 折叠进聚合单，
// 不新建大量单（消息不丢、可回放）。
func TestRateLimitFoldsIntoBurstIncident(t *testing.T) {
	store := incident.NewMemStore()
	audit := NewMemAuditLog()
	// 限额 2/窗口：第 3 条起折叠进 burst 聚合单。
	w := NewIngestWorker(nil, store, audit, time.Second, 10, true, 2, time.Hour, func(string, ...any) {})
	payload := func(fp string) string {
		return `{"labels":{"severity":"critical"},"annotations":{"summary":"disk full"},"startsAt":"2026-09-10T10:00:00Z","endsAt":"0001-01-01T00:00:00Z","fingerprint":"` + fp + `"}`
	}
	for i := 1; i <= 5; i++ {
		if err := w.process(Item{ID: int64(i), Origin: incident.OriginAlertmanager,
			SourceRef: "fp-" + string(rune('a'+i)), Payload: payload("fp-" + string(rune('a'+i)))}); err != nil {
			t.Fatalf("process #%d: %v", i, err)
		}
	}
	list := mustList(store, "")
	if len(list) != 3 { // 2 独立单 + 1 聚合单
		t.Fatalf("incidents = %d, want 3 (2 + 1 burst)", len(list))
	}
	var burst *incident.Incident
	for i := range list {
		if strings.HasPrefix(list[i].SourceRef, "burst:") {
			burst = &list[i]
		}
	}
	if burst == nil {
		t.Fatal("burst incident missing")
	}
	// 限流动作有审计（3 条被折叠）。
	limited := 0
	for _, e := range audit.List("") {
		if e.Action == AuditRateLimited {
			limited++
		}
	}
	if limited != 1 {
		t.Fatalf("rate_limited audit = %d, want 1 (first fold creates burst)", limited)
	}
}

// TestGenericWebhookOrigin 通用 webhook 路由使用 origin=webhook（扩展性验证）。
func TestGenericWebhookOrigin(t *testing.T) {
	q := &fakeQueue{}
	owner := &QueueOwner{}
	owner.SetWriter(q)
	h := &AlertmanagerWebhook{Owner: owner}
	mux := http.NewServeMux()
	h.Register(mux)
	body := `{"status":"firing","alerts":[{"labels":{"alertname":"X"},"startsAt":"2026-09-10T10:00:00Z","endsAt":"0001-01-01T00:00:00Z","fingerprint":"g1"}]}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/ingest/webhook", strings.NewReader(body))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("generic webhook: code=%d body=%s", rec.Code, rec.Body.String())
	}
	if q.lastOrigin != incident.OriginWebhook {
		t.Fatalf("origin = %q, want webhook", q.lastOrigin)
	}
}

// TestTransitionEndpoint 事件状态流转端点（控制台事件页操作闭环）：
// 鉴权必过 / actor 必填（审计）/ R2 由状态机兜底（ack → manual_only）/
// 非法转移拒绝。
func TestTransitionEndpoint(t *testing.T) {
	asm, h := restTest(t)
	asm.REST.SetAudit(NewMemAuditLog())
	if _, err := asm.Incidents.Create("T1", "n2 cpu saturated", "critical", "ops"); err != nil {
		t.Fatalf("create: %v", err)
	}
	asm.REST.token = "tk"
	post := func(body string, withToken bool) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/incidents/T1/transition", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		if withToken {
			req.Header.Set(AuthHeader, "tk")
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}
	// 无 Token → 401。
	if rec := post(`{"to":"acked","actor":"ops"}`, false); rec.Code != http.StatusUnauthorized {
		t.Fatalf("no token: code = %d, want 401", rec.Code)
	}
	// 缺 actor → 400（审计要求）。
	if rec := post(`{"to":"acked"}`, true); rec.Code != http.StatusBadRequest {
		t.Fatalf("no actor: code = %d, want 400", rec.Code)
	}
	// 非法目标态 → 400。
	if rec := post(`{"to":"open","actor":"ops"}`, true); rec.Code != http.StatusBadRequest {
		t.Fatalf("invalid to: code = %d, want 400", rec.Code)
	}
	// 合法：open → acked。
	rec := post(`{"to":"acked","actor":"ops"}`, true)
	if rec.Code != http.StatusOK {
		t.Fatalf("ack: code=%d body=%s", rec.Code, rec.Body.String())
	}
	inc, err := asm.Incidents.Get("T1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if inc.State != incident.StateAcked {
		t.Fatalf("state = %s, want acked", inc.State)
	}
	// R2：人工确认后自动转 manual_only（由状态机保证，不靠调用方）。
	if inc.AutoClosePolicy != "manual_only" {
		t.Fatalf("auto_close_policy = %s, want manual_only (R2)", inc.AutoClosePolicy)
	}
	// 审计留痕。
	entries := asm.REST.audit.List("T1")
	if len(entries) != 1 || entries[0].Action != AuditTransition || entries[0].Actor != "ops" {
		t.Fatalf("audit = %+v, want 1 transition entry by ops", entries)
	}
	// 非法转移：acked → open 状态机拒绝 → 400。
	if rec := post(`{"to":"acked","actor":"ops"}`, true); rec.Code != http.StatusBadRequest {
		t.Fatalf("acked->acked: code = %d, want 400", rec.Code)
	}
}

// TestManualCreatePersistsActorAndIngestFallback
//  1. REST 人工建单把 created_by 落库（审计字段不再被丢弃）；
//  2. 无 DB 队列时外部导入端点显式 503（可诊断，不是 404）。
func TestManualCreatePersistsActorAndIngestFallback(t *testing.T) {
	t.Setenv("OPS_DB_DSN", "") // 确定性：无 DB → 内存 store + 入队端点 503
	asm, err := NewAssembly(newQuietLogger(), "")
	if err != nil {
		t.Fatalf("assembly: %v", err)
	}
	h := asm.Handler()
	asm.REST.token = "tk"

	body := `{"id":"MC-1","title":"manual via rest","severity":"warning","created_by":"zhangsan"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/incidents", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(AuthHeader, "tk")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create: code=%d body=%s", rec.Code, rec.Body.String())
	}
	inc, err := asm.Incidents.Get("MC-1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if inc.CreatedBy != "zhangsan" {
		t.Fatalf("created_by = %q, want zhangsan (persisted)", inc.CreatedBy)
	}
	// 人工建单留痕（create 审计，与外部单 worker 侧对齐）。
	audited := false
	for _, e := range asm.audit.List("MC-1") {
		if e.Action == AuditCreate && e.Actor == "zhangsan" {
			audited = true
		}
	}
	if !audited {
		t.Fatal("manual create not audited (create entry missing)")
	}

	// 无 DB：入队端点显式 503（配置缺失一眼可诊断）。
	ireq := httptest.NewRequest(http.MethodPost, "/api/v1/ingest/alertmanager", strings.NewReader(`{"alerts":[]}`))
	ireq.Header.Set(AuthHeader, "tk")
	irec := httptest.NewRecorder()
	h.ServeHTTP(irec, ireq)
	if irec.Code != http.StatusServiceUnavailable {
		t.Fatalf("ingest without DB: code = %d, want 503 (not 404)", irec.Code)
	}
	if asm.Ingest != nil || asm.Worker != nil {
		t.Fatal("ingest should be unwired without DB")
	}
}

// TestAuthStatusEndpoint 写权限探测：控制台据此决定是否显示 Token 输入框。
// 未配密钥 → open/可写；配了密钥 → 取决于请求是否携带（反代会注入）。
func TestAuthStatusEndpoint(t *testing.T) {
	// 未启用鉴权（回环/内网）：可写，mode=open。
	asm, h := restTest(t)
	asm.REST.token = ""
	code, body := getJSON(t, h, "/api/v1/auth/status")
	if code != http.StatusOK || body["write_authorized"] != true || body["mode"] != "open" {
		t.Fatalf("open mode: code=%d body=%v", code, body)
	}
	// 配了密钥且未携带 → 不可写。
	asm.REST.token = "tk"
	code, body = getJSON(t, h, "/api/v1/auth/status")
	if code != http.StatusOK || body["write_authorized"] != false || body["mode"] != "shared_secret" {
		t.Fatalf("no token: code=%d body=%v", code, body)
	}
	// 携带正确密钥（模拟反代注入）→ 可写。
	req := httptest.NewRequest(http.MethodGet, "/api/v1/auth/status", nil)
	req.Header.Set(AuthHeader, "tk")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var b map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &b); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if b["write_authorized"] != true {
		t.Fatalf("proxy-injected: body=%v, want write_authorized=true", b)
	}
}

// TestWriteEndpointsRequireJSON 写路径 CSRF 准入：缺 Content-Type 或
// 用 text/plain（浏览器"简单请求"仅有的几种类型，免预检）必须 415。
// 这是挡住"跨站免预检写"的关键——浏览器不允许脚本把 Content-Type 设成
// application/json 而不触发预检，而写路径不返回预检响应头。
func TestWriteEndpointsRequireJSON(t *testing.T) {
	asm, h := restTest(t)
	asm.REST.token = "tk"
	if _, err := asm.Incidents.Create("CS1", "csrf probe", "critical", "ops"); err != nil {
		t.Fatalf("create: %v", err)
	}
	cases := []struct{ name, path, body string }{
		{"create", "/api/v1/incidents", `{"title":"x","created_by":"ops"}`},
		{"transition", "/api/v1/incidents/CS1/transition", `{"to":"acked","actor":"ops"}`},
		{"merge", "/api/v1/incidents/CS1/merge", `{"target_id":"CS2","actor":"ops"}`},
	}
	for _, c := range cases {
		for _, ct := range []string{"", "text/plain", "application/x-www-form-urlencoded"} {
			req := httptest.NewRequest(http.MethodPost, c.path, strings.NewReader(c.body))
			req.Header.Set(AuthHeader, "tk")
			if ct != "" {
				req.Header.Set("Content-Type", ct)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != http.StatusUnsupportedMediaType {
				t.Fatalf("%s (content-type %q): code = %d, want 415", c.name, ct, rec.Code)
			}
		}
		// 带正确 Content-Type 时不应因 CSRF 检查被挡（此处 401/400/404 之外的
		// 415 才算失败）。
		req := httptest.NewRequest(http.MethodPost, c.path, strings.NewReader(c.body))
		req.Header.Set(AuthHeader, "tk")
		req.Header.Set("Content-Type", "application/json; charset=utf-8")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code == http.StatusUnsupportedMediaType {
			t.Fatalf("%s: valid JSON content-type must not be rejected", c.name)
		}
	}
}

// TestTopologyDepthRejected depth 必须在 [0,maxTopologyDepth]：超限直接 400，
// 而不是让它拿着一张空图空转 21 亿次（全程持拓扑锁，会阻塞发现入库）。
func TestTopologyDepthRejected(t *testing.T) {
	_, h := restTest(t)
	for _, q := range []string{"depth=11", "depth=2147483647"} {
		code, _ := getJSON(t, h, "/api/v1/topology?node_key=n1&"+q)
		if code != http.StatusBadRequest {
			t.Fatalf("%s: code = %d, want 400", q, code)
		}
	}
	// 边界内不应因 depth 被拒（无拓扑数据 → 404 才是预期，不是 400）。
	if code, _ := getJSON(t, h, "/api/v1/topology?node_key=n1&depth=10"); code == http.StatusBadRequest {
		t.Fatal("depth=10 (within bound) must not be rejected as 400")
	}
}

// TestCreateValidatesIDAndSeverity 人工建单的 id 字符集/长度与 severity 白名单。
func TestCreateValidatesIDAndSeverity(t *testing.T) {
	asm, h := restTest(t)
	asm.REST.token = "tk"
	post := func(body string) (int, map[string]any) {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/incidents", strings.NewReader(body))
		req.Header.Set(AuthHeader, "tk")
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		var out map[string]any
		_ = json.Unmarshal(rec.Body.Bytes(), &out)
		return rec.Code, out
	}
	// 非法 id（含引号 / 尖括号 / 空格）→ 400。
	for _, bad := range []string{`{"id":"a'b","title":"t","created_by":"ops"}`,
		`{"id":"a<b>","title":"t","created_by":"ops"}`,
		`{"id":"a b","title":"t","created_by":"ops"}`} {
		if code, _ := post(bad); code != http.StatusBadRequest {
			t.Fatalf("bad id %s: code = %d, want 400", bad, code)
		}
	}
	// 超长 id → 400。
	long := strings.Repeat("x", incidentIDMaxLen+1)
	if code, _ := post(`{"id":"` + long + `","title":"t","created_by":"ops"}`); code != http.StatusBadRequest {
		t.Fatalf("overlong id: code = %d, want 400", code)
	}
	// severity 白名单外 → 400。
	if code, _ := post(`{"title":"t","severity":"bogus","created_by":"ops"}`); code != http.StatusBadRequest {
		t.Fatalf("bad severity: code = %d, want 400", code)
	}
	// severity 省略 → 201 且落为 info（与 DB 默认一致）。
	code, out := post(`{"title":"sev default probe","created_by":"ops"}`)
	if code != http.StatusCreated {
		t.Fatalf("omitted severity: code = %d, want 201", code)
	}
	if out["severity"] != "info" {
		t.Fatalf("omitted severity = %v, want info", out["severity"])
	}
}

// TestCORSOriginConfigurable D2 决策 B+C：默认**不返回** ACAO 头（仅同源），
// 显式配置后才放行该源。取代早期默认 "*"——读端点含事件与审计数据，
// "任意网页可跨源读"不该是默认行为。
func TestCORSOriginConfigurable(t *testing.T) {
	asm, h := restTest(t)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/incidents", nil))
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Fatalf("default must not emit ACAO header, got %q", got)
	}
	if err := asm.REST.SetCORSOrigin("https://ops.example.com"); err != nil {
		t.Fatalf("valid origin: %v", err)
	}
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/incidents", nil))
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "https://ops.example.com" {
		t.Fatalf("ACAO = %q, want configured origin", got)
	}
	// "*" 必须被拒绝（第七轮 M2：会静默恢复任意源可读）。
	if err := asm.REST.SetCORSOrigin("*"); err == nil {
		t.Fatal(`CORS origin "*" must be rejected`)
	}
	// 非法形态（无 scheme）必须被拒绝。
	if err := asm.REST.SetCORSOrigin("ops.example.com"); err == nil {
		t.Fatal("origin without scheme must be rejected")
	}
	// 清空（同源）再次确认可回退。
	if err := asm.REST.SetCORSOrigin(""); err != nil {
		t.Fatalf("clear: %v", err)
	}
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/incidents", nil))
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Fatalf("empty origin must stop emitting ACAO, got %q", got)
	}
}

// mustList 测试辅助：List 现在返回 error（D5），出错直接失败。
func mustList(s incident.Store, st incident.State) []incident.Incident {
	l, err := s.List(st)
	if err != nil {
		panic(err)
	}
	return l
}

// TestIncidentsPaginationEndpoint D4：REST 层游标翻页——limit/cursor、
// next_cursor 终止、翻页不重不漏、坏游标 400。
func TestIncidentsPaginationEndpoint(t *testing.T) {
	asm, h := restTest(t)
	ids := []string{"PA", "PB", "PC", "PD", "PE"}
	for _, id := range ids {
		if _, err := asm.Incidents.Create(id, "p-"+id, "info", "ops"); err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
	}
	get := func(q string) (int, map[string]any) {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/incidents"+q, nil))
		var out map[string]any
		_ = json.Unmarshal(rec.Body.Bytes(), &out)
		return rec.Code, out
	}
	code, p1 := get("?limit=2")
	if code != http.StatusOK {
		t.Fatalf("page1 code = %d", code)
	}
	if n, _ := p1["count"].(float64); int(n) != 2 {
		t.Fatalf("page1 count = %v, want 2", p1["count"])
	}
	cursor, _ := p1["next_cursor"].(string)
	if cursor == "" {
		t.Fatal("page1 must carry next_cursor")
	}
	if st, ok := p1["stats"].(map[string]any); !ok || st["manual"].(float64) != 5 {
		t.Fatalf("stats missing/wrong: %v", p1["stats"])
	}
	seen := map[string]bool{}
	for _, v := range p1["incidents"].([]any) {
		seen[v.(map[string]any)["id"].(string)] = true
	}
	pages := 1
	for cursor != "" {
		code, p := get("?limit=2&cursor=" + cursor)
		if code != http.StatusOK {
			t.Fatalf("page code = %d", code)
		}
		for _, v := range p["incidents"].([]any) {
			id := v.(map[string]any)["id"].(string)
			if seen[id] {
				t.Fatalf("id repeated across pages: %s", id)
			}
			seen[id] = true
		}
		cursor, _ = p["next_cursor"].(string)
		pages++
		if pages > 10 {
			t.Fatal("pagination did not terminate")
		}
	}
	if len(seen) != 5 {
		t.Fatalf("walked %d unique incidents, want 5", len(seen))
	}
	// 坏游标 → 400。
	if code, _ := get("?cursor=!!!bad"); code != http.StatusBadRequest {
		t.Fatalf("bad cursor code = %d, want 400", code)
	}
	// 非法 state → 400。
	if code, _ := get("?state=bogus"); code != http.StatusBadRequest {
		t.Fatalf("bad state code = %d, want 400", code)
	}
}
