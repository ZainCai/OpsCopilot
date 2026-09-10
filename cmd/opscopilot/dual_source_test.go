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
	if got := len(s.List("")); got != 1 {
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
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("no token: code = %d, want 401", rec.Code)
	}
	// 带 Token → 200 + 审计。
	req2 := httptest.NewRequest(http.MethodPost, "/api/v1/incidents/M2/merge", strings.NewReader(body))
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
	list := store.List("")
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
