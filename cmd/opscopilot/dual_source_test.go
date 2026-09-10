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
	inc, err := s.Create("INC-M1", "manual ticket", "warning")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if inc.Origin != incident.OriginManual {
		t.Fatalf("origin = %s, want manual", inc.Origin)
	}
	if inc.AutoClosePolicy != "manual_only" {
		t.Fatalf("auto_close_policy = %s, want manual_only (R2)", inc.AutoClosePolicy)
	}
}

// TestMergeInto 人工合并（L2 由人触发）：源单关闭 + merged_into + 簇转移。
func TestMergeInto(t *testing.T) {
	s := incident.NewMemStore()
	if _, err := s.Create("A", "a", "critical"); err != nil {
		t.Fatalf("create A: %v", err)
	}
	if _, err := s.Create("B", "b", "warning"); err != nil {
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
	worker := NewIngestWorker(nil, store, time.Second, 10, true, func(string, ...any) {})
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
	seen  bool
	calls int
}

func (f *fakeQueue) Enqueue(origin incident.Origin, sourceRef, payload string) (bool, error) {
	f.calls++
	if f.seen {
		return false, nil
	}
	f.seen = true
	return true, nil
}
