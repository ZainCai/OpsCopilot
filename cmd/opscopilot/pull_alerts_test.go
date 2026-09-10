// W11 拉取侧测试：源映射 / 指纹稳定性 / 调度入队 / worker 支持 prometheus origin。
package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"opscopilot/internal/incident"
)

// mockProm 返回一个假的 Prometheus /api/v1/alerts。
func mockProm(t *testing.T, body string, code int) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/alerts" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_, _ = w.Write([]byte(body))
	}))
}

const promAlertsBody = `{"status":"success","data":{"alerts":[
 {"labels":{"alertname":"DiskFull","instance":"n1","severity":"critical"},
  "annotations":{"summary":"n1 磁盘 96%"},"state":"firing","activeAt":"2026-09-10T12:00:00Z","value":"96"},
 {"labels":{"alertname":"HighCPU","instance":"n2","severity":"warning"},
  "annotations":{"summary":"n2 CPU 92%"},"state":"pending","activeAt":"2026-09-10T12:05:00Z","value":"92"}
]}}`

func TestPrometheusAlertsSourceMapping(t *testing.T) {
	srv := mockProm(t, promAlertsBody, http.StatusOK)
	defer srv.Close()
	src := NewPrometheusAlertsSource(srv.URL, "")
	alerts, err := src.FetchAlerts(context.Background())
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(alerts) != 2 {
		t.Fatalf("alerts=%d, want 2", len(alerts))
	}
	a := alerts[0]
	if a.Labels["alertname"] != "DiskFull" || a.Annotations["summary"] != "n1 磁盘 96%" {
		t.Fatalf("labels/annotations not mapped: %+v", a)
	}
	if a.StartsAt != "2026-09-10T12:00:00Z" {
		t.Fatalf("startsAt=%q, want activeAt", a.StartsAt)
	}
	if a.EndsAt != "" {
		t.Fatalf("endsAt=%q, want empty (pull 不自动关单)", a.EndsAt)
	}
	if a.Fingerprint == "" {
		t.Fatal("fingerprint empty")
	}
	// 标题/严重级经共用映射（amTitle/amSeverity）可用。
	if amTitle(a) != "n1 磁盘 96%" || amSeverity(a) != "critical" {
		t.Fatalf("title=%q sev=%q", amTitle(a), amSeverity(a))
	}
}

// TestPromFingerprintStable 同一 labels 跨轮询指纹稳定（幂等前提）；
// 不同 labels 指纹不同。
func TestPromFingerprintStable(t *testing.T) {
	l1 := map[string]string{"alertname": "A", "instance": "n1"}
	l2 := map[string]string{"instance": "n1", "alertname": "A"} // 键序不同
	if promFingerprint(l1) != promFingerprint(l2) {
		t.Fatal("fingerprint must be order-independent")
	}
	l3 := map[string]string{"alertname": "B", "instance": "n1"}
	if promFingerprint(l1) == promFingerprint(l3) {
		t.Fatal("different labels must differ")
	}
}

func TestPrometheusAlertsSourceHTTPError(t *testing.T) {
	srv := mockProm(t, "boom", http.StatusInternalServerError)
	defer srv.Close()
	if _, err := NewPrometheusAlertsSource(srv.URL, "").FetchAlerts(context.Background()); err == nil {
		t.Fatal("want error on non-200")
	}
}

// recordingQueue 记录入队调用，模拟 (origin,ref) 唯一索引去重。
type recordingQueue struct {
	seen    map[string]bool
	refs    []string
	origins []incident.Origin
}

func (f *recordingQueue) Enqueue(origin incident.Origin, sourceRef, payload string) (bool, error) {
	if f.seen == nil {
		f.seen = map[string]bool{}
	}
	key := string(origin) + "|" + sourceRef
	f.origins = append(f.origins, origin)
	f.refs = append(f.refs, sourceRef)
	if f.seen[key] {
		return false, nil
	}
	f.seen[key] = true
	return true, nil
}

// mutableProm 可切换响应体的 Prometheus 桩（测"变化上报"）。
func mutableProm(t *testing.T, initial string) (*httptest.Server, func(string)) {
	t.Helper()
	var mu sync.Mutex
	body := initial
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv, func(b string) { mu.Lock(); body = b; mu.Unlock() }
}

// TestAlertPollerReportsChangesOnly 只上报变化：内容不变不再入队；
// 内容变化重新入队；告警消失即遗忘（再现时重新入队）。
func TestAlertPollerReportsChangesOnly(t *testing.T) {
	srv, setBody := mutableProm(t, promAlertsBody)
	q := &recordingQueue{}
	owner := &QueueOwner{}
	owner.SetWriter(q)
	p := NewAlertPoller(NewPrometheusAlertsSource(srv.URL, ""), owner, incident.OriginPrometheus, time.Minute, nil)

	p.pollOnce(context.Background())
	if len(q.refs) != 2 {
		t.Fatalf("first poll enqueued=%d, want 2", len(q.refs))
	}
	for _, o := range q.origins {
		if o != incident.OriginPrometheus {
			t.Fatalf("origin=%q, want prometheus", o)
		}
	}

	// 同一内容再拉 → 全部 unchanged，不再入队（避免队列膨胀/updated 噪声）。
	p.pollOnce(context.Background())
	if len(q.refs) != 2 {
		t.Fatalf("unchanged poll enqueued extra: total=%d, want 2", len(q.refs))
	}

	// 一条告警内容变化（summary 变）→ 仅该条重新入队。
	// 注：Prometheus 的采样值 value 不在载荷内（会高频抖动，纳入会破坏去重），
	// 故此处改真正进载荷的 annotations。
	setBody(strings.Replace(promAlertsBody, "n1 磁盘 96%", "n1 磁盘 99%", 1))
	p.pollOnce(context.Background())
	if len(q.refs) != 3 {
		t.Fatalf("changed poll total=%d, want 3 (1 changed re-enqueued)", len(q.refs))
	}

	// 告警全部消失 → 遗忘；再出现时重新入队。
	setBody(`{"status":"success","data":{"alerts":[]}}`)
	p.pollOnce(context.Background())
	if len(q.refs) != 3 {
		t.Fatalf("empty poll should enqueue nothing: total=%d", len(q.refs))
	}
	setBody(promAlertsBody)
	p.pollOnce(context.Background())
	if len(q.refs) != 5 {
		t.Fatalf("reappear poll total=%d, want 5 (2 re-enqueued)", len(q.refs))
	}
}

// TestAlertPollerRunStopsOnCancel Run 能随 ctx 取消退出（不泄漏 goroutine）。
func TestAlertPollerRunStopsOnCancel(t *testing.T) {
	srv := mockProm(t, `{"status":"success","data":{"alerts":[]}}`, http.StatusOK)
	defer srv.Close()
	owner := &QueueOwner{}
	owner.SetWriter(&recordingQueue{})
	p := NewAlertPoller(NewPrometheusAlertsSource(srv.URL, ""), owner, incident.OriginPrometheus, time.Hour, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { p.Run(ctx); close(done) }()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not exit on cancel")
	}
}

// TestIngestWorkerHandlesPrometheusOrigin worker 对 pull 侧 origin 走同一
// 解析路径（建单并入审计）。
func TestIngestWorkerHandlesPrometheusOrigin(t *testing.T) {
	store := incident.NewMemStore()
	audit := NewMemAuditLog()
	w := NewIngestWorker(nil, store, audit, 0, 0, true, 0, 0, nil)

	payload, _ := json.Marshal(amAlert{
		Labels:      map[string]string{"alertname": "DiskFull", "severity": "critical"},
		Annotations: map[string]string{"summary": "磁盘满"},
		Fingerprint: "prom:abc",
	})
	it := Item{ID: 1, Origin: incident.OriginPrometheus, SourceRef: "prom:abc", Payload: string(payload)}
	if err := w.process(it); err != nil {
		t.Fatalf("process: %v", err)
	}
	got := mustList(store, "")
	if len(got) != 1 {
		t.Fatalf("incidents=%d, want 1", len(got))
	}
	if got[0].Origin != incident.OriginPrometheus {
		t.Fatalf("origin=%q, want prometheus", got[0].Origin)
	}
	if got[0].Title != "磁盘满" {
		t.Fatalf("title=%q", got[0].Title)
	}
	// 再幂等投递一条同 source_ref → 不新建。
	if err := w.process(it); err != nil {
		t.Fatalf("reprocess: %v", err)
	}
	if n := len(mustList(store, "")); n != 1 {
		t.Fatalf("after reprocess incidents=%d, want 1 (idempotent)", n)
	}
	if entries := audit.List(got[0].ID); len(entries) == 0 {
		t.Fatal("expected audit entry for created incident")
	}
}

// TestApplyRateLimitCountsOnlyNew 限流只统计"新建"：去重刷新不占额度，
// 否则重复推送的老告警会吃光额度、把真正的新告警误折叠进 burst 聚合单。
func TestApplyRateLimitCountsOnlyNew(t *testing.T) {
	w := NewIngestWorker(nil, incident.NewMemStore(), nil, 0, 0, true, 2, time.Minute, nil)
	if _, burst := w.applyRateLimit("r1", true); burst {
		t.Fatal("1st new alert must not burst")
	}
	if _, burst := w.applyRateLimit("r2", true); burst {
		t.Fatal("2nd new alert must not burst")
	}
	ref, burst := w.applyRateLimit("r3", true)
	if !burst || !strings.HasPrefix(ref, "burst:") {
		t.Fatalf("3rd new alert should fold into burst, got ref=%q burst=%v", ref, burst)
	}
	// 刷新（count=false）即使已超限也必须原样透传、且不再消耗额度。
	if ref, burst := w.applyRateLimit("r4", false); burst || ref != "r4" {
		t.Fatalf("refresh must pass through untouched: ref=%q burst=%v", ref, burst)
	}
	// 子秒窗口不得除零（与 cluster/DedupKeyFor 同一类缺陷）。
	w2 := NewIngestWorker(nil, incident.NewMemStore(), nil, 0, 0, true, 1, 500*time.Millisecond, nil)
	if ref, burst := w2.applyRateLimit("r5", true); burst || ref != "r5" {
		t.Fatalf("sub-second window must be safe (treated as unlimited): ref=%q burst=%v", ref, burst)
	}
}
