// GET /api/v1/incidents/{id}/rca 门面测试（#12/ADR-014，httptest 直调
// Assembly.Handler）：Token 门禁、404/503/200 映射、响应契约字段、
// 证据不足仍 200（结构化报告，conclude pending、conclusion=null）。
// 与 rest_gateway_test.go 同一手法：真装配 + 手工喂数据。
package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"opscopilot/internal/config"
	"opscopilot/internal/connector"
	"opscopilot/internal/topology"
)

// rcaRESTEnv 装配级环境：发现 n1/n2 + 边，告警成簇，人工建单并挂簇，
// （可选）记录变更。返回事件 ID。
func rcaRESTEnv(t *testing.T, cfg *config.Config, withChange bool) (*Assembly, http.Handler, string) {
	t.Helper()
	asm, h := restTestWith(t, cfg)
	now := time.Now()
	if err := asm.Sink.IngestDiscover(nil, &connector.DiscoverResult{
		Nodes: []connector.ResourceNode{
			{Key: "prometheus://nodes/n1", Type: "host", Labels: map[string]string{"instance": "n1"}, ObservedAt: now},
			{Key: "prometheus://nodes/n2", Type: "host", Labels: map[string]string{"instance": "n2"}, ObservedAt: now},
		}, TenantID: cfg.Tenant}); err != nil {
		t.Fatalf("discover: %v", err)
	}
	// 静态边：装配已建，经 parseStaticEdges + AttachStaticEdges 走真实链路。
	edges, err := parseStaticEdges("n1->n2")
	if err != nil {
		t.Fatalf("edges: %v", err)
	}
	asm.Sink.AttachStaticEdges(edges)
	asm.Sink.AttachStaticEdges(nil) // 空批触发挂起边重试落图（端点已在图内）
	if err := asm.Sink.IngestDiscover(nil, &connector.DiscoverResult{
		Nodes: []connector.ResourceNode{
			{Key: "prometheus://nodes/n1", Type: "host", Labels: map[string]string{"instance": "n1"}, ObservedAt: now},
		}, TenantID: cfg.Tenant}); err != nil {
		t.Fatalf("rediscover (land pending edges): %v", err)
	}
	if withChange {
		if _, err := asm.Changes.Record(topology.ChangeEvent{
			ID: "dep-r1", NodeKey: "prometheus://nodes/n1", Type: topology.ChangeDeploy,
			Source: "jenkins", Author: "alice", Summary: "bad release",
			OccurredAt: now.Add(-4 * time.Minute), Confidence: topology.ConfidenceHigh,
		}); err != nil {
			t.Fatalf("record change: %v", err)
		}
	}
	asm.Noise.ProcessAlerts([]connector.Alert{{
		Fingerprint: "fp-rca",
		Labels:      map[string]string{"alertname": "DiskFull", "instance": "n1", "severity": "critical"},
		Severity:    "critical", StartsAt: now,
	}})
	active := asm.Noise.shadow.Clusterer().ActiveClusters()
	if len(active) == 0 {
		t.Fatal("no cluster formed")
	}
	inc, err := asm.Incidents.Create("INC-rca", "磁盘打满", "critical", "tester")
	if err != nil {
		t.Fatalf("create incident: %v", err)
	}
	if err := asm.Incidents.AttachCluster(inc.ID, active[len(active)-1].Key); err != nil {
		t.Fatalf("attach cluster: %v", err)
	}
	return asm, h, inc.ID
}

func getRCARest(t *testing.T, h http.Handler, path, token string) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	if token != "" {
		req.Header.Set(AuthHeader, token)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var body map[string]any
	if rec.Body.Len() > 0 {
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("path %s: non-JSON: %v (%s)", path, err, rec.Body.String())
		}
	}
	return rec.Code, body
}

// TestRESTRCAGateAnd404 配了写密钥：无 token → 401；错 token → 401；
// 对 token + 不存在的事件 → 404。
func TestRESTRCAGateAnd404(t *testing.T) {
	cfg := testAssemblyConfig("sekret")
	_, h, id := rcaRESTEnv(t, cfg, true)
	path := "/api/v1/incidents/" + id + "/rca"

	if code, _ := getRCARest(t, h, path, ""); code != http.StatusUnauthorized {
		t.Fatalf("no token: code = %d, want 401", code)
	}
	if code, _ := getRCARest(t, h, path, "wrong"); code != http.StatusUnauthorized {
		t.Fatalf("bad token: code = %d, want 401", code)
	}
	if code, _ := getRCARest(t, h, "/api/v1/incidents/INC-nope/rca", "sekret"); code != http.StatusNotFound {
		t.Fatalf("missing incident: code = %d, want 404", code)
	}
}

// TestRESTRCADisabled503 OPS_RCA=off → 端点显式 503（可诊断，不静默 404）。
func TestRESTRCADisabled503(t *testing.T) {
	cfg := testAssemblyConfig("")
	cfg.RCA.Enabled = false
	_, h, id := rcaRESTEnv(t, cfg, false)
	code, body := getRCARest(t, h, "/api/v1/incidents/"+id+"/rca", "")
	if code != http.StatusServiceUnavailable {
		t.Fatalf("disabled: code = %d, want 503", code)
	}
	if got, _ := body["error"].(string); got == "" {
		t.Fatalf("503 must carry diagnostic message, got %v", body)
	}
}

// TestRESTRCAReportContract 正常链路 200：六步结构、根因、建议、
// conclusion=null（ADR-003：本期无 LLM 不出结论形态）、审计已落。
func TestRESTRCAReportContract(t *testing.T) {
	cfg := testAssemblyConfig("tok")
	asm, h, id := rcaRESTEnv(t, cfg, true)
	code, body := getRCARest(t, h, "/api/v1/incidents/"+id+"/rca?actor=alice", "tok")
	if code != http.StatusOK {
		t.Fatalf("code = %d body = %v", code, body)
	}
	if body["incident_id"] != id {
		t.Fatalf("incident_id = %v, want %s", body["incident_id"], id)
	}
	if body["conclusion"] != nil || body["llm_used"] != false {
		t.Fatalf("no-llm report must not conclude: conclusion=%v llm=%v", body["conclusion"], body["llm_used"])
	}
	steps, _ := body["steps"].([]any)
	if len(steps) != 6 {
		t.Fatalf("steps = %v, want 6", steps)
	}
	last := steps[5].(map[string]any)
	if last["name"] != "recommend" || last["status"] != "done" {
		t.Fatalf("step 6 = %v, want recommend/done", last)
	}
	conc := steps[4].(map[string]any)
	if conc["name"] != "conclude" || conc["status"] != "pending" {
		t.Fatalf("step 5 = %v, want conclude/pending", conc)
	}
	roots, _ := body["root_causes"].([]any)
	if len(roots) != 1 {
		t.Fatalf("root_causes = %v, want 1 (deploy on n1)", roots)
	}
	r0 := roots[0].(map[string]any)
	if r0["confidence"] != "high" || r0["ref"] != "dep-r1" {
		t.Fatalf("root = %v, want high/dep-r1", r0)
	}
	ev := body["evidence"].(map[string]any)
	if ev["changes"].(float64) < 1 || ev["nodes"].(float64) < 2 {
		t.Fatalf("evidence counts = %v, want nodes>=2 changes>=1", ev)
	}
	if body["persistence"] != "memory" {
		t.Fatalf("persistence = %v, want memory (no DB)", body["persistence"])
	}
	// 审计：rca 动作已 append-only 落账（actor 透传查询参数）。
	entries, err := asm.audit.List(id)
	if err != nil {
		t.Fatalf("audit list: %v", err)
	}
	var found bool
	for _, e := range entries {
		if e.Action == AuditRCA {
			found = e.Actor == "alice"
		}
	}
	if !found {
		t.Fatalf("audit rca entry (actor=alice) missing: %+v", entries)
	}
}

// TestRESTRCAEmptyEvidenceStill200 证据不足（簇在、无变更、拓扑邻域
// 存在）不是错误：200 + 低置信报告（ADR-014 决策：可诊断的运维事实）。
func TestRESTRCAEmptyEvidenceStill200(t *testing.T) {
	cfg := testAssemblyConfig("")
	_, h, id := rcaRESTEnv(t, cfg, false)
	code, body := getRCARest(t, h, "/api/v1/incidents/"+id+"/rca", "")
	if code != http.StatusOK {
		t.Fatalf("empty evidence: code = %d, want 200", code)
	}
	if roots := body["root_causes"]; roots != nil {
		if r, ok := roots.([]any); ok && len(r) != 0 {
			t.Fatalf("no-change case must not claim root causes: %v", r)
		}
	}
	findings, _ := body["findings"].([]any)
	if len(findings) == 0 {
		t.Fatal("evidence report must still carry findings")
	}
}
