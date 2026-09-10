// W5-2.2 REST 网关测试：路由/过滤/错误映射/降噪关闭降级。
package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"opscopilot/internal/connector"
	"opscopilot/internal/topology"
)

// restTest 起带种子数据的装配，返回 handler。
func restTest(t *testing.T) (*Assembly, http.Handler) {
	t.Helper()
	asm, err := NewAssembly(newQuietLogger(), "")
	if err != nil {
		t.Fatalf("assembly: %v", err)
	}
	return asm, asm.Handler()
}

func getJSON(t *testing.T, h http.Handler, path string) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil && rec.Body.Len() > 0 {
		t.Fatalf("path %s: non-JSON response: %v", path, err)
	}
	return rec.Code, body
}

func TestRESTClustersListAndFilter(t *testing.T) {
	asm, h := restTest(t)

	now := time.Now()
	// 簇 1：i1 上两个指纹（同节点并簇）。
	asm.Noise.ProcessAlerts([]connector.Alert{
		{Fingerprint: "fp1", Labels: map[string]string{"instance": "i1"}, StartsAt: now},
	})
	// 簇 2：i3 孤立（拓扑里 i3 无边）。
	asm.Noise.ProcessAlerts([]connector.Alert{
		{Fingerprint: "fp2", Labels: map[string]string{"instance": "i3"}, StartsAt: now.Add(time.Second)},
	})

	code, body := getJSON(t, h, "/api/v1/clusters")
	if code != http.StatusOK {
		t.Fatalf("list clusters: code = %d", code)
	}
	if got := body["count"].(float64); got != 2 {
		t.Fatalf("count = %v, want 2", got)
	}

	// 无关批次后旧簇不重复：resolved 过滤（暂无 resolved → 0）。
	code, body = getJSON(t, h, "/api/v1/clusters?state=resolved")
	if code != http.StatusOK || body["count"].(float64) != 0 {
		t.Fatalf("resolved filter: code=%d count=%v, want 200/0", code, body["count"])
	}
	// 非法 state → 400。
	code, _ = getJSON(t, h, "/api/v1/clusters?state=bogus")
	if code != http.StatusBadRequest {
		t.Fatalf("bogus state: code = %d, want 400", code)
	}
}

func TestRESTClusterDetail(t *testing.T) {
	asm, h := restTest(t)
	now := time.Now()
	asm.Noise.ProcessAlerts([]connector.Alert{
		{Fingerprint: "fp1", Labels: map[string]string{"instance": "i1"}, Severity: "critical", StartsAt: now},
	})

	clusters := asm.Noise.shadow.Clusterer().ActiveClusters()
	if len(clusters) != 1 {
		t.Fatalf("seed clusters = %d, want 1", len(clusters))
	}
	key := clusters[0].Key

	code, body := getJSON(t, h, "/api/v1/clusters/"+key)
	if code != http.StatusOK {
		t.Fatalf("detail: code = %d", code)
	}
	// API 契约 = ClusterRecord（snake_case）：cluster_key/state/severity。
	if body["cluster_key"] != key || body["severity"] != "critical" || body["state"] != "open" {
		t.Fatalf("detail body mismatch: %v", body)
	}

	// 未知簇 → 404。
	code, _ = getJSON(t, h, "/api/v1/clusters/c:nope@0")
	if code != http.StatusNotFound {
		t.Fatalf("unknown cluster: code = %d, want 404", code)
	}
}

func TestRESTTopologyAndErrors(t *testing.T) {
	asm, h := restTest(t)
	now := time.Now()
	if err := asm.Sink.IngestDiscover(nil, &connector.DiscoverResult{
		Nodes: []connector.ResourceNode{
			{Key: "a", Type: "host", ObservedAt: now},
			{Key: "b", Type: "host", ObservedAt: now},
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	addTestEdge(asm, "a", "b", now)

	// 全图。
	code, body := getJSON(t, h, "/api/v1/topology")
	if code != http.StatusOK {
		t.Fatalf("topology: code = %d", code)
	}
	nodes := body["nodes"].([]any)
	if len(nodes) != 2 {
		t.Fatalf("topology nodes = %d, want 2", len(nodes))
	}

	// 邻域：a depth=1 → 只见 a。
	code, body = getJSON(t, h, "/api/v1/topology?node_key=a&depth=0")
	if code != http.StatusOK || len(body["nodes"].([]any)) != 1 {
		t.Fatalf("neighborhood depth0: code=%d nodes=%v, want 200/1", code, body["nodes"])
	}

	// ghost → 404（gRPC NotFound 映射）。
	code, body = getJSON(t, h, "/api/v1/topology?node_key=ghost")
	if code != http.StatusNotFound || body["error"] == nil {
		t.Fatalf("ghost: code=%d body=%v, want 404+error", code, body)
	}

	// 非法 as_of → 400。
	code, _ = getJSON(t, h, "/api/v1/topology?as_of=yesterday")
	if code != http.StatusBadRequest {
		t.Fatalf("bad as_of: code = %d, want 400", code)
	}
}

func TestRESTChangesEndpoint(t *testing.T) {
	asm, h := restTest(t)
	// 用过去的真实时刻：end 空 = now，start 必须早于 now。
	t0 := time.Now().Add(-3 * time.Hour).UTC().Truncate(time.Second)
	if err := asm.Sink.IngestDiscover(nil, &connector.DiscoverResult{
		Nodes: []connector.ResourceNode{{Key: "n1", Type: "host", ObservedAt: t0}},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := asm.Changes.Record(topology.ChangeEvent{
		ID: "ev-1", NodeKey: "n1", Type: topology.ChangeDeploy, Source: "git",
		OccurredAt: t0.Add(time.Hour),
	}); err != nil {
		t.Fatalf("record: %v", err)
	}

	code, body := getJSON(t, h, "/api/v1/changes?node_key=n1&window_start="+t0.Format(time.RFC3339))
	if code != http.StatusOK {
		t.Fatalf("changes: code = %d", code)
	}
	changes := body["changes"].([]any)
	if len(changes) != 1 {
		t.Fatalf("changes = %d, want 1", len(changes))
	}

	// start 必填（gRPC 校验复用）→ 400。
	code, _ = getJSON(t, h, "/api/v1/changes")
	if code != http.StatusBadRequest {
		t.Fatalf("no window_start: code = %d, want 400", code)
	}
}

func TestRESTClustersDisabledWhenNoiseOff(t *testing.T) {
	t.Setenv("OPS_NOISE_SHADOW", "off")
	_, h := restTest(t)

	code, body := getJSON(t, h, "/api/v1/clusters")
	if code != http.StatusServiceUnavailable || body["error"] == nil {
		t.Fatalf("noise off: code=%d body=%v, want 503+error", code, body)
	}
	code, _ = getJSON(t, h, "/api/v1/clusters/c:x@0")
	if code != http.StatusServiceUnavailable {
		t.Fatalf("noise off detail: code = %d, want 503", code)
	}
	// 拓扑/变更不依赖降噪，照常可用。
	code, _ = getJSON(t, h, "/api/v1/topology")
	if code != http.StatusOK {
		t.Fatalf("topology with noise off: code = %d, want 200", code)
	}
}
