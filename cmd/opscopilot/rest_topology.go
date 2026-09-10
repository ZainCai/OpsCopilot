// 簇 / 拓扑 / 变更域 handler（D10 决策 B：自 rest_gateway.go 按域拆出）。
package main

import (
	"net/http"
	"opscopilot/internal/contracts/pb"
	"opscopilot/internal/noise"
	"strconv"
	"strings"
)

func (g *RESTGateway) handleClusters(w http.ResponseWriter, r *http.Request) {
	if g.noise == nil {
		writeErr(w, http.StatusServiceUnavailable, "noise engine disabled (OPS_NOISE_SHADOW=off)")
		return
	}
	state := strings.ToLower(r.URL.Query().Get("state"))
	switch state {
	case "", "active", "resolved", "all":
	default:
		writeErr(w, http.StatusBadRequest, "state must be one of: active, resolved, all")
		return
	}
	limit := 200
	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			writeErr(w, http.StatusBadRequest, "limit must be a positive integer")
			return
		}
		if n > 1000 {
			n = 1000
		}
		limit = n
	}
	var clusters []noise.Cluster
	if state == "active" {
		clusters = g.noise.shadow.Clusterer().ActiveClusters()
	} else {
		clusters = g.noise.shadow.Clusterer().Clusters()
		if state == "resolved" {
			filtered := clusters[:0]
			for _, cl := range clusters {
				if cl.State == noise.StateResolved {
					filtered = append(filtered, cl)
				}
			}
			clusters = filtered
		}
	}
	// API 契约用 ClusterRecord（snake_case json tags，集合导出为有序
	// 数组）——不直接输出内部 Cluster（Go 字段名 + map[string]struct{}
	// 的序列化形态不是给外部消费者的）。
	recs := make([]noise.ClusterRecord, 0, len(clusters))
	for _, cl := range clusters {
		recs = append(recs, cl.ToRecord(g.noise.tenant))
	}
	// Clusters() 按 key 有序 → 截断是确定性的（key 字典序前 limit 个）。
	truncated := false
	if len(recs) > limit {
		recs = recs[:limit]
		truncated = true
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"clusters": recs, "count": len(recs), "truncated": truncated,
	})
}

// handleClusterDetail GET /api/v1/clusters/{key}。
func (g *RESTGateway) handleClusterDetail(w http.ResponseWriter, r *http.Request) {
	if g.noise == nil {
		writeErr(w, http.StatusServiceUnavailable, "noise engine disabled (OPS_NOISE_SHADOW=off)")
		return
	}
	key := r.PathValue("key")
	if key == "" {
		writeErr(w, http.StatusBadRequest, "cluster key is required")
		return
	}
	cl := g.noise.shadow.Clusterer().Get(key)
	if cl == nil {
		writeErr(w, http.StatusNotFound, "cluster not found: "+key)
		return
	}
	writeJSON(w, http.StatusOK, cl.ToRecord(g.noise.tenant))
}

// handleTopology GET /api/v1/topology?as_of=&node_key=&depth=
// 复用 gRPC GetTopology（校验/裁剪/NotFound 语义一套）。
func (g *RESTGateway) handleTopology(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	depth := int32(0)
	if raw := q.Get("depth"); raw != "" {
		d, err := strconv.Atoi(raw)
		if err != nil || d < 0 {
			writeErr(w, http.StatusBadRequest, "depth must be a non-negative integer")
			return
		}
		depth = int32(d)
	}
	resp, err := g.sem.GetTopology(r.Context(), &pb.GetTopologyRequest{
		AsOf:    q.Get("as_of"),
		NodeKey: q.Get("node_key"),
		Depth:   depth,
	})
	if err != nil {
		code, msg := grpcToHTTP(err)
		writeErr(w, code, msg)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

// handleCreateIncident POST /api/v1/incidents —— 链路 B：人工建单。
// 写路径：必须携带共享密钥（复用 ChangeWebhook 的鉴权件，R6 应对），
// 且 origin 恒为 manual、auto_close_policy 恒为 manual_only（R2：人工单
// 不允许外部恢复自动关闭）。
func (g *RESTGateway) handleChanges(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	resp, err := g.sem.GetRecentChanges(r.Context(), &pb.GetRecentChangesRequest{
		NodeKey:     q.Get("node_key"),
		WindowStart: q.Get("window_start"),
		WindowEnd:   q.Get("window_end"),
	})
	if err != nil {
		code, msg := grpcToHTTP(err)
		writeErr(w, code, msg)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}
