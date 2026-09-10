// W5-2.2 REST 网关：只读查询面（簇列表/详情/拓扑/变更）挂到现有 mux。
//
// 设计取舍：
//   - 直接方法调用而非绕 gRPC 回环：SemanticModelServer 的校验与映射
//     逻辑同进程复用，错误统一经 gRPC status → HTTP 映射——一套语义
//     两个门面，不会漂移；
//   - 只读 GET、无写路径：S1 写路径准入（webhook Token）不适用于本
//     文件；默认 bind loopback-only（S1）下无鉴权可接受，非回环暴露
//     必须前置鉴权反代（main 启动警告已覆盖）；
//   - Go 1.22+ ServeMux 增强路由：method + {key} 路径参数 + PathValue。
package main

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "opscopilot/internal/contracts/pb"
	"opscopilot/internal/noise"
)

// RESTGateway 只读查询面。
// noise 可为 nil（影子降噪关闭 → 簇端点 503，其余端点照常）。
type RESTGateway struct {
	noise *NoiseEngine
	sem   *SemanticModelServer
}

// NewRESTGateway 构造。
func NewRESTGateway(noise *NoiseEngine, sem *SemanticModelServer) *RESTGateway {
	return &RESTGateway{noise: noise, sem: sem}
}

// Register 把全部路由挂到 mux（装配层调用）。
// CORS：只读 GET 面放开跨源（Access-Control-Allow-Origin: *）——
// 支撑"静态打开 console.html + ?api= 指向运行中服务"的使用方式；
// 只读、无 cookie、无写路径（S1 写路径准入不涉及），泄露面为零。
// GET 简单请求不触发 CORS 预检，无需 OPTIONS 处理（注册 OPTIONS
// 通配会与 webhook 的写路径模式冲突——踩过）。
// 写路径（webhook）不经过此处，不受影响。
func (g *RESTGateway) Register(mux *http.ServeMux) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		g.route(w, r)
	})
	mux.Handle("GET /api/v1/clusters", h)
	mux.Handle("GET /api/v1/clusters/{key}", h)
	mux.Handle("GET /api/v1/topology", h)
	mux.Handle("GET /api/v1/changes", h)
}

// route 按 path 分发（CORS 包装层之下）。
func (g *RESTGateway) route(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/api/v1/clusters":
		g.handleClusters(w, r)
	case "/api/v1/topology":
		g.handleTopology(w, r)
	case "/api/v1/changes":
		g.handleChanges(w, r)
	default:
		// /api/v1/clusters/{key}：路径参数经 PathValue 取。
		if r.PathValue("key") != "" {
			g.handleClusterDetail(w, r)
			return
		}
		writeErr(w, http.StatusNotFound, "unknown endpoint")
	}
}

// writeJSON 统一成功响应。
func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// writeErr 统一错误响应（内部错误不外泄细节，请求错误带原因）。
func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

// grpcToHTTP gRPC status → HTTP status 映射（只读面只会遇到这三种）。
func grpcToHTTP(err error) (int, string) {
	switch status.Code(err) {
	case codes.InvalidArgument:
		return http.StatusBadRequest, status.Convert(err).Message()
	case codes.NotFound:
		return http.StatusNotFound, status.Convert(err).Message()
	default:
		return http.StatusInternalServerError, "internal error"
	}
}

// handleClusters GET /api/v1/clusters?state=active|resolved|all（默认 active）
// &limit=N（第五轮审核 G3：默认 200 上限 1000——7 天影子期 resolved
// 簇持续累积，无上限的列表响应会随历史膨胀）。
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

// handleChanges GET /api/v1/changes?node_key=&window_start=&window_end=
// 复用 gRPC GetRecentChanges（窗口校验/映射一套语义）。
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
