// W5-2.1 SemanticModel gRPC 服务：拓扑时点查询 + 变更窗口查询。
//
// 纪律对齐：
//   - R2：GetTopology 的 Build→AsOf→裁剪→映射**全部锁内**完成
//     （TopologySink.TopologyAsOf，见其注释）；
//   - C5 三步走：ChangeRecord 契约已扩字段（proto ①），本文件实现
//     ChangeEvent → pb.ChangeRecord 映射（②），类型统一走 ChangeType/
//     Confidence 字符串化（③——两个枚举都是封闭集合，Parse* 保证）；
//   - ChangeStore 的读方法返回**值拷贝**，映射在锁外安全（与拓扑侧
//     R2 相反：那边是共享指针，这边是值）。
package main

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "opscopilot/internal/contracts/pb"
	"opscopilot/internal/topology"
)

// SemanticModelServer 语义模型服务实现（all-in-one 进程内形态）。
// 对外暴露经 transport.DialInProcess（拆分独立进程时仅换传输层）。
type SemanticModelServer struct {
	pb.UnimplementedSemanticModelServer
	sink    *TopologySink
	changes topology.ChangeBackend
}

// NewSemanticModelServer 构造。装配层保证参数非 nil（nil 会 panic 于
// 首次调用——但装配是唯一入口且有测试覆盖，不做运行时防御）。
// changes 为后端接口（内存或 PG 持久化，见 topology.ChangeBackend）。
func NewSemanticModelServer(sink *TopologySink, changes topology.ChangeBackend) *SemanticModelServer {
	return &SemanticModelServer{sink: sink, changes: changes}
}

// maxTopologyDepth 邻域裁剪允许的最大跳数。
//
// 为什么要上限：Neighborhood 是 `for d := 0; d < depth; d++` 的循环——frontier
// 变空后每轮只是空转，但**全程持有拓扑锁**（TopologyAsOf 的 s.mu）。因此
// depth=2147483647 可让一次**匿名**读请求（读面无鉴权）空转 21 亿次，
// 期间阻塞拓扑发现入库与变更事件的节点校验。10 跳对"看一个故障域"足够。
const maxTopologyDepth = 10

// GetTopology 时点拓扑查询。
//
// as_of 语义（v1.2 C15）：RCA 必须传故障时刻 T0；空 = 当前时刻
// （ResolvedAsOf 回执为空串）。node_key 非空做邻域裁剪，节点不存在
// 返回 NotFound——宁可报错，不静默返回空图让调用方误判"当时没有拓扑"。
//
// depth 范围在此**单一收口**校验（REST 与进程内 gRPC 共用本方法）；
// InvalidArgument 经 grpcToHTTP 映射为 HTTP 400。
func (s *SemanticModelServer) GetTopology(_ context.Context, req *pb.GetTopologyRequest) (*pb.GetTopologyResponse, error) {
	if d := int(req.GetDepth()); d < 0 || d > maxTopologyDepth {
		return nil, status.Errorf(codes.InvalidArgument,
			"depth must be in [0,%d], got %d", maxTopologyDepth, d)
	}
	asOf, err := topology.ParseAsOf(req.GetAsOf())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "as_of: %v", err)
	}
	resp, ok := s.sink.TopologyAsOf(asOf, req.GetNodeKey(), int(req.GetDepth()))
	if !ok {
		return nil, status.Errorf(codes.NotFound, "node %q not found (at or before as_of)", req.GetNodeKey())
	}
	return resp, nil
}

// GetRecentChanges 节点变更窗口查询（RCA 证据）。
//
// 窗口语义：window_start 必填（RCA 的证据窗口必须有下界，无下界的
// "全部变更"是全库扫，不是证据查询）；window_end 为空 = 当前时刻。
// 半开区间 [start, end)，与 ChangeStore.ByNodeWithin 一致。
// node_key 为空查全租户窗口（运维面用），非空查单节点。
// tenant_id：M1 单租户，不校验（W7 多租户时在此加过滤，契约字段已备）。
func (s *SemanticModelServer) GetRecentChanges(_ context.Context, req *pb.GetRecentChangesRequest) (*pb.GetRecentChangesResponse, error) {
	if req.GetWindowStart() == "" {
		return nil, status.Error(codes.InvalidArgument, "window_start is required (evidence window must be bounded)")
	}
	start, err := time.Parse(time.RFC3339, req.GetWindowStart())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "window_start: %v", err)
	}
	end := time.Now()
	if req.GetWindowEnd() != "" {
		if end, err = time.Parse(time.RFC3339, req.GetWindowEnd()); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "window_end: %v", err)
		}
	}
	if end.Before(start) {
		return nil, status.Error(codes.InvalidArgument, "window_end before window_start")
	}

	var evs []topology.ChangeEvent
	if nk := req.GetNodeKey(); nk != "" {
		evs = s.changes.ByNodeWithin(nk, start, end)
	} else {
		evs = s.changes.Within(start, end)
	}

	resp := &pb.GetRecentChangesResponse{Changes: make([]*pb.ChangeRecord, 0, len(evs))}
	for _, ev := range evs {
		resp.Changes = append(resp.Changes, changeToProto(ev))
	}
	return resp, nil
}

// changeToProto ChangeEvent → pb.ChangeRecord（C5 ②）。
// 映射约定（对齐 change.go 顶部 DB 落库契约）：
//   - Author → actor（表列语义延续）；
//   - Type/Confidence 走字符串化（C5 ③，封闭集合）；
//   - OccurredAt/FirstSeen 等 RFC3339 字符串——契约用 string 而非
//     google.protobuf.Timestamp 是 v1 契约的既定选择，保持一致。
func changeToProto(ev topology.ChangeEvent) *pb.ChangeRecord {
	return &pb.ChangeRecord{
		NodeKey:    ev.NodeKey,
		ChangeType: string(ev.Type),
		OccurredAt: ev.OccurredAt.Format(time.RFC3339),
		Actor:      ev.Author,
		// W5-2.1 扩展字段（C5 证据）。
		Id:         ev.ID,
		Source:     ev.Source,
		Ref:        ev.Ref,
		Revision:   ev.Revision,
		Summary:    ev.Summary,
		Confidence: string(ev.Confidence),
		TenantId:   ev.TenantID,
	}
}
