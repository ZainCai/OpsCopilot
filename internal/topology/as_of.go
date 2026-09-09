package topology

import (
	"time"

	pb "opscopilot/internal/contracts/pb"
)

// as_of 时点查询（M1 W3 / v2 C15 的内存实现）：
// 给定时刻 t，重建"当时我们知道的拓扑"。
//
// 语义（bi-temporal 的可见性判定）：
//   - 节点在 t 可见 ⇔ ValidFrom ≤ t 且（ValidTo 为零 或 t < ValidTo）；
//   - 边在 t 可见 ⇔ 边自身可见 且 Src/Dst 两端节点都在 t 可见——
//     端点不可见的边是悬空引用，直接剔除（与 Builder.AddEdge 的
//     "端点必须存在"纪律一致）；
//   - 合并规则保证 ValidFrom 取最早（builder.go），因此 t 早于最近
//     一次观测也能正确命中——这是"历史时点拓扑可查"的基础。
//
// 已知局限（如实记录，不掩盖）：当前 Builder 只会在节点"仍被观测到"
// 时清空 ValidTo，尚无"节点消失"的淘汰机制（那需要 N 轮未见后置
// ValidTo 的宿主逻辑），所以内存图上 as_of 只能正确回答"t 时刻已知
// 有哪些节点"，不能回答"t 时刻已消失的节点"。持久层
//（migrations/000001 的时态表）落地后由存储侧补全该语义。

// ParseAsOf 解析 as_of 请求参数（pb GetTopologyRequest.AsOf 用）。
// 空串返回零时间，语义为"当前时刻"（调用方据此走全图路径）。
// 非 RFC3339 格式直接报错——宁可拒绝请求，不静默当"当前"处理。
func ParseAsOf(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}, err // 错误已含格式说明，不重复包装
	}
	return t, nil
}

// AsOf 返回 t 时刻可见的拓扑快照。结果为新 Graph（浅拷贝语义：
// Node/Edge 指针与原图共享——图是不可变约定，调用方不得修改，
// 见 Builder.Build 注释），可见性规则见文件头。
//
// t 为零值时按**当前时刻**处理（W3 审查 P2-1：原实现会跳过 ValidTo
// 判定，导致"零值=当前"的契约下已失效节点被错误返回；统一转
// time.Now() 后零值与显式当前时刻行为完全一致）。
func (g *Graph) AsOf(t time.Time) *Graph {
	if t.IsZero() {
		t = time.Now()
	}
	out := &Graph{Nodes: make(map[string]*Node)}
	if g == nil {
		return out
	}
	for k, n := range g.Nodes {
		if visible(n.ValidFrom, n.ValidTo, t) {
			out.Nodes[k] = n
		}
	}
	for _, e := range g.Edges {
		if !visible(e.ValidFrom, e.ValidTo, t) {
			continue
		}
		if _, ok := out.Nodes[e.SrcKey]; !ok {
			continue // 端点不在 t 可见 → 悬空，剔除
		}
		if _, ok := out.Nodes[e.DstKey]; !ok {
			continue
		}
		out.Edges = append(out.Edges, e)
	}
	return out
}

// visible 单条证据在 t 时刻的可见性判定。
func visible(from, to, t time.Time) bool {
	if !t.IsZero() && from.After(t) {
		return false // t 时还没被观测到
	}
	if !to.IsZero() && !t.Before(to) {
		return false // t 时已失效（ValidTo 为半开区间：[from, to)）
	}
	return true
}

// ToProtoAsOf 全图映射并填写 ResolvedAsOf（契约的时点查询回执字段）。
// resolved 为零值时输出空串——"当前"的契约表达，与 ParseAsOf 配套。
func (g *Graph) ToProtoAsOf(resolved time.Time) *pb.GetTopologyResponse {
	resp := g.ToProto()
	resp.ResolvedAsOf = fmtRFC3339(resolved)
	return resp
}
