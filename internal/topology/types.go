// Package topology 语义模型 v1（M1 W3）：拓扑图构建、置信度分级与因果门禁。
//
// 核心设计见 docs/adr/ADR-007-topology-confidence.md：
//   - 证据分三级置信度：high（直接观测）/ medium（同源推断）/ low（跨源推断）；
//   - low 级节点与边不进入因果推理（CausalSubgraph 硬门禁，v1.1 C1）；
//   - ValidFrom/ValidTo 为 bi-temporal 设计，是 as_of 时点查询（v2 C15）的基础，
//     任何合并操作不得清空 ValidFrom。
//
// 模块纪律（v1.3 §5.2）：本模块不 import internal/connector 等业务模块。
// 输入类型 NodeInput/EdgeInput 与发现层的 ResourceNode 字段对齐，
// 由编排层完成转换（跨模块调用须经 gRPC/transport）。
package topology

import (
	"errors"
	"fmt"
	"time"
)

// Confidence 拓扑证据置信度（强类型；取值与 pb 契约的 string 直接兼容）。
type Confidence string

// 置信度级别（语义见 ADR-007）。
const (
	// ConfidenceHigh 直接观测事实：连接器直接发现的资源、数据源显式声明的关系。
	ConfidenceHigh Confidence = "high"
	// ConfidenceMedium 同源规则推断：单数据源内部规则（同 job/同集群）聚合出的节点与边。
	ConfidenceMedium Confidence = "medium"
	// ConfidenceLow 跨源推断 / 时间近似 / 兜底猜测。**不进入因果推理**。
	ConfidenceLow Confidence = "low"
)

// ErrUnknownConfidence 非法置信度取值（ParseConfidence 返回的错误以此为根因）。
var ErrUnknownConfidence = errors.New("topology: unknown confidence")

// ParseConfidence 解析置信度字符串（pb / 外部输入用）。
// 空串按 low 处理——保守原则：未声明的证据一律按最低档对待。
// 错误以 ErrUnknownConfidence 为根因（%w 包装），调用方可 errors.Is 匹配。
func ParseConfidence(s string) (Confidence, error) {
	switch Confidence(s) {
	case ConfidenceHigh, ConfidenceMedium, ConfidenceLow:
		return Confidence(s), nil
	case "":
		return ConfidenceLow, nil
	default:
		return "", fmt.Errorf("%w %q", ErrUnknownConfidence, s)
	}
}

// rank 置信度排序值，仅用于合并取高。
func rank(c Confidence) int {
	switch c {
	case ConfidenceHigh:
		return 3
	case ConfidenceMedium:
		return 2
	default:
		return 1
	}
}

// maxConf 返回两者中置信度较高的一档（ADR-007 规则 4：合并取高）。
func maxConf(a, b Confidence) Confidence {
	if rank(a) >= rank(b) {
		return a
	}
	return b
}

// Node 拓扑节点。
type Node struct {
	Key  string
	Type string
	// Labels 仅内存与展示用；pb 契约暂无该字段（ADR-007 交叉检查 2），
	// 序列化到 pb 时会被丢弃——不得依赖它承载因果推理必需的信息。
	Labels map[string]string
	// Confidence 证据置信度（ADR-007）。
	Confidence Confidence
	// Source 产生该节点的来源标识（多源合并时保留最新观测者）。
	Source string
	// ValidFrom 首次观测时间（bi-temporal）。**合并不得清空**。
	ValidFrom time.Time
	// ValidTo 失效时间；零值表示当前有效。
	ValidTo time.Time
}

// Edge 拓扑边（有向：Src → Dst）。
type Edge struct {
	SrcKey string
	DstKey string
	// Relation 关系语义，如 "belongs_to" / "depends_on" / "scrapes"。
	Relation   string
	Confidence Confidence
	Source     string
	ValidFrom  time.Time
	ValidTo    time.Time
}

// Graph 拓扑图。节点按 Key 唯一；边允许平行（不同 Relation 即不同边）。
type Graph struct {
	Nodes map[string]*Node
	Edges []*Edge
}

// NodeInput 发现层输入，与连接器归一化的 ResourceNode 字段对齐。
//
// Confidence 约定（ADR-007 规则 1）：零值视为 **high**——连接器直接发现
// 的是观测事实；由规则聚合出的虚拟节点（如"服务"节点）必须由调用方
// 显式给 medium，把"事实"与"聚合"的区分责任留在数据产生处。
type NodeInput struct {
	Key        string
	Type       string
	Labels     map[string]string
	Source     string
	ObservedAt time.Time
	Confidence Confidence
}

// EdgeInput 边输入。
//
// Confidence 约定（ADR-007 规则 2）：零值视为 **low**——边几乎都是推断
// （数据源显式声明的关系才应是 high，必须由调用方显式给出）。
// 宁可先挡在因果推理之外，也不让猜测混进事实。
type EdgeInput struct {
	SrcKey     string
	DstKey     string
	Relation   string
	Source     string
	ObservedAt time.Time
	Confidence Confidence
}
