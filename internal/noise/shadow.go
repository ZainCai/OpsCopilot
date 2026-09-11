// shadow.go 影子模式（W4-1.4）：只标注"应收敛"，不实际收敛。
//
// 为什么叫影子：降噪决策（去重/聚类并入）此时**不生效**——告警照常
// 全量放行，引擎只对每条告警产出一份"如果真降噪会发生什么"的判决
// （Verdict）。影子数据积累满 1 周后（W6）拿"应收敛 vs 实际通知"做
// 对比评估，准确率 >85% 才把降噪从影子切到执行（M1 出口标准）。
//
// 影子期纪律：本类型不提供任何"拦截告警"的路径，调用方拿到 Verdict
// 后仍然必须投递告警。这是刻意设计——影子开关的语义是"记录"，
// 不是"灰度执行"；灰度是 W6 之后的事。
//
// Verdict 是 W4-1.5 落库的行结构来源：字段与评估需求一一对应，
// 不要在这里加"以后可能有用"的字段。
package noise

import (
	"time"

	"opscopilot/pkg/memguard"
)

// Verdict 一条告警的影子判决。
type Verdict struct {
	// Fingerprint / NodeKey / OccurredAt 告警回溯三元组（落库对齐键）。
	Fingerprint string
	NodeKey     string
	OccurredAt  time.Time
	// WouldSuppress 去重器判定：窗口内重复指纹（真降噪时这条不会单独通知）。
	WouldSuppress bool
	// ClusterKey 告警落入的簇（空 = 未聚类：空指纹或聚类关闭）。
	ClusterKey string
	// ClusterCreated 是否新建了簇（true = 这条告警开创了新事件）。
	ClusterCreated bool
	// WouldConverge 综合收敛判定：去重命中 **或** 并入既有簇。
	// true = "如果降噪生效，这条告警应被收敛进簇而不单独通知"。
	WouldConverge bool
	// Reason 收敛原因（评估报告的分类维度）：
	// "dedup-window"（窗口内重复）/"cluster-merge"（故障域并入）/
	// "new-incident"（新事件，不应收敛）。空 = 未参与判定。
	Reason string
	// Severity / Summary 告警严重级与摘要（W6-1：评估报告按严重级
	// 分桶需要；从 Event 原样拷贝）。
	Severity string
	Summary  string
}

// 收敛原因常量。
const (
	ReasonDedupWindow  = "dedup-window"
	ReasonClusterMerge = "cluster-merge"
	ReasonNewIncident  = "new-incident"
)

// Shadow 影子降噪引擎：Dedup + Clusterer 的组合前置。
// 并发安全（成员各自持锁）。
type Shadow struct {
	dedup     *Dedup
	clusterer *Clusterer
}

// NewShadow 构造。window <= 0 时去重与聚类均关闭——Verdict 退化为
// 每条 new-incident（降噪"配置性关闭"与影子开关是两回事）。
func NewShadow(window time.Duration, domain FaultDomainFunc) *Shadow {
	return NewShadowWithLimits(window, domain, nil, nil)
}

// NewShadowWithLimits 构造并给去重器/聚类器分别装配容量护栏
// （优化方案 #6；guard 传 nil = 对应结构不设限）。
func NewShadowWithLimits(window time.Duration, domain FaultDomainFunc, dedupGuard, clusterGuard *memguard.Guard) *Shadow {
	return &Shadow{
		dedup:     NewDedupWithLimits(window, dedupGuard),
		clusterer: NewClustererWithLimits(window, domain, clusterGuard),
	}
}

// MemGuards 聚合底层两个有界结构的护栏（供装配层注册指标）。
func (s *Shadow) MemGuards() []*memguard.Guard {
	out := append(s.dedup.MemGuards(), s.clusterer.MemGuards()...)
	return out
}

// Process 处理一条告警并产出影子判决。告警本身照常放行——本方法
// 的返回值只做记录，不做拦截。
//
// 判定顺序（决定性）：先 Dedup（窗口内重复指纹）再 Clusterer（指纹/
// 故障域并入）。空指纹不进任何环节，Verdict 全零值 + new-incident。
func (s *Shadow) Process(e Event) Verdict {
	v := Verdict{
		Fingerprint: e.Fingerprint,
		NodeKey:     e.NodeKey,
		OccurredAt:  e.OccurredAt,
		Severity:    e.Severity,
		Summary:     e.Summary,
	}
	if e.Fingerprint == "" {
		v.Reason = ReasonNewIncident
		return v
	}
	v.WouldSuppress = !s.dedup.Allow(e.Fingerprint, e.OccurredAt)
	cl, created := s.clusterer.Ingest(e)
	v.ClusterKey = clusterKeyOf(cl)
	v.ClusterCreated = created && cl != nil
	v.WouldConverge = v.WouldSuppress || (cl != nil && !created)
	switch {
	case v.WouldSuppress:
		v.Reason = ReasonDedupWindow
	case cl != nil && !created:
		v.Reason = ReasonClusterMerge
	default:
		v.Reason = ReasonNewIncident
	}
	return v
}

// Clusterer 暴露底层聚类器（Ack 等运维操作与 W4-1.5 落库重建要用）。
func (s *Shadow) Clusterer() *Clusterer { return s.clusterer }

// SetDomain 更换故障域函数（透传给聚类器）。装配层每批采集
// 重建拓扑快照后注入。
func (s *Shadow) SetDomain(f FaultDomainFunc) { s.clusterer.SetDomain(f) }

// Dedup 暴露底层去重器（容量观察与测试用）。
func (s *Shadow) Dedup() *Dedup { return s.dedup }

func clusterKeyOf(cl *Cluster) string {
	if cl == nil {
		return ""
	}
	return cl.Key
}
