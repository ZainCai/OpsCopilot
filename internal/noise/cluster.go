// cluster.go 告警簇（W4-1.3）：时间窗聚类 + 拓扑邻近聚合 + 簇状态机。
//
// 定位（ADR-001）：本结构是**加速层**的内存簇状态，真相源在 alert_cluster
// 表（W4-1.5 落库 + 重建）。内存结构允许丢——丢的代价是重建，不是数据事故。
//
// 聚类语义（两条并入规则，按序判定）：
//  1. 指纹相同 → 同簇（ADR-001 配套的幂等主轴）；
//  2. 故障域相同 → 同簇：新告警的 NodeKey 与某活跃簇内任一节点在
//     CausalSubgraph 上连通（同一故障域），且时间窗未过 → 并入。
//     同一 NodeKey 视为距离 0，天然同域——即使调用方没注入拓扑函数。
//
// 状态机（对齐 alert_cluster.state，封闭集合 open/acked/resolved）：
//   - open    新簇默认态；
//   - acked   算子确认（Ack），窗口内新告警不改变 acked（有人在场处理）；
//   - resolved 窗口超时自动 resolve（LastSeen + window 之后无新告警），
//     或数据库侧状态回放（W4-1.5）。resolved 是终态，Ack 不能拉回。
//
// 决定性契约（测试锁定）：候选簇多于一个时取 LastSeen 最新者（同一持续
// 事故优先），平局取 cluster_key 字典序最小——重放同序告警流得到同序结果。
//
// 降噪宁漏勿杀同样适用：指纹为空的告警不参与聚类（返回 nil），由调用方
// 决定是否直通（v1.2 C17 直通模式），绝不静默丢弃。
//
// 内存边界：resolved 簇本体保留在内存（历史查询用），仅解除二级索引。
// M1 量级（百台规模、7 天影子期）可接受；W4-1.5 落库后由重建/保留策略
// 接管，长跑内存水位是那时的验收项，不是本结构的。
package noise

import (
	"errors"
	"sort"
	"strconv"
	"sync"
	"time"
)

// ClusterState 簇状态（封闭集合，与 alert_cluster.state 一致）。
type ClusterState string

// 状态常量。
const (
	StateOpen     ClusterState = "open"
	StateAcked    ClusterState = "acked"
	StateResolved ClusterState = "resolved"
)

// SeverityRank 严重级排序：数值越大越严重。未知/空级排最低——
// 告警缺 severity 不应抬高簇级，也不应抹掉簇里已有的更高等级。
var SeverityRank = map[string]int{
	"":         0,
	"info":     1,
	"warning":  2,
	"critical": 3,
}

// FaultDomainFunc 判定两个拓扑节点是否属于同一故障域。
// 由 cmd/ 装配层用 topology.Graph.CausalSubgraph 的连通性实现；
// noise 包自己不 import topology（模块纪律）。nil 表示拓扑不可用，
// 此时退化为"同 NodeKey 即同域"。
type FaultDomainFunc func(a, b string) bool

// Event 参与聚类的最小告警（与 connector.Alert 解耦的翻译目标，
// 翻译发生在 cmd/ 装配层）。
type Event struct {
	// Fingerprint 告警指纹（noise.Fingerprint 产物或上游透传）。
	Fingerprint string
	// NodeKey 告警关联的拓扑节点 Key；空表示无法定位（不参与域聚合，
	// 只能靠指纹命中簇）。
	NodeKey string
	// Severity 严重级（info/warning/critical；未知按最低处理）。
	Severity string
	// Summary 一句话摘要（簇 summary 取最新一条）。
	Summary string
	// OccurredAt 告警发生时刻（聚类时间窗的判定基准）。
	OccurredAt time.Time
}

// Cluster 一条告警簇的内存态。
type Cluster struct {
	// Key 幂等键（对齐 alert_cluster.cluster_key）：
	// "c:<首条指纹>@<首条发生时刻的窗口桶号>"——同序重放可复现。
	Key string
	// State 当前状态机状态。
	State ClusterState
	// FirstSeen / LastSeen 簇首末告警时刻。
	FirstSeen time.Time
	LastSeen  time.Time
	// Severity 簇内出现过的最高严重级。
	Severity string
	// Summary 最新一条告警的摘要（新鲜的覆盖旧的）。
	Summary string
	// AlertCount 累计并入告警数（含窗口内重复指纹）。
	AlertCount int
	// Fingerprints 簇内全部指纹（含因域聚合并入的异指纹）。
	Fingerprints map[string]struct{}
	// NodeKeys 簇内涉及的全部拓扑节点。
	NodeKeys map[string]struct{}
}

// Clusterer 内存聚类器（并发安全）。
type Clusterer struct {
	mu     sync.Mutex
	window time.Duration
	domain FaultDomainFunc
	// active 未 resolve 的簇（聚类的唯一作用域）。
	// 第四轮扫描 F2：active 与 resolved 分 map——sweep/域聚合只走 active，
	// 否则随 resolved 无限累积，每次 Ingest 的成本线性上涨（O(n²)/批）。
	active map[string]*Cluster
	// resolved 历史簇（仅查询，不参与聚类）。
	resolved map[string]*Cluster
	// byFingerprint 指纹 → 活跃簇 Key（resolve 后解除索引）。
	byFingerprint map[string]string
	// byNode 节点 → 活跃簇 Key（同上；指纹命中优先于节点命中）。
	byNode map[string]string
}

// NewClusterer 构造聚类器。window <= 0 视为不聚类（每条告警自成簇——
// 与 Dedup 的零窗口语义一致：降噪开关坏了宁可全放行）。
// domain 可为 nil（拓扑不可用）。
func NewClusterer(window time.Duration, domain FaultDomainFunc) *Clusterer {
	return &Clusterer{
		window:        window,
		domain:        domain,
		active:        make(map[string]*Cluster),
		resolved:      make(map[string]*Cluster),
		byFingerprint: make(map[string]string),
		byNode:        make(map[string]string),
	}
}

// ErrUnknownCluster Ack 目标簇不存在。
var ErrUnknownCluster = errors.New("noise: unknown cluster")

// Ingest 喂入一条告警，返回其落入的簇与是否新建簇。
// 指纹为空的告警返回 (nil, false)——不聚类、不丢弃，直通决策归调用方。
func (c *Clusterer) Ingest(e Event) (*Cluster, bool) {
	if e.Fingerprint == "" {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sweepLocked(e.OccurredAt) // 先清场：过期簇 resolve 并解除索引

	if ck, ok := c.byFingerprint[e.Fingerprint]; ok {
		return c.absorbLocked(c.active[ck], e), false
	}
	// 故障域命中：同节点或域函数判定连通。候选取 LastSeen 最新（见包注释决定性契约）。
	// 只在活跃（未 resolve）簇中找——resolved 是历史，不能再吸收新告警
	// （F2：resolved 已分离到独立 map，这里天然只扫活跃簇）。
	var best *Cluster
	if e.NodeKey != "" {
		for _, cl := range c.active {
			if !c.sameDomainLocked(cl, e.NodeKey) {
				continue
			}
			if best == nil || cl.LastSeen.After(best.LastSeen) ||
				(cl.LastSeen.Equal(best.LastSeen) && cl.Key < best.Key) {
				best = cl
			}
		}
	}
	if best != nil {
		return c.absorbLocked(best, e), false
	}
	return c.newClusterLocked(e), true
}

// sameDomainLocked 簇内任一节点与新告警节点同域即为命中。
// 同 NodeKey 距离 0 恒真；有域函数时用域函数，无则退化为同键判定。
func (c *Clusterer) sameDomainLocked(cl *Cluster, node string) bool {
	for k := range cl.NodeKeys {
		if k == node {
			return true
		}
		if c.domain != nil && c.domain(k, node) {
			return true
		}
	}
	return false
}

// absorbLocked 把告警并入既有簇并登记全部索引。
func (c *Clusterer) absorbLocked(cl *Cluster, e Event) *Cluster {
	if e.OccurredAt.Before(cl.FirstSeen) {
		cl.FirstSeen = e.OccurredAt // 重放乱序容忍：首见取最早
	}
	cl.LastSeen = e.OccurredAt
	cl.AlertCount++
	cl.Summary = e.Summary
	if SeverityRank[e.Severity] > SeverityRank[cl.Severity] {
		cl.Severity = e.Severity
	}
	cl.Fingerprints[e.Fingerprint] = struct{}{}
	c.byFingerprint[e.Fingerprint] = cl.Key
	if e.NodeKey != "" {
		cl.NodeKeys[e.NodeKey] = struct{}{}
		c.byNode[e.NodeKey] = cl.Key
	}
	return cl
}

// newClusterLocked 以首条告警建新簇；cluster_key 由指纹 + 窗口桶号构成
// （ADR-001：指纹 + 时间窗幂等键）。
func (c *Clusterer) newClusterLocked(e Event) *Cluster {
	// 窗口桶号：window <= 0（不聚类）时退化为纳秒时间戳——既避免除零，
	// 又保证每条告警自成簇时 key 唯一；同序重放输入相同，key 仍可复现。
	bucket := e.OccurredAt.UnixNano()
	if c.window > 0 {
		bucket = e.OccurredAt.Unix() / int64(c.window/time.Second)
	}
	ck := "c:" + e.Fingerprint + "@" + strconv.FormatInt(bucket, 10)
	cl := &Cluster{
		Key:          ck,
		State:        StateOpen,
		FirstSeen:    e.OccurredAt,
		LastSeen:     e.OccurredAt,
		Severity:     e.Severity,
		Summary:      e.Summary,
		AlertCount:   1,
		Fingerprints: map[string]struct{}{e.Fingerprint: {}},
		NodeKeys:     make(map[string]struct{}),
	}
	if e.NodeKey != "" {
		cl.NodeKeys[e.NodeKey] = struct{}{}
		c.byNode[e.NodeKey] = ck
	}
	c.active[ck] = cl
	c.byFingerprint[e.Fingerprint] = ck
	return cl
}

// SetDomain 更换故障域函数（并发安全）。场景：域函数依赖拓扑快照，
// 装配层每批采集重建一次快照后注入（见 cmd/ 侧接线）。
// 传 nil 退化为"同 NodeKey 即同域"。
func (c *Clusterer) SetDomain(f FaultDomainFunc) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.domain = f
}

// Ack 算子确认：open → acked。acked/resolved 原样返回（幂等，不报错）。
// 未知簇返回 ErrUnknownCluster。
func (c *Clusterer) Ack(clusterKey string) (ClusterState, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cl, ok := c.active[clusterKey]
	if !ok {
		// resolved 历史簇的 Ack 是显式 no-op（终态），不是 ErrUnknownCluster。
		if _, wasResolved := c.resolved[clusterKey]; wasResolved {
			return StateResolved, nil
		}
		return "", ErrUnknownCluster
	}
	if cl.State == StateOpen {
		cl.State = StateAcked
	}
	return cl.State, nil
}

// Sweep 把窗口内无新告警的簇置为 resolved 并解除索引，返回 resolve 条数。
// resolved 是终态：已 resolved 的簇不再重复计数。
func (c *Clusterer) Sweep(now time.Time) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sweepLocked(now)
}

func (c *Clusterer) sweepLocked(now time.Time) int {
	n := 0
	for ck, cl := range c.active {
		if !now.Before(cl.LastSeen.Add(c.window)) {
			cl.State = StateResolved
			c.unindexLocked(ck)
			// F2：迁移到历史区。sweep 只需遍历 active map，
			// resolved 簇的累积不再抬升每次 Ingest 的成本。
			delete(c.active, ck)
			c.resolved[ck] = cl
			n++
		}
	}
	return n
}

// unindexLocked 解除簇的全部二级索引（指纹 + 节点）。
func (c *Clusterer) unindexLocked(ck string) {
	cl := c.active[ck]
	for fp := range cl.Fingerprints {
		if k, ok := c.byFingerprint[fp]; ok && k == ck {
			delete(c.byFingerprint, fp)
		}
	}
	for k := range cl.NodeKeys {
		if v, ok := c.byNode[k]; ok && v == ck {
			delete(c.byNode, k)
		}
	}
}

// Get 按 Key 取簇快照（值拷贝，map 深拷贝——调用方改不动内部状态）。
// 不存在返回 nil。活跃与历史（resolved）簇均可查。
func (c *Clusterer) Get(clusterKey string) *Cluster {
	c.mu.Lock()
	defer c.mu.Unlock()
	cl, ok := c.active[clusterKey]
	if !ok {
		cl, ok = c.resolved[clusterKey]
	}
	if !ok {
		return nil
	}
	cp := *cl
	cp.Fingerprints = copySet(cl.Fingerprints)
	cp.NodeKeys = copySet(cl.NodeKeys)
	return &cp
}

// Clusters 返回全部簇（活跃 + 历史）的快照，按 Key 字典序排列
// （决定性输出，供测试与影子标注比对）。
func (c *Clusterer) Clusters() []Cluster {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]Cluster, 0, len(c.active)+len(c.resolved))
	for _, cl := range c.active {
		out = append(out, cloneCluster(cl))
	}
	for _, cl := range c.resolved {
		out = append(out, cloneCluster(cl))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

// ActiveClusters 返回未 resolve 簇的快照（落库管道的热路径优化：
// resolved 簇状态不再变化，无需重复写入）。
func (c *Clusterer) ActiveClusters() []Cluster {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]Cluster, 0, len(c.active))
	for _, cl := range c.active {
		out = append(out, cloneCluster(cl))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

// ActiveCount 未 resolve 的簇数（容量观察点）。
func (c *Clusterer) ActiveCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.active)
}

// ResolvedCount 历史簇数（内存水位观察点，F2 配套）。
func (c *Clusterer) ResolvedCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.resolved)
}

func cloneCluster(cl *Cluster) Cluster {
	cp := *cl
	cp.Fingerprints = copySet(cl.Fingerprints)
	cp.NodeKeys = copySet(cl.NodeKeys)
	return cp
}

func copySet(m map[string]struct{}) map[string]struct{} {
	out := make(map[string]struct{}, len(m))
	for k := range m {
		out[k] = struct{}{}
	}
	return out
}
