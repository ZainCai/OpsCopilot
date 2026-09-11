// W4-1.4 影子降噪接线：connector.Alert → noise.Event 翻译 +
// topology.CausalSubgraph 故障域适配器 + TopologySink 挂载。
//
// 本文件是 connector / topology / noise 三个 internal 模块的唯一汇合点
// （v1.3 §5.2：internal 禁互 import，编排只落在 cmd/）。
package main

import (
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"opscopilot/internal/connector"
	"opscopilot/internal/noise"
	"opscopilot/internal/notify"
)

// 影子降噪环境变量。
const (
	// envNoiseWindow 聚类/去重时间窗（Go duration，如 10m；默认 10m）。
	envNoiseWindow = "OPS_NOISE_WINDOW"
	// envNoiseShadow 影子开关："off" 关闭采集侧降噪处理（默认开）。
	// 关闭 = 完全不处理（连影子判决都不产生），回到 W3 只计数行为。
	envNoiseShadow = "OPS_NOISE_SHADOW"
	// envNoiseMode 降噪模式（W9-1 转正，ADR-011）：
	//   shadow（默认）——只标注不拦截，告警全量放行（影子纪律）；
	//   enforce —— WouldSuppress 判决交给 Gate 真拦截 + 放行通知；
	// 非法值启动失败（与 OPS_NOISE_WINDOW 同纪律：静默回退会让运维
	// 以为配置生效了）。判决落库两种模式都在跑——enforce 首周
	// shadow 双跑对比的数据基础（ADR-011）。
	envNoiseMode = "OPS_NOISE_MODE"
)

// 降噪模式常量。
const (
	ModeShadow  = "shadow"
	ModeEnforce = "enforce"
)

// defaultNoiseWindow 影子期默认窗口：30s 采集轮询下，10 分钟足以覆盖
// 同一故障的重复告警，又不至于把相隔较远的两次独立故障并成一簇。
const defaultNoiseWindow = 10 * time.Minute

// NoiseEngine 影子降噪引擎的 cmd 侧持有者。
// 并发安全：内部互斥——域函数快照重建与 Process 串行化（采集宿主当前
// 串行调用 Sink，但本结构不依赖这一实现细节）。
type NoiseEngine struct {
	mu      sync.Mutex
	shadow  *noise.Shadow
	sink    *TopologySink // 取因果子图快照（锁内裁剪，见 CausalSnapshot）
	enabled bool
	logger  connector.Logger
	// total / converged 影子统计（启动以来累计）。
	total, converged int
	// tenant 租户标识（M1 单租户；落库记录与键空间前缀用）。
	tenant string
	// records 簇落库出口（W4-1.5，可选；nil = 只跑影子不落库）。
	records noise.RecordSink
	// verdicts 逐告警判决落库出口（W6-1 评估数据链，可选）。
	verdicts noise.VerdictSink
	// saveFailures / verdictFailures 落库失败累计（不中断告警链路）。
	saveFailures, verdictFailures int
	// persistedSig 已落库签名：clusterKey → state|lastSeenUnixNano|alertCount。
	// 第四轮扫描 F3：resolved 簇状态不再变化，全量重写会让 Redis 写放大
	// 随历史线性增长——按签名去重，只有变化过的簇才写。Restore 后清空
	// （重建自存储侧，签名必须重新建立）。
	persistedSig map[string]string
	// enforce 转正模式（W9-1，ADR-011）：true = WouldSuppress 判决交
	// Gate 真拦截、放行项发通知；false = 影子（默认，不碰 Gate）。
	enforce bool
	// gate 通知闸门（enforce 模式的执行件；shadow 模式恒 nil）。
	// Admit 在锁外调用：渠道 Send 自带超时（Notifier 契约），不能拖住
	// 引擎锁——与落库 IO 同一锁边界纪律。
	gate *notify.Gate
	// gateStats Gate 计数真相源出口（nil = 只累计内存，重启清零）。
	gateStats GateStatsSink
	// gateFailures 计数持久化失败累计（尽力而为，不中断告警链路）。
	gateFailures int
}

// DefaultTenant M1 单租户缺省值（alert_cluster.tenant_id 对齐）。
//
// 可用 OPS_TENANT 覆盖。动机：评估/演示环境常需与既有数据隔离——
// 本机 8080/19090 上还有一组旧实例在持续写判决，新评估若同租户会互相
// 污染时间窗。改为包级 var（进程启动时读一次 env），测试不受影响
// （不设 OPS_TENANT 即得 "default"）。
var DefaultTenant = func() string {
	if v := strings.TrimSpace(os.Getenv("OPS_TENANT")); v != "" {
		return v
	}
	return "default"
}()

// NewNoiseEngine 按 env 构造。返回 nil 表示影子降噪关闭（envNoiseShadow=off），
// 调用方须容忍 nil（挂载点与 ProcessAlerts 均做 nil 检查）。
// 窗口解析失败视为配置错误返回 error——静默回退默认值会让运维以为
// 配置生效了。
func NewNoiseEngine(sink *TopologySink, logger connector.Logger) (*NoiseEngine, error) {
	if os.Getenv(envNoiseShadow) == "off" {
		return nil, nil
	}
	window := defaultNoiseWindow
	if raw := os.Getenv(envNoiseWindow); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil || d <= 0 {
			return nil, &invalidNoiseWindowError{raw: raw, err: err}
		}
		window = d
	}
	mode := ModeShadow
	if raw := strings.TrimSpace(os.Getenv(envNoiseMode)); raw != "" {
		switch strings.ToLower(raw) {
		case ModeShadow, ModeEnforce:
			mode = strings.ToLower(raw)
		default:
			return nil, &invalidNoiseModeError{raw: raw}
		}
	}
	return &NoiseEngine{
		shadow:       noise.NewShadow(window, nil), // 域函数按批经 SetDomain 注入
		sink:         sink,
		enabled:      true,
		logger:       logger,
		tenant:       DefaultTenant,
		persistedSig: make(map[string]string),
		enforce:      mode == ModeEnforce,
	}, nil
}

// SetRecordSink 挂载簇落库出口（W4-1.5 双写管道）。传 nil 卸载。
// 必须在首条告警处理前挂好：落库语义是"批后全量幂等快照"，
// 挂载前的状态变更不会补写。
func (n *NoiseEngine) SetRecordSink(rs noise.RecordSink) {
	if n == nil {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.records = rs
}

// SetVerdictSink 挂载逐告警判决落库出口（W6-1）。传 nil 卸载。
func (n *NoiseEngine) SetVerdictSink(vs noise.VerdictSink) {
	if n == nil {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.verdicts = vs
}

type invalidNoiseWindowError struct {
	raw string
	err error
}

// Error 对 err == nil 容错（第四轮扫描 F1）：值非法（如 "0s"）与解析
// 失败都会走到这里——前者没有底层 err，直接解引用会 panic，
// 把配置错误变成进程崩溃而非干净的启动失败。
func (e *invalidNoiseWindowError) Error() string {
	if e.err == nil {
		return "noise: invalid " + envNoiseWindow + " " + e.raw + ": must be a positive duration"
	}
	return "noise: invalid " + envNoiseWindow + " " + e.raw + ": " + e.err.Error()
}

type invalidNoiseModeError struct{ raw string }

func (e *invalidNoiseModeError) Error() string {
	return "noise: invalid " + envNoiseMode + " " + e.raw + ": must be \"shadow\" or \"enforce\""
}

// GateStatsSink Gate 拦截/放行计数的真相源出口（R6-6：转正后拦截数是
// 核心运维指标，重启不得清零）。实现必须幂等——同租户重复 Save 结果
// 不变（覆盖写累计值）。nil = 只累计内存。
type GateStatsSink interface {
	SaveGateStats(tenant string, s notify.Stats) error
	LoadGateStats(tenant string) (notify.Stats, error)
}

// SetGate 挂载通知闸门（W9-1，enforce 模式执行件）。传 nil 卸载。
// 挂载时若配了真相源出口，先恢复历史累计（进程重启不清零）。
// 影子模式下闸门不会被调用，挂了也无副作用——但装配层应只在
// enforce 下挂载，让"配置意图"与"运行时行为"可从装配代码直接对读。
func (n *NoiseEngine) SetGate(g *notify.Gate, s GateStatsSink) {
	if n == nil {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.gate = g
	n.gateStats = s
	if g != nil && s != nil {
		if st, err := s.LoadGateStats(n.tenant); err != nil {
			n.logf("WARNING: gate stats restore skipped (starting from zero): %v", err)
		} else if st.Suppressed > 0 || st.Dispatched > 0 {
			g.SetStats(st)
			n.logf("gate stats restored: suppressed=%d dispatched=%d", st.Suppressed, st.Dispatched)
		}
	}
}

// batchView 一批采集的拓扑视图：因果邻接表 + instance→节点 Key 反查索引。
type batchView struct {
	adj      map[string][]string
	byInsEnv map[string]string // labels.instance → 节点 Key（首个命中，构建序决定）
}

// ProcessAlerts 处理一轮采集的全部告警（影子模式：不拦截，只标注）。
// 每批开始时重建拓扑快照（CausalSubgraph），批内共用——一轮采集期间
// 拓扑视为不变。
//
// 锁边界（第四轮扫描 F3）：mu 保护处理段（快照/判决/签名比对），
// **不持有落库 IO**——慢存储不能拖住 Stats()；同时落库保持同步
// （不 go 出去）：异步会让批 N+1 先写、批 N 后写，旧数据覆盖新数据。
func (n *NoiseEngine) ProcessAlerts(alerts []connector.Alert) {
	if n == nil || len(alerts) == 0 {
		return
	}
	n.mu.Lock()

	view := n.snapshotView()
	n.shadow.SetDomain(func(a, b string) bool { return reachable(view.adj, a, b) })

	var suppressed, merged, created int
	var verdicts []noise.VerdictRecord
	var decisions []notify.Decision
	for _, a := range alerts {
		e := n.toEvent(a, view)
		v := n.shadow.Process(e)
		verdicts = append(verdicts, v.ToRecord(n.tenant))
		n.total++
		switch {
		case v.WouldSuppress:
			suppressed++
		case !v.ClusterCreated && v.ClusterKey != "":
			merged++
		}
		if v.WouldConverge {
			n.converged++
		}
		if v.ClusterCreated {
			created++
		}
		if n.enforce && n.gate != nil {
			// enforce 模式（ADR-011）：每条判决都过闸门——
			// WouldSuppress=true 拦截计数；false 放行并全渠道通知。
			// 判决照常落库：enforce 首周与影子基线对比的数据基础。
			decisions = append(decisions, notify.Decision{
				ClusterKey:    v.ClusterKey,
				Severity:      v.Severity,
				Title:         v.Summary,
				WouldSuppress: v.WouldSuppress,
			})
		}
	}
	n.logf("shadow verdict: %d alerts (new %d, dedup %d, merged %d) — cumulative converge %d/%d",
		len(alerts), created, suppressed, merged, n.converged, n.total)

	var dirty []noise.ClusterRecord
	if n.records != nil {
		dirty = n.collectDirtyClusters()
	}
	var vs noise.VerdictSink
	if n.verdicts != nil {
		vs = n.verdicts
	}
	gate, gs := n.gate, n.gateStats
	n.mu.Unlock()

	if len(dirty) > 0 {
		n.persistClusters(dirty)
	}
	if vs != nil {
		n.persistVerdicts(vs, verdicts)
	}
	if len(decisions) > 0 && gate != nil {
		n.admitDecisions(gate, gs, decisions)
	}
}

// admitDecisions enforce 模式闸门执行（锁外，与落库 IO 同一锁边界纪律：
// 渠道 Send 自带超时，同步调用不 go 出去——异步会丢"放行失败"的
// 可观察性，且渠道间顺序没有契约值得保）。
// 拦截/放行计数持久化：每批批后保存累计值（覆盖写，幂等）；失败只
// 计数+日志，下一批覆盖写自然补齐（累计值语义下无脏写问题）。
func (n *NoiseEngine) admitDecisions(gate *notify.Gate, gs GateStatsSink, decisions []notify.Decision) {
	var admitted int
	for _, d := range decisions {
		ok, err := gate.Admit(d)
		if err != nil {
			// 空判决（无簇无标题）或全渠道失败：计数为闸门异常，
			// 不中断后续告警的闸门判定。
			n.gateFailures++
			n.logf("WARNING: gate admit failed (cumulative %d): %v", n.gateFailures, err)
			continue
		}
		if ok {
			admitted++
		}
	}
	st := gate.Stats()
	if gs != nil {
		if err := gs.SaveGateStats(n.tenant, st); err != nil {
			n.gateFailures++
			n.logf("WARNING: gate stats persist failed (cumulative %d): %v", n.gateFailures, err)
		}
	}
	n.logf("enforce gate: %d decisions (admitted %d, suppressed %d) — cumulative suppressed=%d dispatched=%d",
		len(decisions), admitted, len(decisions)-admitted, st.Suppressed, st.Dispatched)
}

// persistVerdicts 逐告警判决落库（W6-1，锁外）。失败只计数+日志——
// 评估数据缺行会让准确率分母偏小，运维通过 verdictFailures 观察丢失面。
func (n *NoiseEngine) persistVerdicts(vs noise.VerdictSink, verdicts []noise.VerdictRecord) {
	failed := 0
	for _, rec := range verdicts {
		if err := vs.SaveVerdict(rec); err != nil {
			failed++
			n.verdictFailures++
		}
	}
	if failed > 0 {
		n.logf("WARNING: verdict persist failed for %d/%d (cumulative %d)",
			failed, len(verdicts), n.verdictFailures)
	}
}

// clusterSig 簇的落库签名：任何会影响存储行的字段变化都会改变签名。
func clusterSig(cl noise.Cluster) string {
	return string(cl.State) + "|" + strconv.FormatInt(cl.LastSeen.UnixNano(), 10) + "|" +
		strconv.Itoa(cl.AlertCount)
}

// collectDirtyClusters 锁内挑选签名变化的簇。
// 活跃簇 + 本批新 resolve 的簇（状态从 open/acked → resolved）都会入选；
// 早已 resolved 且未变的簇不再重复写（F3 写放大修复）。
func (n *NoiseEngine) collectDirtyClusters() []noise.ClusterRecord {
	clusters := n.shadow.Clusterer().Clusters()
	dirty := make([]noise.ClusterRecord, 0, len(clusters))
	for _, cl := range clusters {
		sig := clusterSig(cl)
		if n.persistedSig[cl.Key] == sig {
			continue
		}
		n.persistedSig[cl.Key] = sig
		dirty = append(dirty, cl.ToRecord(n.tenant))
	}
	return dirty
}

// persistClusters 幂等写入脏簇。
// 第五轮审核 G1：签名在锁内 collectDirtyClusters 已更新，若 Save 失败
// 不回滚，下一批签名比对会说"无变化"——**丢失的写入永不补齐**（对
// resolved 簇是永久丢失），原注释"自动补齐"不成立。失败即回滚该 key
// 的签名，下一批重写。失败只计数+日志，不中断告警链路。
func (n *NoiseEngine) persistClusters(dirty []noise.ClusterRecord) {
	if n.records == nil {
		return
	}
	failed := 0
	for _, rec := range dirty {
		if err := n.records.SaveCluster(rec); err != nil {
			failed++
			n.saveFailures++
			n.mu.Lock()
			delete(n.persistedSig, rec.ClusterKey)
			n.mu.Unlock()
		}
	}
	if failed > 0 {
		n.logf("WARNING: cluster persist failed for %d/%d clusters (rolled back signatures, cumulative failures %d)",
			failed, len(dirty), n.saveFailures)
	}
}

// RestoreFrom 从落库记录重建内存簇状态（ADR-001：Redis 丢失后从真相源
// 拉平的唯一入口）。重建后继续 Process，命中同一批 cluster_key——
// 幂等链不断。失败返回错误且内存状态**不变**（半途而废的重建比不重建
// 更危险）。
func (n *NoiseEngine) RestoreFrom(records []noise.ClusterRecord) error {
	if n == nil {
		return nil
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.shadow.Clusterer().Restore(records); err != nil {
		return err
	}
	// 重建自存储侧：旧签名作废，下一批全量重建签名基线。
	n.persistedSig = make(map[string]string)
	return nil
}

// snapshotView 从当前拓扑图构建批次视图。
// ADR-007 low 门禁天然生效：CausalSubgraph 已剔除 low 节点/边，
// 它们进不了故障域判定。
//
// 走 Sink.CausalSnapshot（锁内裁剪 + 返回拷贝）：不能用
// graphSnapshot().CausalSubgraph()——那是在锁外遍历共享图，属无保护读。
func (n *NoiseEngine) snapshotView() batchView {
	g := n.sink.CausalSnapshot()
	view := batchView{
		adj:      make(map[string][]string, len(g.Nodes)),
		byInsEnv: make(map[string]string, len(g.Nodes)),
	}
	for _, e := range g.Edges {
		view.adj[e.SrcKey] = append(view.adj[e.SrcKey], e.DstKey)
		view.adj[e.DstKey] = append(view.adj[e.DstKey], e.SrcKey)
	}
	for k, node := range g.Nodes {
		if ins := node.Labels["instance"]; ins != "" {
			if _, ok := view.byInsEnv[ins]; !ok {
				view.byInsEnv[ins] = k
			}
		}
	}
	return view
}

// toEvent 翻译 connector.Alert → noise.Event。
// Fingerprint：上游为空时用 noise.Fingerprint(labels) 兜底（规范化哈希）。
// NodeKey：经 instance→Key 索引反查；反查不到置空——无法定位的告警
// 不参与域聚合，只能靠指纹命中簇。
// OccurredAt：上游 startsAt 缺失/格式坏时 parseTime 返回零值
// （第五轮审核 G2）——零值会掉进 1970 年窗口桶且被去重器判为窗口内
// 重复而吞掉，兜底为当前观察时刻。noise 保持纯语义（调用方给确定时间），
// 时间修补责任在装配层。
func (n *NoiseEngine) toEvent(a connector.Alert, view batchView) noise.Event {
	fp := a.Fingerprint
	if fp == "" {
		fp = noise.Fingerprint(a.Labels)
	}
	at := a.StartsAt
	if at.IsZero() {
		at = time.Now()
	}
	e := noise.Event{
		Fingerprint: fp,
		Severity:    a.Severity,
		OccurredAt:  at,
		NodeKey:     view.byInsEnv[a.Labels["instance"]],
	}
	if name := a.Labels["alertname"]; name != "" {
		e.Summary = name
	}
	return e
}

// reachable 邻接表 BFS 连通判定（同一故障域 = causal 图上可达）。
func reachable(adj map[string][]string, a, b string) bool {
	if a == b {
		return true
	}
	seen := map[string]bool{a: true}
	queue := []string{a}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, next := range adj[cur] {
			if next == b {
				return true
			}
			if !seen[next] {
				seen[next] = true
				queue = append(queue, next)
			}
		}
	}
	return false
}

// Stats 影子统计快照（运维自检）。
func (n *NoiseEngine) Stats() (total, converged int) {
	if n == nil {
		return 0, 0
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.total, n.converged
}

// Enabled 影子降噪是否启用。
func (n *NoiseEngine) Enabled() bool { return n != nil && n.enabled }

// Mode 降噪模式（shadow / enforce）。nil 引擎（整体关闭）返回空串。
func (n *NoiseEngine) Mode() string {
	if n == nil {
		return ""
	}
	if n.enforce {
		return ModeEnforce
	}
	return ModeShadow
}

func (n *NoiseEngine) logf(format string, args ...interface{}) {
	if n.logger != nil {
		n.logger.Printf(format, args...)
	}
}
