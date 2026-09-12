// noise_process.go——NoiseEngine 处理段与出口路径（#9 同包拆分自 noise.go，
// 函数体逐字未动）：ProcessAlerts 主流程 + enforce 闸门执行 + 判决/簇落库 +
// RestoreFrom 重建。
//
// 锁纪律声明：本包 n.mu 的锁边界跨文件仍然成立——权威定义见 noise.go 的
// NoiseEngine 结构注释与下方 ProcessAlerts 的"锁边界"注释。要点：mu 只保护
// 处理段（快照/判决/签名比对），落库 IO、Gate 渠道 Send 一律在 Unlock 之后
// 运行；saveFailures/verdictFailures/gateFailures 因在锁外自增而是 atomic。
// 拓扑读取经 noise_view.go 的 snapshotView（走 Sink 锁内裁剪），本文件不
// 触碰任何共享图。
package main

import (
	"errors"
	"strconv"
	"strings"
	"time"

	"opscopilot/internal/connector"
	"opscopilot/internal/incident"
	"opscopilot/internal/noise"
	"opscopilot/internal/notify"
)

// pendingDecision 待放行判决 + 它的告警发射时刻（打点用）。
// fired 为零值表示上游没给 startsAt —— 不观测该条延迟（用合成时间打点
// 等于自欺，宁可少一个样本）。
type pendingDecision struct {
	d     notify.Decision
	fired time.Time
}

// ProcessAlerts 处理一轮采集的全部告警（影子模式：不拦截，只标注）。
// 每批开始时重建拓扑快照（CausalSubgraph），批内共用——一轮采集期间
// 拓扑视为不变。
//
// 锁边界（第四轮扫描 F3）：mu 保护处理段（快照/判决/签名比对），
// **不持有落库 IO**——慢存储不能拖住 Stats()。
//
// 落库路径（优化方案 #8，队满丢弃取向）：
//   - 簇快照仍同步：快照是"最新覆盖"语义，异步会让批 N+1 先写、批 N
//     后写（旧数据覆盖新数据），必须保持调用方 goroutine 内串行；
//   - 判决（append-only）投递到带缓冲队列（vq）即返回，单 writer 攒批
//     落库——慢 DB 不占采集节拍。**队列永不阻塞、永不回退同步写**：
//     队满/停机即丢持久化任务（计 opscopilot_noise_sink_drops_total +
//     slog ERROR），判决内存态照常走 Gate——宁漏库存，不丢通知；
//   - 队列未启动（测试/嵌入式）保持同步落库（#8 之前的原行为）。
func (n *NoiseEngine) ProcessAlerts(alerts []connector.Alert) {
	if n == nil || len(alerts) == 0 {
		return
	}
	n.mu.Lock()

	view := n.snapshotView()
	n.shadow.SetDomain(func(a, b string) bool { return reachable(view.adj, a, b) })

	var suppressed, merged, created int
	var verdicts []noise.VerdictRecord
	// fired 与 verdicts 平行：告警发射时刻（零值 = 上游没给，不打点）。
	var fired []time.Time
	var pending []pendingDecision
	// W10-6 自动挂簇（OPS_AUTOATTACH）：enforce 判决 new-incident（成簇即
	// 达建单阈值）收集联动任务，锁外执行建单+挂簇 IO——与落库/Gate 同一
	// 锁边界纪律。三重与条件缺一不可（enforce 开关 × autoAttach × 出口
	// 已挂载），shadow 引擎零行为。
	autoAttachOn := n.enforce && n.autoAttach && n.incStore != nil
	var attachReqs []autoAttachRequest
	var attachStore incident.Store
	var attachAudit AuditLog
	if autoAttachOn {
		attachStore = n.incStore
		attachAudit = n.auditLog
	}
	for _, a := range alerts {
		e := n.toEvent(a, view)
		v := n.shadow.Process(e)
		verdicts = append(verdicts, v.ToRecord(n.tenant))
		fired = append(fired, a.StartsAt) // 上游 startsAt 原值（toEvent 会兜底成 now，这里不兜）
		n.total++
		if n.m != nil {
			n.m.CountAlert()
			n.m.CountVerdict(v.Reason)
		}
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
			if autoAttachOn && v.ClusterKey != "" {
				attachReqs = append(attachReqs, autoAttachRequest{
					labels:      a.Labels,
					fingerprint: v.Fingerprint,
					clusterKey:  v.ClusterKey,
					severity:    v.Severity,
					summary:     v.Summary,
				})
			}
		}
		if n.enforce && n.gate != nil {
			// enforce 模式（ADR-011）：每条判决都过闸门——
			// W9-2 语义只有新事件放行（dedup/merge 都不重复通知）；
			// 判决照常落库：enforce 首周与影子基线对比的数据基础。
			pending = append(pending, pendingDecision{
				d: notify.Decision{
					TenantID:      n.tenant,
					ClusterKey:    v.ClusterKey,
					Severity:      v.Severity,
					Title:         v.Summary,
					WouldSuppress: v.WouldSuppress,
					Reason:        v.Reason,
				},
				fired: a.StartsAt,
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
	q := n.vq // 异步判决队列（#8；nil = 未启动，走同步落库）
	gate, gs := n.gate, n.gateStats
	n.mu.Unlock()

	if len(dirty) > 0 {
		n.persistClusters(dirty)
	}
	if vs != nil {
		n.submitVerdicts(q, vs, verdicts, fired)
	}
	if len(pending) > 0 && gate != nil {
		n.admitDecisions(gate, gs, pending)
	}
	if len(attachReqs) > 0 {
		// W10-6：建单+挂簇放在 Gate 之后——通知是告警关键路径，事件域联动
		// （DB 写）不得把渠道 Send 的节拍拖慢；两者优先级由调用次序表达。
		n.autoAttachClusters(attachStore, attachAudit, attachReqs)
	}
}

// admitDecisions enforce 模式闸门执行（锁外，与落库 IO 同一锁边界纪律：
// 渠道 Send 自带超时，同步调用不 go 出去——异步会丢"放行失败"的
// 可观察性，且渠道间顺序没有契约值得保）。
// 拦截/放行计数持久化：每批批后保存累计值（覆盖写，幂等）；失败只
// 计数+日志，下一批覆盖写自然补齐（累计值语义下无脏写问题）。
func (n *NoiseEngine) admitDecisions(gate *notify.Gate, gs GateStatsSink, decisions []pendingDecision) {
	var admitted int
	for _, p := range decisions {
		ok, err := gate.Admit(p.d)
		if err != nil {
			// 空判决（无簇无标题）或全渠道失败：计数为闸门异常，
			// 不中断后续告警的闸门判定。
			n.gateFailures.Add(1)
			n.logf("WARNING: gate admit failed (cumulative %d): %v", n.gateFailures.Load(), err)
			continue
		}
		if ok {
			admitted++
			// W9-4 打点：Admit 返回 = 全渠道 Send 已完成 →
			// time.Since(fired) 即"发射 → 通知送达"的端到端延迟。
			// 批次内后闸门的判决含前面几条的发送耗时（同步顺序投递），
			// 这是真实发生的队列效应，不做修正。
			if !p.fired.IsZero() {
				n.m.ObserveNotifyLatency(time.Since(p.fired))
			} else {
				// 缺发射时刻：留痕（口径同上，见 submitVerdicts）。
				n.m.CountLatencySkipped("notify")
			}
		}
	}
	st := gate.Stats()
	if gs != nil {
		if err := gs.SaveGateStats(n.tenant, st); err != nil {
			n.gateFailures.Add(1)
			n.logf("WARNING: gate stats persist failed (cumulative %d): %v", n.gateFailures.Load(), err)
		}
	}
	n.logf("enforce gate: %d decisions (admitted %d, suppressed %d) — cumulative suppressed=%d dispatched=%d",
		len(decisions), admitted, len(decisions)-admitted, st.Suppressed, st.Dispatched)
}

// autoAttachActor W10-6 自动挂簇的建单/审计署名（人工操作永远不是这个值）。
const autoAttachActor = "system:autoattach"

// autoAttachRequest 一条 new-incident 判决的建单+挂簇联动任务（锁内收集，
// 锁外执行）。labels 用于按链路 A 同款算法算 source_ref（见 autoAttachClusters）。
type autoAttachRequest struct {
	labels      map[string]string
	fingerprint string // 降噪侧身份（审计/日志回显；非 source_ref）
	clusterKey  string
	severity    string
	summary     string
}

// autoAttachClusters W10-6 簇→事件生产自动挂簇（OPS_AUTOATTACH，enforce 判决
// new-incident 出口）：成簇即达建单阈值 → UpsertExternal 建单 + AttachCluster
// 挂簇 + attach_cluster 审计（AuditAttachCluster 自此不再是死代码）。
//
// 建单幂等键与链路 A 收敛（方案《双链路事件来源》L1 口径）：
// source_ref 用 promFingerprint(labels)——与拉取链路逐字同源的算法、同
// origin=prometheus。同一 Prometheus 告警两链路写库天然命中同一单
// （刷新分支共享），不会双建；labels 缺失算不出幂等键 → skipped 计数
// （宁可不建，不造第二身份）。与 OPS_INCIDENT_AUTOCREATE 无硬依赖：
// 该开关管链路 A 的队列消费，autoattach 走判决直写——两路都开也只是一单。
//
// 语义纪律（W10-6 验收）：
//   - AttachCluster 幂等：重复挂同簇同单不产生副本、不覆盖首挂（Store 双实现
//     契约，见 internal/incident）；
//   - 一簇一事件唯一索引（idx_incident_cluster_unique）：簇已被他单占用
//     （errors.Is(err, incident.ErrClusterTaken)）→ **跳过并计 conflict，
//     不报错不级联**——多指纹共簇仅首单持故障域：后续指纹被聚类器判
//     cluster-merge 并入首簇、不再触发挂点，其自身事件（若由链路 A 建立）
//     无域；处置口径 = 人工 MergeInto 归并到首单（L2 只提示不自动，
//     自动挂簇绝不抢挂/覆盖首挂）。见 docs/M2执行排期 W10-6 注记；
//   - 三出口只计数不抛错：opscopilot_autoattach_total{outcome=
//     attached|conflict|skipped}——skipped 兜住建单/挂簇 IO 异常与幂等键
//     缺失（同时累计 attachFailures + WARNING），单条失败不影响批内后续，
//     更不触碰通知链路。
func (n *NoiseEngine) autoAttachClusters(store incident.Store, audit AuditLog, reqs []autoAttachRequest) {
	if store == nil || len(reqs) == 0 {
		return
	}
	var attached, conflict, skipped int
	for _, r := range reqs {
		ref := promFingerprint(r.labels)
		if ref == "" {
			skipped++
			n.attachFailures.Add(1)
			n.logf("WARNING: autoattach skipped (no labels to derive source_ref; cluster %s fp %s) — cumulative %d",
				r.clusterKey, r.fingerprint, n.attachFailures.Load())
			n.m.CountAutoAttach("skipped")
			continue
		}
		title := strings.TrimSpace(r.summary)
		if title == "" {
			title = "告警新事件 " + r.fingerprint
		}
		inc, _, err := store.UpsertExternal(incident.OriginPrometheus, ref, title, r.severity, autoAttachActor, "{}")
		if err != nil || inc == nil || inc.ID == "" {
			skipped++
			n.attachFailures.Add(1)
			n.logf("WARNING: autoattach create incident failed (skipped, cluster %s): %v — cumulative %d",
				r.clusterKey, err, n.attachFailures.Load())
			n.m.CountAutoAttach("skipped")
			continue
		}
		err = store.AttachCluster(inc.ID, r.clusterKey)
		switch {
		case err == nil:
			attached++
			if audit != nil {
				audit.Append(AuditEntry{
					IncidentID: inc.ID,
					Action:     AuditAttachCluster,
					Actor:      autoAttachActor,
					Detail: map[string]any{
						"cluster_key": r.clusterKey,
						"fingerprint": r.fingerprint,
						"source_ref":  ref,
						"path":        "enforce-new-incident",
					},
				})
			}
		case errors.Is(err, incident.ErrClusterTaken):
			// 共簇冲突：一簇一事件，首单持域，跳过即可（不是故障）。
			conflict++
			n.logf("autoattach: cluster %s already owned by another incident — skipped (one-cluster-one-incident, first ticket holds domain): %v",
				r.clusterKey, err)
		default:
			skipped++
			n.attachFailures.Add(1)
			n.logf("WARNING: autoattach AttachCluster failed (skipped, inc %s cluster %s): %v — cumulative %d",
				inc.ID, r.clusterKey, err, n.attachFailures.Load())
		}
		n.m.CountAutoAttach(autoAttachOutcomeFor(err == nil, err))
	}
	n.logf("autoattach: %d new-incident verdicts (attached %d, conflict %d, skipped %d) — cumulative failures %d",
		len(reqs), attached, conflict, skipped, n.attachFailures.Load())
}

// autoAttachOutcomeFor 把"挂簇结果"映射到 outcome 标签（attached/conflict/
// skipped 固定维度，未知一律归 skipped 兜底——对齐 CountVerdict 的
// "忘了分类宁可进兜底桶"纪律，绝不让维度集合在运行期扩张）。
func autoAttachOutcomeFor(ok bool, err error) string {
	switch {
	case ok:
		return "attached"
	case errors.Is(err, incident.ErrClusterTaken):
		return "conflict"
	default:
		return "skipped"
	}
}

// submitVerdicts 判决落库分发（W6-1，锁外）——优化方案 #8 的入口：
//  1. 先按**判决生成时刻**观测 fired→verdict 延迟（口径见 metrics.go 与
//     docs/W9-4 §9：异步后落库完成时刻移交 writer，继续等它会混入队列
//     积压；分位口径收敛为"判决生成"）；
//  2. 队列在跑 → 非阻塞投递即返回，队满/停机丢持久化不丢通知
//     （sink_drops 计数 + slog ERROR，见 enqueueVerdicts）；
//     队列未启动 → 整批同步落库（#8 之前的原行为）。
//
// 先投递后 Admit 的批内顺序保持不变（ProcessAlerts 调用次序）；Gate
// 不读判决存储（审查结论，docs/W9-4 §9），"入队即完成"不影响放行时序。
//
// fired 与 verdicts 平行等长；某条 fired 为零值（上游没给 startsAt）
// 跳过观测但必须留痕（CountLatencySkipped），否则"processed 多、观测少"
// 会被误读成丢样本。
func (n *NoiseEngine) submitVerdicts(q *verdictWriter, vs noise.VerdictSink,
	verdicts []noise.VerdictRecord, fired []time.Time) {
	for i := range verdicts {
		if i < len(fired) && !fired[i].IsZero() {
			n.m.ObserveVerdictLatency(time.Since(fired[i]))
		} else {
			// 缺发射时刻（上游没给 startsAt）：观测不了但必须留痕——否则
			// "processed 多、观测少"会被误读成丢样本。
			n.m.CountLatencySkipped("verdict")
		}
	}
	if q != nil {
		n.enqueueVerdicts(q, verdicts) // 投递即返回；丢弃自有记账，不回退同步
		return
	}
	n.persistVerdicts(vs, verdicts)
}

// persistVerdicts 同步落库路径（队列未启用，或投递退化时的兜底）。
// 失败只计数+日志——评估数据缺行会让准确率分母偏小，运维通过
// verdictFailures（/metrics 镜像为 opscopilot_noise_write_dropped_total）
// 观察丢失面。延迟打点不在此处（#8 后统一在 submitVerdicts 生成时刻）。
func (n *NoiseEngine) persistVerdicts(vs noise.VerdictSink, verdicts []noise.VerdictRecord) {
	failed := 0
	for _, rec := range verdicts {
		if err := vs.SaveVerdict(rec); err != nil {
			failed++
			n.verdictFailures.Add(1)
		}
	}
	if failed > 0 {
		n.logf("WARNING: verdict persist failed for %d/%d (cumulative %d)",
			failed, len(verdicts), n.verdictFailures.Load())
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
// 收尾做签名缓存的容量 GC（#6）：簇已从 Clusterer 消失（内存淘汰或窗口
// 老化）后其签名永久无用——超上限先清这类孤儿键；活簇的签名不动
// （动了指纹缓存只是多写一次，但留孤儿才是只进不出的真泄漏）。
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
	if excess := n.sigGuard.Over(len(n.persistedSig)); excess > 0 {
		live := make(map[string]struct{}, len(clusters))
		for _, cl := range clusters {
			live[cl.Key] = struct{}{}
		}
		dropped := 0
		for k := range n.persistedSig {
			if dropped >= excess {
				break
			}
			if _, ok := live[k]; !ok {
				delete(n.persistedSig, k)
				dropped++
			}
		}
		n.sigGuard.Evicted(dropped) // dropped=0（签名全对应活簇）时不计数——容量此时由簇上限护栏兜底
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
			n.saveFailures.Add(1)
			n.mu.Lock()
			delete(n.persistedSig, rec.ClusterKey)
			n.mu.Unlock()
		}
	}
	if failed > 0 {
		n.logf("WARNING: cluster persist failed for %d/%d clusters (rolled back signatures, cumulative failures %d)",
			failed, len(dirty), n.saveFailures.Load())
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
