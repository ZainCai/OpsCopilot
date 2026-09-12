// rca_auto.go 升级联动自动 RCA（二期池波二 #4 / ADR-014 二期挂点转正）。
//
// 背景：#12 交付了按需 RCA（GET /api/v1/incidents/{id}/rca），ADR-014
// "本期不做"里明确留桩了"自动触发（Escalation 联动）"。本文件把它落地：
// **critical 事件被值班升级成功催办的时刻**（escalation 已认定"这单没人管
// 且很急"），异步跑一次该事件的 RCA，结论落审计（action='rca'，actor="auto"）
// ——复用 #12 的 RCAOrchestrator 全链路（取证→六步→审计），本文件只做
// 触发编排，不复制任何分析逻辑。
//
// 三条硬纪律（对齐任务口径）：
//  1. **绝不阻塞 escalation**：Trigger 只做内存去重 + 有界队列投递，投递
//     即返回（select/default 满则丢弃计数）；真正的分析在独立 worker
//     goroutine 串行消费，升级扫描循环与此完全解耦。
//  2. **防风暴——同一 incident 只自动触发一次（内存 + PG 双保险）**：
//     - 内存：seen 集合，命中即跳过（覆盖"重复升级同一单/同簇连带多次
//     催办同一单"，进程内去重）；
//     - PG：worker 分析前回读该 incident 的审计——已有 action='rca'
//     actor='auto' 记录则跳过（覆盖"重启后内存 seen 清空"——重启不重跑，
//     与 #12 的 append-only 审计同源于 incident_audit）。
//     同簇连带的多个 incident 各自独立触发一次（按 incidentID 去重，
//     不做簇级级联——一簇烧了多张单，每张单都值得单独归因）。
//  3. **开关 OPS_RCA_AUTO（默认 off）**：见 internal/config；on 须 OPS_RCA=on
//     （Validate 强制），off 时本类型根本不会被装配（零行为）。
package main

import (
	"context"
	"sync"

	"opscopilot/internal/incident"
)

// autoTriggerActor 自动触发写审计的 actor 值（REST 手动请求默认 "system:rca"，
// 查询方 actor 由参数提供；自动链路统一记 "auto"，PG 双保险据此回读判定）。
const autoTriggerActor = "auto"

// defaultRCATriggerQueue 自动触发有界队列容量。自动 RCA 是"升级后补一次
// 归因"的旁路，不是实时链路：队深几个足以吸收偶发的 critical 升级小高峰；
// 超出即丢弃计 opscopilot_rca_autotrigger_dropped_total（运维可事后按需
// GET /rca 补分析），宁可不跑也不堆 goroutine/堆内存。
const defaultRCATriggerQueue = 64

// rcaAnalyzer 自动触发对分析编排器的最小依赖面（*RCAOrchestrator 满足）。
// 抽成接口只为单测注入计数替身——运行时装配仍传真实的同一个 orchestrator
// （复用 #12 全链路，不复制逻辑），零行为差异。
type rcaAnalyzer interface {
	Analyze(ctx context.Context, incidentID, actor string) (*RCAResult, error)
}

// RCATrigger 自动 RCA 触发器：escalation 的 OnEscalate 挂点 → 有界队列 →
// 单 worker 串行消费（回读审计双保险 + 复用 orchestrator 分析）。并发安全：
// Trigger 会被 escalation 循环调用，Run 在独立 goroutine 消费。
type RCATrigger struct {
	orch  rcaAnalyzer
	audit AuditLog
	m     *AppMetrics
	logf  func(string, ...any)

	queue chan string

	mu   sync.Mutex
	seen map[string]struct{} // 内存双保险：本进程内已触发过的 incident 集
}

// NewRCATrigger 构造触发器。orch 必非 nil（装配保证：仅 RCA 启用时才建）；
// audit 可为 nil（嵌入式测试无审计，则 PG 双保险退化为仅内存去重）；
// queueSize<=0 取默认。worker 由 Run 驱动，构造本身不起 goroutine。
func NewRCATrigger(orch rcaAnalyzer, audit AuditLog, m *AppMetrics,
	queueSize int, logf func(string, ...any)) *RCATrigger {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	if queueSize <= 0 {
		queueSize = defaultRCATriggerQueue
	}
	return &RCATrigger{
		orch: orch, audit: audit, m: m, logf: logf,
		queue: make(chan string, queueSize),
		seen:  map[string]struct{}{},
	}
}

// Trigger 非阻塞登记一次自动 RCA 请求（escalation OnEscalate 直接挂本方法）。
// 四道闸门，任何一道命中即"不投递"，全程不查库不 IO：
//  1. 非 critical 事件直接跳过——升级只对超时未 ack 触发，但自动 RCA 只服务
//     最紧急的那一档（critical），避免低级别事件也烧分析；
//  2. 空 ID 忽略；
//  3. 内存 seen 命中 → 已触发过（同簇连带/重复催办）直接跳过；
//  4. 队列满 → 丢弃 + 计数 + WARN 日志（PG 双保险仍会在真正分析前兜住重启重跑）。
//
// 关键：投递成功才置 seen——避免"标记了却没跑成"的永久漏触发。
func (t *RCATrigger) Trigger(inc incident.Incident) {
	if inc.ID == "" || inc.Severity != "critical" {
		return
	}
	t.mu.Lock()
	if _, ok := t.seen[inc.ID]; ok {
		t.mu.Unlock()
		return // 内存双保险：本进程已触发过
	}
	t.mu.Unlock()

	select {
	case t.queue <- inc.ID:
		t.mu.Lock()
		t.seen[inc.ID] = struct{}{}
		t.mu.Unlock()
	default:
		// 队列满：丢弃计数，绝不阻塞 escalation（这是可接受的降级——
		// 丢的是自动归因，不是升级通知本身）。
		t.m.CountRCATriggerDrop()
		t.logf("WARNING: rca auto-trigger queue full, dropped incident %s (opscopilot_rca_autotrigger_dropped_total)", inc.ID)
	}
}

// Run worker 主循环：从队列取事件、回读审计双保险、按需分析。ctx 取消即退。
// 单 goroutine 串行消费——一次分析（含取证）本就重（最长 OPS_RCA_TIMEOUT），
// 并行多路只会加剧下游负载，与"旁路补分析"定位相悖。
func (t *RCATrigger) Run(ctx context.Context) {
	t.logf("rca auto-trigger: worker started (queue %d)", cap(t.queue))
	defer t.logf("rca auto-trigger: worker stopped")
	for {
		select {
		case <-ctx.Done():
			return
		case id := <-t.queue:
			t.process(ctx, id)
		}
	}
}

// process 处理一次触发：PG 双保险回读 → 未跑过才复用 orchestrator 分析。
func (t *RCATrigger) process(ctx context.Context, incidentID string) {
	if t.alreadyAutoAnalyzed(incidentID) {
		return // PG 双保险：重启后内存 seen 没了，审计替它记着
	}
	// 复用 #12 全链路：Analyze 自带 Get incident / 取证 / 六步 / 落审计
	// （actor="auto"）。这里不复制任何分析逻辑，只是换了个触发入口。
	if _, err := t.orch.Analyze(ctx, incidentID, autoTriggerActor); err != nil {
		// 分析失败（事件已被并发关闭/取证故障/超时）只记日志：自动链路
		// 尽力而为，运维仍可按需 GET /rca 重试。不改事件状态、不额外告警。
		t.logf("WARNING: rca auto-trigger analyze %s: %v", incidentID, err)
		return
	}
	t.logf("rca auto-trigger: incident %s analyzed (audit action=rca actor=auto)", incidentID)
}

// alreadyAutoAnalyzed 回读该 incident 的审计，判断是否已有一条自动 RCA 记录
// （重启不重跑的 PG 双保险）。审计后端不可用（List 返错）时**跳过分析**：
// 无法确认状态就不冒险重跑，宁缺毋滥（与升级风暴防护目标同向）。
func (t *RCATrigger) alreadyAutoAnalyzed(incidentID string) bool {
	if t.audit == nil {
		return false // 无审计（嵌入式测试）：退化为仅内存去重
	}
	entries, err := t.audit.List(incidentID)
	if err != nil {
		t.logf("WARNING: rca auto-trigger audit check %s unavailable, skipping (storm-guard): %v", incidentID, err)
		return true
	}
	for _, e := range entries {
		if e.Action == AuditRCA && e.Actor == autoTriggerActor {
			return true
		}
	}
	return false
}
