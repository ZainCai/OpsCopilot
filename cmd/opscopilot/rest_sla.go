// rest_sla.go W10-2（F-04）SLA 时钟的**只读派生视图**。
//
// 口径（对齐排期"M2 只记录与展示、不自动升级"——escalation 链路零改动）：
//   - 有效目标时长 = 事件覆盖 sla_minutes（>0）优先，否则按 severity 取
//     config 默认（OPS_SLA_CRITICAL/WARNING/INFO_MINUTES，装配层注入
//     RESTLimits）；severity 白名单外按 info 档兜底（与建单入口"空 →
//     info"的收口同向，绝不因未知级别给出 0 目标）；
//   - sla_deadline = created_at + 有效目标；
//   - 参考时刻：未闭环单按 now；**终态（resolved，含被合并单——合并同样
//     落 resolved_at）冻结在 resolved_at 判定**，闭环的历史单不再随墙钟漂
//     红；mitigated 不是终态（处置未完成，时钟继续走）；
//   - sla_remaining_seconds = deadline − 参考时刻（秒，可为负=已超时的量）；
//   - sla_breached = 参考时刻晚于 deadline。
//
// 为什么"只读派生、不落冗余列"：deadline 的真相只有
// (created_at, severity, sla_minutes, 默认值) 四元组，物化 deadline 会制造
// 第二真相源（改默认值/改级后旧列不自动跟进 = 漂移）；M2 读量是控制台
// 页面级，写少读多的重算成本可忽略。迁移头注释同口径。
//
// 为什么算在 cmd 层：默认值来自 internal/config，而 internal/incident 禁
// import config（边界纪律）——派生视图天然是装配层的读侧组合。
package main

import (
	"time"

	"opscopilot/internal/incident"
)

// slaMinutesMax 单事件 SLA 覆盖的防御性上限（100 年的分钟数）：DB 列是
// int32，JSON 里塞天文数字必须在入口拦住（pgx 溢出报错变 500 太难排查）。
const slaMinutesMax = 52_560_000

// slaView GET 响应的 SLA 派生字段（内嵌进 incidentView，JSON 平铺）。
type slaView struct {
	SLADeadline         time.Time `json:"sla_deadline"`
	SLARemainingSeconds int64     `json:"sla_remaining_seconds"`
	SLABreached         bool      `json:"sla_breached"`
}

// incidentView 事件读视图 = Store 实体原样 + SLA 派生三字段。
// 嵌入（而非拷贝字段表）保证 DTO 加列时视图零维护；拷贝隔离：值嵌入，
// 不共享指针，调用方改视图不影响任何缓存对象。
type incidentView struct {
	incident.Incident
	slaView
}

// slaTargetMinutes 有效目标时长（覆盖优先，否则按级默认）。
func (g *RESTGateway) slaTargetMinutes(inc incident.Incident) time.Duration {
	if inc.SLAMinutes > 0 {
		return time.Duration(inc.SLAMinutes) * time.Minute
	}
	switch inc.Severity {
	case "critical":
		return g.limits.SLACritical
	case "warning":
		return g.limits.SLAWarning
	default: // info 与白名单外兜底（见文件头）
		return g.limits.SLAInfo
	}
}

// slaViewOf 计算一条事件的派生视图字段。now 由调用方注入（测试可控）。
func (g *RESTGateway) slaViewOf(inc incident.Incident, now time.Time) slaView {
	deadline := inc.CreatedAt.Add(g.slaTargetMinutes(inc))
	ref := now
	if inc.State == incident.StateResolved && !inc.ResolvedAt.IsZero() {
		ref = inc.ResolvedAt // 终态冻结（含被合并单）
	}
	d := deadline.Sub(ref)
	return slaView{
		SLADeadline:         deadline,
		SLARemainingSeconds: int64(d.Seconds()),
		SLABreached:         ref.After(deadline),
	}
}

// incidentViews 批量装配读视图（列表响应；拷贝语义，绝不回写 Store 对象）。
func (g *RESTGateway) incidentViews(list []incident.Incident, now time.Time) []incidentView {
	out := make([]incidentView, len(list))
	for i, inc := range list {
		out[i] = incidentView{Incident: inc, slaView: g.slaViewOf(inc, now)}
	}
	return out
}
