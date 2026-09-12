// statemachine.go 状态机单一实现（#5）：转移合法性判定 + 副作用计划，Mem/PG 双 Store 共用。
package incident

import (
	"strings"
	"time"
)

// ---- 状态机单一实现（#5：双 Store 共用的转移判定 + 副作用计划）----
//
// 转移的"合法性判定 + 副作用"只有下面这一份代码。PGStore 的 UPDATE 语句
// 只按 transitionPlan 产出的参数打 CASE（不在 SQL 里藏第二套判定）；
// MemStore 经 applyTransitionPlan 直接落字段。契约测试（store_contract_
// test.go 契约 2/11）锁死两侧可观测结果必须一致。

// transitionPlan 一次合法转移的副作用计划。
type transitionPlan struct {
	To State
	// FlipManualOnly → 转入 acked：人工确认过的单，外部恢复不得自动关闭（R2）。
	FlipManualOnly bool
	// StampResolved → 转入 resolved：resolved_at == updated_at
	//（Mem 同 clock 值；PG 同语句双 now()，同一事务时间戳）。
	StampResolved bool
	// StampAckedAt → 转入 acked：acked_at 首戳（W10-2，MTTA 数据源）。
	// 语义"只落一次"：Mem 侧 AckedAt 零值守卫（下方 applyTransitionPlan），
	// PG 侧 `acked_at IS NULL` 守卫——转移表本就拒绝重复 acked，双保险兜住
	// 并发/后门写入，杜绝戳被后移。新增确定性行为，契约测试硬断言双 Store 一致。
	StampAckedAt bool
	// AckBy → resolved 且 actor 去空白后非空：记录处置人；空串 = 不改写。
	AckBy string
}

// planTransition 判定 from→to 合法性并产出副作用计划；非法返回 ErrInvalidTransition。
func planTransition(from, to State, actor string) (transitionPlan, error) {
	if !CanTransition(from, to) {
		return transitionPlan{}, ErrInvalidTransition{From: from, To: to}
	}
	p := transitionPlan{To: to}
	if to == StateAcked {
		p.FlipManualOnly = true
		p.StampAckedAt = true
	}
	if to == StateResolved {
		p.StampResolved = true
		if strings.TrimSpace(actor) != "" {
			p.AckBy = actor
		}
	}
	return p, nil
}

// applyTransitionPlan 把计划落到实体（MemStore 侧；now 由调用方时钟注入）。
func applyTransitionPlan(inc *Incident, p transitionPlan, now time.Time) {
	inc.State = p.To
	inc.UpdatedAt = now
	if p.FlipManualOnly {
		inc.AutoClosePolicy = "manual_only"
	}
	if p.StampResolved {
		inc.ResolvedAt = now
	}
	if p.StampAckedAt && inc.AckedAt.IsZero() {
		inc.AckedAt = now // 首戳只落一次（见 transitionPlan.StampAckedAt）
	}
	if p.AckBy != "" {
		inc.AckBy = p.AckBy
	}
}

// closeAsMerged 关闭被合并单：resolved + merged_into，resolved_at == updated_at
// （落戳语义与 planTransition 的 StampResolved 同源；PG 侧 MergeInto 用同语句
// now() 双写兑现同一契约）。合并**不走转移表**——已 resolved 的单仍可被合并。
func closeAsMerged(inc *Incident, targetID string, at time.Time) {
	inc.State = StateResolved
	inc.MergedInto = targetID
	inc.ResolvedAt = at
	inc.UpdatedAt = at
}
