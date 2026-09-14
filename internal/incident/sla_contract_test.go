// sla_contract_test.go W10-2（F-04）新增确定性行为的双 Store 契约（硬断言）。
//
// 纪律同 store_contract_test.go：能同时硬断言的放 runContract 测试体。
// 本文件覆盖两个新契约（KPI 契约 19 见 kpi_contract_test.go）：
//   - 契约 17：acked_at 首戳只落一次（转移拒绝不移动、跳级保持 NULL、合并不落）；
//   - 契约 18：SetSLA 覆盖持久化（含清除回 0、ErrNotFound、拷贝隔离）。
package incident

import (
	"errors"
	"testing"
)

// ---------------------------------------------------------------------------
// 契约 17：acked_at 首戳（W10-2；planTransition 单点，PG CASE 同语义）
// ---------------------------------------------------------------------------

func TestContractAckedAtFirstStamp(t *testing.T) {
	runContract(t, func(t *testing.T, s Store, px string) {
		inc := contractCreate(t, s, px, "ACKTS", "critical", "ops")

		// 未 acked 前恒零值。
		if got := contractGet(t, s, inc.ID); !got.AckedAt.IsZero() {
			t.Fatalf("acked_at must be zero before ack: %+v", got)
		}

		ret, err := s.Transition(inc.ID, StateAcked, "bob")
		if err != nil {
			t.Fatalf("ack: %v", err)
		}
		// 返回值与落库行一致，且 == updated_at（Mem 同 clock 值；PG 同语句双 now()）。
		if ret.AckedAt.IsZero() || !ret.AckedAt.Equal(ret.UpdatedAt) {
			t.Fatalf("ack stamp mismatch on return: %+v", ret)
		}
		g1 := contractGet(t, s, inc.ID)
		if !g1.AckedAt.Equal(ret.AckedAt) {
			t.Fatalf("acked_at read-back %v != return %v", g1.AckedAt, ret.AckedAt)
		}
		if g1.AckedAt.Before(g1.CreatedAt) {
			t.Fatalf("acked_at %v before created_at %v", g1.AckedAt, g1.CreatedAt)
		}

		// "重复 ack 不移动"：转移表本就拒绝 acked→acked（非法路径零副作用），
		// 首戳守卫（IS NULL / 零值守卫）兜住任何后到写入——戳必须纹丝不动。
		if _, err := s.Transition(inc.ID, StateAcked, "mallory"); err == nil {
			t.Fatal("re-ack accepted")
		}
		if after := contractGet(t, s, inc.ID); !after.AckedAt.Equal(g1.AckedAt) {
			t.Fatalf("rejected re-ack moved acked_at: %v -> %v", g1.AckedAt, after.AckedAt)
		}

		// 后续转移不改写首戳；resolved 落自己的戳。
		if _, err := s.Transition(inc.ID, StateMitigated, "bob"); err != nil {
			t.Fatalf("mitigate: %v", err)
		}
		res, err := s.Transition(inc.ID, StateResolved, "bob")
		if err != nil {
			t.Fatalf("resolve: %v", err)
		}
		if !res.AckedAt.Equal(g1.AckedAt) {
			t.Fatalf("later transitions moved acked_at: %v -> %v", g1.AckedAt, res.AckedAt)
		}
		if res.ResolvedAt.Before(res.AckedAt) {
			t.Fatalf("resolved_at %v before acked_at %v", res.ResolvedAt, res.AckedAt)
		}

		// 跳级单（open→resolved 直达）：acked_at 保持 NULL——MTTA 不发明样本。
		skip := contractCreate(t, s, px, "SKIP", "critical", "ops")
		if _, err := s.Transition(skip.ID, StateResolved, "ops"); err != nil {
			t.Fatalf("resolve skip: %v", err)
		}
		if got := contractGet(t, s, skip.ID); !got.AckedAt.IsZero() || got.ResolvedAt.IsZero() {
			t.Fatalf("skip-ack row wrong: %+v", got)
		}

		// 合并关闭（closeAsMerged）不落 acked_at（无人 ack 过它）。
		src := contractCreate(t, s, px, "MRG", "critical", "ops")
		if err := s.MergeInto(src.ID, skip.ID); err != nil {
			t.Fatalf("merge: %v", err)
		}
		if got := contractGet(t, s, src.ID); !got.AckedAt.IsZero() || got.ResolvedAt.IsZero() {
			t.Fatalf("merged row wrong: %+v", got)
		}
	})
}

// ---------------------------------------------------------------------------
// 契约 18：SetSLA 覆盖持久化
// ---------------------------------------------------------------------------

func TestContractSetSLA(t *testing.T) {
	runContract(t, func(t *testing.T, s Store, px string) {
		inc := contractCreate(t, s, px, "SLA", "critical", "ops")
		if inc.SLAMinutes != 0 {
			t.Fatalf("fresh incident must default to override=0 (按级默认): %+v", inc)
		}
		if err := s.SetSLA(inc.ID, 45); err != nil {
			t.Fatalf("set sla: %v", err)
		}
		g := contractGet(t, s, inc.ID)
		if g.SLAMinutes != 45 {
			t.Fatalf("sla_minutes read-back = %d, want 45", g.SLAMinutes)
		}
		// 覆盖不动状态机语义：state 仍 open、acked_at 仍零、resolved 仍零。
		if g.State != StateOpen || !g.AckedAt.IsZero() || !g.ResolvedAt.IsZero() {
			t.Fatalf("SetSLA must not touch state-machine fields: %+v", g)
		}
		// 拷贝隔离：改返回值不影响库内。
		g.SLAMinutes = 999
		if again := contractGet(t, s, inc.ID); again.SLAMinutes != 45 {
			t.Fatalf("Get return aliases store state: %d", again.SLAMinutes)
		}
		// 0 = 清除覆盖回按级默认；负数拒绝。
		if err := s.SetSLA(inc.ID, 0); err != nil {
			t.Fatalf("clear sla: %v", err)
		}
		if got := contractGet(t, s, inc.ID); got.SLAMinutes != 0 {
			t.Fatalf("sla clear failed: %d", got.SLAMinutes)
		}
		if err := s.SetSLA(inc.ID, -1); err == nil {
			t.Fatal("negative sla accepted")
		}
		if err := s.SetSLA(px+"-MISS", 10); !errors.Is(err, ErrNotFound) {
			t.Fatalf("SetSLA missing must be ErrNotFound, got %v", err)
		}
	})
}

// ---------------------------------------------------------------------------
// 契约 19（P2-D5）：TransitionWithSLA 原子提交
// ---------------------------------------------------------------------------

func TestContractTransitionWithSLA(t *testing.T) {
	runContract(t, func(t *testing.T, s Store, px string) {
		// 带覆盖流转：状态推进 + SLA 同时落库。
		inc := contractCreate(t, s, px, "TSLA", "critical", "ops")
		v45 := 45
		ret, err := s.TransitionWithSLA(inc.ID, StateAcked, "bob", &v45)
		if err != nil {
			t.Fatalf("transition with sla: %v", err)
		}
		if ret.State != StateAcked || ret.SLAMinutes != 45 {
			t.Fatalf("return = %+v, want acked + sla 45", ret)
		}
		g := contractGet(t, s, inc.ID)
		if g.State != StateAcked || g.SLAMinutes != 45 {
			t.Fatalf("read-back = %+v, want acked + sla 45", g)
		}

		// sla=nil 等价 Transition（不动 SLA）。
		plain := contractCreate(t, s, px, "TSLA-NIL", "warning", "ops")
		if _, err := s.TransitionWithSLA(plain.ID, StateAcked, "bob", nil); err != nil {
			t.Fatalf("transition nil sla: %v", err)
		}
		if got := contractGet(t, s, plain.ID); got.State != StateAcked || got.SLAMinutes != 0 {
			t.Fatalf("nil sla must not touch sla: %+v", got)
		}

		// 0 = 清除覆盖回按级默认（随流转原子落）。
		v0 := 0
		if _, err := s.TransitionWithSLA(inc.ID, StateMitigated, "bob", &v0); err != nil {
			t.Fatalf("transition clear sla: %v", err)
		}
		if got := contractGet(t, s, inc.ID); got.State != StateMitigated || got.SLAMinutes != 0 {
			t.Fatalf("clear sla must land with transition: %+v", got)
		}

		// 非法流转 → 整笔回滚：SLA 也不落（原子性的关键断言）。
		bad := contractCreate(t, s, px, "TSLA-BAD", "critical", "ops")
		v99 := 99
		if _, err := s.TransitionWithSLA(bad.ID, StateOpen, "bob", &v99); err == nil {
			t.Fatal("illegal transition accepted")
		}
		if got := contractGet(t, s, bad.ID); got.State != StateOpen || got.SLAMinutes != 0 {
			t.Fatalf("illegal transition must roll back SLA too: %+v", got)
		}

		// 不存在的单 → ErrNotFound（sla 一并拒绝）。
		if _, err := s.TransitionWithSLA(px+"-MISS", StateAcked, "ops", &v45); !errors.Is(err, ErrNotFound) {
			t.Fatalf("TransitionWithSLA missing must be ErrNotFound, got %v", err)
		}
		// 负数拒绝（与 SetSLA 同口径）。
		neg := -1
		if _, err := s.TransitionWithSLA(inc.ID, StateResolved, "bob", &neg); err == nil {
			t.Fatal("negative sla accepted")
		}
	})
}
