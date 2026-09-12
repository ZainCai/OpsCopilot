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
