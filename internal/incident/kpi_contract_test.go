// kpi_contract_test.go W10-3（F-07）新增确定性行为的双 Store 契约（硬断言，
// 纪律同 store_contract_test.go / sla_contract_test.go）。
//
//	契约 19：KPI 聚合与"从同源行手算"逐字段一致（双 Store），空窗零值 + 样本数。
//
// 隔离手段：severity 用本前缀独占的字符串（Store 层不校验枚举，枚举白名单在
// REST 层）——PG 共享库里其他测试/其他包不会撞上该 severity，聚合数字因此
// 可比。这同时就是"KPI 口径唯一（kpi.go）+ PG SQL 是其翻译"的锁。
package incident

import (
	"math"
	"testing"
	"time"
)

func TestContractKPI(t *testing.T) {
	runContract(t, func(t *testing.T, s Store, px string) {
		sv := "sev-" + px // 本用例独占 severity（PG 共享库隔离手段，见文件头）

		// A：全链（ack+resolve）；B：只 ack；C：跳级直达 resolved；D：留在 open。
		a := contractCreate(t, s, px, "KA", sv, "ops")
		if _, err := s.Transition(a.ID, StateAcked, "ops"); err != nil {
			t.Fatalf("A ack: %v", err)
		}
		if _, err := s.Transition(a.ID, StateMitigated, "ops"); err != nil {
			t.Fatalf("A mitigate: %v", err)
		}
		if _, err := s.Transition(a.ID, StateResolved, "ops"); err != nil {
			t.Fatalf("A resolve: %v", err)
		}
		b := contractCreate(t, s, px, "KB", sv, "ops")
		if _, err := s.Transition(b.ID, StateAcked, "ops"); err != nil {
			t.Fatalf("B ack: %v", err)
		}
		c := contractCreate(t, s, px, "KC", sv, "ops")
		if _, err := s.Transition(c.ID, StateResolved, "ops"); err != nil {
			t.Fatalf("C resolve: %v", err)
		}
		contractCreate(t, s, px, "KD", sv, "ops") // 滞留 open：只进 created 计数
		// 干扰行：不同 severity，绝不得混入本聚合。
		contractCreate(t, s, px, "KE", "info", "ops")

		got, err := s.KPI(time.Time{}, sv)
		if err != nil {
			t.Fatalf("kpi: %v", err)
		}

		// 手算：从**同一批落库行**逐条算（正是 KPI 验收"与手工计算一致"的
		// Store 级形态；REST 级对账见 cmd 层 kpi_rest_test.go）。
		rows, err := s.List("")
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		var want KPIStats
		for _, r := range rows {
			if r.Severity != sv {
				continue
			}
			want.CreatedCount++
			if !r.AckedAt.IsZero() {
				want.AckedSamples++
				want.MTTASumSeconds += r.AckedAt.Sub(r.CreatedAt).Seconds()
			}
			if !r.ResolvedAt.IsZero() {
				want.ResolvedSamples++
				want.ResolvedCount++
				want.MTTRSumSeconds += r.ResolvedAt.Sub(r.CreatedAt).Seconds()
			}
		}
		want.finalize()

		if got.CreatedCount != 4 || want.CreatedCount != 4 {
			t.Fatalf("created_count got=%d manual=%d, want 4", got.CreatedCount, want.CreatedCount)
		}
		if got.AckedSamples != want.AckedSamples || got.ResolvedSamples != want.ResolvedSamples ||
			got.ResolvedCount != want.ResolvedCount {
			t.Fatalf("samples mismatch: got %+v want %+v", got, want)
		}
		if !nearSeconds(got.MTTASeconds, want.MTTASeconds) || !nearSeconds(got.MTTRSeconds, want.MTTRSeconds) {
			t.Fatalf("means mismatch: got MTTA=%v MTTR=%v, manual MTTA=%v MTTR=%v",
				got.MTTASeconds, got.MTTRSeconds, want.MTTASeconds, want.MTTRSeconds)
		}

		// 空窗：零值 + 样本数 0（均值不谎报）。
		future, err := s.KPI(time.Now().Add(time.Hour), sv)
		if err != nil {
			t.Fatalf("kpi future: %v", err)
		}
		if future != (KPIStats{}) {
			t.Fatalf("empty window must be all-zero, got %+v", future)
		}

		// severity 过滤生效：info 干扰行自成一队（弱断言——并发包可能同时写
		// info 行，只查"我们的 sv 队没被算进 ≥4 之外的异常"没有可比基线，
		// 真正的互不污染由上面逐字段对账兑现）。
		other, err := s.KPI(time.Time{}, "info")
		if err != nil {
			t.Fatalf("kpi info: %v", err)
		}
		if other.CreatedCount < 1 {
			t.Fatalf("info cohort must contain our interference row, got %+v", other)
		}
	})
}

// nearSeconds KPI 均值比对容差：浮点求和次序差 + PG epoch 微秒精度，1ms 足够
// （Mem 用例两侧共用同一批 time.Time，理论上精确相等；PG 侧 SUM 行序不定）。
func nearSeconds(a, b float64) bool { return math.Abs(a-b) < 0.001 }
