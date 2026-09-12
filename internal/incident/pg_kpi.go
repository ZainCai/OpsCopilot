// pg_kpi.go PGStore 的 SetSLA（SLA 覆盖落库）与 KPI 聚合（W10-3 F-07）。
// KPI SQL 是 kpi.go 口径的逐字段翻译——Mem/PG 一致性由契约测试
// TestContractKPI 锁死；"数据源=时间线"验收口径的对账在 cmd 层
// （TestKPIEndpointMatchesManualReckoning，用同源时间戳手算）。
package incident

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// SetSLA 覆盖 SLA 目标时长（分钟；0 = 清除覆盖回按级默认）。只写
// sla_minutes + updated_at，不动状态机字段。行不存在 → ErrNotFound。
func (s *PGStore) SetSLA(id string, minutes int) error {
	if minutes < 0 {
		return errors.New("incident: sla minutes must be >= 0")
	}
	ctx, cancel := s.ctx()
	defer cancel()
	tag, err := s.pool.Exec(ctx, `
UPDATE incident SET sla_minutes=$3, updated_at=now()
WHERE tenant_id=$1 AND incident_id=$2`, s.tenantID, id, minutes)
	if err != nil {
		return fmt.Errorf("incident pg: set sla: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// KPI 聚合 SQL（一条语句，无分页——控制台 KPI 行读它，写少读多）。
// FILTER 子句逐一对应 kpi.go kpiAccumulate 的分支；均值不在 SQL 里算——
// 取回 sum/count 后走同一个 finalize()（Mem/PG 共用函数），杜绝"SQL 的
// AVG 与 Go 的除法"成为第二套口径。空窗：count=0、SUM NULL→COALESCE 0，
// 均值 0 且 samples=0（空窗返回零值 + 样本数，语义不变）。
//
// 时间差用 EXTRACT(EPOCH FROM ...) 秒级浮点，与 Mem 侧
// time.Time.Sub().Seconds() 同精度域（微秒时钟差由测试容差吸收）。
func (s *PGStore) KPI(since time.Time, severity string) (KPIStats, error) {
	ctx, cancel := s.ctx()
	defer cancel()
	var k KPIStats
	err := s.pool.QueryRow(ctx, `
SELECT count(*),
       count(*) FILTER (WHERE acked_at IS NOT NULL),
       COALESCE(SUM(EXTRACT(EPOCH FROM (acked_at - created_at)))
                FILTER (WHERE acked_at IS NOT NULL), 0),
       count(*) FILTER (WHERE resolved_at IS NOT NULL),
       COALESCE(SUM(EXTRACT(EPOCH FROM (resolved_at - created_at)))
                FILTER (WHERE resolved_at IS NOT NULL), 0)
FROM incident
WHERE tenant_id=$1 AND created_at >= $2 AND ($3 = '' OR severity = $3)`,
		s.tenantID, since, strings.TrimSpace(severity)).
		Scan(&k.CreatedCount, &k.AckedSamples, &k.MTTASumSeconds,
			&k.ResolvedSamples, &k.MTTRSumSeconds)
	if err != nil {
		return KPIStats{}, fmt.Errorf("incident pg: kpi: %w", err)
	}
	k.ResolvedCount = k.ResolvedSamples // 同判据（resolved_at 非 NULL 即闭环，含被合并单）
	k.finalize()                        // 权威均值：与 Mem 侧同一函数重算（sum/count）
	return k, nil
}
