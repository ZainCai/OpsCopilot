// kpi.go 运维 KPI 聚合口径（W10-3 F-07）：双 Store 共用的数据结构与
// MemStore 侧计算。PGStore 用聚合 SQL（见 pg_kpi.go），MemStore 线性扫描
// 同一批字段——**口径只在本文件定义一次**，SQL 是其翻译，两处不漂移由
// 契约测试（TestContractKPI）锁死。
//
// 数据源 = incident 表本身（KPI 是聚合问题，走聚合 SQL；时间线端点是
// "这一单的叙事"三源汇合，是展示源不是计算源——排期 W10-3 验收"与手工
// 按时间线计算一致"指的是与同一批事件的 (created/acked/resolved) 时间戳
// 手算一致，见 cmd 层对账测试 TestKPIEndpointMatchesManualReckoning）。
package incident

import "time"

// KPIStats 窗口聚合结果。窗口口径（进 README/REST 注释）：
//   - 队列（cohort）= created_at >= since 的事件（severity 过滤可选、精确匹配）；
//   - MTTA = avg(acked_at − created_at)，仅 acked_at 非 NULL 样本（acked_samples）；
//   - MTTR = avg(resolved_at − created_at)，仅 resolved_at 非 NULL 样本
//     （resolved_samples，含被合并单——合并也落 resolved_at）；
//   - **平均闭环时长 = MTTR 同口径**（resolved − created），合并为一个字段
//     报告，不做两个数字的伪双口径；
//   - 吞吐计数：created_count = 队列大小；resolved_count = 队列中已闭环数。
//
// 空窗返回零值 + 样本数 0（均值 0 不谎报——samples=0 即"无数据"，消费方
// 按 samples 判空，不按均值判零）。
type KPIStats struct {
	CreatedCount    int64   `json:"created_count"`
	ResolvedCount   int64   `json:"resolved_count"`
	AckedSamples    int64   `json:"acked_samples"`
	ResolvedSamples int64   `json:"resolved_samples"`
	MTTASeconds     float64 `json:"mtta_seconds"`
	MTTRSeconds     float64 `json:"mttr_seconds"`
	MTTASumSeconds  float64 `json:"-"` // 中间量（手算对账/调试用），不上 JSON
	MTTRSumSeconds  float64 `json:"-"`
}

// finalize 用累加量算均值（sum/count；count=0 ⇒ 均值 0）。
func (k *KPIStats) finalize() {
	if k.AckedSamples > 0 {
		k.MTTASeconds = k.MTTASumSeconds / float64(k.AckedSamples)
	}
	if k.ResolvedSamples > 0 {
		k.MTTRSeconds = k.MTTRSumSeconds / float64(k.ResolvedSamples)
	}
}

// kpiMatch 队列过滤（Mem/PG 同一判据的 Go 表达）：created_at 入窗 +
// severity 精确匹配（空串 = 不过滤）。
func kpiMatch(inc *Incident, since time.Time, severity string) bool {
	if inc.CreatedAt.Before(since) {
		return false
	}
	if severity != "" && inc.Severity != severity {
		return false
	}
	return true
}

// kpiAccumulate 把一条事件计入聚合（与 PG SQL 的 FILTER 分支逐一对应）。
func kpiAccumulate(k *KPIStats, inc *Incident) {
	k.CreatedCount++
	if !inc.AckedAt.IsZero() {
		k.AckedSamples++
		k.MTTASumSeconds += inc.AckedAt.Sub(inc.CreatedAt).Seconds()
	}
	if !inc.ResolvedAt.IsZero() {
		k.ResolvedSamples++
		k.ResolvedCount++
		k.MTTRSumSeconds += inc.ResolvedAt.Sub(inc.CreatedAt).Seconds()
	}
}
