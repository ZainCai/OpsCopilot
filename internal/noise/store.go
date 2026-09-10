// store.go 簇持久化契约（W4-1.5，ADR-001 配套）。
//
// 分层（ADR-001"加速层非真相源"）：
//   - 内存 Clusterer = 加速层，允许丢；
//   - ClusterRecord = 落库行结构（对齐 alert_cluster 表），幂等 upsert；
//   - RecordSink = 落库出口（cmd/ 装配层接 Redis / TimescaleDB 实现）；
//   - Clusterer.Restore = 重建入口：存储侧记录 → 内存状态，
//     "清空内存/Redis 丢失后从真相源拉平"的唯一路径。
//
// 幂等语义：记录以 ClusterKey 为主键，全量覆盖写（upsert）。重复投递
// 同一份记录结果不变——重放告警流、重放落库序列都收敛到同一状态。
package noise

import (
	"errors"
	"fmt"
	"sort"
	"time"
)

// ClusterRecord 簇落库行（对齐 migrations/000001 alert_cluster 列）：
//   - ClusterKey → cluster_key（UNIQUE(tenant_id, cluster_key) 幂等键）
//   - State → state（open/acked/resolved 封闭集合）
//   - FirstSeen/LastSeen → first_seen_at/last_seen_at
//   - Severity/Summary → severity/summary
//   - Fingerprints/NodeKeys/AlertCount → evidence JSONB（表列 + 扩展状态）
//
// 表里没有 Fingerprints/NodeKeys 独立列：它们进 evidence JSONB，
// 重建（Restore）需要完整的指纹/节点索引，只有簇 key 本身不够。
type ClusterRecord struct {
	TenantID     string       `json:"tenant_id"`
	ClusterKey   string       `json:"cluster_key"`
	State        ClusterState `json:"state"`
	FirstSeen    time.Time    `json:"first_seen_at"`
	LastSeen     time.Time    `json:"last_seen_at"`
	Severity     string       `json:"severity"`
	Summary      string       `json:"summary"`
	AlertCount   int          `json:"alert_count"`
	Fingerprints []string     `json:"fingerprints"`
	NodeKeys     []string     `json:"node_keys"`
}

// ToRecord 导出为落库行。调用方填 TenantID（noise 包不持有租户概念，
// 租户是装配层的配置）。
func (c *Cluster) ToRecord(tenantID string) ClusterRecord {
	rec := ClusterRecord{
		TenantID:     tenantID,
		ClusterKey:   c.Key,
		State:        c.State,
		FirstSeen:    c.FirstSeen,
		LastSeen:     c.LastSeen,
		Severity:     c.Severity,
		Summary:      c.Summary,
		AlertCount:   c.AlertCount,
		Fingerprints: make([]string, 0, len(c.Fingerprints)),
		NodeKeys:     make([]string, 0, len(c.NodeKeys)),
	}
	for fp := range c.Fingerprints {
		rec.Fingerprints = append(rec.Fingerprints, fp)
	}
	for k := range c.NodeKeys {
		rec.NodeKeys = append(rec.NodeKeys, k)
	}
	sort.Strings(rec.Fingerprints)
	sort.Strings(rec.NodeKeys)
	return rec
}

// RecordSink 落库出口（由 cmd/ 装配层实现：Redis 加速层 / DB 真相源）。
// 实现必须满足幂等：同 (TenantID, ClusterKey) 重复 Save 结果不变。
type RecordSink interface {
	SaveCluster(rec ClusterRecord) error
}

// Restore 用落库记录重建内存簇状态，**替换**当前全部状态。
// 这是"Redis/内存丢失后从真相源拉平"的重建入口。
//
// 语义与约束：
//   - 记录的 ClusterKey 原样保留（不重新生成）——重建后继续 Ingest，
//     命中的还是同一个簇 key，幂等链不断；
//   - 未 resolve 的记录重建指纹/节点二级索引（可继续吸收新告警）；
//     resolved 记录只恢复本体（历史查询用，不再参与聚类）；
//   - 同 key 重复记录报错（真相源出现重复主键属于数据事故，静默
//     去重会把事故藏起来）。
func (c *Clusterer) Restore(records []ClusterRecord) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	seen := make(map[string]struct{}, len(records))
	for _, rec := range records {
		if rec.ClusterKey == "" {
			return errors.New("noise: restore record with empty cluster key")
		}
		if _, dup := seen[rec.ClusterKey]; dup {
			return fmt.Errorf("noise: duplicate cluster key %q in restore records", rec.ClusterKey)
		}
		seen[rec.ClusterKey] = struct{}{}
	}
	clusters := make(map[string]*Cluster, len(records))
	resolvedClusters := make(map[string]*Cluster)
	byFingerprint := make(map[string]string)
	byNode := make(map[string]string)
	for _, rec := range records {
		cl := &Cluster{
			Key:          rec.ClusterKey,
			State:        rec.State,
			FirstSeen:    rec.FirstSeen,
			LastSeen:     rec.LastSeen,
			Severity:     rec.Severity,
			Summary:      rec.Summary,
			AlertCount:   rec.AlertCount,
			Fingerprints: make(map[string]struct{}, len(rec.Fingerprints)),
			NodeKeys:     make(map[string]struct{}, len(rec.NodeKeys)),
		}
		if cl.State == "" {
			cl.State = StateOpen
		}
		for _, fp := range rec.Fingerprints {
			if fp == "" {
				continue
			}
			cl.Fingerprints[fp] = struct{}{}
			if cl.State != StateResolved {
				byFingerprint[fp] = cl.Key
			}
		}
		for _, k := range rec.NodeKeys {
			if k == "" {
				continue
			}
			cl.NodeKeys[k] = struct{}{}
			if cl.State != StateResolved {
				byNode[k] = cl.Key
			}
		}
		if cl.State == StateResolved {
			resolvedClusters[cl.Key] = cl
		} else {
			clusters[cl.Key] = cl
		}
	}
	c.active = clusters
	c.resolved = resolvedClusters
	c.byFingerprint = byFingerprint
	c.byNode = byNode
	return nil
}

// VerdictRecord 逐告警影子判决落库行（W6-1，对齐 alert_event 表：
// cluster_key 可空、payload JSONB 承载评估字段）。评估脚本按
// OccurredAt 对照注入器 answerbook 时间线打分——这是 W6-3 的数据源。
type VerdictRecord struct {
	TenantID      string    `json:"tenant_id"`
	Fingerprint   string    `json:"fingerprint"`
	NodeKey       string    `json:"node_key"`
	OccurredAt    time.Time `json:"occurred_at"`
	ClusterKey    string    `json:"cluster_key"`
	Severity      string    `json:"severity"`
	Summary       string    `json:"summary"`
	WouldSuppress bool      `json:"would_suppress"`
	WouldConverge bool      `json:"would_converge"`
	Reason        string    `json:"reason"`
}

// ToRecord 导出为落库行（调用方填租户）。
func (v Verdict) ToRecord(tenantID string) VerdictRecord {
	return VerdictRecord{
		TenantID:      tenantID,
		Fingerprint:   v.Fingerprint,
		NodeKey:       v.NodeKey,
		OccurredAt:    v.OccurredAt,
		ClusterKey:    v.ClusterKey,
		Severity:      v.Severity,
		Summary:       v.Summary,
		WouldSuppress: v.WouldSuppress,
		WouldConverge: v.WouldConverge,
		Reason:        v.Reason,
	}
}

// VerdictSink 逐告警判决落库出口（与 RecordSink 簇快照并存）。
// 实现必须容忍高频调用（每告警一次）且尽力而为——失败由调用方
// 计数，不中断告警链路。
type VerdictSink interface {
	SaveVerdict(rec VerdictRecord) error
}
