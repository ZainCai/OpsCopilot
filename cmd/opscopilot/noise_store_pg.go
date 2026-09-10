// W6-1 簇与判决的 TimescaleDB 真相源出口（ADR-001：Redis 只是加速层，
// 库才是"清空后可重建"的依据）。
//
// 表对齐 migrations/000001 + 000003：
//   - alert_cluster：UNIQUE(tenant_id, cluster_key) 幂等 upsert，
//     Fingerprints/NodeKeys/AlertCount 进 evidence JSONB（与
//     ClusterRecord 注释的列映射一致）；
//   - alert_event（hypertable）：逐告警 Verdict 追加，评估字段进
//     payload JSONB，cluster_key 可空。
//
// 租户：alert_cluster.tenant_id 有 FK，写簇前先确保 tenant 行存在
// （幂等 INSERT ON CONFLICT DO NOTHING）。
package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"opscopilot/internal/noise"
)

// PGClusterSink 真相源出口：同时实现 RecordSink（簇 upsert）与
// VerdictSink（判决追加）。
type PGClusterSink struct {
	pool     *pgxpool.Pool
	tenantID string
}

// NewPGClusterSink 建连接池（默认 MaxConns=2：单进程写路径足够）。
func NewPGClusterSink(ctx context.Context, dsn, tenantID string) (*PGClusterSink, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("pg sink: %w", err)
	}
	return &PGClusterSink{pool: pool, tenantID: tenantID}, nil
}

// Close 释放连接池（main 停机时调用）。
func (s *PGClusterSink) Close() { s.pool.Close() }

// ensureTenant 幂等确保租户行（FK 要求；M1 单租户，仅首次插入生效）。
func (s *PGClusterSink) ensureTenant(ctx context.Context) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO tenant (id, name) VALUES ($1, $1) ON CONFLICT (id) DO NOTHING`,
		s.tenantID)
	return err
}

// SaveCluster 幂等 upsert：UNIQUE(tenant_id, cluster_key) 冲突时覆盖
// 状态列与 evidence——与 Redis 镜像同一语义（重复保存结果不变）。
func (s *PGClusterSink) SaveCluster(rec noise.ClusterRecord) error {
	if rec.TenantID == "" {
		rec.TenantID = s.tenantID
	}
	if rec.ClusterKey == "" {
		return fmt.Errorf("pg sink: empty cluster key")
	}
	ctx := context.Background()
	if err := s.ensureTenant(ctx); err != nil {
		return fmt.Errorf("pg sink: ensure tenant: %w", err)
	}
	evidence := map[string]any{
		"fingerprints": rec.Fingerprints,
		"node_keys":    rec.NodeKeys,
		"alert_count":  rec.AlertCount,
	}
	blob, err := json.Marshal(evidence)
	if err != nil {
		return fmt.Errorf("pg sink: marshal evidence: %w", err)
	}
	_, err = s.pool.Exec(ctx, `
INSERT INTO alert_cluster
  (tenant_id, cluster_key, first_seen_at, last_seen_at, state, severity, summary, evidence)
VALUES ($1, $2, $3, $4, $5, NULLIF($6,''), NULLIF($7,''), $8::jsonb)
ON CONFLICT (tenant_id, cluster_key) DO UPDATE SET
  last_seen_at = EXCLUDED.last_seen_at,
  state        = EXCLUDED.state,
  severity     = EXCLUDED.severity,
  summary      = EXCLUDED.summary,
  evidence     = EXCLUDED.evidence`,
		rec.TenantID, rec.ClusterKey, rec.FirstSeen, rec.LastSeen,
		string(rec.State), rec.Severity, rec.Summary, string(blob))
	if err != nil {
		return fmt.Errorf("pg sink: upsert cluster %s: %w", rec.ClusterKey, err)
	}
	return nil
}

// SaveVerdict 逐告警判决追加（alert_event hypertable）。
// 幂等说明：alert_event 无业务幂等键（自增 id），重复投递会产生重复行
// ——调用方保证每条告警只投一次（ProcessAlerts 每告警恰好一条判决）。
func (s *PGClusterSink) SaveVerdict(rec noise.VerdictRecord) error {
	if rec.TenantID == "" {
		rec.TenantID = s.tenantID
	}
	ctx := context.Background()
	payload, err := json.Marshal(map[string]any{
		"node_key":       rec.NodeKey,
		"severity":       rec.Severity,
		"summary":        rec.Summary,
		"would_suppress": rec.WouldSuppress,
		"would_converge": rec.WouldConverge,
		"reason":         rec.Reason,
	})
	if err != nil {
		return fmt.Errorf("pg sink: marshal payload: %w", err)
	}
	_, err = s.pool.Exec(ctx, `
INSERT INTO alert_event (tenant_id, cluster_key, fingerprint, source, occurred_at, payload)
VALUES ($1, NULLIF($2,''), $3, 'shadow', $4, $5::jsonb)`,
		rec.TenantID, rec.ClusterKey, rec.Fingerprint, rec.OccurredAt, string(payload))
	if err != nil {
		return fmt.Errorf("pg sink: insert verdict %s: %w", rec.Fingerprint, err)
	}
	return nil
}

// multiRecordSink 组合多个簇出口（Redis 镜像 + DB 真相源双写）。
// 两边都成功才算成功；一侧失败记另一侧成功——调用方（NoiseEngine）
// 的签名回滚只看整体结果，失败侧由下一批重写补齐。
type multiRecordSink struct {
	a, b noise.RecordSink
}

func (m *multiRecordSink) SaveCluster(rec noise.ClusterRecord) error {
	errA := m.a.SaveCluster(rec)
	errB := m.b.SaveCluster(rec)
	if errA != nil {
		return errA
	}
	return errB
}
