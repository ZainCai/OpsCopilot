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
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"opscopilot/internal/noise"
)

// pgSinkTimeout 单语句超时。此前 SaveCluster/SaveVerdict 用
// context.Background()（无超时），DB 卡住会挂住降噪落库路径。
const pgSinkTimeout = 5 * time.Second

// PGClusterSink 真相源出口：同时实现 RecordSink（簇 upsert）与
// VerdictSink（判决追加）。
type PGClusterSink struct {
	pool     *pgxpool.Pool
	tenantID string

	// 租户存在性的进程内记忆（tenant 行只建一次，避免每次落库都多一次往返）。
	tenantMu   sync.Mutex
	tenantDone bool
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

// ensureTenant 幂等确保租户行（FK 要求；M1 单租户）。
// 进程内记忆：成功一次后不再重复插（每簇一次纯属多余往返）。
func (s *PGClusterSink) ensureTenant(ctx context.Context) error {
	s.tenantMu.Lock()
	done := s.tenantDone
	s.tenantMu.Unlock()
	if done {
		return nil
	}
	if _, err := s.pool.Exec(ctx,
		`INSERT INTO tenant (id, name) VALUES ($1, $1) ON CONFLICT (id) DO NOTHING`,
		s.tenantID); err != nil {
		return err
	}
	s.tenantMu.Lock()
	s.tenantDone = true
	s.tenantMu.Unlock()
	return nil
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
	ctx, cancel := context.WithTimeout(context.Background(), pgSinkTimeout)
	defer cancel()
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
	ctx, cancel := context.WithTimeout(context.Background(), pgSinkTimeout)
	defer cancel()
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

// LoadClusters 从 PG 真相源拉取全部簇记录（ADR-012 failover 时序 3 的兜底
// 重建入口：Redis 镜像不可用时按本表拉平内存簇，对齐 ADR-001"库才是重建
// 依据"）。列映射是 SaveCluster 的逆运算：evidence JSONB →
// Fingerprints/NodeKeys/AlertCount。单条 evidence 损坏跳过不拖垮整体（与
// RedisClusterSink.LoadClusters 同款口径）。按 cluster_key 排序（决定性）。
func (s *PGClusterSink) LoadClusters(ctx context.Context) ([]noise.ClusterRecord, error) {
	return loadAlertClustersFromPG(ctx, s.pool, s.tenantID)
}

// loadAlertClustersFromPG 独立成函数（不依赖 sink 构造）：leader OnPromote
// 钩子直接拿共享池读——PGClusterSink 建立失败（DSN 坏了）不该把选举也拖死，
// 选举本身只用同池的 advisory lock，池活着就够。
func loadAlertClustersFromPG(ctx context.Context, pool *pgxpool.Pool, tenantID string) ([]noise.ClusterRecord, error) {
	if pool == nil {
		return nil, errors.New("load clusters: no pool (DB not configured)")
	}
	rows, err := pool.Query(ctx, `
SELECT cluster_key, state, first_seen_at, last_seen_at,
       COALESCE(severity, ''), COALESCE(summary, ''), evidence
FROM alert_cluster WHERE tenant_id = $1 ORDER BY cluster_key`, tenantID)
	if err != nil {
		return nil, fmt.Errorf("load clusters: query alert_cluster: %w", err)
	}
	defer rows.Close()
	var recs []noise.ClusterRecord
	for rows.Next() {
		var rec noise.ClusterRecord
		var evidence []byte
		if err := rows.Scan(&rec.ClusterKey, &rec.State, &rec.FirstSeen, &rec.LastSeen,
			&rec.Severity, &rec.Summary, &evidence); err != nil {
			return nil, fmt.Errorf("load clusters: scan: %w", err)
		}
		rec.TenantID = tenantID
		var ev struct {
			Fingerprints []string `json:"fingerprints"`
			NodeKeys     []string `json:"node_keys"`
			AlertCount   int      `json:"alert_count"`
		}
		if err := json.Unmarshal(evidence, &ev); err != nil {
			continue // 损坏行跳过：宁可少一簇（该簇按新簇重判）也不半途而废
		}
		rec.Fingerprints = ev.Fingerprints
		rec.NodeKeys = ev.NodeKeys
		rec.AlertCount = ev.AlertCount
		recs = append(recs, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("load clusters: rows: %w", err)
	}
	return recs, nil
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
