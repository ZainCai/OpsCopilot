// W4-1.5 簇落库出口：Redis 加速层实现（ADR-001）。
//
// 分层对齐 ADR-001"加速层非真相源"：
//   - Redis（本文件）承载内存簇状态的实时镜像，进程重启/热路径查询走它；
//   - TimescaleDB alert_cluster 表是真相源，upsert 实现随 W5 引入 pgx
//     后落地（列映射见 internal/noise/store.go ClusterRecord 注释；
//     SQL 形态：INSERT ... ON CONFLICT (tenant_id, cluster_key)
//     DO UPDATE SET state/last_seen/severity/summary/evidence=EXCLUDED...）。
//   - RecordSink 接口两侧通用：DB 实现落地后与本实现并列注入，
//     内存簇状态通过 Clusterer.Restore 从任一真相侧拉平。
//
// 键空间：hash `opscopilot:{tenant}:alert_clusters`，field=cluster_key，
// value=ClusterRecord JSON。选 hash 而非散列 string key：一次 HGETALL
// 拉全租户簇（重建路径），且同租户簇可整体 DEL（ADR-001 演练场景）。
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/redis/go-redis/v9"

	"opscopilot/internal/noise"
)

// redisClusterKey 单租户簇 hash 的键名。
func redisClusterKey(tenantID string) string {
	return "opscopilot:{" + tenantID + "}:alert_clusters"
}

// RedisClusterSink 把簇记录幂等镜像进 Redis（HSET 覆盖写即 upsert）。
type RedisClusterSink struct {
	rdb      *redis.Client
	tenantID string
}

// NewRedisClusterSink 构造。rdb 须非 nil（nil 防线在装配层做——
// 与 sessionstore 不同，本类型只在显式接线时创建）。
func NewRedisClusterSink(rdb *redis.Client, tenantID string) *RedisClusterSink {
	return &RedisClusterSink{rdb: rdb, tenantID: tenantID}
}

// SaveCluster 幂等写入：同 (tenant, cluster_key) 重复保存结果不变。
func (s *RedisClusterSink) SaveCluster(rec noise.ClusterRecord) error {
	if rec.TenantID == "" {
		rec.TenantID = s.tenantID
	}
	if rec.ClusterKey == "" {
		return fmt.Errorf("redis cluster sink: empty cluster key")
	}
	blob, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("redis cluster sink: marshal %s: %w", rec.ClusterKey, err)
	}
	return s.rdb.HSet(context.Background(), redisClusterKey(rec.TenantID), rec.ClusterKey, blob).Err()
}

// LoadClusters 拉取全部簇记录（重建入口的数据源）。
// 返回按 ClusterKey 排序的记录（决定性，便于测试与日志比对）。
func (s *RedisClusterSink) LoadClusters(ctx context.Context) ([]noise.ClusterRecord, error) {
	raw, err := s.rdb.HGetAll(ctx, redisClusterKey(s.tenantID)).Result()
	if err != nil {
		return nil, fmt.Errorf("redis cluster sink: hgetall: %w", err)
	}
	recs := make([]noise.ClusterRecord, 0, len(raw))
	for _, blob := range raw {
		var rec noise.ClusterRecord
		if err := json.Unmarshal([]byte(blob), &rec); err != nil {
			// 单条损坏不拖垮整体：跳过并让调用方在日志里看到条数差。
			continue
		}
		recs = append(recs, rec)
	}
	sort.Slice(recs, func(i, j int) bool { return recs[i].ClusterKey < recs[j].ClusterKey })
	return recs, nil
}
