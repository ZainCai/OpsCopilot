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
	"time"

	"github.com/redis/go-redis/v9"

	"opscopilot/internal/noise"
)

// Redis 超时（W9-5，第八轮审核建议 11）。
//
// 为什么必须显式给：RedisClusterSink 的写路径在**告警链路的同步路径**上
// （persistClusters/persistVerdicts 在引擎锁外同步调用）。此前写路径用
// context.Background()，只有 go-redis 客户端的默认 IO 超时兜底——若 Redis
// 是"连得上但不响应"（防火墙 DROP、实例假死），默认 MaxRetries 下的多次
// 重试会把一条告警的处理时间拉长到十几秒量级，直接吃掉 E2E 延迟预算
// （W9-4 实测处理段 P95 仅 0.54s）。
//
// 用 ctx deadline 而不是只靠客户端读写超时：ctx 是**整个操作**（含重试）的
// 硬上界，客户端超时只是单次 round-trip 的上界。
//
// **前置条件（实测踩到）**：go-redis v9 的 `Options.ContextTimeoutEnabled`
// 默认为 false，此时 `baseClient.context()` 会把命令收到的 ctx 直接换成
// `context.Background()`——ctx 上界被静默丢弃，"补超时"变成假修复（回归测试
// TestRedisSinkWriteTimeoutBounded 就是这么发现的：3s 客户端 ReadTimeout
// 生效，而 300ms ctx deadline 毫无作用）。装配侧必须同时设
// ContextTimeoutEnabled: true（见 main.go）。
//
// 取值 3s 的依据：Redis 在 ADR-001 下是"加速层、非真相源"，真相源是
// TimescaleDB——镜像写失败只降级查询速度，不该拖慢告警链路。ADR-001 的
// 原则落到超时上就是"宁可快速失败"。
const (
	redisDialTimeout = 3 * time.Second
	redisIOTimeout   = 3 * time.Second
)

// redisClusterKey 单租户簇 hash 的键名。
func redisClusterKey(tenantID string) string {
	return "opscopilot:{" + tenantID + "}:alert_clusters"
}

// RedisClusterSink 把簇记录幂等镜像进 Redis（HSET 覆盖写即 upsert）。
type RedisClusterSink struct {
	rdb      *redis.Client
	tenantID string
	// timeout 单次操作的硬上界（含客户端重试）。构造时取 redisIOTimeout；
	// 包内测试可覆盖成更短的值来验证"挂死的 Redis 不会挂死告警链路"。
	timeout time.Duration
}

// NewRedisClusterSink 构造。rdb 须非 nil（nil 防线在装配层做——
// 与 sessionstore 不同，本类型只在显式接线时创建）。
func NewRedisClusterSink(rdb *redis.Client, tenantID string) *RedisClusterSink {
	return &RedisClusterSink{rdb: rdb, tenantID: tenantID, timeout: redisIOTimeout}
}

// opCtx 为一次 Redis 操作派生带上界的上下文（见文件头 redisIOTimeout）。
func (s *RedisClusterSink) opCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), s.timeout)
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
	ctx, cancel := s.opCtx()
	defer cancel()
	return s.rdb.HSet(ctx, redisClusterKey(rec.TenantID), rec.ClusterKey, blob).Err()
}

// LoadClusters 拉取全部簇记录（重建入口的数据源）。
// 返回按 ClusterKey 排序的记录（决定性，便于测试与日志比对）。
//
// W9-5：调用方的 ctx 再包一层 opCtx 上界——启动期调用传的是
// context.Background()（无 deadline），只靠客户端超时不足以保证不挂起。
func (s *RedisClusterSink) LoadClusters(ctx context.Context) ([]noise.ClusterRecord, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
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

// verdictMaxEntries Redis 判决流上限（7 天评估期 × 告警量级的安全阀）。
const verdictMaxEntries = 200000

// SaveVerdict 逐告警判决追加（W6-1）：RPUSH JSON 线 + LTRIM 上限。
// 评估脚本 LRANGE 全量读取；单条损坏不拖垮整体（JSON 线按行解析）。
func (s *RedisClusterSink) SaveVerdict(rec noise.VerdictRecord) error {
	if rec.TenantID == "" {
		rec.TenantID = s.tenantID
	}
	blob, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("redis verdict sink: marshal: %w", err)
	}
	key := "opscopilot:{" + s.tenantID + "}:verdicts"
	ctx, cancel := s.opCtx()
	defer cancel()
	pipe := s.rdb.Pipeline()
	pipe.RPush(ctx, key, blob)
	pipe.LTrim(ctx, key, -verdictMaxEntries, -1)
	_, err = pipe.Exec(ctx)
	return err
}
