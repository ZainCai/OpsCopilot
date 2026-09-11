// W4-1.5 落库与重建测试：round-trip + 清 Redis 重建演练（miniredis）。
package main

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"opscopilot/internal/connector"
	"opscopilot/internal/noise"
)

// newTestRedis 起 miniredis 并返回客户端与实例。
func newTestRedis(t *testing.T) (*redis.Client, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb, mr
}

func TestRedisSinkSaveAndLoadRoundTrip(t *testing.T) {
	rdb, _ := newTestRedis(t)
	sink := NewRedisClusterSink(rdb, "default")

	rec := noise.ClusterRecord{
		TenantID:     "default",
		ClusterKey:   "c:fp1@0",
		State:        noise.StateOpen,
		FirstSeen:    time.Now(),
		LastSeen:     time.Now(),
		Severity:     "critical",
		Summary:      "HighCPU",
		AlertCount:   3,
		Fingerprints: []string{"fp1", "fp2"},
		NodeKeys:     []string{"n1", "n2"},
	}
	if err := sink.SaveCluster(rec); err != nil {
		t.Fatalf("SaveCluster: %v", err)
	}
	// 幂等：重复保存不报错，加载结果一致。
	if err := sink.SaveCluster(rec); err != nil {
		t.Fatalf("SaveCluster#2: %v", err)
	}
	got, err := sink.LoadClusters(context.Background())
	if err != nil {
		t.Fatalf("LoadClusters: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("loaded %d records, want 1", len(got))
	}
	g := got[0]
	if g.ClusterKey != rec.ClusterKey || g.Severity != "critical" || g.AlertCount != 3 ||
		len(g.Fingerprints) != 2 || len(g.NodeKeys) != 2 {
		t.Fatalf("round-trip mismatch: %+v", g)
	}
}

func TestRedisSinkRejectsEmptyKey(t *testing.T) {
	rdb, _ := newTestRedis(t)
	sink := NewRedisClusterSink(rdb, "default")
	if err := sink.SaveCluster(noise.ClusterRecord{TenantID: "default"}); err == nil {
		t.Fatal("empty cluster key must be rejected")
	}
}

func TestRedisSinkLoadDeterministicOrder(t *testing.T) {
	rdb, _ := newTestRedis(t)
	sink := NewRedisClusterSink(rdb, "default")
	ctx := context.Background()
	_ = sink.SaveCluster(noise.ClusterRecord{ClusterKey: "c:b@1"})
	_ = sink.SaveCluster(noise.ClusterRecord{ClusterKey: "c:a@1"})
	_ = sink.SaveCluster(noise.ClusterRecord{ClusterKey: "c:c@1"})
	got, err := sink.LoadClusters(ctx)
	if err != nil {
		t.Fatalf("LoadClusters: %v", err)
	}
	if len(got) != 3 || got[0].ClusterKey != "c:a@1" || got[2].ClusterKey != "c:c@1" {
		t.Fatalf("records not sorted by key: %v", got)
	}
}

// TestClearMemoryAndRebuild 演练（ADR-001 验收）：内存簇状态清空 →
// 从落库记录重建 → 继续喂告警命中同一批 cluster_key。
// 对应生产场景"清 Redis 后从真相源拉平"，本测试用 miniredis 扮演存储侧。
func TestClearMemoryAndRebuild(t *testing.T) {
	sink := noiseTestSink(t)
	engine := newTestNoiseEngine(t, sink)
	rdb, _ := newTestRedis(t)
	engine.SetRecordSink(NewRedisClusterSink(rdb, "default"))

	now := time.Now()
	// 第一轮：两条告警经因果边并成一簇。
	engine.ProcessAlerts([]connector.Alert{
		{Fingerprint: "fpA", Labels: map[string]string{"instance": "i1", "alertname": "HighCPU"}, Severity: "warning", StartsAt: now},
		{Fingerprint: "fpB", Labels: map[string]string{"instance": "i2", "alertname": "HighLoad"}, Severity: "critical", StartsAt: now.Add(time.Second)},
	})
	before := engine.shadow.Clusterer().Clusters()
	if len(before) != 1 {
		t.Fatalf("pre-rebuild cluster count = %d, want 1", len(before))
	}

	// ---- 模拟"清 Redis / 进程重启"：全新引擎（内存为空），从存储侧拉平 ----
	engine2 := newTestNoiseEngine(t, sink)
	records, err := NewRedisClusterSink(rdb, "default").LoadClusters(context.Background())
	if err != nil {
		t.Fatalf("load for rebuild: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("persisted %d records, want 1 (双写管道失效)", len(records))
	}
	if err := engine2.RestoreFrom(records); err != nil {
		t.Fatalf("RestoreFrom: %v", err)
	}

	// 重建后继续喂同一指纹：必须续接原簇（key 不变、不新建）。
	v := engine2.shadow.Process(noise.Event{
		Fingerprint: "fpA", NodeKey: "prom://n1", OccurredAt: now.Add(2 * time.Second),
	})
	if v.ClusterCreated || v.ClusterKey != before[0].Key {
		t.Fatalf("rebuild broke idempotent chain: created=%v key=%s want %s",
			v.ClusterCreated, v.ClusterKey, before[0].Key)
	}
	after := engine2.shadow.Clusterer().Clusters()
	if len(after) != 1 || after[0].AlertCount != 3 {
		t.Fatalf("post-rebuild cluster: count=%d alertCount=%d, want 1/3", len(after), len(after[0:1]))
	}
}

// TestRebuildInvalidRecordsRejected 重建的防线：空 key / 重复 key 拒绝，
// 且失败后内存状态不变。
func TestRebuildInvalidRecordsRejected(t *testing.T) {
	engine := newTestNoiseEngine(t, noiseTestSink(t))
	// 先留一个正常状态。
	engine.shadow.Process(noise.Event{Fingerprint: "fpKeep", OccurredAt: time.Now()})

	if err := engine.RestoreFrom([]noise.ClusterRecord{{ClusterKey: ""}}); err == nil {
		t.Fatal("empty cluster key must be rejected")
	}
	dup := noise.ClusterRecord{ClusterKey: "c:x@0", State: noise.StateOpen}
	if err := engine.RestoreFrom([]noise.ClusterRecord{dup, dup}); err == nil {
		t.Fatal("duplicate cluster keys must be rejected (数据事故不许静默去重)")
	}
	// 失败后内存状态未被动过。
	clusters := engine.shadow.Clusterer().Clusters()
	if len(clusters) != 1 || clusters[0].Fingerprints == nil || len(clusters[0].Fingerprints) != 1 {
		t.Fatalf("failed restore must leave state untouched, got %+v", clusters)
	}
}

func TestPersistFailureDoesNotBreakAlertPath(t *testing.T) {
	engine := newTestNoiseEngine(t, noiseTestSink(t))
	engine.SetRecordSink(failSink{})
	// 落库全失败：处理照常，统计照常，不 panic。
	engine.ProcessAlerts([]connector.Alert{
		{Fingerprint: "fp1", Labels: map[string]string{"instance": "i1"}, StartsAt: time.Now()},
	})
	total, converged := engine.Stats()
	if total != 1 || converged != 0 {
		t.Fatalf("alert path broken by persist failure: total=%d converged=%d", total, converged)
	}
}

type failSink struct{}

func (failSink) SaveCluster(noise.ClusterRecord) error { return errPersistFailed }

var errPersistFailed = &persistError{}

type persistError struct{}

func (*persistError) Error() string { return "persist unavailable" }

// TestRedisSinkWriteTimeoutBounded W9-5（第八轮审核建议 11）：写路径必须有
// 硬上界——挂在"接受连接但永不响应"的假 Redis 上，SaveCluster 必须在
// sink 超时（包内可覆盖，这里设 300ms）附近返回，而不是无限等待。
//
// 动机：写路径在**告警链路的同步路径**上（persistClusters 在引擎锁外同步
// 调用）。此前只有 go-redis 客户端默认 IO 超时兜底，若 Redis 是"连得上但
// 不响应"（防火墙 DROP / 实例假死），多次重试会把单条告警的处理时间拉长
// 到十几秒，直接吃掉 E2E 延迟预算（W9-4 实测处理段 P95 仅 0.54s）。
func TestRedisSinkWriteTimeoutBounded(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	// 接受连接、读走请求、但永不回复——模拟假死 Redis。
	done := make(chan struct{})
	defer close(done)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				buf := make([]byte, 512)
				for {
					if _, err := conn.Read(buf); err != nil {
						_ = conn.Close()
						return
					}
					select {
					case <-done:
						_ = conn.Close()
						return
					default:
					}
				}
			}(c)
		}
	}()

	rdb := redis.NewClient(&redis.Options{
		Addr:                  ln.Addr().String(),
		DialTimeout:           redisDialTimeout,
		ReadTimeout:           redisIOTimeout,
		WriteTimeout:          redisIOTimeout,
		ContextTimeoutEnabled: true, // 必须：否则 go-redis 丢弃命令 ctx 的截止时间
	})
	defer func() { _ = rdb.Close() }()

	sink := NewRedisClusterSink(rdb, "default")
	sink.timeout = 300 * time.Millisecond // 覆盖成短超时，让测试在毫秒级跑完

	start := time.Now()
	err = sink.SaveCluster(noise.ClusterRecord{TenantID: "default", ClusterKey: "c:test"})
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("SaveCluster against a hung redis must return an error, got nil")
	}
	// 上界：ctx deadline（300ms）+ 少量调度余量。远小于 3s 默认值即证明
	// 生效的是 sink 的 ctx 上界，而不是靠运气。
	if elapsed > 2*time.Second {
		t.Fatalf("SaveCluster blocked %v, want bounded by sink timeout (300ms)", elapsed)
	}
}
