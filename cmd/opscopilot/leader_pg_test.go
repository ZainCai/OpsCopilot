// leader_pg_test.go #11/ADR-012 验收标准 2/3 的真实 PG 验证
// （OPS_TEST_PG_DSN 门控，无 DSN skip）：
//   - advisory lock 互斥：双选主环并发竞选，任意时刻恰一 leader；
//   - failover：杀掉（Stop 显式 unlock）leader 后另一实例在一个节拍量级内接管；
//   - OnPromote 门禁：簇恢复失败期间不翻转 leader，恢复成功后才开闸。
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"opscopilot/internal/noise"
)

// leaderTestDSN 门控读取 OPS_TEST_PG_DSN，返回两个独立连接池（同库）。
// 两条池 = 两个 PG 后端会话——advisory lock 的跨"实例"互斥必须跨会话验证，
// 同池拿两条连接也行但语义更像多进程，这里刻意用两池。
func leaderTestPools(t *testing.T) (*pgxpool.Pool, *pgxpool.Pool) {
	t.Helper()
	dsn := os.Getenv("OPS_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("OPS_TEST_PG_DSN not set — leader election integration skipped")
	}
	mk := func() *pgxpool.Pool {
		p, err := pgxpool.New(context.Background(), dsn)
		if err != nil {
			t.Fatalf("pool: %v", err)
		}
		t.Cleanup(p.Close)
		return p
	}
	return mk(), mk()
}

func quietLogf() func(string, ...any) { return func(string, ...any) {} }

// TestLeaderElectionMutualExclusionAndTakeover 任意时刻 is_leader 之和恰 1；
// leader Stop（显式 pg_advisory_unlock，模拟"杀进程释放锁"）后另一环限时接管。
func TestLeaderElectionMutualExclusionAndTakeover(t *testing.T) {
	poolA, poolB := leaderTestPools(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const retry = 100 * time.Millisecond
	eA := NewLeaderElector(poolA, true, retry, quietLogf())
	eB := NewLeaderElector(poolB, true, retry, quietLogf())
	go eA.Run(ctx)
	go eB.Run(ctx)
	t.Cleanup(func() { eA.Stop(); eB.Stop() })

	// 1) 限时内出现恰一 leader。
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if n := leaderCount(eA, eB); n == 1 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if n := leaderCount(eA, eB); n != 1 {
		t.Fatalf("no single leader emerged within 15s (count=%d)", n)
	}

	// 2) 抽样 1s：两环之和恒 = 1（advisory lock 互斥，验收标准 2）。
	for i := 0; i < 20; i++ {
		if n := leaderCount(eA, eB); n != 1 {
			t.Fatalf("leader count must stay exactly 1, got %d at sample %d", n, i)
		}
		time.Sleep(50 * time.Millisecond)
	}

	// 3) 杀掉 leader（Stop = unlock + 归还连接）→ 另一实例 ≤15s 接管（验收标准 3）。
	leader, follower := eA, eB
	if !eA.IsLeader() {
		leader, follower = eB, eA
	}
	leader.Stop()
	if leader.IsLeader() {
		t.Fatal("stopped elector still reports leader")
	}
	deadline = time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) && !follower.IsLeader() {
		time.Sleep(50 * time.Millisecond)
	}
	if !follower.IsLeader() {
		t.Fatal("follower did not take over within 15s after leader stop")
	}
	// 双保险：接管后老 leader（已 Stop）不得复活——Stop 是终态。
	if leader.IsLeader() {
		t.Fatal("stopped elector revived as leader")
	}
}

// TestLeaderOnPromoteGateBlocksFlip ADR failover 时序 3：OnPromote 返回
// error = 持锁但**不翻转 leader**、退避重试；恢复成功那一拍才开闸。
// （单选举器即可验证门禁；"另一实例抢不到锁"由互斥测试覆盖——放个不带钩子
// 的竞争者反而会先赢锁、让钩子永不被调，测试自己先挂。）
func TestLeaderOnPromoteGateBlocksFlip(t *testing.T) {
	poolA, _ := leaderTestPools(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var calls atomic.Int64
	gateEntered := make(chan struct{}, 8)
	eA := NewLeaderElector(poolA, true, 100*time.Millisecond, quietLogf())
	eA.SetOnPromote(func(context.Context) error {
		n := calls.Add(1)
		select {
		case gateEntered <- struct{}{}:
		default:
		}
		if n < 3 {
			return errors.New("simulated cluster restore failure")
		}
		return nil
	})
	go eA.Run(ctx)
	t.Cleanup(func() { eA.Stop() })

	// 等钩子至少被敲了两次（确认确实在重试而非卡死）。
	for i := 0; i < 2; i++ {
		select {
		case <-gateEntered:
		case <-time.After(10 * time.Second):
			t.Fatalf("onPromote hook invoked fewer than %d times", i+1)
		}
	}
	if eA.IsLeader() && calls.Load() < 3 {
		t.Fatal("restore gate failed but leader flag flipped early")
	}

	// 第三次成功 → eA 开闸。
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) && !eA.IsLeader() {
		time.Sleep(50 * time.Millisecond)
	}
	if !eA.IsLeader() {
		t.Fatalf("eA never promoted after restore succeeded (hook calls=%d)", calls.Load())
	}
}

func newTestClusterRecord(tenant, key string, first, last time.Time) noise.ClusterRecord {
	return noise.ClusterRecord{
		TenantID:     tenant,
		ClusterKey:   key,
		State:        noise.StateOpen,
		FirstSeen:    first,
		LastSeen:     last,
		Severity:     "critical",
		Summary:      "leader pg load test",
		AlertCount:   3,
		Fingerprints: []string{"fp1", "fp2"},
		NodeKeys:     []string{"prometheus://nodes/n1"},
	}
}

// TestPGClusterSinkLoadClustersRoundtrip PG 真相源重建入口（OnPromote 兜底
// 路）：SaveCluster → loadAlertClustersFromPG 列映射逆运算无损（evidence
// JSONB 解回 Fingerprints/NodeKeys/AlertCount）。
func TestPGClusterSinkLoadClustersRoundtrip(t *testing.T) {
	sink := pgSinkForTest(t)
	tenant := fmt.Sprintf("ldrload-%d", time.Now().UnixNano())
	cleanup := func() {
		pgExec(t, sink, `DELETE FROM alert_cluster WHERE tenant_id = $1`, tenant)
		pgExec(t, sink, `DELETE FROM tenant WHERE id = $1`, tenant)
	}
	t.Cleanup(cleanup)
	// 独立租户隔离断言（alert_cluster.tenant_id 有 FK，先补 tenant 行）。
	pgExec(t, sink, `INSERT INTO tenant (id, name) VALUES ($1, $1)`, tenant)

	first := time.Now().Add(-time.Hour).UTC().Truncate(time.Millisecond)
	last := time.Now().UTC().Truncate(time.Millisecond)
	rec := newTestClusterRecord(tenant, "c:ldrload@1", first, last)
	if err := sink.SaveCluster(rec); err != nil {
		t.Fatalf("SaveCluster: %v", err)
	}
	// OnPromote 钩子走的同款入口（pool + tenant 显式参数，不依赖 sink 租户）。
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	got, err := loadAlertClustersFromPG(ctx, sink.pool, tenant)
	if err != nil {
		t.Fatalf("loadAlertClustersFromPG: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("records = %d, want 1", len(got))
	}
	g := got[0]
	if g.ClusterKey != rec.ClusterKey || g.State != rec.State ||
		g.Severity != rec.Severity || g.Summary != rec.Summary ||
		g.AlertCount != rec.AlertCount ||
		len(g.Fingerprints) != 2 || g.Fingerprints[0] != "fp1" ||
		len(g.NodeKeys) != 1 || g.NodeKeys[0] != "prometheus://nodes/n1" {
		t.Fatalf("roundtrip mismatch: %+v", g)
	}
	if !g.FirstSeen.Equal(first) || !g.LastSeen.Equal(last) {
		t.Fatalf("timestamps: got %v/%v want %v/%v", g.FirstSeen, g.LastSeen, first, last)
	}
}

func leaderCount(electors ...*LeaderElector) int {
	n := 0
	for _, e := range electors {
		if e.IsLeader() {
			n++
		}
	}
	return n
}
