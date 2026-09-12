// leader_test.go #11/ADR-012：leader 选举与门禁监督的无 DB 单测。
//
// 覆盖：降级路径恒 leader（与单实例现状一致）、runLeaderGated 的
// "暂停→恢复→再暂停"生命周期、Stop 语义（Run 前/后调用都安全）。
// PG advisory lock 的互斥/接管/开闸门禁在 DSN 测试
// （leader_pg_test.go / cluster_test.go，OPS_TEST_PG_DSN 门控）。
package main

import (
	"context"
	"sync"
	"testing"
	"time"
)

// ---------- 假 leader 状态源（驱动 runLeaderGated，不碰 PG） ----------

type fakeGate struct {
	mu      sync.Mutex
	leader  bool
	changed chan struct{}
}

func newFakeGate(leader bool) *fakeGate {
	return &fakeGate{leader: leader, changed: make(chan struct{})}
}

func (g *fakeGate) IsLeader() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.leader
}

func (g *fakeGate) Notify() <-chan struct{} {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.changed
}

func (g *fakeGate) set(v bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.leader == v {
		return
	}
	g.leader = v
	close(g.changed)
	g.changed = make(chan struct{})
}

// countingLoop 记录"起了几轮、当前在跑几轮"的可重启循环入口（Run 语义：
// ctx 取消即返回）。
type countingLoop struct {
	mu       sync.Mutex
	starts   int
	stops    int
	running  int
	entered  chan struct{} // 每轮启动后广播一次（测试等"真的在跑"）
	released chan struct{} // 每轮退出后广播一次（测试等"安全点停下了"）
}

func newCountingLoop() *countingLoop {
	return &countingLoop{entered: make(chan struct{}, 16), released: make(chan struct{}, 16)}
}

func (c *countingLoop) start(ctx context.Context) {
	c.mu.Lock()
	c.starts++
	c.running++
	c.mu.Unlock()
	select {
	case c.entered <- struct{}{}:
	default:
	}
	<-ctx.Done()
	c.mu.Lock()
	c.running--
	c.stops++
	c.mu.Unlock()
	select {
	case c.released <- struct{}{}:
	default:
	}
}

func (c *countingLoop) snapshot() (starts, stops, running int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.starts, c.stops, c.running
}

// ---------- runLeaderGated ----------

// TestRunLeaderGatedPauseResumeAndStop 门禁监督核心语义：非 leader 不启动；
// 成为 leader 启动；失去 leader 在安全点全停；再当选再拉起；ctx 取消监督器
// 与全部循环退出。
func TestRunLeaderGatedPauseResumeAndStop(t *testing.T) {
	gate := newFakeGate(false)
	loopA, loopB := newCountingLoop(), newCountingLoop()
	ctx, cancel := context.WithCancel(context.Background())
	supDone := make(chan struct{})
	go func() {
		runLeaderGated(ctx, gate, loopA.start, loopB.start)
		close(supDone)
	}()

	// 非 leader：一秒钟内不得启动任何 gated 循环。
	time.Sleep(150 * time.Millisecond)
	if s, _, r := loopA.snapshot(); s != 0 || r != 0 {
		t.Fatalf("while non-leader: starts=%d running=%d, want 0/0", s, r)
	}

	// 成为 leader：两路循环都跑起来。
	gate.set(true)
	waitEntered(t, loopA)
	waitEntered(t, loopB)
	if _, _, r := loopA.snapshot(); r != 1 {
		t.Fatalf("after promote: running=%d, want 1", r)
	}

	// 失去 leader：全部在安全点退出（ctx 取消 → start 函数返回）。
	gate.set(false)
	waitReleased(t, loopA)
	waitReleased(t, loopB)
	if _, _, r := loopA.snapshot(); r != 0 {
		t.Fatalf("after demote: still running=%d, want 0", r)
	}

	// 再当选：第二轮拉起（starts 累加、running 回 1）。
	gate.set(true)
	waitEntered(t, loopA)
	if s, _, r := loopA.snapshot(); s != 2 || r != 1 {
		t.Fatalf("after re-promote: starts=%d running=%d, want 2/1", s, r)
	}

	// 停机：监督器与循环全部退出。
	cancel()
	select {
	case <-supDone:
	case <-time.After(2 * time.Second):
		t.Fatal("supervisor did not exit on ctx cancel")
	}
	if _, stops, r := loopA.snapshot(); r != 0 || stops < 2 {
		t.Fatalf("after shutdown: running=%d stops=%d, want 0/>=2", r, stops)
	}
}

func waitEntered(t *testing.T, c *countingLoop) {
	t.Helper()
	select {
	case <-c.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("gated loop did not start")
	}
}

func waitReleased(t *testing.T, c *countingLoop) {
	t.Helper()
	select {
	case <-c.released:
	case <-time.After(2 * time.Second):
		t.Fatal("gated loop did not stop at safe point")
	}
}

// ---------- LeaderElector 降级路径（无 DB / off） ----------

// TestLeaderDegradedAlwaysLeader 无 pool（或 election off）→ 构造即恒 leader，
// IsLeader 在 Run 之前就是 true（gauge 从装配起就是 1——与单实例现状一致）；
// Run 只是挂着等停机；Stop 幂等、先于 Run 调用也安全。
func TestLeaderDegradedAlwaysLeader(t *testing.T) {
	l := NewLeaderElector(nil, true, time.Second, nil) // pool=nil → 降级
	if !l.IsLeader() {
		t.Fatal("degraded elector must be leader from construction")
	}
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() {
		l.Run(ctx)
		close(stopped)
	}()
	l.Stop()
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("degraded Run must return after Stop")
	}
	cancel()
	if !l.IsLeader() {
		t.Fatal("degraded mode must never demote (permanently leader)")
	}
	l.Stop() // 幂等
}

// TestLeaderElectionOffWithPoolIsLeader OPS_LEADER_ELECTION=off（有 pool）：
// 恒 leader 且不跑任何锁操作（本测试给的是真 nil-pool 构造同路径——
// enabled=false 时连 Acquire 都不发生；pool 传 nil 即可验证不 panic）。
func TestLeaderElectionOffIsAlwaysLeader(t *testing.T) {
	l := NewLeaderElector(nil, false, 0, nil) // retry=0 → 回退默认，不得 panic
	if !l.IsLeader() {
		t.Fatal("election off must be permanently leader")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { l.Run(ctx); close(done) }()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run must exit on ctx cancel")
	}
}

// TestLeaderStopBeforeRunParksRun Stop 先于 Run：后来的 Run 直接退出，不去
// 竞选（装配测试常见 Close 无 Run 形态的兜底）。
func TestLeaderStopBeforeRunParksRun(t *testing.T) {
	l := NewLeaderElector(nil, false, time.Second, nil)
	l.Stop()
	runReturned := make(chan struct{})
	go func() { l.Run(context.Background()); close(runReturned) }()
	select {
	case <-runReturned:
	case <-time.After(2 * time.Second):
		t.Fatal("Run after Stop must not start campaigning")
	}
}

// TestClusterRestoreOnPromoteNilEngine 降噪关闭（engine=nil）没有簇态可恢复，
// 开闸钩子恒成功。
func TestClusterRestoreOnPromoteNilEngine(t *testing.T) {
	if err := clusterRestoreOnPromote(context.Background(), nil, nil, nil, "t", nil); err != nil {
		t.Fatalf("nil engine must pass promote gate open, got %v", err)
	}
}

// TestClusterRestoreOnPromoteBothSourcesFailed 两路都不可读 = 恢复失败：
// 返回 error（选举环不开闸）。engine 非 nil 的最小替身：直接借装配产物。
func TestClusterRestoreOnPromoteBothSourcesFailed(t *testing.T) {
	cfg := testAssemblyConfig("tk")
	cfg.DB.DSN = "" // 无 DB（pool=nil）→ PG 路必失败
	asm, err := NewAssembly(newQuietLogger(), cfg)
	if err != nil {
		t.Fatalf("assembly: %v", err)
	}
	defer asm.Close()
	if asm.Noise == nil {
		t.Skip("noise engine not enabled in this config baseline")
	}
	err = clusterRestoreOnPromote(context.Background(), asm.Noise, nil, nil, cfg.Tenant, func(string, ...any) {})
	if err == nil {
		t.Fatal("both sources unwired must fail the restore gate, got nil")
	}
}
