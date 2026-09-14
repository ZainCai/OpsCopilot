// concurrency_p2a_test.go —— round10 P2-A5 针对性并发用例（M3 阶段 1）。
// 补足 round10 清单指出的并发用例缺口：
//
//  1. leader heldConn 并发（P2-A1 回归：Acquire 移锁外，只钉一条）；
//  2. N 路并发 ProcessAlerts（P2-A2 回归：签名 commit 语义，失败补写不丢）；
//  3. 并发 ProcessAlerts + 双调 StopVerdictWriter / SetVerdictSink(nil)
//     （P2-A3/A5：停机/卸载与处理并发安全；drain 超时残量只计一次）；
//  4. memguard 淘汰并发 gauge 求值（同清单项，见 pkg/memguard/memguard_test.go）。
//
// 真库用例由 OPS_TEST_PG_DSN 门控（与 leader_pg_test.go 同款）；其余纯单测，
// 全部配合 CI -race 兜底（数据竞争会直接红）。
package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"opscopilot/internal/config"
	"opscopilot/internal/connector"
	"opscopilot/internal/noise"
)

// ---- 1. leader heldConn 并发（真库门控） ----

// TestLeaderHeldConnConcurrent P2-A1 回归：N 路并发 heldConn 只钉一条
// 连接。Acquire 在锁外执行（慢 DB 不把 IsLeader/Stop 串行化）；竞态输家
// Release 自己取的那条。并发后 pinned 恰一、所有调用成功、-race 无竞争。
func TestLeaderHeldConnConcurrent(t *testing.T) {
	dsn := os.Getenv("OPS_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("OPS_TEST_PG_DSN not set — leader held-conn concurrency skipped")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	cfg.MaxConns = 32 // 并发 Acquire 需要 ≥workers 的连接槽（默认池只有 max(4,NumCPU) 个）
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)
	e := NewLeaderElector(pool, true, 100*time.Millisecond, quietLogf())
	// 本用例不启动 Run（Stop 在未启动时只置停用标记、不归还 pinned 连接）——
	// 测试结尾必须显式归还独占连接，否则池 Close 永远等它（60s 超时挂测）。
	defer e.releasePinned(false)
	defer e.Stop()

	const workers = 16
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	conns := make(chan *pgxpool.Conn, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := e.heldConn(context.Background())
			if err != nil {
				errs <- err
				return
			}
			conns <- c
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("heldConn: %v", err)
	}
	close(conns)
	seen := make(map[*pgxpool.Conn]int, workers)
	for c := range conns {
		seen[c]++
	}
	e.mu.Lock()
	pinned := e.pinned
	e.mu.Unlock()
	if pinned == nil {
		t.Fatal("pinned conn must be set after concurrent heldConn")
	}
	if len(seen) != 1 {
		t.Fatalf("concurrent heldConn returned %d distinct conns, want exactly 1 (pinned)", len(seen))
	}
	if _, ok := seen[pinned]; !ok {
		t.Fatal("pinned conn not among the returned conns")
	}
}

// ---- 2. N 路并发 ProcessAlerts（P2-A2 commit 语义） ----

// concurrencySink 原子计数的 RecordSink + VerdictSink（并发用例：裸计数在
// N 路并发下是数据竞争，-race 会误报业务问题）。
type concurrencySink struct {
	calls     atomic.Int64 // SaveCluster 总调用
	okCalls   atomic.Int64 // 成功调用
	failFirst int64        // 前 N 次调用失败（原子分配：谁先到谁失败）
}

func (s *concurrencySink) SaveCluster(noise.ClusterRecord) error {
	n := s.calls.Add(1)
	if n <= s.failFirst {
		return errPersistFailed
	}
	s.okCalls.Add(1)
	return nil
}

func (s *concurrencySink) SaveVerdict(noise.VerdictRecord) error { return nil }

// TestProcessAlertsConcurrentPersistNoLostWrite P2-A2 回归：签名在持久化
// **成功后**提交（commit 语义）——N 路并发 ProcessAlerts 且前几批 Save
// 失败时，失败簇必须被后续批次补写（永不丢写）；并发下无数据竞争（-race）。
// 旧实现"锁内预更新签名 + 失败回滚"依赖宿主串行：两路并发 A 预更新 →
// B 比对跳过 → A 回滚 → 该簇永不再写（G1 同款丢失，触发面放大到并发）。
func TestProcessAlertsConcurrentPersistNoLostWrite(t *testing.T) {
	engine := newTestNoiseEngine(t, noiseTestSink(t))
	const workers = 8
	sink := &concurrencySink{failFirst: 4} // 前 4 次 Save 失败（恰 4 个簇未提交）
	engine.SetRecordSink(sink)

	var wg sync.WaitGroup
	now := time.Now()
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			engine.ProcessAlerts([]connector.Alert{{
				Fingerprint: fmt.Sprintf("fp-conc-%d", n),
				Labels:      map[string]string{"instance": fmt.Sprintf("i%d", n)},
				StartsAt:    now.Add(time.Duration(n) * time.Second),
			}})
		}(i)
	}
	wg.Wait()
	// 并发期断言：失败次数恰为 failFirst（原子分配：前 N 次 Save 失败）。
	// **不**断言总 Save 次数 == 簇数——commit 语义下多路 collect 会重复选中
	// 尚未提交签名的簇（A 落库前 B 已 collect 到 A），重复写幂等无害；
	// 唯一恒定不变量是"失败恰 N 次、其余全成功"。
	if got := sink.calls.Load() - sink.okCalls.Load(); got != sink.failFirst {
		t.Fatalf("concurrent failed saves = %d, want %d", got, sink.failFirst)
	}
	// 收敛批 + 追加批（各一个无关新告警）：并发更新窗口内，"提交签名后又
	// 有新告警并入"的簇会幂等重写（>1）——这是 commit 语义的正常表现，
	// 不做严格 +1 断言；断言的是**最终收敛**：补写窗口闭合后，每批只写
	// 自己的新簇。永不丢写由"失败簇必须被重写进最终收敛"保证。
	// 注意：追加批时刻必须与并发批同处窗口内（距 now < 10m）——跨窗口的
	// 告警会把旧簇 resolve（State open→resolved，签名变化）→ 正当重写，
	// 会让本用例把"窗口老化"误读成"不收敛"。
	for i := 0; i < 3; i++ {
		bI := sink.calls.Load()
		engine.ProcessAlerts([]connector.Alert{{
			Fingerprint: fmt.Sprintf("fp-settle-%d", i),
			Labels:      map[string]string{"instance": "iS"},
			StartsAt:    now.Add(time.Duration(8+i) * time.Second)}})
		t.Logf("settle %d: calls %d → %d (+%d)", i, bI, sink.calls.Load(), sink.calls.Load()-bI)
	}
	before := sink.calls.Load()
	engine.ProcessAlerts([]connector.Alert{{
		Fingerprint: "fp-final", Labels: map[string]string{"instance": "iF"}, StartsAt: now.Add(11 * time.Second)}})
	if got := sink.calls.Load(); got != before+1 {
		t.Fatalf("final batch calls = %d, want %d (all clusters must have converged; failed ones rewritten)", got, before+1)
	}
}

// ---- 3. 并发 ProcessAlerts + 停机/卸载（P2-A3/A5） ----

// slowVerdictSink SaveVerdict 阻塞——制造 drain 必然超时（P2-A3 残量路径）。
// close(release) 后全部放行（writer 正常排空退出，不泄漏 goroutine）。
type slowVerdictSink struct {
	release chan struct{}
	once    sync.Once
}

func newSlowVerdictSink() *slowVerdictSink {
	return &slowVerdictSink{release: make(chan struct{})}
}

func (s *slowVerdictSink) SaveVerdict(noise.VerdictRecord) error {
	s.once.Do(func() { <-s.release })
	return nil
}

// TestStopVerdictWriterDoubleCallCountsOnce P2-A3 回归：main 停机序列与
// Assembly.Close 双保险都会调 StopVerdictWriter——drain 超时残量只计一次
// sink_drops。第二次调用再给 writer 一次 drain 预算，但残量已含在第一次
// 口径内（重复计数会让指标虚高）。
func TestStopVerdictWriterDoubleCallCountsOnce(t *testing.T) {
	engine := NewNoiseEngine(noiseTestSink(t), newQuietLogger(), testTenant,
		noiseSpec(func(s *config.NoiseSection) { s.SinkDrain = 150 * time.Millisecond }),
		config.MemLimitSection{})
	if engine == nil {
		t.Fatal("expected enabled engine")
	}
	slow := newSlowVerdictSink()
	engine.SetVerdictSink(slow)
	am := NewAppMetrics()
	engine.SetMetrics(am)
	engine.StartVerdictWriter()

	// 塞入超过 batch 上限（200）的判决：writer 取满一批（200）后卡死在慢
	// sink，channel 必有残量 → drain 必然超时且残量 > 0。
	for i := 0; i < 250; i++ {
		engine.ProcessAlerts([]connector.Alert{{
			Fingerprint: fmt.Sprintf("fp-dbl-%d", i),
			Labels:      map[string]string{"instance": "i1"},
			StartsAt:    time.Now(),
		}})
	}

	engine.StopVerdictWriter() // 第一次：drain 超时残量计 sink_drops
	first := am.SinkDrops["other"].Value()
	if first == 0 {
		t.Fatal("first StopVerdictWriter must count undrained residue")
	}
	engine.StopVerdictWriter() // 第二次（双保险语义）：不再重复计数
	if got := am.SinkDrops["other"].Value(); got != first {
		t.Fatalf("second StopVerdictWriter changed sink_drops: %d → %d (must count once)", first, got)
	}
	close(slow.release) // 放行慢 sink：writer 排空退出
}

// TestProcessAlertsConcurrentWithStopAndDetach P2-A3/A5：处理链路与停机
// 路径并发——N 路 ProcessAlerts 同时双调 StopVerdictWriter + 卸载判决
// 出口（SetVerdictSink(nil)）。断言：全程无 panic、-race 无竞争；停机后
// 新投递走丢弃计数不阻塞（不回头同步写）。具体丢弃计数语义由既有
// noise_writequeue_test.go 覆盖。
func TestProcessAlertsConcurrentWithStopAndDetach(t *testing.T) {
	engine := newTestNoiseEngine(t, noiseTestSink(t))
	engine.StartVerdictWriter()
	engine.SetVerdictSink(&concurrencySink{})

	const workers = 8
	var wg sync.WaitGroup
	now := time.Now()
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			engine.ProcessAlerts([]connector.Alert{{
				Fingerprint: fmt.Sprintf("fp-stop-%d", n),
				Labels:      map[string]string{"instance": fmt.Sprintf("i%d", n)},
				StartsAt:    now.Add(time.Duration(n) * time.Second),
			}})
		}(i)
	}
	// 与处理并发执行停机路径（main.go 与 Assembly.Close 双调 + 卸载出口）。
	engine.StopVerdictWriter()
	engine.SetVerdictSink(nil)
	engine.StopVerdictWriter()
	wg.Wait()
}
