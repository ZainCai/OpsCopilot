// cluster_test.go #11 水平扩展：双实例同跑的集成验证（OPS_TEST_PG_DSN 门控，
// 无 DSN 整文件 skip）。同进程构造两个 Assembly 共享同一套 PG，对拍
// ADR-012"双实例语义验收标准"：
//
//	(a) ingest：批量入队 → 双 worker 并发消费 → 每条恰一实例认领处理、
//	    全队列 processed、incident 恰 N 单（无重复建单）；
//	(b) leader：见 leader_pg_test.go（恰一 is_leader / Stop 后限时接管）；
//	(c) escalation：双实例同扫同一逾期单 → PG 台账恰一 Claim 成功、恰发
//	    一次升级通知；另附并发双 Claim 恰一成功的原子性单测。
package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"opscopilot/internal/incident"
)

func dualDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("OPS_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("OPS_TEST_PG_DSN not set — dual-instance integration skipped")
	}
	return dsn
}

func dualAssembly(t *testing.T, dsn, tenant string) *Assembly {
	t.Helper()
	cfg := testAssemblyConfig("tk")
	cfg.DB.DSN = dsn
	cfg.Tenant = tenant
	cfg.Ingest.AutoCreate = true // 双 worker 都要真消费
	cfg.Ingest.Interval = 200 * time.Millisecond
	cfg.Ingest.Batch = 3                 // 小批量 → 多轮认领，制造交错窗口
	cfg.Notify.EscalationEnabled = false // 本文件的升级环手工构造（要注入计数出口）
	asm, err := NewAssembly(newQuietLogger(), cfg)
	if err != nil {
		t.Fatalf("assembly(%s): %v", tenant, err)
	}
	t.Cleanup(asm.Close)
	if asm.Queue == nil || asm.Worker == nil {
		t.Fatal("ingest pipeline not wired despite DSN")
	}
	return asm
}

// TestDualInstanceIngestExclusive 验收标准 1：同库双实例并发入队 N 条
// （source_ref 唯一）→ 最终 incident 恰 N、每行 ingest_queue 恰一 owner、
// 全部 processed。
func TestDualInstanceIngestExclusive(t *testing.T) {
	dsn := dualDSN(t)
	tenant := fmt.Sprintf("dual-ing-%d", time.Now().UnixNano())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	asmA := dualAssembly(t, dsn, tenant)
	asmB := dualAssembly(t, dsn, tenant)

	const n = 8
	refs := make([]string, 0, n)
	for i := 0; i < n; i++ {
		ref := fmt.Sprintf("%s-fp%d", tenant, i)
		payload := `{"labels":{"alertname":"DualDemo","severity":"critical"},"fingerprint":"` + ref + `"}`
		queued, err := asmA.Queue.Enqueue(incident.OriginAlertmanager, ref, payload)
		if err != nil || !queued {
			t.Fatalf("enqueue %s: queued=%v err=%v", ref, queued, err)
		}
		refs = append(refs, ref)
	}
	t.Cleanup(func() { cleanupDualTenant(t, asmA.pool, tenant) })

	// 双 worker 并发消费（各自连接池、各自 owner 标识——000016 认领租约互斥）。
	go asmA.Worker.Run(ctx)
	go asmB.Worker.Run(ctx)

	deadline := time.Now().Add(30 * time.Second)
	for {
		p, err := asmA.Queue.Pending()
		if err != nil {
			t.Fatalf("pending: %v", err)
		}
		if p == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("queue not drained by both workers within 30s (pending=%d)", p)
		}
		time.Sleep(100 * time.Millisecond)
	}

	// 每行恰一 owner：认领互斥 + 确认 CAS 下无重做 → processed 恰 n、
	// attempts 恒 0（任何"确认落空被重领"都会把 attempts 推上去或留下残行）。
	var processed, attempts int
	if err := asmA.pool.QueryRow(ctx,
		`SELECT count(*) FILTER (WHERE processed_at IS NOT NULL),
		        COALESCE(sum(attempts), 0)
		 FROM ingest_queue WHERE tenant_id = $1`, tenant).Scan(&processed, &attempts); err != nil {
		t.Fatalf("queue stats: %v", err)
	}
	if processed != n {
		t.Fatalf("processed rows = %d, want %d", processed, n)
	}
	if attempts != 0 {
		t.Fatalf("attempts sum = %d, want 0 (double-claim/rework detected)", attempts)
	}

	// incident 恰 n 单、无重复建单。
	var incidents int
	if err := asmA.pool.QueryRow(ctx,
		`SELECT count(*) FROM incident WHERE tenant_id = $1`, tenant).Scan(&incidents); err != nil {
		t.Fatalf("incident count: %v", err)
	}
	if incidents != n {
		t.Fatalf("incidents = %d, want exactly %d (no duplicate create)", incidents, n)
	}
}

// TestDualIngestClaimDisjoint 租约认领层直证：两个 owner 并发跑 processBatch
// （处理回调记录 id 并小睡，模拟处理窗口重叠）——同一行至多进一个 owner 的
// 结果集，全部行恰被认领一次。
func TestDualIngestClaimDisjoint(t *testing.T) {
	dsn := dualDSN(t)
	tenant := fmt.Sprintf("dual-claim-%d", time.Now().UnixNano())
	poolA, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("poolA: %v", err)
	}
	defer poolA.Close()
	poolB, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("poolB: %v", err)
	}
	defer poolB.Close()
	defer func() {
		_, _ = poolA.Exec(context.Background(), `DELETE FROM ingest_queue WHERE tenant_id=$1`, tenant)
	}()

	qA := NewPGIngestQueue(poolA, tenant, 0, 0, "inst-A", quietLogf())
	qB := NewPGIngestQueue(poolB, tenant, 0, 0, "inst-B", quietLogf())
	const n = 12
	for i := 0; i < n; i++ {
		ref := fmt.Sprintf("%s-r%d", tenant, i)
		if _, err := qA.Enqueue(incident.OriginWebhook, ref, `{"labels":{}}`); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}

	var mu sync.Mutex
	claimedA, claimedB := map[int64]int{}, map[int64]int{}
	collect := func(bucket map[int64]int) func(Item) error {
		return func(it Item) error {
			time.Sleep(10 * time.Millisecond) // 拉开处理窗口，逼出跨实例重叠
			mu.Lock()
			bucket[it.ID]++
			mu.Unlock()
			return nil
		}
	}

	var wg sync.WaitGroup
	drainAll := func(q *PGIngestQueue, bucket map[int64]int) {
		defer wg.Done()
		for round := 0; round < 20; round++ {
			got, err := q.processBatch(5, collect(bucket))
			if err != nil {
				t.Errorf("processBatch(%s): %v", q.owner, err)
				return
			}
			if got == 0 {
				return
			}
		}
		t.Errorf("%s still claiming after 20 rounds", q.owner)
	}
	wg.Add(2)
	go drainAll(qA, claimedA)
	go drainAll(qB, claimedB)
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(claimedA)+len(claimedB) != n {
		t.Fatalf("claimed union %d+%d != %d rows", len(claimedA), len(claimedB), n)
	}
	for id, c := range claimedA {
		if _, dup := claimedB[id]; dup {
			t.Fatalf("row %d claimed by BOTH owners (lease violated)", id)
		}
		if c != 1 {
			t.Fatalf("row %d claimed %d times by owner A", id, c)
		}
	}
}

// TestDualInstanceEscalationSingleClaim 验收标准 4：同一 open 逾期事件在
// 两实例并发升级扫描下**恰发一次**通知（PG 台账 (tenant, incident) 主键认领）。
func TestDualInstanceEscalationSingleClaim(t *testing.T) {
	dsn := dualDSN(t)
	tenant := fmt.Sprintf("dual-esc-%d", time.Now().UnixNano())
	ctx := context.Background()

	asmA := dualAssembly(t, dsn, tenant)
	asmB := dualAssembly(t, dsn, tenant)
	defer cleanupDualTenantEsc(t, asmA.pool, tenant)

	if _, err := asmA.Incidents.Create("dual-esc-1", "EscDemo", "critical", "tester"); err != nil {
		t.Fatalf("create incident: %v", err)
	}
	base := time.Now()
	mkPoller := func(incStore incident.Store, pool *pgxpool.Pool, disp *fakeDispatcher) *EscalationPoller {
		p := NewEscalationPoller(incStore, disp, newPGEscalationLedger(pool, tenant),
			tenant, 15*time.Minute, time.Minute, quietLogf())
		p.now = func() time.Time { return base.Add(20 * time.Minute) } // 已过阈值
		return p
	}
	dispA, dispB := &fakeDispatcher{}, &fakeDispatcher{}
	pA := mkPoller(asmA.Incidents, asmA.pool, dispA)
	pB := mkPoller(asmB.Incidents, asmB.pool, dispB)

	// 并发扫描同一逾期单（双"实例"各自台账句柄、同一个 DB）。
	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, p := range []*EscalationPoller{pA, pB} {
		wg.Add(1)
		go func(p *EscalationPoller) { defer wg.Done(); <-start; p.pollOnce(ctx) }(p)
	}
	close(start)
	wg.Wait()

	if total := dispA.count() + dispB.count(); total != 1 {
		t.Fatalf("escalation dispatches total = %d (A=%d B=%d), want exactly 1",
			total, dispA.count(), dispB.count())
	}
	var rows int
	if err := asmA.pool.QueryRow(ctx,
		`SELECT count(*) FROM incident_escalation WHERE tenant_id=$1 AND incident_id=$2`,
		tenant, "dual-esc-1").Scan(&rows); err != nil {
		t.Fatalf("ledger count: %v", err)
	}
	if rows != 1 {
		t.Fatalf("ledger rows = %d, want 1", rows)
	}
}

// TestPGEscalationLedgerConcurrentClaim 台账原子性直证：两 goroutine 并发
// Claim 同一事件恰一成功；Release 回滚后可再 Claim。
func TestPGEscalationLedgerConcurrentClaim(t *testing.T) {
	dsn := dualDSN(t)
	tenant := fmt.Sprintf("esc-conc-%d", time.Now().UnixNano())
	poolA, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("poolA: %v", err)
	}
	defer poolA.Close()
	poolB, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("poolB: %v", err)
	}
	defer poolB.Close()
	ctx := context.Background()
	defer func() {
		_, _ = poolA.Exec(ctx, `DELETE FROM incident_escalation WHERE tenant_id=$1`, tenant)
	}()

	inc := "INC-CONC-1"
	la, lb := newPGEscalationLedger(poolA, tenant), newPGEscalationLedger(poolB, tenant)
	// runConcurrent：两路以同一栅栏起跑（真并发窗口），每波都新建栅栏通道。
	runConcurrent := func(fnA, fnB func() bool) (bool, bool) {
		start := make(chan struct{})
		var wg sync.WaitGroup
		var a, b bool
		wg.Add(2)
		go func() { defer wg.Done(); <-start; a = fnA() }()
		go func() { defer wg.Done(); <-start; b = fnB() }()
		close(start)
		wg.Wait()
		return a, b
	}
	claim := func(l *pgEscalationLedger) func() bool {
		return func() bool {
			ok, err := l.Claim(ctx, inc)
			if err != nil {
				t.Errorf("claim %s: %v", inc, err)
				return false
			}
			return ok
		}
	}
	okA, okB := runConcurrent(claim(la), claim(lb))
	if okA == okB || !(okA || okB) {
		t.Fatalf("concurrent claim = A:%v B:%v, want exactly one true", okA, okB)
	}
	// 再次并发：都失败（已认领，只发一次）。
	if againA, againB := runConcurrent(claim(la), claim(lb)); againA || againB {
		t.Fatalf("re-claim after success: A=%v B=%v, want false/false", againA, againB)
	}
	// 发送失败回滚语义：Release 后恰一实例可再次认领。
	if err := la.Release(ctx, inc); err != nil {
		t.Fatalf("release: %v", err)
	}
	if ok, err := lb.Claim(ctx, inc); err != nil || !ok {
		t.Fatalf("claim after release = %v/%v, want true/nil", ok, err)
	}
}

// cleanupDualTenant 清掉双实例测试写入的行（incident 域 + 队列；FK 级联
// 覆盖 incident_cluster，audit/escalation 独立成表单独删）。
func cleanupDualTenant(t *testing.T, pool *pgxpool.Pool, tenant string) {
	t.Helper()
	ctx := context.Background()
	for _, q := range []string{
		`DELETE FROM ingest_queue WHERE tenant_id=$1`,
		`DELETE FROM incident_audit WHERE tenant_id=$1`,
		`DELETE FROM incident_escalation WHERE tenant_id=$1`,
		`DELETE FROM incident WHERE tenant_id=$1`,
		`DELETE FROM tenant WHERE id=$1`,
	} {
		if _, err := pool.Exec(ctx, q, tenant); err != nil {
			t.Logf("cleanup %q: %v", q, err)
		}
	}
}

func cleanupDualTenantEsc(t *testing.T, pool *pgxpool.Pool, tenant string) {
	t.Helper()
	cleanupDualTenant(t, pool, tenant)
}
