// rca_session_pg_test.go RCA 复盘会话 PG 真相端到端（二期池 #7 S2，
// OPS_TEST_PG_DSN 门控，仓库惯例同 rca_e2e_pg_test.go：未设置即 skip；
// 缺表先 bash scripts/reset_test_pg.sh 应用 000018）。覆盖内存假件给不了的
// 三层：
//  1. seq 并发双写恰一序（行锁发号 vs MAX(seq) 竞态的分水岭）；
//  2. 懒恢复真往返（Redis FLUSHALL → PG 重建 → 回填，created_by/participants
//     与轮次完整性从真库读出）；
//  3. REST 门面全链路（建单→挂簇→GET /rca→POST /rca/session→GET 懒恢复断言）
//     与 OPS_SESSION=off 零行为（端点 503 + 两张新表零写入）。
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"opscopilot/internal/connector"
	"opscopilot/internal/incident"
	"opscopilot/internal/sessionstore"
	"opscopilot/internal/topology"
)

// pgTestPool 门控取池：DSN 未设/不可达 → skip（仓库惯例）。
func pgTestPool(t *testing.T) (string, *pgxpool.Pool) {
	t.Helper()
	dsn := os.Getenv("OPS_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("OPS_TEST_PG_DSN not set — rca session pg tests skipped")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Skipf("pg unreachable (create pool): %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("OPS_TEST_PG_DSN unreachable — skipped: %v", err)
	}
	t.Cleanup(pool.Close)
	return dsn, pool
}

// TestSessionSeqConcurrencyPG 双"实例"（同 pool 两路 goroutine）对同一会话
// 并发追加共 2N 轮 → seq 必为 1..2N 恰一序（无重复无空洞）——rca_session.
// next_seq 行锁发号的直接验收。
func TestSessionSeqConcurrencyPG(t *testing.T) {
	_, pool := pgTestPool(t)
	ctx := context.Background()
	suffix := time.Now().UnixNano()
	tenant := fmt.Sprintf("sess-seq-%d", suffix)
	incID := "INC-seq"
	truth := newPGSessionTruth(pool, tenant)

	const n = 8
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	note := func(err error) {
		if err == nil {
			return
		}
		mu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		mu.Unlock()
	}
	for w := 0; w < 2; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < n; i++ {
				if _, err := truth.appendTurn(ctx, incID, fmt.Sprintf("u%d", worker),
					sessionstore.RoleUser, fmt.Sprintf("w%d-t%d", worker, i), nil); err != nil {
					note(err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	if firstErr != nil {
		t.Fatalf("concurrent appendTurn: %v", firstErr)
	}

	rows, err := pool.Query(ctx, `SELECT seq FROM rca_session_turn WHERE tenant_id=$1 AND incident_id=$2 ORDER BY seq`, tenant, incID)
	if err != nil {
		t.Fatalf("query seqs: %v", err)
	}
	defer rows.Close()
	var seqs []int64
	for rows.Next() {
		var s int64
		if err := rows.Scan(&s); err != nil {
			t.Fatal(err)
		}
		seqs = append(seqs, s)
	}
	if len(seqs) != 2*n {
		t.Fatalf("turn count = %d, want %d", len(seqs), 2*n)
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	for i, s := range seqs {
		if s != int64(i+1) {
			t.Fatalf("seqs must be gapless unique 1..%d, got %v", 2*n, seqs)
		}
	}
	// created_by=首发 worker、participants 收敛两人（行锁路径顺带验证）。
	var createdBy string
	var parts []byte
	if err := pool.QueryRow(ctx, `SELECT created_by, participants FROM rca_session WHERE tenant_id=$1 AND incident_id=$2`,
		tenant, incID).Scan(&createdBy, &parts); err != nil {
		t.Fatalf("read session row: %v", err)
	}
	if createdBy != "u0" && createdBy != "u1" {
		t.Fatalf("created_by = %s", createdBy)
	}
	var ps []string
	if err := json.Unmarshal(parts, &ps); err != nil {
		t.Fatalf("participants jsonb: %v (%s)", err, parts)
	}
	if len(ps) != 2 {
		t.Fatalf("participants = %v, want 2 distinct", ps)
	}
	// 测试租户数据即删（评估环境纪律）。
	if _, err := pool.Exec(ctx, `DELETE FROM rca_session WHERE tenant_id=$1`, tenant); err != nil {
		t.Logf("cleanup: %v", err)
	}
}

// TestSessionLazyRestorePG 真库懒恢复往返：PG 落 2 轮（user+assistant 带
// llm_meta）→ 热态命中读 → FLUSHALL 模拟 TTL 蒸发 → GET 从 PG 重建、轮次
// 完整、created_by 落表、回填热态。
func TestSessionLazyRestorePG(t *testing.T) {
	_, pool := pgTestPool(t)
	ctx := context.Background()
	suffix := time.Now().UnixNano()
	tenant := fmt.Sprintf("sess-lazy-%d", suffix)
	incID := "INC-lazy"

	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	truth := newPGSessionTruth(pool, tenant)
	if _, err := truth.appendTurn(ctx, incID, "alice", sessionstore.RoleUser, "根因是哪次发布?", nil); err != nil {
		t.Fatalf("append user: %v", err)
	}
	if _, err := truth.appendTurn(ctx, incID, "system:llm", sessionstore.RoleAssistant, "证据指向 dep-42。",
		map[string]any{"model": "m1", "prompt_hash": "abc", "prompt_chars": 12}); err != nil {
		t.Fatalf("append assistant: %v", err)
	}

	inc := incident.NewMemStore()
	if _, err := inc.Create(incID, "懒恢复目标", "critical", "e2e"); err != nil {
		t.Fatalf("seed incident: %v", err)
	}
	o := NewSessionOrchestrator(truth, sessionstore.New(rdb, "alert"), inc,
		&sessionStubAnalyzer{}, nil, 200, tenant, NewAppMetrics(), nil)

	// 热态冷启动 → 懒恢复重建。
	res, err := o.ReadSession(ctx, incID)
	if err != nil {
		t.Fatalf("cold ReadSession: %v", err)
	}
	if len(res.Turns) != 2 || res.Turns[0].Role != "user" || res.Turns[1].Role != "assistant" {
		t.Fatalf("lazy restored turns = %+v", res.Turns)
	}
	if res.Meta.CreatedBy != "alice" || len(res.Meta.Participants) != 1 {
		t.Fatalf("lazy meta = %+v, want created_by=alice/1 participant", res.Meta)
	}
	if res.Persistence != "timescaledb" {
		t.Fatalf("persistence = %s, want timescaledb", res.Persistence)
	}
	key := sessionstore.SessionKey(tenant, incID)
	if env, err := o.hot.LoadEnvelope(ctx, key); err != nil || len(env.Turns) != 2 {
		t.Fatalf("hot must be refilled by lazy restore: (%+v,%v)", env, err)
	}
	// 蒸发再来一次：PG 真相仍在，读完整。
	mr.FlushAll()
	res2, err := o.ReadSession(ctx, incID)
	if err != nil {
		t.Fatalf("warm ReadSession: %v", err)
	}
	if len(res2.Turns) != 2 || res2.Turns[1].Content != "证据指向 dep-42。" || res2.Turns[0].Seq != 1 {
		t.Fatalf("second restore drifted: %+v", res2.Turns)
	}
	// llm_meta 只存 PG 真相（视图/热态不携带）；直接验库内 JSONB。
	var raw []byte
	if err := pool.QueryRow(ctx, `SELECT llm_meta FROM rca_session_turn WHERE tenant_id=$1 AND incident_id=$2 AND seq=2`,
		tenant, incID).Scan(&raw); err != nil {
		t.Fatalf("read llm_meta: %v", err)
	}
	if !strings.Contains(string(raw), "prompt_hash") || strings.Contains(string(raw), "证据指向") {
		t.Fatalf("llm_meta must carry hash/length only, got %s", raw)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM rca_session WHERE tenant_id=$1`, tenant); err != nil {
		t.Logf("cleanup: %v", err)
	}
}

// ---- REST 端到端（真装配 + 真 PG + miniredis 热态）----

// TestSessionEndToEndWithPG 任务验收脚本：建单→挂簇→GET /rca→POST 会话
// user 轮（无 LLM → pending）→真库 created_by 落表→FLUSHALL→GET 懒恢复轮次
// 完整；随后 OPS_SESSION=off 的同型装配零行为（503 + 两表零写入）。
func TestSessionEndToEndWithPG(t *testing.T) {
	dsn, pool := pgTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	suffix := time.Now().UnixNano()
	tenant := fmt.Sprintf("sess-e2e-%d", suffix)
	nodeKey := fmt.Sprintf("prometheus://nodes/sess-e2e-n1-%d", suffix)
	incID := fmt.Sprintf("INC-sess-e2e-%d", suffix)
	token := fmt.Sprintf("sess-e2e-token-%d", suffix)
	mr := miniredis.RunT(t)

	cfg := testAssemblyConfig(token)
	cfg.DB.DSN = dsn
	cfg.Tenant = tenant
	cfg.RCA.Enabled = true
	cfg.Session.Enabled = true
	cfg.Redis.Alert.Addr = mr.Addr()

	asm, err := NewAssembly(newQuietLogger(), cfg)
	if err != nil {
		t.Fatalf("assembly: %v", err)
	}
	defer asm.Close()
	if asm.Session == nil {
		t.Fatal("session not wired although OPS_SESSION=on with DB")
	}
	h := asm.Handler()

	cleanup := func() {
		cctx, cc := context.WithTimeout(context.Background(), 5*time.Second)
		defer cc()
		_, _ = pool.Exec(cctx, `DELETE FROM rca_session_turn WHERE tenant_id=$1`, tenant)
		_, _ = pool.Exec(cctx, `DELETE FROM rca_session WHERE tenant_id=$1`, tenant)
		_, _ = pool.Exec(cctx, `DELETE FROM incident_cluster WHERE incident_id=$1`, incID)
		_, _ = pool.Exec(cctx, `DELETE FROM incident WHERE tenant_id=$1`, tenant)
		_, _ = pool.Exec(cctx, `DELETE FROM incident_audit WHERE tenant_id=$1`, tenant)
	}
	cleanup()
	defer cleanup()

	// ① 发现入图 → ② 变更入库 → ③ 告警成簇（同 rca e2e 铺证据）。
	if err := asm.Sink.IngestDiscover(ctx, &connector.DiscoverResult{
		Nodes: []connector.ResourceNode{{Key: nodeKey, Type: "host",
			Labels: map[string]string{"instance": fmt.Sprintf("sess-e2e-%d", suffix)}}},
		TenantID: tenant}); err != nil {
		t.Fatalf("discover: %v", err)
	}
	if _, err := asm.Changes.Record(topology.ChangeEvent{
		ID: fmt.Sprintf("chg-%d", suffix), NodeKey: nodeKey, Type: topology.ChangeDeploy,
		Source: "jenkins", Author: "e2e-bot", Summary: "bad release",
		OccurredAt: time.Now().Add(-2 * time.Minute), Confidence: topology.ConfidenceHigh,
	}); err != nil {
		t.Fatalf("record change: %v", err)
	}
	asm.Noise.ProcessAlerts([]connector.Alert{{
		Fingerprint: "fp-sess",
		Labels:      map[string]string{"alertname": "DiskFull", "instance": fmt.Sprintf("sess-e2e-%d", suffix), "severity": "critical"},
		Severity:    "critical", StartsAt: time.Now(),
	}})
	active := asm.Noise.shadow.Clusterer().ActiveClusters()
	if len(active) == 0 {
		t.Fatal("no cluster formed")
	}
	// ④ PG 建单挂簇。
	inc, err := asm.Incidents.Create(incID, "E2E 复盘", "critical", "e2e")
	if err != nil {
		t.Fatalf("create incident: %v", err)
	}
	if err := asm.Incidents.AttachCluster(inc.ID, active[len(active)-1].Key); err != nil {
		t.Fatalf("attach cluster: %v", err)
	}
	// ⑤ GET /rca（先出报告，会话复用同链路）。
	req := httptest.NewRequest(http.MethodGet, "/api/v1/incidents/"+incID+"/rca?actor=e2e", nil)
	req.Header.Set(AuthHeader, token)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /rca: code = %d body = %s", rec.Code, rec.Body.String())
	}

	// ⑥ POST 会话 user 轮（LLM 未配置 → 显式 pending，user 轮仍落 PG）。
	sessPath := "/api/v1/incidents/" + incID + "/rca/session"
	postJSON := func(body string, token string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, sessPath, strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set(AuthHeader, token)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	w := postJSON(`{"content":"这次发布的哪些变更最可疑?","actor":"alice"}`, token)
	if w.Code != http.StatusOK {
		t.Fatalf("POST session: code = %d body = %s", w.Code, w.Body.String())
	}
	var pv sessionView
	if err := json.Unmarshal(w.Body.Bytes(), &pv); err != nil {
		t.Fatalf("decode POST: %v", err)
	}
	if pv.AssistantStatus != "pending" || len(pv.Turns) != 1 || pv.Persistence != "timescaledb" {
		t.Fatalf("no-llm POST view = %+v, want 1 turn + pending + timescaledb", pv)
	}

	// created_by/轮次真的在 PG（拍板⑤身份钩子）。
	var createdBy string
	if err := pool.QueryRow(ctx, `SELECT created_by FROM rca_session WHERE tenant_id=$1 AND incident_id=$2`,
		tenant, incID).Scan(&createdBy); err != nil {
		t.Fatalf("session row missing in PG: %v", err)
	}
	if createdBy != "alice" {
		t.Fatalf("created_by = %s, want alice", createdBy)
	}

	// ⑦ 蒸发热态 → GET 懒恢复：轮次完整、seq 连续。
	mr.FlushAll()
	g := httptest.NewRequest(http.MethodGet, sessPath, nil)
	g.Header.Set(AuthHeader, token)
	gw := httptest.NewRecorder()
	h.ServeHTTP(gw, g)
	if gw.Code != http.StatusOK {
		t.Fatalf("GET session: code = %d body = %s", gw.Code, gw.Body.String())
	}
	var gv sessionView
	if err := json.Unmarshal(gw.Body.Bytes(), &gv); err != nil {
		t.Fatalf("decode GET: %v", err)
	}
	if len(gv.Turns) != 1 || gv.Turns[0].Content != "这次发布的哪些变更最可疑?" || gv.Turns[0].Seq != 1 ||
		gv.Turns[0].CreatedBy != "alice" || gv.CreatedBy != "alice" {
		t.Fatalf("lazy restore view drifted: %+v", gv)
	}

	// ⑧ OPS_SESSION=off 零行为：同 DSN/租户换装配 → 503 + 两张新表零写入。
	cutoff := time.Now()
	cfgOff := testAssemblyConfig(token)
	cfgOff.DB.DSN = dsn
	cfgOff.Tenant = tenant
	cfgOff.RCA.Enabled = true // off 用例只关会话，RCA 保持在线（变量最小化）
	asmOff, err := NewAssembly(newQuietLogger(), cfgOff)
	if err != nil {
		t.Fatalf("off assembly: %v", err)
	}
	defer asmOff.Close()
	if asmOff.Session != nil || asmOff.SessionHot != nil {
		t.Fatal("off assembly must not construct session components")
	}
	ho := asmOff.Handler()
	rg := httptest.NewRequest(http.MethodGet, sessPath, nil)
	rg.Header.Set(AuthHeader, token)
	wg2 := httptest.NewRecorder()
	ho.ServeHTTP(wg2, rg)
	if wg2.Code != http.StatusServiceUnavailable {
		t.Fatalf("off GET: code = %d, want 503", wg2.Code)
	}
	rp := httptest.NewRequest(http.MethodPost, sessPath, strings.NewReader(`{"content":"q","actor":"mallory"}`))
	rp.Header.Set("Content-Type", "application/json")
	rp.Header.Set(AuthHeader, token)
	wp := httptest.NewRecorder()
	ho.ServeHTTP(wp, rp)
	if wp.Code != http.StatusServiceUnavailable {
		t.Fatalf("off POST: code = %d, want 503", wp.Code)
	}
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM rca_session_turn WHERE tenant_id=$1 AND created_at > $2`,
		tenant, cutoff).Scan(&n); err != nil {
		t.Fatalf("count writes: %v", err)
	}
	if n != 0 {
		t.Fatalf("OPS_SESSION=off must write ZERO turns, got %d", n)
	}
}
