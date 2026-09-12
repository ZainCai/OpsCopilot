// 优化方案 #12/ADR-014 端到端（PG 真跑，OPS_TEST_PG_DSN 门控，仓库惯例
// 同 assembly_change_test.go：未设置即 skip）：
//
//	发现入图 → 变更 webhook 同源入库（PGChangeStore：真相源 + 回放）→
//	告警成簇 → 建单挂簇 → GET /api/v1/incidents/{id}/rca →
//	断言证据链非空 + 根因归因 + 审计 action='rca' **真的落进 PG**
//	（验证 migration 000017 扩 CHECK 生效——内存用例覆盖不到这一层）。
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"opscopilot/internal/connector"
	"opscopilot/internal/topology"
)

func TestRCAEndToEndWithPG(t *testing.T) {
	dsn := os.Getenv("OPS_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("OPS_TEST_PG_DSN not set — rca pg end-to-end skipped")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Skipf("pg unreachable (create pool): %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("OPS_TEST_PG_DSN unreachable — skipped: %v", err)
	}
	pool.Close() // 装配自持连接池，测试不共用

	suffix := time.Now().UnixNano()
	nodeKey := fmt.Sprintf("prometheus://nodes/rca-e2e-n1-%d", suffix)
	incID := fmt.Sprintf("INC-rca-e2e-%d", suffix)
	chgID := fmt.Sprintf("chg-rca-e2e-%d", suffix)
	token := fmt.Sprintf("rca-e2e-token-%d", suffix)

	// 独立租户，避免与常驻实例互污（评估/演示纪律，见 .env.example 注释）。
	cfg := testAssemblyConfig(token)
	cfg.DB.DSN = dsn
	cfg.Tenant = fmt.Sprintf("rca-e2e-%d", suffix)
	cfg.RCA.Enabled = true

	asm, err := NewAssembly(newQuietLogger(), cfg)
	if err != nil {
		t.Fatalf("assembly: %v", err)
	}
	defer asm.Close()
	if asm.RCA == nil {
		t.Fatal("rca orchestrator not wired although OPS_RCA on")
	}
	if asm.Changes.Persistence() != "timescaledb" {
		t.Fatalf("changes backend = %s, want timescaledb（#4 持久化装配未生效）", asm.Changes.Persistence())
	}

	// 清理（defer 先行注册，DSN 库为评估环境，独立租户行可安全直删）。
	cleanup := func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer ccancel()
		p, perr := pgxpool.New(cctx, dsn)
		if perr != nil {
			return
		}
		defer p.Close()
		_, _ = p.Exec(cctx, `DELETE FROM incident_cluster WHERE incident_id=$1`, incID)
		_, _ = p.Exec(cctx, `DELETE FROM incident WHERE incident_id=$1`, incID)
		_, _ = p.Exec(cctx, `DELETE FROM incident_audit WHERE incident_id=$1`, incID)
		_, _ = p.Exec(cctx, `DELETE FROM change_record WHERE event_id=$1`, chgID)
	}
	cleanup()
	defer cleanup()

	h := asm.Handler()

	// ① 发现入图（变更严格节点校验要求节点先进拓扑——"先发现、后变更"）。
	if err := asm.Sink.IngestDiscover(ctx, &connector.DiscoverResult{
		Nodes: []connector.ResourceNode{{
			Key: nodeKey, Type: "host",
			Labels: map[string]string{"instance": fmt.Sprintf("rca-e2e-%d", suffix)},
		}}, TenantID: cfg.Tenant}); err != nil {
		t.Fatalf("discover: %v", err)
	}

	// ② 变更入库（走 ChangeBackend——与 webhook 同一 Record 校验路径）。
	instLabel := fmt.Sprintf("rca-e2e-%d", suffix)
	// Confidence=high：模拟"变更平台同机直采"的直接观测声明（change.go
	// 文件头约定）——规则链只有"high 变更 × 非 low 节点 × 命中本体"才给
	// high 归因（root_causes），medium 声明会止步于候选（宁缺毋滥）。
	if _, err := asm.Changes.Record(topology.ChangeEvent{
		ID: chgID, NodeKey: nodeKey, Type: topology.ChangeDeploy,
		Source: "jenkins", Author: "e2e-bot", Summary: "root cause deploy",
		OccurredAt: time.Now().Add(-2 * time.Minute),
		Confidence: topology.ConfidenceHigh,
	}); err != nil {
		t.Fatalf("record change: %v", err)
	}

	// ③ 告警成簇（instance 反查节点 → 故障域含 nodeKey）。
	asm.Noise.ProcessAlerts([]connector.Alert{{
		Fingerprint: "fp-e2e",
		Labels:      map[string]string{"alertname": "DiskFull", "instance": instLabel, "severity": "critical"},
		Severity:    "critical", StartsAt: time.Now(),
	}})
	active := asm.Noise.shadow.Clusterer().ActiveClusters()
	if len(active) == 0 {
		t.Fatal("no cluster formed from alert")
	}
	clusterKey := active[len(active)-1].Key

	// ④ PG 事件链路建单 + 挂簇（Incidents 是 PGStore + publish 装饰）。
	inc, err := asm.Incidents.Create(incID, "E2E 磁盘打满", "critical", "e2e")
	if err != nil {
		t.Fatalf("create incident: %v", err)
	}
	if err := asm.Incidents.AttachCluster(inc.ID, clusterKey); err != nil {
		t.Fatalf("attach cluster: %v", err)
	}

	// ⑤ REST 门面：GET /api/v1/incidents/{id}/rca（Token 门禁）。
	req := httptest.NewRequest(http.MethodGet, "/api/v1/incidents/"+incID+"/rca?actor=e2e", nil)
	req.Header.Set(AuthHeader, token)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("rca: code = %d body = %s", rec.Code, rec.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	// 证据链非空：邻域节点 ≥1、窗口变更 ≥1。
	ev, _ := body["evidence"].(map[string]any)
	if ev == nil || ev["nodes"].(float64) < 1 || ev["changes"].(float64) < 1 {
		t.Fatalf("evidence chain empty: %v", body["evidence"])
	}
	roots, _ := body["root_causes"].([]any)
	if len(roots) != 1 {
		findings, _ := body["findings"].([]any)
		t.Fatalf("root_causes = %v (findings %+v), want 1", body["root_causes"], findings)
	}
	if r0 := roots[0].(map[string]any); r0["ref"] != chgID {
		t.Fatalf("root cause ref = %v, want %s", r0["ref"], chgID)
	}
	if steps, _ := body["steps"].([]any); len(steps) != 6 {
		t.Fatalf("steps = %v, want 6", steps)
	}
	// persistence 透出真相源形态（R6-4 口径）。
	if body["persistence"] != "timescaledb" {
		t.Fatalf("persistence = %v, want timescaledb", body["persistence"])
	}

	// ⑥ 审计真的落 PG：action='rca' 行存在（000017 CHECK 生效的证据）。
	p, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("verify pool: %v", err)
	}
	defer p.Close()
	deadline := time.Now().Add(5 * time.Second)
	var action, actor string
	for {
		err := p.QueryRow(ctx, `
SELECT action, actor FROM incident_audit
WHERE tenant_id=$1 AND incident_id=$2 AND action='rca' LIMIT 1`, cfg.Tenant, incID).Scan(&action, &actor)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("rca audit row not persisted: %v", err)
		}
		time.Sleep(50 * time.Millisecond) // PGAuditLog 异步语义下的短暂等待（正常路径毫秒级）
	}
	if action != "rca" || actor != "e2e" {
		t.Fatalf("audit row = (%s,%s), want (rca,e2e)", action, actor)
	}
	// 审计 List 通道（REST 同款读径）也能取到。
	entries, err := asm.audit.List(incID)
	if err != nil {
		t.Fatalf("audit list: %v", err)
	}
	var sawRCA bool
	for _, e := range entries {
		if e.Action == AuditRCA {
			sawRCA = true
		}
	}
	if !sawRCA {
		t.Fatalf("audit.List missing rca entry: %+v", entries)
	}
}
