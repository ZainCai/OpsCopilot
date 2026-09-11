// D8 决策 C：保留策略测试 —— resolved 满期归档（事件+簇+审计 打包），
// 原行清除。保留期解析（ParseRetention）及其表驱动测试已随 #2 收敛进
// internal/config（唯一实现 + 唯一测试），本文件只测归档扫描行为。
package main

import (
	"context"
	"os"
	"testing"
	"time"

	"opscopilot/internal/incident"
)

// TestRetentionSweepArchivesResolved PG 门控：resolved 满期的事件被归档
// （payload 含 事件本体 + 簇 + 审计），原行与审计行被清除。
func TestRetentionSweepArchivesResolved(t *testing.T) {
	dsn := os.Getenv("OPS_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("OPS_TEST_PG_DSN not set — retention integration skipped")
	}
	cfg := testAssemblyConfig("tk")
	cfg.DB.DSN = dsn // #2：DSN 经 Config 注入
	asm, err := NewAssembly(newQuietLogger(), cfg)
	if err != nil {
		t.Fatalf("assembly: %v", err)
	}
	defer asm.Close()
	ctx := context.Background()

	// 清历史残留（前缀清理，含上一次失败运行留下的行）。
	cleanup := func() {
		asm.pool.Exec(ctx, `DELETE FROM incident_archive WHERE incident_id LIKE 'RET-%'`)
		asm.pool.Exec(ctx, `DELETE FROM incident WHERE incident_id LIKE 'RET-%'`)
	}
	cleanup()
	t.Cleanup(cleanup)

	ref := "RET-" + time.Now().Format("150405.000000")
	inc, err := asm.Incidents.Create(ref, "retention probe", "critical", "ops")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := asm.Incidents.AttachCluster(inc.ID, "c:ret@1"); err != nil {
		t.Fatalf("attach: %v", err)
	}
	if _, err := asm.Incidents.Transition(inc.ID, incident.StateResolved, "ops"); err != nil {
		t.Fatalf("resolve: %v", err)
	}
	// 回拨 resolved_at（模拟"早就该归档"），并补一条人工审计。
	if _, err := asm.pool.Exec(ctx,
		`UPDATE incident SET resolved_at = now() - interval '200 days' WHERE tenant_id=$1 AND incident_id=$2`,
		testTenant, ref); err != nil {
		t.Fatalf("backdate: %v", err)
	}
	asm.audit.Append(AuditEntry{IncidentID: ref, Action: AuditTransition, Actor: "ops",
		Detail: map[string]any{"to": "resolved"}})

	sw := NewRetentionSweeper(asm.pool, testTenant, 90*24*time.Hour, nil)
	n, err := sw.sweepOnce(ctx)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if n < 1 {
		t.Fatalf("archived %d, want >=1", n)
	}
	// 原表已无此单。
	var cnt int
	if err := asm.pool.QueryRow(ctx,
		`SELECT count(*) FROM incident WHERE tenant_id=$1 AND incident_id=$2`,
		testTenant, ref).Scan(&cnt); err != nil {
		t.Fatal(err)
	}
	if cnt != 0 {
		t.Fatal("archived incident must be removed from incident")
	}
	// 审计行已随单清除。
	if err := asm.pool.QueryRow(ctx,
		`SELECT count(*) FROM incident_audit WHERE tenant_id=$1 AND incident_id=$2`,
		testTenant, ref).Scan(&cnt); err != nil {
		t.Fatal(err)
	}
	if cnt != 0 {
		t.Fatal("archived incident's audit rows must be removed")
	}
	// 归档表里有完整快照：本体 + 簇 + 审计。
	var payload map[string]any
	if err := asm.pool.QueryRow(ctx,
		`SELECT payload FROM incident_archive WHERE tenant_id=$1 AND incident_id=$2`,
		testTenant, ref).Scan(&payload); err != nil {
		t.Fatalf("archive row: %v", err)
	}
	incObj, _ := payload["incident"].(map[string]any)
	if incObj == nil || incObj["title"] != "retention probe" {
		t.Fatalf("archive payload missing incident: %v", payload)
	}
	clusters, _ := payload["clusters"].([]any)
	if len(clusters) != 1 || clusters[0] != "c:ret@1" {
		t.Fatalf("archive payload clusters = %v, want [c:ret@1]", clusters)
	}
	auditArr, _ := payload["audit"].([]any)
	if len(auditArr) == 0 {
		t.Fatal("archive payload must embed audit entries")
	}
}
