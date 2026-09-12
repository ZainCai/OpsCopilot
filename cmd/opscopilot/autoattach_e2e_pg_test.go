// autoattach_e2e_pg_test.go W10-6 簇→事件生产自动挂簇端到端（PG 真库，
// OPS_TEST_PG_DSN 门控，仓库惯例同 rca_auto_e2e_pg_test.go）：
//
//	装配开 OPS_AUTOATTACH（enforce）→ ProcessAlerts new-incident 判决 →
//	断言 PG 里事件按链路 A 同款幂等键（origin=prometheus，
//	source_ref=promFingerprint(labels)）建成且 incident_cluster 挂上首簇、
//	incident_audit 出现 action='attach_cluster' actor='system:autoattach'
//	行（真的落 PG）、窗口内重复告警不产生第二单/第二次挂簇（幂等+去重）。
package main

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"opscopilot/internal/config"
	"opscopilot/internal/connector"
	"opscopilot/internal/incident"
)

func TestAutoAttachEndToEndWithPG(t *testing.T) {
	dsn := os.Getenv("OPS_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("OPS_TEST_PG_DSN not set — autoattach pg end-to-end skipped")
	}
	suffix := time.Now().UnixNano()

	cfg := testAssemblyConfig(fmt.Sprintf("autoattach-token-%d", suffix))
	cfg.DB.DSN = dsn
	cfg.Tenant = fmt.Sprintf("autoattach-%d", suffix)
	cfg.Noise.Mode = config.NoiseModeEnforce
	cfg.Noise.AutoAttach = true // W10-6 开关（Validate 保证 on ⇒ enforce+启用）

	asm, err := NewAssembly(newQuietLogger(), cfg)
	if err != nil {
		t.Fatalf("assembly: %v", err)
	}
	defer asm.Close()
	if asm.Noise == nil {
		t.Fatal("noise engine expected")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cleanup := func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer ccancel()
		p, perr := pgxpool.New(cctx, dsn)
		if perr != nil {
			return
		}
		defer p.Close()
		_, _ = p.Exec(cctx, `DELETE FROM incident_cluster WHERE incident_row_id IN
			(SELECT id FROM incident WHERE tenant_id=$1)`, cfg.Tenant)
		_, _ = p.Exec(cctx, `DELETE FROM incident_audit WHERE tenant_id=$1`, cfg.Tenant)
		_, _ = p.Exec(cctx, `DELETE FROM incident WHERE tenant_id=$1`, cfg.Tenant)
	}
	cleanup()
	defer cleanup()

	labels := map[string]string{"alertname": "AutoAttachPG", "instance": fmt.Sprintf("i-%d", suffix)}
	now := time.Now()
	asm.Noise.ProcessAlerts([]connector.Alert{{
		Fingerprint: fmt.Sprintf("fp-pg-%d", suffix), Labels: labels,
		Severity: "critical", StartsAt: now,
	}})

	wantRef := promFingerprint(labels)
	var inc incident.Incident
	deadline := time.Now().Add(10 * time.Second)
	for {
		list, lerr := asm.Incidents.List("")
		if lerr != nil {
			t.Fatalf("list: %v", lerr)
		}
		if len(list) == 1 {
			inc = list[0]
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("PG 事件未按 new-incident 联动建出，got %d 条", len(list))
		}
		time.Sleep(50 * time.Millisecond)
	}
	if inc.SourceRef != wantRef || inc.Origin != incident.OriginPrometheus {
		t.Fatalf("建单幂等键 = (%q,%q), want (prometheus,%q)", inc.Origin, inc.SourceRef, wantRef)
	}
	if inc.CreatedBy != autoAttachActor {
		t.Fatalf("created_by = %q", inc.CreatedBy)
	}
	if len(inc.ClusterKeys) != 1 {
		t.Fatalf("首簇应挂上，got %v", inc.ClusterKeys)
	}

	// 审计真落 PG：action='attach_cluster' & actor='system:autoattach' 恰一行。
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("verify pool: %v", err)
	}
	defer pool.Close()
	var audits int
	if err := pool.QueryRow(ctx, `
SELECT count(*) FROM incident_audit
WHERE tenant_id=$1 AND incident_id=$2 AND action='attach_cluster' AND actor=$3`,
		cfg.Tenant, inc.ID, autoAttachActor).Scan(&audits); err != nil {
		t.Fatalf("count audit: %v", err)
	}
	if audits != 1 {
		t.Fatalf("attach_cluster 审计行数 = %d, want 1", audits)
	}

	// 窗口内重复告警：dedup 判决不再触发建单/挂簇（幂等链不产生副本）。
	asm.Noise.ProcessAlerts([]connector.Alert{{
		Fingerprint: fmt.Sprintf("fp-pg-%d", suffix), Labels: labels,
		Severity: "critical", StartsAt: now.Add(time.Second),
	}})
	list2, err := asm.Incidents.List("")
	if err != nil {
		t.Fatalf("list after replay: %v", err)
	}
	if len(list2) != 1 || len(list2[0].ClusterKeys) != 1 {
		t.Fatalf("重复告警后应仍 1 单/1 簇，got %d 单", len(list2))
	}
	if got := asm.Metrics.AutoAttach["attached"].Value(); got != 1 {
		t.Fatalf("attached 计数 = %d, want 1", got)
	}
	if got := asm.Metrics.AutoAttach["conflict"].Value() + asm.Metrics.AutoAttach["skipped"].Value(); got != 0 {
		t.Fatalf("conflict+skipped = %d, want 0", got)
	}
}
