// rca_auto_e2e_pg_test.go 二期池波二 #4 端到端（PG 真跑，OPS_TEST_PG_DSN
// 门控，仓库惯例同 rca_e2e_pg_test.go）：
//
//	发现入图 → 变更入库 → 告警成簇 → PG 建单（critical, open）挂簇 →
//	模拟"逾期升级"（escalationAfter 压到 1ms 手动 pollOnce）→
//	断言 incident_audit 出现 action='rca' **actor='auto'** 行（真的落 PG）、
//	conclusion 形态符合"LLM 未配置"（conclude pending：steps_pending≥1 且
//	llm_used=false）、且**恰一触发**（重复 pollOnce 不产生第二行——台账 +
//	双保险联合锁定）。
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"opscopilot/internal/connector"
	"opscopilot/internal/topology"
)

func TestRCAAutoTriggerEndToEndWithPG(t *testing.T) {
	dsn := os.Getenv("OPS_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("OPS_TEST_PG_DSN not set — rca auto-trigger pg end-to-end skipped")
	}
	suffix := time.Now().UnixNano()
	nodeKey := fmt.Sprintf("prometheus://nodes/rca-auto-n1-%d", suffix)
	incID := fmt.Sprintf("INC-rca-auto-%d", suffix)
	chgID := fmt.Sprintf("chg-rca-auto-%d", suffix)
	token := fmt.Sprintf("rca-auto-token-%d", suffix)

	cfg := testAssemblyConfig(token)
	cfg.DB.DSN = dsn
	cfg.Tenant = fmt.Sprintf("rca-auto-%d", suffix)
	cfg.RCA.Enabled = true
	cfg.RCA.Auto = true // 二期池波二 #4 开关（on 须 OPS_RCA=on，Validate 保证）
	cfg.Notify.EscalationEnabled = true
	cfg.Notify.EscalationAfter = time.Millisecond // "逾期"阈值压到瞬时（本轮手动驱动扫描）
	cfg.Notify.EscalationInterval = time.Hour     // 不起 ticker，pollOnce 直驱

	asm, err := NewAssembly(newQuietLogger(), cfg)
	if err != nil {
		t.Fatalf("assembly: %v", err)
	}
	defer asm.Close()
	if asm.RCA == nil || asm.RCATrigger == nil || asm.Escalation == nil || asm.Escalation.OnEscalate == nil {
		t.Fatalf("auto wiring incomplete: rca=%v trig=%v esc=%v hook=%v",
			asm.RCA != nil, asm.RCATrigger != nil, asm.Escalation != nil,
			asm.Escalation != nil && asm.Escalation.OnEscalate != nil)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 清理（评估库 + 独立租户，直删安全）。
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
		_, _ = p.Exec(cctx, `DELETE FROM incident_escalation WHERE incident_id=$1`, incID)
		_, _ = p.Exec(cctx, `DELETE FROM change_record WHERE event_id=$1`, chgID)
	}
	cleanup()
	defer cleanup()

	// ① 发现入图 + 变更入库（high 置信——与 #12 e2e 同口径拿 high 归因）。
	if err := asm.Sink.IngestDiscover(ctx, &connector.DiscoverResult{
		Nodes: []connector.ResourceNode{{
			Key: nodeKey, Type: "host",
			Labels: map[string]string{"instance": fmt.Sprintf("rca-auto-%d", suffix)},
		}}, TenantID: cfg.Tenant}); err != nil {
		t.Fatalf("discover: %v", err)
	}
	if _, err := asm.Changes.Record(topology.ChangeEvent{
		ID: chgID, NodeKey: nodeKey, Type: topology.ChangeDeploy,
		Source: "jenkins", Author: "e2e-bot", Summary: "auto-trigger suspect",
		OccurredAt: time.Now().Add(-2 * time.Minute),
		Confidence: topology.ConfidenceHigh,
	}); err != nil {
		t.Fatalf("record change: %v", err)
	}
	// ② 告警成簇 → 建单（critical/open）→ 挂簇。
	asm.Noise.ProcessAlerts([]connector.Alert{{
		Fingerprint: "fp-auto",
		Labels:      map[string]string{"alertname": "DiskFull", "instance": fmt.Sprintf("rca-auto-%d", suffix), "severity": "critical"},
		Severity:    "critical", StartsAt: time.Now(),
	}})
	active := asm.Noise.shadow.Clusterer().ActiveClusters()
	if len(active) == 0 {
		t.Fatal("no cluster formed from alert")
	}
	clusterKey := active[len(active)-1].Key
	inc, err := asm.Incidents.Create(incID, "自动触发 E2E", "critical", "e2e")
	if err != nil {
		t.Fatalf("create incident: %v", err)
	}
	if err := asm.Incidents.AttachCluster(inc.ID, clusterKey); err != nil {
		t.Fatalf("attach cluster: %v", err)
	}

	// ③ worker 起停（独立 goroutine——Trigger 只投递不等待）。
	go asm.RCATrigger.Run(ctx)

	// ④ 模拟逾期升级：等 CreatedAt 超过 EscalationAfter 后手动扫一轮。
	time.Sleep(20 * time.Millisecond)
	asm.Escalation.pollOnce(ctx)

	// ⑤ 断言 incident_audit 真落 PG：action=rca & actor=auto 恰一行的形态。
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("verify pool: %v", err)
	}
	defer pool.Close()
	type auditRow struct {
		actor  string
		detail map[string]any
	}
	var rows []auditRow
	deadline := time.Now().Add(20 * time.Second)
	for {
		rset, qerr := pool.Query(ctx, `
SELECT actor, detail FROM incident_audit
WHERE tenant_id=$1 AND incident_id=$2 AND action='rca'`, cfg.Tenant, incID)
		if qerr != nil {
			t.Fatalf("query audit: %v", qerr)
		}
		rows = rows[:0]
		for rset.Next() {
			var actor, detail string
			if serr := rset.Scan(&actor, &detail); serr != nil {
				rset.Close()
				t.Fatalf("scan audit: %v", serr)
			}
			var dm map[string]any
			_ = json.Unmarshal([]byte(detail), &dm)
			rows = append(rows, auditRow{actor: actor, detail: dm})
		}
		rset.Close()
		if len(rows) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("rca audit row not persisted within deadline")
		}
		time.Sleep(50 * time.Millisecond)
	}

	// ⑥ 恰一触发：auto 行有且只有一行，且 LLM 未配置形态（conclude pending）。
	autoRows := 0
	for _, r := range rows {
		if r.actor != autoTriggerActor {
			continue
		}
		autoRows++
		d := r.detail
		pending, _ := d["steps_pending"].(float64)
		if pending < 1 {
			t.Fatalf("OPS_LLM_* 未配置时 conclude 必须 pending（steps_pending>=1），detail=%v", d)
		}
		if used, _ := d["llm_used"].(bool); used {
			t.Fatalf("llm_used 必须 false（禁止伪 RCA），detail=%v", d)
		}
		if cn, _ := d["evidence"].(map[string]any); cn == nil || cn["changes"].(float64) < 1 {
			t.Fatalf("注入的变更必须进证据链，detail=%v", d)
		}
	}
	if autoRows != 1 {
		t.Fatalf("auto rca audit rows = %d, want exactly 1", autoRows)
	}

	// ⑦ 防风暴复扫：再扫两轮（通知台账幂等挡住重发）→ 审计仍是恰一 auto 行；
	// 直接再 Trigger 一次（内存 seen 已挡）也不产生第二行。
	asm.Escalation.pollOnce(ctx)
	asm.Escalation.pollOnce(ctx)
	time.Sleep(200 * time.Millisecond)
	var extra int
	if err := pool.QueryRow(ctx, `
SELECT count(*) FROM incident_audit
WHERE tenant_id=$1 AND incident_id=$2 AND action='rca' AND actor='auto'`,
		cfg.Tenant, incID).Scan(&extra); err != nil {
		t.Fatalf("count auto rows: %v", err)
	}
	if extra != 1 {
		t.Fatalf("auto rca rows after repeat scans = %d, want still 1 (storm-guard)", extra)
	}
}
