// rest_timeline_e2e_pg_test.go W10-1（F-03）事件混合时间线端到端（PG 真库，
// OPS_TEST_PG_DSN 门控，仓库惯例同 autoattach_e2e_pg_test.go）：
//
//	W10-6 生产路径（enforce+autoattach 建单挂簇）→ 注入变更 → 人工 ack →
//	GET /api/v1/incidents/{id}/timeline。
//
// P2-D3：断言段不再单独写——本文件是契约体 runTimelineHandlerContract 的
// PG 驱动器（expectAlert=true、minActions=2：生产路径多一条 attach_cluster
// 审计），与 mem 装配（rest_timeline_test.go）共用同一套断言，防双份单侧
// 测漂移。装配保留生产全链路（真库 + PGClusterSink 判决落库 + 自动建单
// 挂簇 + transition 写审计）。
package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"opscopilot/internal/config"
	"opscopilot/internal/connector"
	"opscopilot/internal/topology"
)

func TestTimelineEndToEndWithPG(t *testing.T) {
	dsn := os.Getenv("OPS_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("OPS_TEST_PG_DSN not set — timeline pg end-to-end skipped")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Skipf("pg unreachable (create pool): %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("OPS_TEST_PG_DSN unreachable — skipped: %v（先跑 bash scripts/reset_test_pg.sh）", err)
	}
	pool.Close()

	runTimelineHandlerContract(t, timelineContractEnv{
		name: "pg",
		newHandler: func(t *testing.T) (http.Handler, string) {
			t.Helper()
			suffix := time.Now().UnixNano()
			token := fmt.Sprintf("tl-e2e-token-%d", suffix)
			nodeKey := fmt.Sprintf("prometheus://nodes/tl-e2e-%d", suffix)
			instLabel := fmt.Sprintf("tl-e2e-%d", suffix)
			chgID := fmt.Sprintf("chg-tl-e2e-%d", suffix)

			cfg := testAssemblyConfig(token)
			cfg.DB.DSN = dsn
			cfg.Tenant = fmt.Sprintf("tl-e2e-%d", suffix)
			cfg.Noise.Mode = config.NoiseModeEnforce
			cfg.Noise.AutoAttach = true // W10-6 生产自动挂簇：本用例的建单+挂簇路径

			asm, err := NewAssembly(newQuietLogger(), cfg)
			if err != nil {
				t.Fatalf("assembly: %v", err)
			}
			t.Cleanup(func() { asm.Close() })
			h := asm.Handler()

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
				_, _ = p.Exec(cctx, `DELETE FROM alert_event WHERE tenant_id=$1`, cfg.Tenant)
				_, _ = p.Exec(cctx, `DELETE FROM change_record WHERE tenant_id=$1`, cfg.Tenant)
			}
			cleanup()
			t.Cleanup(cleanup)

			// ① 发现入图（变更 nodeCheck 要求节点先在拓扑）。
			if err := asm.Sink.IngestDiscover(ctx, &connector.DiscoverResult{
				Nodes: []connector.ResourceNode{{
					Key: nodeKey, Type: "host",
					Labels: map[string]string{"instance": instLabel},
				}}, TenantID: cfg.Tenant,
			}); err != nil {
				t.Fatalf("discover: %v", err)
			}

			// ② 变更入库（故障发生**前** 2 分钟的部署——变更回看窗口的靶心）。
			if _, err := asm.Changes.Record(topology.ChangeEvent{
				ID: chgID, NodeKey: nodeKey, Type: topology.ChangeDeploy,
				Source: "jenkins", Author: "e2e-bot", Summary: "timeline e2e deploy",
				OccurredAt: time.Now().Add(-2 * time.Minute),
				Confidence: topology.ConfidenceHigh,
			}); err != nil {
				t.Fatalf("record change: %v", err)
			}

			// ②′ 判决落库出口（镜像 main.go 的 DB 部署形态：NewAssembly 不挂
			// sink，真进程由 main 挂 PGClusterSink——不挂则影子判决永不进
			// alert_event，W6-1 起的装配事实）。
			psink, err := NewPGClusterSink(ctx, dsn, cfg.Tenant)
			if err != nil {
				t.Fatalf("pg verdict sink: %v", err)
			}
			t.Cleanup(func() { psink.Close() })
			asm.Noise.SetVerdictSink(psink)

			// ③ W10-6 生产路径：告警 → enforce new-incident → 自动建单+挂簇+审计。
			asm.Noise.ProcessAlerts([]connector.Alert{{
				Fingerprint: fmt.Sprintf("fp-tl-%d", suffix),
				Labels:      map[string]string{"alertname": "DiskFull", "instance": instLabel, "severity": "critical"},
				Severity:    "critical", StartsAt: time.Now(),
			}})
			deadline := time.Now().Add(15 * time.Second)
			var incID string
			for {
				list, lerr := asm.Incidents.List("")
				if lerr != nil {
					t.Fatalf("list: %v", lerr)
				}
				if len(list) == 1 && len(list[0].ClusterKeys) == 1 {
					incID = list[0].ID
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("autoattach 建单挂簇未出现，got %+v", list)
				}
				time.Sleep(50 * time.Millisecond)
			}

			// ④ 人工处置：POST transition ack（写路径鉴权 + requireJSON）。
			req := httptest.NewRequest(http.MethodPost, "/api/v1/incidents/"+incID+"/transition",
				strings.NewReader(`{"to":"acked","actor":"ops-e2e"}`))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set(AuthHeader, token)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				t.Fatalf("transition: code = %d body = %s", rec.Code, rec.Body.String())
			}
			return h, incID
		},
		expectAlert: true, // PG 生产路径三源齐备（契约体轮询 alert_in 落库）
		minActions:  2,    // attach_cluster（system:autoattach）+ transition（ops-e2e）
	})
}
