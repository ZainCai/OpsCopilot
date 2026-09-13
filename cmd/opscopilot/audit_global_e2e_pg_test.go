// W12 审计解锁包：带 OPS_TEST_PG_DSN 的真库端到端——
// 装配（PG 真相源）→ 多事件多动作落审计（含 rca / autoattach 的
// attach_cluster）→ GET /api/v1/audit 全局列表时间倒序、过滤、游标、
// persistence=timescaledb（且不带 partial_hint——不伪装内存易失声明）、
// 000021 两条索引就位（读路径性能承诺的落库证据）。
package main

import (
	"context"
	"encoding/json"
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
)

func TestAuditGlobalEndToEndWithPG(t *testing.T) {
	dsn := os.Getenv("OPS_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("OPS_TEST_PG_DSN not set — audit global pg end-to-end skipped")
	}
	suffix := time.Now().UnixNano()
	tenant := fmt.Sprintf("audit-global-%d", suffix)

	cfg := testAssemblyConfig(fmt.Sprintf("audit-token-%d", suffix))
	cfg.DB.DSN = dsn
	cfg.Tenant = tenant
	cfg.Noise.Mode = config.NoiseModeEnforce
	cfg.Noise.AutoAttach = true // attach_cluster 动作的真实生产者（W10-6 语义）

	asm, err := NewAssembly(newQuietLogger(), cfg)
	if err != nil {
		t.Fatalf("assembly: %v", err)
	}
	defer asm.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	defer pool.Close()
	cleanup := func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer ccancel()
		_, _ = pool.Exec(cctx, `DELETE FROM incident_audit WHERE tenant_id=$1`, tenant)
		_, _ = pool.Exec(cctx, `DELETE FROM incident_cluster WHERE incident_row_id IN
			(SELECT id FROM incident WHERE tenant_id=$1)`, tenant)
		_, _ = pool.Exec(cctx, `DELETE FROM incident WHERE tenant_id=$1`, tenant)
	}
	cleanup()
	defer cleanup()

	// 000021 索引就位断言（迁移链路由 reset_test_pg.sh/scripts/migrate 应用，
	// 测试只验证读路径依赖真实存在）。
	for _, idx := range []string{"idx_incident_audit_tenant_time", "idx_incident_audit_tenant_actor_time"} {
		var found bool
		if err := pool.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM pg_indexes WHERE indexname=$1)`, idx).Scan(&found); err != nil {
			t.Fatalf("check index %s: %v", idx, err)
		}
		if !found {
			t.Fatalf("index %s missing — 跑 scripts/reset_test_pg.sh 应用 000021 后重试", idx)
		}
	}

	// 多事件多动作：自动链路（enforce 判决 new-incident → create +
	// attach_cluster，actor=system:*）+ 人工动作（transition/merge/rca），
	// 全部真落 incident_audit。
	labels := map[string]string{"alertname": "AuditGlobalPG", "instance": fmt.Sprintf("i-%d", suffix)}
	asm.Noise.ProcessAlerts([]connector.Alert{{
		Fingerprint: fmt.Sprintf("fp-audit-%d", suffix), Labels: labels,
		Severity: "critical", StartsAt: time.Now(),
	}})
	var incID string
	deadline := time.Now().Add(10 * time.Second)
	for {
		list, lerr := asm.Incidents.List("")
		if lerr != nil {
			t.Fatalf("incidents list: %v", lerr)
		}
		if len(list) == 1 {
			incID = list[0].ID
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("auto-attach incident not visible, got %+v", list)
		}
		time.Sleep(50 * time.Millisecond)
	}
	// 人工动作 + RCA 留痕（直接经装配的 AuditLog 追加——与 REST 写路径同款
	// 接口，PG 同步 Exec，返回即可见）。
	asm.audit.Append(AuditEntry{IncidentID: incID, Action: AuditTransition, Actor: "zhang",
		Detail: map[string]any{"to": "acked"}})
	asm.audit.Append(AuditEntry{IncidentID: incID, Action: AuditRCA, Actor: "li",
		Detail: map[string]any{"root_causes": 1, "findings": 3, "duration_ms": 900}})
	// create 行走真实 REST 写路径（链路 B 人工建单：POST 带 Token → audit
	// create，actor=created_by）——全局面同时覆盖 A/B 两链路。
	{
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/incidents",
			strings.NewReader(`{"title":"审计全局人工单","severity":"warning","created_by":"wang"}`))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(AuthHeader, cfg.Security.WebhookToken)
		asm.Handler().ServeHTTP(rec, req)
		if rec.Code != http.StatusCreated {
			t.Fatalf("create incident → %d %s", rec.Code, rec.Body.String())
		}
	}

	// --- GET /api/v1/audit ---
	get := func(query string) (int, map[string]any) {
		rec := httptest.NewRecorder()
		asm.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/audit"+query, nil))
		var body map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil && rec.Body.Len() > 0 {
			t.Fatalf("audit%s: non-JSON %q", query, rec.Body.String())
		}
		return rec.Code, body
	}
	code, body := get("")
	if code != http.StatusOK {
		t.Fatalf("global audit → %d %v", code, body)
	}
	if body["persistence"] != "timescaledb" {
		t.Fatalf("persistence = %v, want timescaledb", body["persistence"])
	}
	if _, leaked := body["partial_hint"]; leaked {
		t.Fatalf("partial_hint must be absent on durable backend: %v", leaked)
	}
	entries := decodeEntries(t, body)
	if len(entries) < 4 {
		t.Fatalf("expected ≥4 rows (create/attach_cluster/transition/rca), got %v", entries)
	}
	// 时间倒序（PG now() 微秒精度，跨动作严格可判）。
	for i := 1; i < len(entries); i++ {
		if mustTime(t, entries[i-1]["occurred_at"]).Before(mustTime(t, entries[i]["occurred_at"])) {
			t.Fatalf("rows %d,%d out of DESC order: %v then %v",
				i-1, i, entries[i-1]["occurred_at"], entries[i]["occurred_at"])
		}
	}
	// 含自动动作（autoattach 的 attach_cluster）与人工 rca——全局面不再"只见
	// 人工半边"。
	acts := map[string]bool{}
	for _, e := range entries {
		acts[fmt.Sprint(e["action"])] = true
	}
	for _, want := range []string{"create", "attach_cluster", "transition", "rca"} {
		if !acts[want] {
			t.Fatalf("global list missing action %q, got %v", want, acts)
		}
	}
	// action 过滤走真库 SQL。
	code, body = get("?action=attach_cluster")
	if code != http.StatusOK {
		t.Fatalf("filter → %d", code)
	}
	entries = decodeEntries(t, body)
	if len(entries) == 0 {
		t.Fatal("attach_cluster filter empty")
	}
	for _, e := range entries {
		if e["action"] != "attach_cluster" || fmt.Sprint(e["actor"]) != autoAttachActor {
			t.Fatalf("filter leak: %v", e)
		}
	}
	if !strings.HasPrefix(fmt.Sprint(entries[0]["summary"]), "挂簇 ") || entries[0]["incident_id"] != incID {
		t.Fatalf("attach summary/incident = %v / %v", entries[0]["summary"], entries[0]["incident_id"])
	}
	// actor 过滤（000021 第二条索引服务的确切形状）。
	code, body = get("?actor=" + autoAttachActor)
	if code != http.StatusOK {
		t.Fatalf("actor filter → %d", code)
	}
	if entries = decodeEntries(t, body); len(entries) == 0 {
		t.Fatal("actor filter empty")
	}
	// 游标翻页：limit=1 逐页走完，不重不漏 ≥4 条。
	seen := 0
	cur := ""
	for i := 0; i < 20; i++ {
		_, body = get(fmt.Sprintf("?limit=1&cursor=%s", cur))
		entries = decodeEntries(t, body)
		seen += len(entries)
		next, _ := body["next_cursor"].(string)
		if next == "" {
			break
		}
		cur = next
	}
	if seen < 4 {
		t.Fatalf("cursor walk saw %d rows, want >=4", seen)
	}
}

func decodeEntries(t *testing.T, body map[string]any) []map[string]any {
	t.Helper()
	raw, _ := json.Marshal(body["entries"])
	var out []map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("entries shape: %v", err)
	}
	return out
}

func mustTime(t *testing.T, s any) time.Time {
	t.Helper()
	v, err := time.Parse(time.RFC3339Nano, fmt.Sprint(s))
	if err != nil {
		t.Fatalf("bad occurred_at %q: %v", s, err)
	}
	return v
}
