// rest_alerts_e2e_pg_test.go 告警中心 keyset 分页 + 时间过滤端到端（PG 真库，
// OPS_TEST_PG_DSN 门控，仓库惯例同 rest_timeline_e2e_pg_test.go）：
// 直插 alert_event → handleAlerts 逐页翻 → 断言不重不漏、游标终止、
// since/until 过滤、坏游标 400。
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
)

type alertRowView struct {
	OccurredAt  time.Time `json:"occurred_at"`
	Fingerprint string    `json:"fingerprint"`
}

type alertsResponse struct {
	Alerts     []alertRowView `json:"alerts"`
	Count      int            `json:"count"`
	NextCursor string         `json:"next_cursor"`
}

func TestAlertsPaginationWithPG(t *testing.T) {
	dsn := os.Getenv("OPS_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("OPS_TEST_PG_DSN not set — alerts pg end-to-end skipped")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Skipf("pg unreachable (create pool): %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("OPS_TEST_PG_DSN unreachable — skipped: %v", err)
	}

	tenant := fmt.Sprintf("alerts-e2e-%d", time.Now().UnixNano())
	base := time.Now().UTC().Truncate(time.Second).Add(-2 * time.Hour)
	const n = 7
	for i := 0; i < n; i++ {
		fp := fmt.Sprintf("%s-fp-%02d", tenant, i)
		if _, err := pool.Exec(ctx, `
INSERT INTO alert_event (tenant_id, fingerprint, source, occurred_at, payload)
VALUES ($1, $2, 'shadow', $3, '{"summary":"e2e","severity":"warning","node_key":"n1"}')`,
			tenant, fp, base.Add(time.Duration(i)*time.Second)); err != nil {
			t.Fatalf("insert alert: %v", err)
		}
	}
	defer func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM alert_event WHERE tenant_id=$1`, tenant)
		pool.Close()
	}()

	g := &RESTGateway{db: pool, tenant: tenant, logf: func(string, ...any) {}}

	fetch := func(path string) (int, alertsResponse) {
		t.Helper()
		rec := httptest.NewRecorder()
		g.handleAlerts(rec, httptest.NewRequest(http.MethodGet, path, nil))
		var body alertsResponse
		_ = json.Unmarshal(rec.Body.Bytes(), &body)
		return rec.Code, body
	}

	// 全量翻页：limit=3 → 3+3+1，不重不漏，末页游标为空、翻页终止。
	var all []string
	cur := ""
	for page := 0; ; page++ {
		path := "/api/v1/alerts?limit=3"
		if cur != "" {
			path += "&cursor=" + cur
		}
		code, body := fetch(path)
		if code != http.StatusOK {
			t.Fatalf("page %d: status %d", page, code)
		}
		for _, a := range body.Alerts {
			all = append(all, a.Fingerprint)
		}
		if body.NextCursor == "" {
			break
		}
		cur = body.NextCursor
		if page > 10 {
			t.Fatal("pagination did not terminate")
		}
	}
	if len(all) != n {
		t.Fatalf("collected %d rows across pages, want %d", len(all), n)
	}
	seen := map[string]bool{}
	for _, fp := range all {
		if seen[fp] {
			t.Fatalf("duplicate row across pages: %s", fp)
		}
		seen[fp] = true
	}

	// 时间过滤：since/until 框住 i=2..4 三条（occurred_at 递增）。
	mid := base.Add(2 * time.Second)
	hi := base.Add(4 * time.Second)
	code, body := fetch(fmt.Sprintf("/api/v1/alerts?since=%s&until=%s",
		mid.Format(time.RFC3339), hi.Format(time.RFC3339)))
	if code != http.StatusOK {
		t.Fatalf("filter status: %d", code)
	}
	if body.Count != 3 {
		t.Fatalf("filtered count = %d, want 3", body.Count)
	}
	if body.NextCursor != "" {
		t.Fatalf("filtered page should be single page, got cursor %q", body.NextCursor)
	}

	// 坏游标 → 400。
	path := "/api/v1/alerts?cursor=%21%21%21bad%21%21%21"
	if code, _ := fetch(path); code != http.StatusBadRequest {
		t.Fatalf("bad cursor status = %d, want 400", code)
	}
}
