// rest_alerts_test.go 告警中心端点分页/过滤的契约测试（无 DB 可跑）：
// 游标编解码 round-trip 与坏游标拒绝 + 参数校验 400 路径（校验先于查询，
// 用懒连接池构造 RESTGateway 即可覆盖，不触达 DB）；真库翻页端到端见
// rest_alerts_e2e_pg_test.go（OPS_TEST_PG_DSN 门控）。
package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// testAlertGateway 构造仅用于校验路径的 RESTGateway：db 指向不可达端口的
// 懒连接池（pgxpool.New 不建连，400 校验路径不会触达查询）。
func testAlertGateway(t *testing.T) *RESTGateway {
	t.Helper()
	pool, err := pgxpool.New(context.Background(),
		"postgres://alertstest:alertstest@127.0.0.1:1/alertstest?sslmode=disable")
	if err != nil {
		t.Fatalf("create lazy pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return &RESTGateway{db: pool, tenant: "t-alerts", logf: func(string, ...any) {}}
}

func TestAlertCursorRoundTrip(t *testing.T) {
	at := time.Date(2026, 9, 14, 10, 30, 0, 123456789, time.UTC)
	cases := []struct {
		at time.Time
		id int64
	}{
		{at, 42},
		{at.Add(-3 * time.Hour), 1},
		{time.Unix(0, 0).UTC(), 9223372036854775807},
	}
	for _, c := range cases {
		got, id, err := decodeAlertCursor(encodeAlertCursor(c.at, c.id))
		if err != nil {
			t.Fatalf("round trip %v/%d: %v", c.at, c.id, err)
		}
		if !got.Equal(c.at) || id != c.id {
			t.Fatalf("round trip = %v/%d, want %v/%d", got, id, c.at, c.id)
		}
	}
}

func TestAlertCursorRejectsBadInput(t *testing.T) {
	bad := []string{
		"!!!not-base64!!!",
		"abc123",                       // base64 合法但内容无分隔符
		"bm9zZXBhcmF0b3I=",             // "noseparator"
		"MjAyNi0wOS0xNFQxMDozMDowMFo=", // 只有时间戳，无 \x1f id
	}
	for _, s := range bad {
		if _, _, err := decodeAlertCursor(s); err == nil {
			t.Errorf("decode(%q) should fail", s)
		}
	}
}

func TestAlertsRejectBadParams(t *testing.T) {
	g := testAlertGateway(t)
	cases := []struct {
		path string
	}{
		{"/api/v1/alerts?limit=abc"},
		{"/api/v1/alerts?limit=0"},
		{"/api/v1/alerts?limit=-5"},
		{"/api/v1/alerts?since=not-a-time"},
		{"/api/v1/alerts?until=not-a-time"},
		{"/api/v1/alerts?since=2026-09-14T10:00:00Z&until=2026-09-14T09:00:00Z"},
		{"/api/v1/alerts?cursor=!!!bad!!!"},
		{"/api/v1/alerts?cursor=bm9zZXBhcmF0b3I="}, // 合法 base64 但无 id 段
	}
	for _, c := range cases {
		rec := httptest.NewRecorder()
		g.handleAlerts(rec, httptest.NewRequest(http.MethodGet, c.path, nil))
		if rec.Code != http.StatusBadRequest {
			t.Errorf("GET %s = %d, want 400 (body=%s)", c.path, rec.Code, rec.Body.String())
		}
	}
}
