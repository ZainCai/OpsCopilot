// W9 双链路装配级集成测试（OPS_TEST_PG_DSN 门控，无 DB 自动跳过）。
//
// 目的：证明"入队 → worker → 建单 → 审计"整链**真的接进了装配**——
// 此前这些组件只在单元测试里单独跑通，运行态从未被 NewAssembly 接线。
package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"opscopilot/internal/incident"
)

func TestAssemblyWiresIngestEndToEnd(t *testing.T) {
	dsn := os.Getenv("OPS_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("OPS_TEST_PG_DSN not set — assembly ingest integration skipped")
	}
	t.Setenv("OPS_DB_DSN", dsn)
	t.Setenv("OPS_INCIDENT_AUTOCREATE", "on") // 影子期关闭；集成验证需消费

	asm, err := NewAssembly(newQuietLogger(), "tk")
	if err != nil {
		t.Fatalf("assembly: %v", err)
	}
	defer asm.Close()

	// 装配必须把入队路由、队列、worker 全部接上（无 DB 时才为 nil）。
	if asm.Ingest == nil || asm.Worker == nil || asm.Queue == nil {
		t.Fatal("ingest pipeline not wired despite DB configured")
	}
	// DB 可用时事件 Store 必须是持久化实现，且审计随之落库。
	if asm.Incidents.Persistence() != "timescaledb" {
		t.Fatalf("persistence = %s, want timescaledb", asm.Incidents.Persistence())
	}
	if _, ok := asm.audit.(*PGAuditLog); !ok {
		t.Fatalf("audit = %T, want *PGAuditLog (shared pool)", asm.audit)
	}

	ctx := context.Background()
	fp := "fp-it-" + time.Now().Format("150405.000000")
	incID := "alertmanager:" + fp
	cleanup := func() {
		asm.pool.Exec(ctx, `DELETE FROM ingest_queue WHERE source_ref=$1`, fp)
		asm.pool.Exec(ctx, `DELETE FROM incident_audit WHERE incident_id=$1`, incID)
		asm.pool.Exec(ctx, `DELETE FROM incident WHERE incident_id=$1`, incID)
	}
	cleanup()
	defer cleanup()

	body := `{"status":"firing","alerts":[{"labels":{"alertname":"ITDown","severity":"critical"},` +
		`"annotations":{"summary":"integration down"},"startsAt":"2026-09-10T10:00:00Z",` +
		`"endsAt":"0001-01-01T00:00:00Z","fingerprint":"` + fp + `"}]}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/ingest/alertmanager", strings.NewReader(body))
	req.Header.Set(AuthHeader, "tk")
	rec := httptest.NewRecorder()
	asm.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("ingest: code=%d body=%s", rec.Code, rec.Body.String())
	}

	// 驱动一次消费（等价于 worker 的一个 tick）。
	asm.Worker.drain()

	inc, err := asm.Incidents.Get(incID)
	if err != nil {
		t.Fatalf("worker did not create incident: %v", err)
	}
	if inc.Origin != incident.OriginAlertmanager || inc.SourceRef != fp {
		t.Fatalf("incident wrong: origin=%s source_ref=%s", inc.Origin, inc.SourceRef)
	}

	// 审计留痕（PGAuditLog 经共享池写入）。
	hasCreate := false
	for _, e := range asm.audit.List(incID) {
		if e.Action == AuditCreate {
			hasCreate = true
		}
	}
	if !hasCreate {
		t.Fatalf("audit missing create entry for %s", incID)
	}

	// 队列已置 processed：再次 drain 不重复建单（幂等）。
	asm.Worker.drain()
	if n, err := asm.Queue.Pending(); err != nil || n != 0 {
		t.Fatalf("pending = %d (err %v), want 0 after processing", n, err)
	}
}

// TestNewAssemblyNilLoggerWithDB 回归：logger 传 nil 且配了 DB 时装配不得 panic。
// 历史缺陷：装配里直接取 nil 接口的方法值 `logger.Printf`（取方法值即解引用
// itab）会立即 panic——而同函数的注释恰好写着"不再直接调 logger.Printf"。
func TestNewAssemblyNilLoggerWithDB(t *testing.T) {
	dsn := os.Getenv("OPS_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("OPS_TEST_PG_DSN not set — assembly nil-logger integration skipped")
	}
	t.Setenv("OPS_DB_DSN", dsn)
	asm, err := NewAssembly(nil, "tk")
	if err != nil {
		t.Fatalf("assembly with nil logger: %v", err)
	}
	defer asm.Close()
	if asm.Incidents == nil || asm.Incidents.Persistence() != "timescaledb" {
		t.Fatalf("persistence = %v, want timescaledb", asm.Incidents.Persistence())
	}
}
