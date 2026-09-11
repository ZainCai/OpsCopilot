// W9-5（第八轮审核 D1）：审计查询错误透传的测试。
package main

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

// mustAuditList 取审计列表；测试中失败即 Fatal——顺带把"List 现在会返回错误"
// 这件事在每个调用点都显式化。
func mustAuditList(t *testing.T, l AuditLog, incidentID string) []AuditEntry {
	t.Helper()
	entries, err := l.List(incidentID)
	if err != nil {
		t.Fatalf("audit list(%q): %v", incidentID, err)
	}
	return entries
}

// failingAuditLog 恒失败的审计实现（模拟"后端不可用"）。
type failingAuditLog struct{}

func (failingAuditLog) Append(AuditEntry) {}

func (failingAuditLog) List(string) ([]AuditEntry, error) {
	return nil, fmt.Errorf("%w: boom: host=secret-db.internal", ErrAuditUnavailable)
}

// TestMemAuditListNoError 内存实现恒不失败（error 为 nil，统一接口形态）。
func TestMemAuditListNoError(t *testing.T) {
	l := NewMemAuditLog()
	l.Append(AuditEntry{IncidentID: "A", Action: AuditCreate, Actor: "ops"})
	l.Append(AuditEntry{IncidentID: "B", Action: AuditCreate, Actor: "ops"})

	all, err := l.List("")
	if err != nil {
		t.Fatalf("mem list all: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("len(all)=%d, want 2", len(all))
	}
	one, err := l.List("A")
	if err != nil {
		t.Fatalf("mem list A: %v", err)
	}
	if len(one) != 1 || one[0].IncidentID != "A" {
		t.Fatalf("got %+v, want single entry for A", one)
	}
}

// TestPGAuditListSurfacesUnavailable 后端不可用必须返回**类型化错误**，
// 绝不返回空列表（D1 的核心：故障不能被静默降级成"没有记录"）。
func TestPGAuditListSurfacesUnavailable(t *testing.T) {
	l := &PGAuditLog{} // 无池：模拟后端不可用
	entries, err := l.List("X")
	if err == nil {
		t.Fatal("nil pool must surface an error, not an empty list (D1)")
	}
	if !errors.Is(err, ErrAuditUnavailable) {
		t.Fatalf("err = %v, want wrapped ErrAuditUnavailable", err)
	}
	if entries != nil {
		t.Fatalf("entries = %+v, want nil alongside the error", entries)
	}
}

// TestAuditEndpointSurfacesBackendFailure PG 门控：审计后端故障时 REST 必须回
// 500（而不是 200 + 空列表），且 DB 细节不外泄（D5 口径）。
func TestAuditEndpointSurfacesBackendFailure(t *testing.T) {
	dsn := os.Getenv("OPS_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("OPS_TEST_PG_DSN not set")
	}
	cfg := testAssemblyConfig("")
	cfg.DB.DSN = dsn // #2：DSN 经 Config 注入，测试不依赖全局 env
	asm, err := NewAssembly(newQuietLogger(), cfg)
	if err != nil {
		t.Fatalf("assembly: %v", err)
	}
	defer asm.Close()

	// 换成恒失败的审计实现（同一接口），专测 HTTP 层的错误分支。
	asm.REST.audit = failingAuditLog{}

	rec := httptest.NewRecorder()
	asm.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/incidents/X/audit", nil))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("code = %d, want 500 (backend failure must not look like an empty audit trail)", rec.Code)
	}
	if body := rec.Body.String(); strings.Contains(body, "secret-db") || strings.Contains(body, "boom") {
		t.Fatalf("DB error detail leaked to client: %s", body)
	}
}
