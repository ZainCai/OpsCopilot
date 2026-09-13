// rest_runbook_e2e_pg_test.go W11-4（F-12）Runbook 记录版全链闭环（PG 真库，
// OPS_TEST_PG_DSN 门控，仓库惯例同 rest_timeline_e2e_pg_test.go）：
//
//	建单 → 建手册 → 挂到事件（重复挂载幂等）→ 记一次执行（只记不执行）→
//	GET 挂载列表/执行列表全链断言 → 解挂后历史仍在。
//
// 覆盖错误面：404（无单/无手册/未挂载点记执行/重复解挂）、409（重复手册 id）、
// 400（refs 非数组）、201/200/503 之外的 persistence=timescaledb 透出。
// 缺库时先 bash scripts/reset_test_pg.sh。
package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// runbookE2EAssembly DSN 门控装配：唯一租户键空间 + t.Cleanup 物理清行。
func runbookE2EAssembly(t *testing.T, token string) (*Assembly, http.Handler) {
	t.Helper()
	dsn := os.Getenv("OPS_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("OPS_TEST_PG_DSN not set — runbook pg end-to-end skipped")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Skipf("pg unreachable (create pool): %v（先跑 bash scripts/reset_test_pg.sh）", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("OPS_TEST_PG_DSN unreachable — skipped: %v（先跑 bash scripts/reset_test_pg.sh）", err)
	}
	pool.Close()

	cfg := testAssemblyConfig(token)
	cfg.DB.DSN = dsn
	cfg.Tenant = fmt.Sprintf("rb-e2e-%d", time.Now().UnixNano())
	asm, err := NewAssembly(newQuietLogger(), cfg)
	if err != nil {
		t.Fatalf("assembly: %v", err)
	}
	t.Cleanup(func() {
		asm.Close()
		cctx, ccancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer ccancel()
		p, perr := pgxpool.New(cctx, dsn)
		if perr != nil {
			return
		}
		defer p.Close()
		for _, tbl := range []string{"runbook_execution_log", "incident_runbook", "runbook",
			"incident_cluster", "incident_audit", "incident"} {
			_, _ = p.Exec(cctx, "DELETE FROM "+tbl+" WHERE tenant_id = $1", cfg.Tenant)
		}
	})
	if asm.Runbooks == nil {
		t.Fatal("runbook store not wired although DB DSN set")
	}
	return asm, asm.Handler()
}

func TestRunbookEndToEndWithPG(t *testing.T) {
	token := "rb-e2e-token"
	_, h := runbookE2EAssembly(t, token)

	// 1) 建单（既有 POST /api/v1/incidents，链路 B 人工建单）。
	code, inc := doRunbook(t, h, http.MethodPost, "/api/v1/incidents", token, "application/json",
		`{"title":"磁盘打满","severity":"warning","created_by":"alice"}`)
	if code != http.StatusCreated {
		t.Fatalf("create incident: code = %d body = %v", code, inc)
	}
	incID, _ := inc["id"].(string)
	if incID == "" {
		t.Fatalf("create incident: no id in %v", inc)
	}

	// 2) 建手册（内容 markdown + scope 标签）。
	code, rb := doRunbook(t, h, http.MethodPost, "/api/v1/runbooks", token, "application/json",
		`{"id":"rb-disk","title":"磁盘清理手册","content":"# 磁盘满\n1. df -h\n2. 清理 /var/log","scope_severity":"warning","scope_service":"mysql","created_by":"alice"}`)
	if code != http.StatusCreated {
		t.Fatalf("create runbook: code = %d body = %v", code, rb)
	}
	rbv, _ := rb["runbook"].(map[string]any)
	if rbv == nil || rbv["id"] != "rb-disk" || rb["persistence"] != "timescaledb" {
		t.Fatalf("create runbook view = %v", rb)
	}
	// 重复 id → 409。
	if code, _ := doRunbook(t, h, http.MethodPost, "/api/v1/runbooks", token, "application/json",
		`{"id":"rb-disk","title":"dup","created_by":"bob"}`); code != http.StatusConflict {
		t.Fatalf("duplicate runbook id: code = %d, want 409", code)
	}
	// 库列表可见。
	code, lib := doRunbook(t, h, http.MethodGet, "/api/v1/runbooks", "", "", "")
	items, _ := lib["runbooks"].([]any)
	if code != http.StatusOK || len(items) != 1 {
		t.Fatalf("GET /runbooks: code = %d body = %v", code, lib)
	}

	// 3) 挂载 + 幂等（重复挂载恰一条、首挂留痕不刷新）。
	mountPath := "/api/v1/incidents/" + incID + "/runbooks"
	code, m1 := doRunbook(t, h, http.MethodPost, mountPath, token, "application/json",
		`{"runbook_id":"rb-disk","mounted_by":"alice"}`)
	if code != http.StatusOK || m1["mounted"] != true {
		t.Fatalf("mount: code = %d body = %v", code, m1)
	}
	doRunbook(t, h, http.MethodPost, mountPath, token, "application/json",
		`{"runbook_id":"rb-disk","mounted_by":"mallory"}`) // 幂等重挂：不改写首挂人
	code, ml := doRunbook(t, h, http.MethodGet, mountPath, "", "", "")
	mounts, _ := ml["runbooks"].([]any)
	if code != http.StatusOK || len(mounts) != 1 {
		t.Fatalf("GET mounts after idempotent re-mount: code = %d body = %v", code, ml)
	}
	m0 := mounts[0].(map[string]any)
	if m0["runbook_id"] != "rb-disk" || m0["mounted_by"] != "alice" || m0["title"] != "磁盘清理手册" ||
		m0["execution_count"].(float64) != 0 {
		t.Fatalf("mount view = %v", m0)
	}

	// 4) 记一次执行（只记不执行：auto_execution:false 契约声明）。
	execPath := mountPath + "/rb-disk/executions"
	code, ev := doRunbook(t, h, http.MethodPost, execPath, token, "application/json",
		`{"executed_by":"alice","result":"清理了 3 天前归档日志，水位回到 62%","refs":["https://ticket/42",{"url":"https://shot/1","label":"df -h"}]}`)
	if code != http.StatusCreated {
		t.Fatalf("append execution: code = %d body = %v", code, ev)
	}
	exec, _ := ev["execution"].(map[string]any)
	if exec["seq"].(float64) < 1 || exec["executed_by"] != "alice" || ev["auto_execution"] != false {
		t.Fatalf("execution view = %v", ev)
	}
	// refs 非数组 → 400。
	if code, _ := doRunbook(t, h, http.MethodPost, execPath, token, "application/json",
		`{"executed_by":"a","result":"x","refs":{"k":1}}`); code != http.StatusBadRequest {
		t.Fatalf("refs object must be rejected: code = %d, want 400", code)
	}

	// 5) GET 全链：执行列表回显 1 条 + 挂载列表计数 1。
	code, el := doRunbook(t, h, http.MethodGet, execPath, "", "", "")
	execs, _ := el["executions"].([]any)
	if code != http.StatusOK || len(execs) != 1 || el["auto_execution"] != false {
		t.Fatalf("GET executions: code = %d body = %v", code, el)
	}
	e0 := execs[0].(map[string]any)
	if refs, _ := e0["refs"].([]any); len(refs) != 2 {
		t.Fatalf("refs roundtrip = %v, want 2 elements", e0["refs"])
	}
	code, ml2 := doRunbook(t, h, http.MethodGet, mountPath, "", "", "")
	if m := ml2["runbooks"].([]any)[0].(map[string]any); m["execution_count"].(float64) != 1 {
		t.Fatalf("execution_count after append = %v, want 1", m["execution_count"])
	}

	// 6) 解挂 → 挂载消失、执行历史保留（append-only 不随挂载关系消亡）。
	unmountPath := execPath[:len(execPath)-len("/executions")]
	if code, _ := doRunbook(t, h, http.MethodDelete, unmountPath, token, "", ""); code != http.StatusOK {
		t.Fatalf("unmount: code = %d", code)
	}
	if code, bl := doRunbook(t, h, http.MethodGet, mountPath, "", "", ""); code != http.StatusOK || len(bl["runbooks"].([]any)) != 0 {
		t.Fatalf("GET mounts after unmount: code = %d body = %v", code, bl)
	}
	if code, bl := doRunbook(t, h, http.MethodGet, execPath, "", "", ""); code != http.StatusOK || len(bl["executions"].([]any)) != 1 {
		t.Fatalf("execution history must survive unmount: code = %d body = %v", code, bl)
	}

	// 7) 错误面：无单 404（挂/记/看全链）；未挂载点记执行 404；重复解挂 404。
	ghost := "/api/v1/incidents/INC-ghost-not-exist/runbooks"
	if code, _ := doRunbook(t, h, http.MethodPost, ghost, token, "application/json",
		`{"runbook_id":"rb-disk","mounted_by":"a"}`); code != http.StatusNotFound {
		t.Fatalf("mount to unknown incident: code = %d, want 404", code)
	}
	if code, _ := doRunbook(t, h, http.MethodGet, ghost, "", "", ""); code != http.StatusNotFound {
		t.Fatalf("GET mounts of unknown incident: code = %d, want 404", code)
	}
	if code, _ := doRunbook(t, h, http.MethodGet, ghost+"/rb-disk/executions", "", "", ""); code != http.StatusNotFound {
		t.Fatalf("GET executions of unknown incident: code = %d, want 404", code)
	}
	if code, _ := doRunbook(t, h, http.MethodPost, execPath, token, "application/json",
		`{"executed_by":"a","result":"x"}`); code != http.StatusNotFound {
		t.Fatalf("append to unmounted pair: code = %d, want 404 (ErrNotMounted)", code)
	}
	if code, _ := doRunbook(t, h, http.MethodPost, mountPath, token, "application/json",
		`{"runbook_id":"rb-ghost","mounted_by":"a"}`); code != http.StatusNotFound {
		t.Fatalf("mount unknown runbook: code = %d, want 404", code)
	}
	if code, _ := doRunbook(t, h, http.MethodDelete, unmountPath, token, "", ""); code != http.StatusNotFound {
		t.Fatalf("double unmount: code = %d, want 404", code)
	}
}

// TestRunbookPersistenceShape 契约锁：手册创建响应字段名逐一对齐
// docs/前端契约-runbook.md（并行 web 任务按此消费，键名漂移即红）。
func TestRunbookPersistenceShape(t *testing.T) {
	_, h := runbookE2EAssembly(t, "tok")
	_, resp := doRunbook(t, h, http.MethodPost, "/api/v1/runbooks", "tok", "application/json",
		`{"id":"rb-shape","title":"t","content":"c","created_by":"a"}`)
	rb, _ := resp["runbook"].(map[string]any)
	if rb == nil {
		t.Fatalf("create response missing runbook envelope: %v", resp)
	}
	for _, k := range []string{"id", "title", "content", "scope_severity", "scope_service",
		"created_by", "created_at", "updated_at"} {
		if _, ok := rb[k]; !ok {
			t.Fatalf("runbook create response missing contract key %q: %v", k, rb)
		}
	}
	if s, ok := rb["created_at"].(string); !ok || s == "" {
		t.Fatalf("created_at must be RFC3339 string, got %T %v", rb["created_at"], rb["created_at"])
	}
}
