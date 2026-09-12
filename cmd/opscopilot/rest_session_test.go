// rest_session_test.go RCA 复盘会话 REST 契约（二期池 #7 S2，httptest 直调
// Assembly.Handler，无 DSN——真相层走内存降级形态）：Token 门禁 401、
// OPS_SESSION=off 显式 503（与 RCA 口径同款）、POST 追加（LLM 未配置 →
// user 轮落 + assistant_status=pending）、GET 读会话、404/400/415 映射、
// 事件不存在零污染。PG 真库端到端与懒恢复在 rca_session_pg_test.go。
package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"

	"opscopilot/internal/config"
)

// sessionRESTEnv 装配级会话环境：miniredis 顶替告警实例 + OPS_SESSION=on +
// 一个已存在的事件。cfg.LLM.Endpoint 留空 = assistant 走 pending 语义。
func sessionRESTEnv(t *testing.T, token string) (*Assembly, http.Handler, string, *config.Config) {
	t.Helper()
	mr := miniredis.RunT(t)
	cfg := testAssemblyConfig(token)
	cfg.Session.Enabled = true
	cfg.Redis.Alert.Addr = mr.Addr()
	asm, err := NewAssembly(newQuietLogger(), cfg)
	if err != nil {
		t.Fatalf("assembly: %v", err)
	}
	t.Cleanup(asm.Close)
	if asm.Session == nil {
		t.Fatal("session orchestrator not wired although OPS_SESSION=on (and RCA on by default)")
	}
	inc, err := asm.Incidents.Create("INC-sess", "复盘目标", "critical", "creator")
	if err != nil {
		t.Fatalf("create incident: %v", err)
	}
	return asm, asm.Handler(), inc.ID, cfg
}

func doSession(t *testing.T, h http.Handler, method, path, token, ctype, body string) (int, map[string]any) {
	t.Helper()
	var r *http.Request
	if body == "" {
		r = httptest.NewRequest(method, path, nil)
	} else {
		r = httptest.NewRequest(method, path, strings.NewReader(body))
	}
	if ctype != "" {
		r.Header.Set("Content-Type", ctype)
	}
	if token != "" {
		r.Header.Set(AuthHeader, token)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	var out map[string]any
	if rec.Body.Len() > 0 {
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatalf("%s %s: non-JSON: %v (%s)", method, path, err, rec.Body.String())
		}
	}
	return rec.Code, out
}

// TestRESTSessionDisabled503 OPS_SESSION=off（默认）：GET/POST 均显式 503
// ——off 全链路零行为变化（不建表读写、不进编排器），503 比 404 可诊断。
func TestRESTSessionDisabled503(t *testing.T) {
	cfg := testAssemblyConfig("tok") // Session off
	asm, err := NewAssembly(newQuietLogger(), cfg)
	if err != nil {
		t.Fatalf("assembly: %v", err)
	}
	defer asm.Close()
	if asm.Session != nil || asm.SessionHot != nil {
		t.Fatal("session components must not exist when OPS_SESSION=off")
	}
	h := asm.Handler()
	if code, body := doSession(t, h, http.MethodGet, "/api/v1/incidents/INC-x/rca/session", "tok", "", ""); code != http.StatusServiceUnavailable {
		t.Fatalf("off GET: code = %d body = %v, want 503", code, body)
	}
	if code, _ := doSession(t, h, http.MethodPost, "/api/v1/incidents/INC-x/rca/session", "tok", "application/json", `{"content":"q"}`); code != http.StatusServiceUnavailable {
		t.Fatalf("off POST: code = %d, want 503", code)
	}
}

// TestRESTSessionTokenGate 门禁：无/错 Token → 401（GET 与 POST 同闸——
// 拍板⑤：本期单 Token 闸门；引入用户身份后读会话须过 incident 权限）。
func TestRESTSessionTokenGate(t *testing.T) {
	_, h, id, _ := sessionRESTEnv(t, "sekret")
	path := "/api/v1/incidents/" + id + "/rca/session"
	if code, _ := doSession(t, h, http.MethodGet, path, "", "", ""); code != http.StatusUnauthorized {
		t.Fatalf("GET no token: code = %d, want 401", code)
	}
	if code, _ := doSession(t, h, http.MethodPost, path, "wrong", "application/json", `{"content":"q"}`); code != http.StatusUnauthorized {
		t.Fatalf("POST bad token: code = %d, want 401", code)
	}
}

// TestRESTSessionPostGetContract POST 追加 user 轮（LLM 未配置 → pending，
// 不伪造 assistant）→ GET 读回同一轮；轮次字段、created_by、persistence
// 透出；错 incident → 404；空正文 → 400；非 JSON → 415。
func TestRESTSessionPostGetContract(t *testing.T) {
	_, h, id, cfg := sessionRESTEnv(t, "tok")
	path := "/api/v1/incidents/" + id + "/rca/session"

	code, post := doSession(t, h, http.MethodPost, path, "tok", "application/json",
		`{"content":"根因是哪次发布?","actor":"alice"}`)
	if code != http.StatusOK {
		t.Fatalf("POST: code = %d body = %v", code, post)
	}
	if post["assistant_status"] != "pending" {
		t.Fatalf("no-llm POST must return explicit pending, got %v", post["assistant_status"])
	}
	if post["persistence"] != "memory" { // 无 DB 装配（本用例形态）必须透出
		t.Fatalf("persistence = %v, want memory", post["persistence"])
	}
	turns, _ := post["turns"].([]any)
	if len(turns) != 1 {
		t.Fatalf("turns = %v, want exactly 1 (no fabricated assistant)", turns)
	}
	t0 := turns[0].(map[string]any)
	if t0["seq"].(float64) != 1 || t0["role"] != "user" || t0["content"] != "根因是哪次发布?" {
		t.Fatalf("turn view = %v", t0)
	}

	_, got := doSession(t, h, http.MethodGet, path, "tok", "", "")
	if got["created_by"] != "alice" {
		t.Fatalf("GET created_by = %v, want alice（拍板⑤身份钩子落表）", got["created_by"])
	}
	if parts, _ := got["participants"].([]any); len(parts) != 1 || parts[0] != "alice" {
		t.Fatalf("participants = %v, want [alice]", got["participants"])
	}
	if t2, _ := got["turns"].([]any); len(t2) != 1 {
		t.Fatalf("GET turns = %v, want 1", t2)
	}

	// 错误面。
	if c, _ := doSession(t, h, http.MethodGet, "/api/v1/incidents/INC-nope/rca/session", "tok", "", ""); c != http.StatusNotFound {
		t.Fatalf("unknown incident GET: code = %d, want 404", c)
	}
	if c, _ := doSession(t, h, http.MethodPost, "/api/v1/incidents/INC-nope/rca/session", "tok", "application/json", `{"content":"q"}`); c != http.StatusNotFound {
		t.Fatalf("unknown incident POST: code = %d, want 404", c)
	}
	if c, _ := doSession(t, h, http.MethodPost, path, "tok", "application/json", `{"content":"  "}`); c != http.StatusBadRequest {
		t.Fatalf("empty content: code = %d, want 400", c)
	}
	if c, _ := doSession(t, h, http.MethodPost, path, "tok", "text/plain", `x`); c != http.StatusUnsupportedMediaType {
		t.Fatalf("non-json POST: code = %d, want 415", c)
	}
	_ = cfg
}

// TestRESTSessionMultiTurnContextSameIncident 设计验收"同 incident 多轮上下文
// 跨请求可读"：两次 POST（不同 actor）→ seq 连续、participants 收敛两人。
func TestRESTSessionMultiTurnContextSameIncident(t *testing.T) {
	_, h, id, _ := sessionRESTEnv(t, "")
	path := "/api/v1/incidents/" + id + "/rca/session"
	if code, _ := doSession(t, h, http.MethodPost, path, "", "application/json",
		`{"content":"第一问","actor":"alice"}`); code != http.StatusOK {
		t.Fatalf("post1 code = %d", code)
	}
	_, p2 := doSession(t, h, http.MethodPost, path, "", "application/json",
		`{"content":"第二问","actor":"bob"}`)
	turns, _ := p2["turns"].([]any)
	if len(turns) != 2 {
		t.Fatalf("turns = %v, want 2", turns)
	}
	if last := turns[len(turns)-1].(map[string]any); last["seq"].(float64) != 2 {
		t.Fatalf("last seq = %v, want 2 (memory truth keeps per-incident counter)", last["seq"])
	}
	_, got := doSession(t, h, http.MethodGet, path, "", "", "")
	parts, _ := got["participants"].([]any)
	if len(parts) != 2 {
		t.Fatalf("participants = %v, want alice+bob", got["participants"])
	}
}
