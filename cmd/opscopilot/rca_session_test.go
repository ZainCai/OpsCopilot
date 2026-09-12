// rca_session_test.go 复盘会话编排器单测（二期池 #7 S2，内存真相 + miniredis
// 热态 + 假 analyzer/chat 注入）：fail-open pending（超时/坏应答/未配置）、
// answered 全链（prompt 携带 RCA findings、assistant 落库带 llm_meta、热态
// 回填）、懒恢复重建（Redis 清空 → snapshot 重建 + 回填）。
// PG 真相形态（seq 并发恰一序、真库懒恢复）在 rca_session_pg_test.go（DSN 门控）。
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"opscopilot/internal/config"
	"opscopilot/internal/incident"
	"opscopilot/internal/llmgw"
	"opscopilot/internal/rca"
	"opscopilot/internal/sessionstore"
)

// ---- 假件 ----

// sessionStubAnalyzer 返回固定 RCA 结果（prompt 证据来源断言用）。
type sessionStubAnalyzer struct {
	mu    sync.Mutex
	calls []string
	res   *RCAResult
	err   error
}

func (f *sessionStubAnalyzer) Analyze(_ context.Context, incidentID, _ string) (*RCAResult, error) {
	f.mu.Lock()
	f.calls = append(f.calls, incidentID)
	f.mu.Unlock()
	return f.res, f.err
}

// fakeChat llmgw 假出口：捕获 messages，返回预设 (content, err)。
type fakeChat struct {
	mu      sync.Mutex
	prompts []string // 历次 user 消息原文（prompt JSON）
	content string
	err     error
}

func (f *fakeChat) Chat(_ context.Context, messages []llmgw.Message, _ float64) (string, llmgw.Stats, error) {
	f.mu.Lock()
	if len(messages) > 1 {
		f.prompts = append(f.prompts, messages[1].Content)
	}
	f.mu.Unlock()
	if f.err != nil {
		return "", llmgw.Stats{Model: "test-model", PromptChars: 10, PromptHash: "deadbeefcafe"}, f.err
	}
	return f.content, llmgw.Stats{Model: "test-model", PromptChars: 10, PromptHash: "deadbeefcafe",
		HTTPStatus: 200, RespBytes: len(f.content)}, nil
}

func (f *fakeChat) lastPrompt() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.prompts) == 0 {
		return ""
	}
	return f.prompts[len(f.prompts)-1]
}

// sessionTestEnv 内存真相 + miniredis 热态 + 假 analyzer 的编排器。
func sessionTestEnv(t *testing.T, chat sessionChat, res *RCAResult, aerr error, m *AppMetrics) (
	*SessionOrchestrator, *memSessionTruth, *miniredis.Miniredis, *sessionStubAnalyzer) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	inc := incident.NewMemStore()
	if _, err := inc.Create("INC-1", "磁盘打满", "critical", "creator"); err != nil {
		t.Fatalf("create incident: %v", err)
	}
	truth := newMemSessionTruth()
	an := &sessionStubAnalyzer{res: res, err: aerr}
	return NewSessionOrchestrator(truth, sessionstore.New(rdb, "alert"), inc, an,
		chat, config.DefaultRCAMaxFindings, config.DefaultTenant, m, nil), truth, mr, an
}

func rcaResultFixture() *RCAResult {
	rep := &rca.Report{
		Input: rca.Input{T0: time.Now().Add(-time.Hour), Window: 30 * time.Minute,
			AlertedNodes: []string{"prometheus://nodes/n1"}},
		Findings: []rca.Finding{
			{Step: rca.StepAttribution, Summary: "根因：dep-42 发布", Confidence: "high", Ref: "dep-42"},
		},
		FindingsTruncated: 7,
	}
	full := append([]rca.Finding{}, rep.Findings...)
	return &RCAResult{IncidentID: "INC-1", T0: rep.Input.T0, Report: rep, FindingsFull: full}
}

// ---- fail-open pending ----

// TestSessionAssistantTimeoutPending LLM 超时：user 轮保留、assistant 轮不落库
// 不伪造，assistant_status=pending，指标计 timeout（宁 pending 不假答）。
func TestSessionAssistantTimeoutPending(t *testing.T) {
	m := NewAppMetrics()
	chat := &fakeChat{err: fmt.Errorf("send: %w", context.DeadlineExceeded)}
	o, truth, _, _ := sessionTestEnv(t, chat, rcaResultFixture(), nil, m)

	res, err := o.AddTurn(context.Background(), "INC-1", "alice", "根因是哪次发布?")
	if err != nil {
		t.Fatalf("AddTurn: %v", err)
	}
	if res.AssistantStatus != "pending" {
		t.Fatalf("assistant_status = %s, want pending", res.AssistantStatus)
	}
	if len(res.Turns) != 1 || res.Turns[0].Role != sessionstore.RoleUser {
		t.Fatalf("turns = %+v, want exactly 1 user turn (no fabricated assistant)", res.Turns)
	}
	// 真相层同样只有 user 轮。
	turns, _, _ := truth.snapshot(context.Background(), "INC-1")
	if len(turns) != 1 {
		t.Fatalf("truth turns = %d, want 1", len(turns))
	}
	if got := gatherMetrics(t, m); !strings.Contains(got, `opscopilot_session_llm_requests_total{outcome="timeout"} 1`) {
		t.Fatalf("metrics must count session llm timeout:\n%s", got)
	}
}

// TestSessionAssistantBadAnswerPending 网关错误（坏 JSON/非 200 归 error）→
// pending + error 计数（"响应 200 但没内容"的降级面同纪律）。
func TestSessionAssistantBadAnswerPending(t *testing.T) {
	m := NewAppMetrics()
	chat := &fakeChat{content: "   "} // 200 空应答
	o, _, _, _ := sessionTestEnv(t, chat, rcaResultFixture(), nil, m)
	res, err := o.AddTurn(context.Background(), "INC-1", "alice", "q")
	if err != nil {
		t.Fatalf("AddTurn: %v", err)
	}
	if res.AssistantStatus != "pending" {
		t.Fatalf("empty answer must stay pending, got %s", res.AssistantStatus)
	}
	if got := gatherMetrics(t, m); !strings.Contains(got, `opscopilot_session_llm_requests_total{outcome="error"} 1`) {
		t.Fatalf("empty 200-answer must count as error:\n%s", got)
	}
}

// TestSessionLLMNotConfiguredPending chat=nil（OPS_LLM_ENDPOINT 空）：不发起
// 请求（无 llm 出站计数）、user 轮落库、pending——与 RCA 注入 nil Summarizer
// 同款现状语义。
func TestSessionLLMNotConfiguredPending(t *testing.T) {
	m := NewAppMetrics()
	o, _, _, an := sessionTestEnv(t, nil, rcaResultFixture(), nil, m)
	res, err := o.AddTurn(context.Background(), "INC-1", "alice", "q")
	if err != nil {
		t.Fatalf("AddTurn: %v", err)
	}
	if res.AssistantStatus != "pending" || len(res.Turns) != 1 {
		t.Fatalf("no-llm AddTurn = %+v, want 1 user turn + pending", res)
	}
	if len(an.calls) != 0 {
		t.Fatalf("no LLM configured must not even run RCA analyze for the prompt: %v", an.calls)
	}
	got := gatherMetrics(t, m)
	// 计数器在构造期恒注册（0 值行也在输出里）——断言"没有任何一次非零
	// 记账"，而非"行不存在"（对齐本仓 metrics 暴露形态）。
	for _, o := range []string{"ok", "timeout", "error"} {
		line := "opscopilot_session_llm_requests_total{outcome=\"" + o + "\"} 0"
		if !strings.Contains(got, line) {
			t.Fatalf("unconfigured llm must leave %s at zero:\n%s", line, got)
		}
	}
}

// ---- answered 全链 + prompt 携带 RCA findings（拍板④硬要求）----

func TestSessionAnsweredCarriesRCAFindings(t *testing.T) {
	m := NewAppMetrics()
	chat := &fakeChat{content: "根据证据链，根因是 dep-42 发布。"}
	o, truth, _, an := sessionTestEnv(t, chat, rcaResultFixture(), nil, m)

	res, err := o.AddTurn(context.Background(), "INC-1", "alice", "根因是哪次发布?")
	if err != nil {
		t.Fatalf("AddTurn: %v", err)
	}
	if res.AssistantStatus != "answered" || len(res.Turns) != 2 {
		t.Fatalf("answered result = %+v", res)
	}
	a := res.Turns[1]
	if a.Role != sessionstore.RoleAssistant || a.Seq != 2 || a.CreatedBy != "system:llm" {
		t.Fatalf("assistant turn = %+v", a)
	}
	if a.LLMMeta["model"] != "test-model" || a.LLMMeta["prompt_hash"] != "deadbeefcafe" {
		t.Fatalf("llm_meta must carry model+hash (脱敏摘要): %+v", a.LLMMeta)
	}
	for k := range a.LLMMeta {
		if k == "content" || k == "prompt" {
			t.Fatalf("llm_meta must NOT carry prompt/answer body: %+v", a.LLMMeta)
		}
	}
	// 真相层两轮齐全（assistant 真落库，非只回显）。
	turns, _, _ := truth.snapshot(context.Background(), "INC-1")
	if len(turns) != 2 {
		t.Fatalf("truth turns = %d, want 2", len(turns))
	}
	// RCA 复用恰好一次；prompt 携带截断版 findings + 计数 + 本轮提问。
	if len(an.calls) != 1 {
		t.Fatalf("analyzer calls = %v, want 1", an.calls)
	}
	prompt := chat.lastPrompt()
	for _, want := range []string{"dep-42", "findings_truncated", "根因：dep-42 发布", "根因是哪次发布?", "alerted_nodes"} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("prompt must carry %q; got %s", want, prompt)
		}
	}
	// 热态回填：Redis 里应有信封且轮次齐全。
	env, err := o.hot.LoadEnvelope(context.Background(), sessionstore.SessionKey(config.DefaultTenant, "INC-1"))
	if err != nil || len(env.Turns) != 2 {
		t.Fatalf("hot envelope after answer = (%+v,%v), want 2 turns", env, err)
	}
	if got := gatherMetrics(t, m); !strings.Contains(got, `opscopilot_session_llm_requests_total{outcome="ok"} 1`) {
		t.Fatalf("metrics must count session llm ok:\n%s", got)
	}
}

// ---- 懒恢复（拍板①：Redis 清空 → 真相重建往返）----

func TestSessionLazyRestoreFromTruth(t *testing.T) {
	o, _, mr, _ := sessionTestEnv(t, nil, rcaResultFixture(), nil, nil)
	if _, err := o.AddTurn(context.Background(), "INC-1", "alice", "第一问"); err != nil {
		t.Fatalf("AddTurn: %v", err)
	}
	key := sessionstore.SessionKey(config.DefaultTenant, "INC-1")
	mr.FlushAll()
	if _, err := o.hot.LoadEnvelope(context.Background(), key); !errors.Is(err, sessionstore.ErrNotFound) {
		t.Fatalf("hot must be empty after flush, got %v", err)
	}
	res, err := o.ReadSession(context.Background(), "INC-1")
	if err != nil {
		t.Fatalf("ReadSession: %v", err)
	}
	if len(res.Turns) != 1 || res.Turns[0].Content != "第一问" || res.Turns[0].Seq != 1 {
		t.Fatalf("lazy restore lost turns: %+v", res.Turns)
	}
	if res.Meta.CreatedBy != "alice" {
		t.Fatalf("meta.created_by = %s, want alice", res.Meta.CreatedBy)
	}
	// 回填发生：再读热态直接命中。
	if env, err := o.hot.LoadEnvelope(context.Background(), key); err != nil || len(env.Turns) != 1 {
		t.Fatalf("hot must be refilled after lazy restore: (%+v,%v)", env, err)
	}
}

// TestSessionRPCEvidenceFailureStillAnswers RCA 取证失败**不阻断提问**：prompt
// 带 evidence_note（如实告知模型无证据），assistant 正常回答不伪造证据。
func TestSessionRPCEvidenceFailureStillAnswers(t *testing.T) {
	chat := &fakeChat{content: "当前拿不到证据链，无法定因。"}
	o, _, _, _ := sessionTestEnv(t, chat, nil, errors.New("topology unreachable"), nil)
	res, err := o.AddTurn(context.Background(), "INC-1", "alice", "怎么办?")
	if err != nil {
		t.Fatalf("AddTurn: %v", err)
	}
	if res.AssistantStatus != "answered" {
		t.Fatalf("RCA failure must not block answering with note, got %s", res.AssistantStatus)
	}
	var doc sessionPromptDoc
	if perr := json.Unmarshal([]byte(chat.lastPrompt()), &doc); perr != nil {
		t.Fatalf("prompt must be valid JSON: %v", perr)
	}
	if doc.EvidenceNote == "" || len(doc.Findings) != 0 {
		t.Fatalf("evidence_note must explain missing findings: %+v", doc)
	}
}

// TestSessionUnknownIncident404 事件不存在 → ErrRCAIncidentNotFound（REST 404）。
func TestSessionUnknownIncident404(t *testing.T) {
	o, _, _, _ := sessionTestEnv(t, nil, nil, nil, nil)
	if _, err := o.AddTurn(context.Background(), "INC-nope", "a", "q"); !errors.Is(err, ErrRCAIncidentNotFound) {
		t.Fatalf("unknown incident err = %v, want ErrRCAIncidentNotFound", err)
	}
	if _, err := o.ReadSession(context.Background(), "INC-nope"); !errors.Is(err, ErrRCAIncidentNotFound) {
		t.Fatalf("read unknown incident err = %v", err)
	}
}

// TestSessionEmptyAndOverlongTurn 空正文/超限正文 → ErrSessionTurnEmpty（400）。
func TestSessionEmptyAndOverlongTurn(t *testing.T) {
	o, _, _, _ := sessionTestEnv(t, nil, nil, nil, nil)
	if _, err := o.AddTurn(context.Background(), "INC-1", "a", "   "); !errors.Is(err, ErrSessionTurnEmpty) {
		t.Fatalf("empty content err = %v", err)
	}
	long := strings.Repeat("问", maxSessionTurnChars+1)
	if _, err := o.AddTurn(context.Background(), "INC-1", "a", long); !errors.Is(err, ErrSessionTurnEmpty) {
		t.Fatalf("overlong content err = %v, want ErrSessionTurnEmpty (400)", err)
	}
}
