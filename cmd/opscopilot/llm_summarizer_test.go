// llm_summarizer 测试（二期池波二 #3 / ADR-015）：httptest mock 网关表驱动
// （成功 / 超时 / 坏 JSON / 401 / 空结论降级 pending）+ RCA conclude 集成
// （注入真 Summarizer 后 conclusion 非空、llm_used=true；失败时报告退回
// pending 证据链形态且 Analyze 不报错）+ 禁用路径构造断言。
// 全部打 httptest 回环，零真实外网调用。
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"opscopilot/internal/config"
	"opscopilot/internal/rca"
	"opscopilot/internal/topology"
)

// gatherMetrics 抓取 /metrics 文本（AppMetrics.Handler 是标准 handler）。
func gatherMetrics(t *testing.T, m *AppMetrics) string {
	t.Helper()
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	return rec.Body.String()
}

// changeFixture 编排器集成用例的变更证据（与 rca_orchestrator_test 同款字段）。
func changeFixture(id, nodeKey string) topology.ChangeEvent {
	return topology.ChangeEvent{
		ID: id, NodeKey: nodeKey, Type: topology.ChangeDeploy,
		Source: "jenkins", Author: "ci", Summary: "v9",
		OccurredAt: time.Now().Add(-3 * time.Minute),
		Confidence: topology.ConfidenceHigh,
	}
}

// llmSection 构造 LLM 配置片段（禁用路径契约测试用）。
func llmSection(endpoint, model string) config.LLMSection {
	return config.LLMSection{Endpoint: endpoint, Model: model,
		Timeout: config.DefaultLLMTimeout, MaxTokens: config.DefaultLLMMaxTokens}
}

// mockGateway 起一个 OpenAI-compatible 假网关，返回 URL 与收到的请求体。
func mockGateway(t *testing.T, handler http.HandlerFunc) (string, *[]chatCapture) {
	t.Helper()
	caps := &[]chatCapture{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var cap chatCapture
		_ = json.Unmarshal(body, &cap)
		cap.auth = r.Header.Get("Authorization")
		*caps = append(*caps, cap)
		handler(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv.URL, caps
}

type chatCapture struct {
	Model    string `json:"model"`
	Messages []struct {
		Role    string `json:"role"`
		Content string `json:"content"`
	} `json:"messages"`
	Temperature float64 `json:"temperature"`
	MaxTokens   int     `json:"max_tokens"`
	auth        string
}

func completion(content string) string {
	b, _ := json.Marshal(map[string]any{
		"choices": []any{map[string]any{
			"message": map[string]string{"role": "assistant", "content": content},
		}},
	})
	return string(b)
}

// llmSectionTO 带传输超时的配置片段（经 newLLMGateway 全流程构造——
// Sender 注入路径与装配一致，测试不旁路 transport）。
func llmSectionTO(endpoint string, timeout time.Duration) config.LLMSection {
	return config.LLMSection{Endpoint: endpoint, APIKey: "sk-test", Model: "mock-1",
		Timeout: timeout, MaxTokens: 512}
}

func newTestSummarizer(t *testing.T, endpoint string, timeout time.Duration) (*llmSummarizer, *AppMetrics) {
	t.Helper()
	gw, err := newLLMGateway(llmSectionTO(endpoint, timeout))
	if err != nil {
		t.Fatalf("llmgw new: %v", err)
	}
	m := NewAppMetrics()
	return newLLMSummarizer(gw, m, func(string, ...any) {}, config.DefaultRCAMaxFindings), m
}

func sampleInput() rca.Input {
	return rca.Input{
		TenantID: "default", ClusterKey: "ck-1", T0: time.Now(), Window: 30 * time.Minute,
		AlertedNodes: []string{"prometheus://nodes/n1"},
		Evidence: rca.Evidence{
			Changes: []rca.ChangeFact{{ID: "dep-1", NodeKey: "prometheus://nodes/n1", ChangeType: "deploy", Confidence: "high"}},
		},
	}
}

func sampleFindings() []rca.Finding {
	return []rca.Finding{
		{Step: rca.StepCollect, Summary: "取证：故障域 1 节点", Confidence: "high"},
		{Step: rca.StepAttribution, Summary: "根因：变更 dep-1（deploy）", Confidence: "high", Ref: "dep-1"},
	}
}

// TestLLMSummarizerTable 表驱动锁定成功与降级语义：任何网关故障都返回
// 包装 rca.ErrNotImplemented 的错误（→ conclude 记 pending 的 fail-open
// 链路），绝不返回可直接进报告的半成品结论。
func TestLLMSummarizerTable(t *testing.T) {
	cases := []struct {
		name     string
		delay    time.Duration
		status   int
		body     string
		wantText string
		wantOK   bool
		outcome  string
	}{
		{
			name:     "成功：JSON 结论 + caveats 拼接",
			body:     completion(`{"conclusion":"根因是 dep-1 部署引入故障","confidence":"high","caveats":["窗口仅 30m","拓扑 medium"]}`),
			wantText: "根因是 dep-1 部署引入故障；注意：窗口仅 30m；拓扑 medium", wantOK: true, outcome: "ok",
		},
		{
			name:     "成功：markdown 围栏剥除",
			body:     completion("```json\n{\"conclusion\":\"围栏结论\"}\n```"),
			wantText: "围栏结论", wantOK: true, outcome: "ok",
		},
		{
			name: "降级：模型输出不是 JSON", status: 200, body: completion("我觉得大概是发布的问题"),
			outcome: "error",
		},
		{
			name: "降级：conclusion 字段为空", status: 200, body: completion(`{"conclusion":"  "}`),
			outcome: "error",
		},
		{
			name: "降级：网关 401", status: http.StatusUnauthorized, body: `{"error":"bad key"}`,
			outcome: "error",
		},
		{
			name: "降级：网关 500", status: http.StatusInternalServerError, body: "boom",
			outcome: "error",
		},
		{
			name: "降级：超时", delay: 300 * time.Millisecond, outcome: "timeout",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			url, caps := mockGateway(t, func(w http.ResponseWriter, r *http.Request) {
				if tc.delay > 0 {
					time.Sleep(tc.delay)
				}
				if tc.status != 0 && tc.status != http.StatusOK {
					w.WriteHeader(tc.status)
				}
				_, _ = io.WriteString(w, tc.body)
			})
			// 超时用例：客户端预算 50ms < 300ms 延迟。
			timeout := 2 * time.Second
			if tc.delay > 0 {
				timeout = 50 * time.Millisecond
			}
			s, m := newTestSummarizer(t, url, timeout)
			text, err := s.Summarize(sampleInput(), sampleFindings())
			if tc.wantOK {
				if err != nil {
					t.Fatalf("want success, got %v", err)
				}
				if text != tc.wantText {
					t.Fatalf("text = %q, want %q", text, tc.wantText)
				}
				// 请求形态：固定中文系统提示 + 证据 JSON（含 root_causes 计数）。
				last := (*caps)[len(*caps)-1]
				if last.Model != "mock-1" || last.MaxTokens != 512 || last.auth != "Bearer sk-test" {
					t.Fatalf("request meta = %+v", last)
				}
				if !strings.Contains(last.Messages[0].Content, "仅依据") {
					t.Errorf("system prompt drifted: %q", last.Messages[0].Content)
				}
				var doc llmEvidenceDoc
				if err := json.Unmarshal([]byte(last.Messages[1].Content), &doc); err != nil {
					t.Fatalf("evidence payload not JSON: %v", err)
				}
				if len(doc.Findings) != 2 || len(doc.RootCauses) != 1 || doc.EvidenceCounts["changes"] != 1 {
					t.Fatalf("evidence doc = %+v", doc)
				}
			} else {
				if !errors.Is(err, rca.ErrNotImplemented) {
					t.Fatalf("want wrapped ErrNotImplemented (fail-open→pending), got %v", err)
				}
				if text != "" {
					t.Fatalf("degraded path must produce no conclusion text, got %q", text)
				}
			}
			// 指标：每次请求恰记一次、outcome 分桶正确（降级必须可计数）。
			body := gatherMetrics(t, m)
			if !strings.Contains(body, `opscopilot_llm_requests_total{outcome="`+tc.outcome+`"} 1`) {
				t.Fatalf("metric outcome=%s missing:\n%s", tc.outcome, body)
			}
		})
	}
}

// TestLLMSummarizerNeverLeaks 脱敏纪律：成功/失败日志行都不得含 key 与
// prompt 正文（证据内容特征串），只允许长度/哈希/模型/耗时类字段。
func TestLLMSummarizerNeverLeaks(t *testing.T) {
	url, _ := mockGateway(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = io.WriteString(w, `upstream rejected prompt evidence-secret-marker`)
	})
	var logs []string
	gw, err := newLLMGateway(config.LLMSection{Endpoint: url, APIKey: "sk-leak-canary", Model: "mock-1", Timeout: time.Second, MaxTokens: 512})
	if err != nil {
		t.Fatal(err)
	}
	s := newLLMSummarizer(gw, NewAppMetrics(), func(f string, a ...any) { logs = append(logs, fmt.Sprintf(f, a...)) }, config.DefaultRCAMaxFindings)
	if _, err := s.Summarize(sampleInput(), sampleFindings()); !errors.Is(err, rca.ErrNotImplemented) {
		t.Fatalf("want fail-open sentinel, got %v", err)
	}
	joined := strings.Join(logs, "\n")
	for _, banned := range []string{"sk-leak-canary", "evidence-secret-marker", "根因：变更 dep-1"} {
		if strings.Contains(joined, banned) {
			t.Fatalf("log leaked %q:\n%s", banned, joined)
		}
	}
	if !strings.Contains(joined, "prompt_hash=") || !strings.Contains(joined, "model=mock-1") {
		t.Fatalf("log must carry sanitized fields, got:\n%s", joined)
	}
}

// TestLLMSummarizerTruncatesLongConclusion 防御：超长结论按 rune 截断，
// 不产出非法 UTF-8、不撑爆响应。
func TestLLMSummarizerTruncatesLongConclusion(t *testing.T) {
	long := strings.Repeat("故", llmConclusionCharLimit+500)
	url, _ := mockGateway(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, completion(`{"conclusion":"`+long+`"}`))
	})
	s, _ := newTestSummarizer(t, url, 2*time.Second)
	text, err := s.Summarize(sampleInput(), sampleFindings())
	if err != nil {
		t.Fatal(err)
	}
	if got := len([]rune(text)); got > llmConclusionCharLimit+1 {
		t.Fatalf("conclusion runes = %d, want <= %d", got, llmConclusionCharLimit+1)
	}
	if !strings.HasSuffix(text, "…") {
		t.Fatalf("truncated text must end with ellipsis, got %q", text[len(text)-6:])
	}
}

// TestLLMSummarizerPromptRespectsFindingsCap 二期池波二 #6：prompt 证据
// 行同守 OPS_RCA_MAX_FINDINGS——超限按置信度优先截断（llmgw 输入不随大
// 规模簇失控），并把 findings_truncated 如实告知模型（静默丢证据骗模型）。
func TestLLMSummarizerPromptRespectsFindingsCap(t *testing.T) {
	url, caps := mockGateway(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, completion(`{"conclusion":"上限内结论"}`))
	})
	gw, err := newLLMGateway(llmSectionTO(url, 2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	s := newLLMSummarizer(gw, NewAppMetrics(), nil, 10)
	var prev []rca.Finding
	for i := 0; i < 15; i++ { // 15 条 low：同低档里再靠时序垫底
		prev = append(prev, rca.Finding{Step: rca.StepVerify, Summary: fmt.Sprintf("low-%d", i),
			Confidence: "low", Ref: fmt.Sprintf("lv%d", i)})
	}
	for i := 0; i < 10; i++ { // 10 条 high：置信度优先，必须全部存活
		prev = append(prev, rca.Finding{Step: rca.StepAttribution, Summary: fmt.Sprintf("high-%d", i),
			Confidence: "high", Ref: fmt.Sprintf("at%d", i)})
	}
	if _, err := s.Summarize(sampleInput(), prev); err != nil {
		t.Fatalf("summarize: %v", err)
	}
	last := (*caps)[len(*caps)-1]
	var doc llmEvidenceDoc
	if err := json.Unmarshal([]byte(last.Messages[1].Content), &doc); err != nil {
		t.Fatalf("prompt not JSON: %v", err)
	}
	if len(doc.Findings) != 10 || doc.FindingsTruncated != 15 {
		t.Fatalf("prompt cap wrong: findings=%d truncated=%v, want 10/15", len(doc.Findings), doc.FindingsTruncated)
	}
	for _, f := range doc.Findings {
		if f.Confidence != "high" {
			t.Fatalf("confidence-first violated, low survived: %+v", doc.Findings)
		}
	}
	if len(doc.RootCauses) != 10 { // high attribution 全保留 → root_causes 不缩水
		t.Fatalf("root causes = %d, want 10", len(doc.RootCauses))
	}
}

// TestRCAConcludeWiredThroughOrchestrator 端到端（内存形态）：注入接了假
// 网关的真 Summarizer 后，conclude 步 done、finding 非空；REST 视图层面
// conclusion 非 null 且 llm_used=true。
func TestRCAConcludeWiredThroughOrchestrator(t *testing.T) {
	url, _ := mockGateway(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, completion(`{"conclusion":"变更 dep-wired 部署触及故障节点，建议回滚","confidence":"high","caveats":["仅一条变更证据"]}`))
	})
	e := newRCAEnv(t, nil)
	e.seedIncident(t, "INC-gw", "n1")
	e.recordChange(t, changeFixture("dep-wired", "prometheus://nodes/n1"))
	gw, err := newLLMGateway(llmSectionTO(url, 2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	e.orch.SetSummarizer(newLLMSummarizer(gw, NewAppMetrics(), nil, config.DefaultRCAMaxFindings))
	res, err := e.orch.Analyze(context.Background(), "INC-gw", "")
	if err != nil {
		t.Fatalf("analyze: %v", err)
	}
	var done bool
	for _, sr := range res.Report.Steps {
		if sr.Name == rca.StepConclude {
			done = sr.Status == rca.StatusDone
		}
	}
	if !done {
		t.Fatalf("conclude step not done: %+v", res.Report.Steps)
	}
	v := newRCAView(res, "mem", false)
	if v.Conclusion == nil || !strings.Contains(*v.Conclusion, "dep-wired") || !strings.Contains(*v.Conclusion, "仅一条变更证据") {
		t.Fatalf("view conclusion = %v", v.Conclusion)
	}
	if !v.LLMUsed {
		t.Fatal("llm_used must be true after wiring")
	}
}

// TestRCAConcludeFailOpenThroughOrchestrator 端到端降级：网关 401 时
// Analyze 必须成功返回 200 语义（非 error），conclude 步 pending、
// conclusion=null——/rca 绝不因 LLM 挂掉而 5xx。
func TestRCAConcludeFailOpenThroughOrchestrator(t *testing.T) {
	url, _ := mockGateway(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	})
	e := newRCAEnv(t, nil)
	e.seedIncident(t, "INC-deg", "n1")
	e.recordChange(t, changeFixture("dep-deg", "prometheus://nodes/n1"))
	gw, err := newLLMGateway(llmSectionTO(url, 2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	e.orch.SetSummarizer(newLLMSummarizer(gw, NewAppMetrics(), nil, config.DefaultRCAMaxFindings))
	res, err := e.orch.Analyze(context.Background(), "INC-deg", "")
	if err != nil {
		t.Fatalf("degraded analyze must not error: %v", err)
	}
	var pending bool
	for _, sr := range res.Report.Steps {
		if sr.Name == rca.StepConclude {
			pending = sr.Status == rca.StatusPending
		}
	}
	if !pending {
		t.Fatalf("conclude step must be pending on gateway failure: %+v", res.Report.Steps)
	}
	v := newRCAView(res, "mem", false)
	if v.Conclusion != nil || v.LLMUsed {
		t.Fatalf("fail-open view must keep conclusion=null/llm_used=false, got %v/%v", v.Conclusion, v.LLMUsed)
	}
	// 建议步仍在（conclude pending 不中止流水线——ADR-014 语义保持）。
	var rec bool
	for _, f := range v.Findings {
		if f.Step == rca.StepRecommend {
			rec = true
		}
	}
	if !rec {
		t.Fatal("recommend step lost after conclude degrade")
	}
}

// TestNewLLMGatewayDisabledContract OPS_LLM_ENDPOINT 空 = (nil, nil)：
// 装配层据此根本不注入 Summarizer（现状逐字节一致路径的构造性保证）；
// 禁用态下全新指标集的 llm 计数保持 0（不发请求 = 零样本）。
func TestNewLLMGatewayDisabledContract(t *testing.T) {
	gw, err := newLLMGateway(llmSection("", ""))
	if gw != nil || err != nil {
		t.Fatalf("empty endpoint must yield (nil, nil), got (%v, %v)", gw, err)
	}
	if _, err := newLLMGateway(llmSection("not-a-url", "m")); err == nil {
		t.Fatal("malformed endpoint must be rejected at construction")
	}
	for _, line := range strings.Split(gatherMetrics(t, NewAppMetrics()), "\n") {
		if strings.HasPrefix(line, "opscopilot_llm_requests_total") && !strings.HasSuffix(line, " 0") {
			t.Fatalf("llm counters must start at zero: %s", line)
		}
	}
}

// TestRCAConcludeUnwiredStaysPending 未注入（= OPS_LLM_ENDPOINT 未配置的
// 装配缺省形态）：conclude pending、conclusion=null、llm_used=false——
// 与 ADR-014 现状逐字节一致（该形态同时被现有零修改测试面锁死）。
func TestRCAConcludeUnwiredStaysPending(t *testing.T) {
	e := newRCAEnv(t, nil) // 不 SetSummarizer——装配缺省形态
	e.seedIncident(t, "INC-off", "n1")
	res, err := e.orch.Analyze(context.Background(), "INC-off", "")
	if err != nil {
		t.Fatalf("analyze: %v", err)
	}
	v := newRCAView(res, "mem", false)
	if v.Conclusion != nil || v.LLMUsed {
		t.Fatalf("unwired must keep conclusion=null/llm_used=false, got %v/%v", v.Conclusion, v.LLMUsed)
	}
	for _, sr := range res.Report.Steps {
		if sr.Name == rca.StepConclude && sr.Status != rca.StatusPending {
			t.Fatalf("conclude status = %s, want pending", sr.Status)
		}
	}
}
