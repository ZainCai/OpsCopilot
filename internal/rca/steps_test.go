// #12 最小链路步骤单测（纯逻辑，零 IO）：取证/假设/验证/归因/结论/建议
// 六步的规则语义 + 置信度纪律（ADR-007 保守取低、ADR-003 无出口不出结论）。
package rca

import (
	"errors"
	"strings"
	"testing"
	"time"
)

// happyInput 一条"部署变更直接命中告警节点"的标准证据链。
func happyInput() Input {
	t0 := time.Date(2026, 9, 12, 10, 0, 0, 0, time.UTC)
	return Input{
		TenantID:     "default",
		ClusterKey:   "c:fp1@0",
		T0:           t0,
		Window:       30 * time.Minute,
		AlertedNodes: []string{"n1"},
		Evidence: Evidence{
			Nodes: []NodeFact{
				{Key: "n1", Type: "host", Confidence: "high", Source: "prom"},
				{Key: "n2", Type: "host", Confidence: "medium", Source: "prom"},
			},
			Edges: []EdgeFact{{SrcKey: "n2", DstKey: "n1", Relation: "depends_on", Confidence: "medium"}},
			Changes: []ChangeFact{
				{ID: "c-deploy", NodeKey: "n1", ChangeType: "deploy", Source: "jenkins",
					Actor: "alice", Summary: "v2 上线", OccurredAt: t0.Add(-10 * time.Minute), Confidence: "high"},
			},
		},
	}
}

// runStep 执行单步并按 step 过滤产出（内部步骤名即 s.Name()）。
func runStep(t *testing.T, s Step, in Input, prev []Finding) []Finding {
	t.Helper()
	fs, err := s.Run(in, prev)
	if err != nil {
		t.Fatalf("%s: %v", s.Name(), err)
	}
	var out []Finding
	for _, f := range fs {
		if f.Step == s.Name() {
			out = append(out, f)
		}
	}
	return out
}

// ---------- collect ----------

func TestCollectConfidenceReflectsEvidenceCompleteness(t *testing.T) {
	in := happyInput()
	fs := runStep(t, collectStep{}, in, nil)
	if len(fs) != 1 || fs[0].Confidence != "high" {
		t.Fatalf("complete evidence: %+v, want single high", fs)
	}
	// 故障域含 T0 不可见节点 → medium。
	in.AlertedNodes = []string{"n1", "ghost"}
	fs = runStep(t, collectStep{}, in, nil)
	if fs[0].Confidence != "medium" || !strings.Contains(fs[0].Summary, "ghost") {
		t.Fatalf("invisible domain node: %+v, want medium + named", fs[0])
	}
	// 域节点 low 置信（因果门禁拦下）→ medium。
	in = happyInput()
	in.Evidence.Nodes[0].Confidence = "low"
	in.AlertedNodes = []string{"n1"}
	fs = runStep(t, collectStep{}, in, nil)
	if fs[0].Confidence != "medium" {
		t.Fatalf("gated node: %+v, want medium", fs[0])
	}
	// 空域 → low。
	in = happyInput()
	in.AlertedNodes = nil
	fs = runStep(t, collectStep{}, in, nil)
	if fs[0].Confidence != "low" {
		t.Fatalf("empty domain: %+v, want low", fs[0])
	}
}

// ---------- hypothesize ----------

func TestHypothesizeRulesAndGates(t *testing.T) {
	in := happyInput()
	fs := runStep(t, hypothesizeStep{}, in, nil)
	if len(fs) != 1 || fs[0].Ref != "c-deploy" || fs[0].Confidence != "high" {
		t.Fatalf("direct hit: %+v, want one high hypothesis on c-deploy", fs)
	}

	// 邻域命中：降一档（high 变更 + medium 节点 → 取低 medium → 邻域再降 low，
	// low 且非域内 → 噪声丢弃）。
	in = happyInput()
	in.Evidence.Changes[0].NodeKey = "n2"
	fs = runStep(t, hypothesizeStep{}, in, nil)
	// medium 变更 × medium 节点 = medium，邻域再降 low——low 且非域内按噪声丢弃，
	// 只剩"无变更证据"兜底行（Ref 为空）。
	if len(fs) != 1 || fs[0].Ref != "" || !strings.Contains(fs[0].Summary, "无触及故障域") {
		t.Fatalf("neighbor low hypothesis should be dropped, got %+v", fs)
	}
	// 邻域 + high 节点：medium 保留。
	in.Evidence.Nodes[1].Confidence = "high"
	in.Evidence.Changes[0].Confidence = "high" // min(high,high)=high → 邻域降 medium
	fs = runStep(t, hypothesizeStep{}, in, nil)
	if len(fs) != 1 || fs[0].Confidence != "medium" {
		t.Fatalf("neighbor strong: %+v, want single medium", fs)
	}

	// 窗口外（晚于 T0 / 早于 T0-Window）不构成假设；Window=0 只要求不晚于 T0。
	in = happyInput()
	in.Evidence.Changes[0].OccurredAt = in.T0.Add(time.Minute)
	fs = runStep(t, hypothesizeStep{}, in, nil)
	if len(fs) != 1 || !strings.Contains(fs[0].Summary, "无触及故障域") {
		t.Fatalf("post-T0 change: %+v, want no-change hypothesis", fs)
	}
	in.Evidence.Changes[0].OccurredAt = in.T0.Add(-90 * time.Minute)
	in.Window = 0
	fs = runStep(t, hypothesizeStep{}, in, nil)
	if len(fs) != 1 || fs[0].Ref != "c-deploy" {
		t.Fatalf("unbounded window: %+v, want hypothesis kept", fs)
	}

	// 与故障域无空间关系的变更被忽略。
	in = happyInput()
	in.Evidence.Changes[0].NodeKey = "far-away"
	in.Evidence.Nodes = append(in.Evidence.Nodes, NodeFact{Key: "far-away", Confidence: "high"})
	fs = runStep(t, hypothesizeStep{}, in, nil)
	if !strings.Contains(fs[0].Summary, "无触及故障域") || fs[0].Confidence != "low" {
		t.Fatalf("unrelated change: %+v, want low no-change finding", fs[0])
	}
}

// ---------- verify ----------

func TestVerifyScoring(t *testing.T) {
	in := happyInput()
	hyps := runStep(t, hypothesizeStep{}, in, nil)
	fs := runStep(t, verifyStep{}, in, hyps)
	if len(fs) != 1 || fs[0].Confidence != "high" || !strings.Contains(fs[0].Summary, "通过") {
		t.Fatalf("verified high: %+v", fs)
	}

	// 节点被因果门禁拦下（low）→ 验证失败 low。
	in.Evidence.Nodes[0].Confidence = "low"
	fs = runStep(t, verifyStep{}, in, hyps)
	if fs[0].Confidence != "low" || !strings.Contains(fs[0].Summary, "未通过") {
		t.Fatalf("gated node: %+v, want failed low", fs[0])
	}

	// medium 变更 + 域内节点 → medium。
	in = happyInput()
	in.Evidence.Changes[0].Confidence = "medium"
	hyps = runStep(t, hypothesizeStep{}, in, nil)
	fs = runStep(t, verifyStep{}, in, hyps)
	if fs[0].Confidence != "medium" {
		t.Fatalf("medium change: %+v, want medium", fs[0])
	}

	// 假设悬空（变更证据被裁剪掉）→ low。
	in.Evidence.Changes = nil
	fs = runStep(t, verifyStep{}, in, hyps)
	if fs[0].Confidence != "low" || !strings.Contains(fs[0].Summary, "悬空") {
		t.Fatalf("dangling ref: %+v, want low", fs[0])
	}

	// 上游无变更假设：验证步承认"无可验证假设"。
	in = happyInput()
	in.Evidence.Changes = nil
	noHyp := runStep(t, hypothesizeStep{}, in, nil)
	fs = runStep(t, verifyStep{}, in, noHyp)
	if len(fs) != 1 || fs[0].Ref != "" || fs[0].Confidence != "low" {
		t.Fatalf("no hypotheses: %+v, want single low ack", fs)
	}
}

// ---------- attribution ----------

func TestAttributionPicksMostSuspect(t *testing.T) {
	in := happyInput()
	// 加一条更早、medium 的候选：high 首要（贴近 T0 且在验证里 medium/high 皆可入链）。
	earlier := ChangeFact{ID: "c-earlier", NodeKey: "n1", ChangeType: "config_change",
		Source: "manual", Actor: "bob", OccurredAt: in.T0.Add(-25 * time.Minute), Confidence: "medium"}
	in.Evidence.Changes = append(in.Evidence.Changes, earlier)

	chain := run(t, NewPipeline(collectStep{}, hypothesizeStep{}, verifyStep{}, attributionStep{}), in)
	attrs := findingsOf(chain, StepAttribution)
	if len(attrs) == 0 {
		t.Fatalf("want attributions, got steps %+v", chain.Steps)
	}
	if attrs[0].Ref != "c-deploy" || attrs[0].Confidence != "high" {
		t.Fatalf("primary: %+v, want high c-deploy first", attrs[0])
	}
	if len(attrs) != 2 || attrs[1].Confidence != "medium" || !strings.HasPrefix(attrs[1].Summary, "次要候选") {
		t.Fatalf("secondary: %+v", attrs)
	}
	// rootCauses 只挑 high（既有 rca_test.go 已锁框架语义，这里锁端到端）。
	if len(chain.RootCauses) != 1 || chain.RootCauses[0].Ref != "c-deploy" {
		t.Fatalf("root causes = %+v, want only c-deploy", chain.RootCauses)
	}
}

func TestAttributionNoEvidenceStaysLow(t *testing.T) {
	in := happyInput()
	in.Evidence.Changes = nil
	chain := run(t, NewPipeline(collectStep{}, hypothesizeStep{}, verifyStep{}, attributionStep{}), in)
	attrs := findingsOf(chain, StepAttribution)
	if len(attrs) != 1 || attrs[0].Confidence != "low" || attrs[0].Ref != "" {
		t.Fatalf("no-evidence attribution: %+v, want single low without ref", attrs)
	}
	if len(chain.RootCauses) != 0 {
		t.Fatalf("must not claim root cause: %+v", chain.RootCauses)
	}
}

// ---------- conclude ----------

type fakeSummarizer struct {
	text string
	err  error
}

func (f fakeSummarizer) Summarize(Input, []Finding) (string, error) {
	return f.text, f.err
}

func TestConcludePendingWithoutLLM(t *testing.T) {
	rep, err := run2(t, NewDefaultPipeline(nil), happyInput())
	if err != nil {
		t.Fatalf("pipeline: %v", err)
	}
	for _, sr := range rep.Steps {
		if sr.Name == StepConclude && sr.Status != StatusPending {
			t.Fatalf("conclude status = %s, want pending", sr.Status)
		}
	}
	for _, f := range rep.Findings {
		if f.Step == StepConclude {
			t.Fatalf("no LLM must produce no conclusion finding: %+v", f)
		}
	}
}

func TestConcludeWithLLMEgress(t *testing.T) {
	rep, err := run2(t, NewDefaultPipeline(fakeSummarizer{text: "根因是 v2 上线"}), happyInput())
	if err != nil {
		t.Fatalf("pipeline: %v", err)
	}
	fs := findingsOf(rep, StepConclude)
	if len(fs) != 1 || fs[0].Summary != "根因是 v2 上线" || fs[0].Confidence != "high" {
		t.Fatalf("conclude: %+v, want high LLM text", fs)
	}
	// gateway 真失败不得被洗成 pending（R6 约定）。
	p := NewDefaultPipeline(fakeSummarizer{err: errors.New("gateway down")})
	if _, err := run2(t, p, happyInput()); err == nil || !strings.Contains(err.Error(), "gateway down") {
		t.Fatalf("llm failure must abort: %v", err)
	}
}

// ---------- recommend ----------

func TestRecommendRollbackAndIntegrity(t *testing.T) {
	rep := run(t, NewDefaultPipeline(nil), happyInput())
	recs := findingsOf(rep, StepRecommend)
	if len(recs) != 1 || !strings.Contains(recs[0].Summary, "回滚变更 c-deploy") || recs[0].Confidence != "high" {
		t.Fatalf("recommend: %+v, want high rollback of c-deploy", recs)
	}
	// 首要嫌疑是回滚 → 核查完整性而非再次回退。
	in := happyInput()
	in.Evidence.Changes[0].ChangeType = "rollback"
	rep = run(t, NewDefaultPipeline(nil), in)
	if recs := findingsOf(rep, StepRecommend); !strings.Contains(recs[0].Summary, "核查回滚完整性") {
		t.Fatalf("rollback suspect: %+v", recs[0])
	}
	// 无从归因 → 通用取证建议（low，明示建议非结论）。
	in = happyInput()
	in.Evidence.Changes = nil
	rep = run(t, NewDefaultPipeline(nil), in)
	recs = findingsOf(rep, StepRecommend)
	if len(recs) != 1 || recs[0].Confidence != "low" || !strings.Contains(recs[0].Summary, "OPS_RCA_WINDOW") {
		t.Fatalf("fallback recommend: %+v", recs)
	}
}

// ---------- 默认流水线端到端 ----------

func TestDefaultPipelineSixStepsShape(t *testing.T) {
	rep, err := run2(t, NewDefaultPipeline(nil), happyInput())
	if err != nil {
		t.Fatalf("pipeline: %v", err)
	}
	want := []string{StepCollect, StepHypothesize, StepVerify, StepAttribution, StepConclude, StepRecommend}
	if len(rep.Steps) != len(want) {
		t.Fatalf("steps = %d, want 6", len(rep.Steps))
	}
	for i, sr := range rep.Steps {
		if sr.Name != want[i] {
			t.Fatalf("step %d = %s, want %s", i, sr.Name, want[i])
		}
	}
	// 五步 done + conclude pending；根因非空、建议非空。
	done := 0
	for _, sr := range rep.Steps {
		if sr.Status == StatusDone {
			done++
		} else if sr.Status != StatusPending {
			t.Fatalf("step %s status = %s, want done/pending", sr.Name, sr.Status)
		}
	}
	if done != 5 {
		t.Fatalf("done steps = %d, want 5", done)
	}
	if len(rep.RootCauses) != 1 || len(findingsOf(rep, StepRecommend)) != 1 {
		t.Fatalf("report: root=%+v recs=%+v", rep.RootCauses, rep.Findings)
	}
}

// ---------- 共享小工具 ----------

func TestHelperSemantics(t *testing.T) {
	if got := normalizeConf("bogus"); got != ConfidenceLow {
		t.Fatalf("normalizeConf: %v, want low", got)
	}
	if got := minConf("high", "medium"); got != ConfidenceMedium {
		t.Fatalf("minConf: %v, want medium", got)
	}
	if got := downgrade(Confidence("high")); got != ConfidenceMedium {
		t.Fatalf("downgrade high: %v", got)
	}
	if got := downgrade(Confidence("low")); got != ConfidenceLow {
		t.Fatalf("downgrade low: %v", got)
	}
	if got := dedupSorted([]string{"b", "a", "b", ""}); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("dedupSorted: %v", got)
	}
	t0 := time.Now()
	if !withinWindow(t0.Add(-time.Minute), t0, 0) {
		t.Fatal("unbounded window must allow pre-T0")
	}
	if withinWindow(t0.Add(time.Second), t0, time.Hour) {
		t.Fatal("post-T0 must be rejected")
	}
	if withinWindow(t0.Add(-2*time.Hour), t0, time.Hour) {
		t.Fatal("older than window must be rejected")
	}
}

// ---------- 测试脚手架 ----------

func run(t *testing.T, p *Pipeline, in Input) *Report {
	t.Helper()
	rep, err := p.Run(in)
	if err != nil {
		t.Fatalf("pipeline run: %v", err)
	}
	return rep
}

func run2(t *testing.T, p *Pipeline, in Input) (*Report, error) {
	t.Helper()
	return p.Run(in)
}

func findingsOf(rep *Report, step string) []Finding {
	var out []Finding
	for _, f := range rep.Findings {
		if f.Step == step {
			out = append(out, f)
		}
	}
	return out
}
