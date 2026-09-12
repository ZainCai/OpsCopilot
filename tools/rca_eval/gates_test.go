// gates_test.go 转正三门禁判定核的表驱动测试——全部用 mock /rca 响应
// （JSON 字符串反序列化进 rcaResp，形态对齐 rest_rca.go 真实载荷），零
// PG/零 HTTP/零外网：本轮 LLM 端点尚未配置，逻辑正确性以离线样本为准。
package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

// mockRCA 把 mock 的 GET /rca JSON 解进 rcaResp 并模拟 scoreFaultSegment
// 的字段搬运（RootRefs + hydrateUnit），返回填充完毕的 unit。
func mockRCA(t *testing.T, body string, expected string, myPrefix string, logicalNode map[string]string) unit {
	t.Helper()
	var rr rcaResp
	if err := json.Unmarshal([]byte(body), &rr); err != nil {
		t.Fatalf("mock /rca 响应解析失败: %v", err)
	}
	u := unit{Kind: "rca", Status: "OK", ClusterID: "A-n1", SegCode: "A", ExpectedChange: expected}
	for _, rc := range rr.RootCauses {
		u.RootRefs = append(u.RootRefs, rc.Ref)
	}
	u.StepsDone, u.StepsPending, u.StepsFailed = countSteps(rr)
	hydrateUnit(&u, &rr, myPrefix, logicalNode)
	if len(u.RootRefs) > 0 && u.RootRefs[0] == u.ExpectedChange {
		u.Top1 = true
	}
	return u
}

const mockPrefix = "chg-20260101-000000-"

var mockNodes = map[string]string{"A-root": "prometheus://nodes/n1", "A-d1": "prometheus://nodes/n1"}

// rcaBody 构造一份 /rca 响应 JSON（六步形态按 conclude 状态与 LLM 开关拼装）。
func rcaBody(topRef, conclusion string, llmUsed bool, concludeStatus string) string {
	steps := `[{"name":"fetch_alerts","status":"done"},{"name":"load_topology","status":"done"},` +
		`{"name":"replay_changes","status":"done"},{"name":"correlate","status":"done"},` +
		`{"name":"rank_root_causes","status":"done"},` +
		`{"name":"conclude","status":"` + concludeStatus + `"}]`
	con := "null"
	if conclusion != "" {
		con = fmt.Sprintf(`"n1 磁盘打满由变更 %s 发版引入"`, conclusion)
	}
	root := fmt.Sprintf(`[{"step":"rank_root_causes","summary":"n1 发版致磁盘打满","confidence":"high","ref":"%s"}]`, topRef)
	return fmt.Sprintf(`{"incident_id":"inc-1","llm_used":%s,"conclusion":%s,"steps":%s,"root_causes":%s}`,
		boolStr(llmUsed), con, steps, root)
}

func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

func TestGatesTableDriven(t *testing.T) {
	wantTop1 := mockPrefix + "A-root"
	cases := []struct {
		name     string
		unit     unit
		wantG2OK bool
		wantG3   string // pass | suspect | fail | ""(未评)
	}{
		{
			name: "证据版形态（pending/null/false）出现在 LLM 模式 → G2 挂、G3 不评",
			unit: mockRCA(t, rcaBody(wantTop1, "", false, "pending"), wantTop1, mockPrefix, mockNodes),
		},
		{
			name:     "done+结论含 ref 锚点 → G2 过、G3 pass",
			unit:     mockRCA(t, rcaBody(wantTop1, wantTop1, true, "done"), wantTop1, mockPrefix, mockNodes),
			wantG2OK: true, wantG3: "pass",
		},
		{
			name:     "done+结论含 node_key（大小写/分隔符变体）→ G2 过、G3 pass",
			unit:     mockRCA(t, rcaBody(wantTop1, "根因位于 PROMETHEUS//Nodes/N1，建议回滚", true, "done"), wantTop1, mockPrefix, mockNodes),
			wantG2OK: true, wantG3: "pass",
		},
		{
			name:     "done+锚点均未命中 → G3 suspect（不判死）",
			unit:     mockRCA(t, rcaBody(wantTop1, "数据库连接池耗尽导致整体超时", true, "done"), wantTop1, mockPrefix, mockNodes),
			wantG2OK: true, wantG3: "suspect",
		},
		{
			name:     "done+矛盾话术（证据不足/无法定因）→ G3 硬性失败",
			unit:     mockRCA(t, rcaBody(wantTop1, "证据不足，无法定因", true, "done"), wantTop1, mockPrefix, mockNodes),
			wantG2OK: true, wantG3: "fail",
		},
		{
			name: "llm_used=false 但有文本结论 → G2 挂（缺 llm_used）",
			unit: func() unit {
				u := mockRCA(t, rcaBody(wantTop1, wantTop1, false, "done"), wantTop1, mockPrefix, mockNodes)
				return u
			}(),
			wantG3: "pass", // G3 独立计数：锚点命中照常评
		},
		{
			name: "conclude 步未 done（结论漏记状态）→ G2 挂",
			unit: func() unit {
				u := mockRCA(t, rcaBody(wantTop1, "n1 变更 chg-20260101-000000-A-root 发版引入", true, "pending"), wantTop1, mockPrefix, mockNodes)
				return u
			}(),
			wantG3: "pass",
		},
	}
	// 用例 1 的 G2/G3 字段基线（零值）在表里显式编码为 wantG2OK=false/wantG3=""。
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotG2OK := tc.unit.LLMUsed && strings.TrimSpace(tc.unit.Conclusion) != "" && tc.unit.ConcludeDone
			if gotG2OK != tc.wantG2OK {
				t.Errorf("G2 = %v, want %v（llm_used=%v conclusion=%q conclude_done=%v）",
					gotG2OK, tc.wantG2OK, tc.unit.LLMUsed, tc.unit.Conclusion, tc.unit.ConcludeDone)
			}
			if tc.unit.G3Status != tc.wantG3 {
				t.Errorf("G3 = %q, want %q（conclusion=%q anchors=%v）",
					tc.unit.G3Status, tc.wantG3, tc.unit.Conclusion, tc.unit.G3Anchors)
			}
		})
	}
}

func TestEvalPromotionVerdicts(t *testing.T) {
	top1Ref := mockPrefix + "A-root"
	mk := func(t *testing.T, body string, top1 bool) unit {
		u := mockRCA(t, body, top1Ref, mockPrefix, mockNodes)
		if !top1 {
			u.Top1 = false
		}
		return u
	}
	all := func(us ...unit) []unit {
		out := append(us, unit{Kind: "silence_guard", Status: "GUARD_PASS"})
		return out
	}
	t.Run("三门全过+守护过 → PASS", func(t *testing.T) {
		var us []unit
		for i := 0; i < 4; i++ {
			us = append(us, mk(t, rcaBody(top1Ref, top1Ref, true, "done"), true))
		}
		p := evalPromotion(all(us...), false, 0.85, 1, 1)
		if !p.Pass || p.G1Hits != 4 || p.G2Pass != 4 || p.G3Pass != 4 {
			t.Fatalf("want PASS 4/4/4, got %+v", p)
		}
	})
	t.Run("G2 缺一例（100% 硬线）→ FAIL", func(t *testing.T) {
		us := []unit{
			mk(t, rcaBody(top1Ref, top1Ref, true, "done"), true),
			mk(t, rcaBody(top1Ref, top1Ref, true, "done"), true),
			mk(t, rcaBody(top1Ref, top1Ref, true, "done"), true),
			mk(t, rcaBody(top1Ref, "", true, "pending"), true),
		}
		p := evalPromotion(all(us...), false, 0.85, 1, 1)
		if p.Pass || p.G2Pass != 3 || p.G2Total != 4 || len(p.G2Fails) != 1 {
			t.Fatalf("want G2 3/4 FAIL, got %+v", p)
		}
	})
	t.Run("G1 3/4=75%<85% → FAIL（其余全过）", func(t *testing.T) {
		us := []unit{
			mk(t, rcaBody(top1Ref, top1Ref, true, "done"), true),
			mk(t, rcaBody(top1Ref, top1Ref, true, "done"), true),
			mk(t, rcaBody(top1Ref, top1Ref, true, "done"), true),
			mk(t, rcaBody("chg-other", "chg-other 相关结论", true, "done"), false),
		}
		p := evalPromotion(all(us...), false, 0.85, 1, 1)
		if p.Pass || p.G1Pass || p.G1Rate > 0.76 {
			t.Fatalf("want G1 FAIL@75%%, got %+v", p)
		}
	})
	t.Run("G3 存疑不判死：suspect=1 其余全过 → PASS", func(t *testing.T) {
		us := []unit{
			mk(t, rcaBody(top1Ref, top1Ref, true, "done"), true),
			mk(t, rcaBody(top1Ref, "连接池耗尽", true, "done"), true), // 锚点未命中
			mk(t, rcaBody(top1Ref, top1Ref, true, "done"), true),
			mk(t, rcaBody(top1Ref, top1Ref, true, "done"), true),
		}
		p := evalPromotion(all(us...), false, 0.85, 1, 1)
		if !p.Pass || p.G3Suspect != 1 || len(p.Suspects) != 1 {
			t.Fatalf("want PASS with 1 suspect, got %+v", p)
		}
		if p.Suspects[0].Conclusion == "" || p.Suspects[0].Ref != top1Ref {
			t.Errorf("存疑清单须带 conclusion 摘要 + ref，got %+v", p.Suspects[0])
		}
	})
	t.Run("G3 硬性失败一票否决（即使锚点本可命中）", func(t *testing.T) {
		us := []unit{
			mk(t, rcaBody(top1Ref, "证据不足（锚点 "+top1Ref+" 虽然在内）", true, "done"), true),
			mk(t, rcaBody(top1Ref, top1Ref, true, "done"), true),
			mk(t, rcaBody(top1Ref, top1Ref, true, "done"), true),
			mk(t, rcaBody(top1Ref, top1Ref, true, "done"), true),
		}
		p := evalPromotion(all(us...), false, 0.85, 1, 1)
		if p.Pass || p.G3Fail != 1 || len(p.HardFails) != 1 {
			t.Fatalf("want G3 hard-fail veto, got %+v", p)
		}
	})
	t.Run("守护 FAIL 一票否决（三门全过也不行）", func(t *testing.T) {
		us := []unit{mk(t, rcaBody(top1Ref, top1Ref, true, "done"), true)}
		p := evalPromotion(append(us, unit{Kind: "silence_guard", Status: "GUARD_FAIL"}), false, 0.85, 1, 0)
		if p.Pass || len(p.GuardNotes) == 0 {
			t.Fatalf("want guard veto, got %+v", p)
		}
	})
	t.Run("未跑通单元挂 G1/G2 分母（无从消失刷通过率）", func(t *testing.T) {
		bad := unit{Kind: "rca", Status: "ALERT_MISS", SegCode: "B", ClusterID: "B-n2"}
		us := []unit{
			mk(t, rcaBody(top1Ref, top1Ref, true, "done"), true),
			mk(t, rcaBody(top1Ref, top1Ref, true, "done"), true),
			mk(t, rcaBody(top1Ref, top1Ref, true, "done"), true),
			bad,
		}
		p := evalPromotion(all(us...), false, 0.85, 1, 1)
		if p.Pass || p.G1Total != 4 || p.G2Total != 4 || p.G3Total != 3 {
			t.Fatalf("want incomplete unit counted in G1/G2 but not G3, got %+v", p)
		}
	})
	t.Run("证据版基线：转正判定不适用", func(t *testing.T) {
		p := evalPromotion(nil, true, 0.85, 0, 0)
		if p.Applicable || p.Pass {
			t.Fatalf("evidence mode must be not-applicable, got %+v", p)
		}
	})
}

func TestG3HeuristicBoundaries(t *testing.T) {
	cases := []struct {
		name       string
		conclusion string
		anchors    []string
		wantStatus string
	}{
		{"ref 原样命中", "变更 chg-20260101-000000-A-root 于 n1 发版", []string{"chg-20260101-000000-A-root"}, "pass"},
		{"ref 大小写变体命中", "CHG-20260101-000000-A-ROOT 已回滚", []string{"chg-20260101-000000-a-root"}, "pass"},
		{"ref 去连字符变体命中", "变更 chg20260101 000000 A root（转写）", []string{"chg-20260101-000000-A-root"}, "pass"},
		{"node_key 双斜杠分隔符命中", "根因在 prometheus://nodes/n1", []string{"prometheus://nodes/n1"}, "pass"},
		{"node_key 单斜杠/裸写变体命中", "根因在 prometheus/nodes/n1 节点", []string{"prometheus://nodes/n1"}, "pass"},
		{"结论只提裸节点名不算命中长 node_key 锚点 → suspect", "n1 磁盘打满", []string{"chg-x-A-root", "prometheus://nodes/n1"}, "suspect"},
		{"全部锚点未命中 → suspect", "数据库慢查询拖垮网关", []string{"chg-x-A-root", "prometheus://nodes/n1"}, "suspect"},
		{"英文矛盾话术大小写不敏感", "Unable To Determine the root cause", []string{"any"}, "fail"},
		{"中文矛盾话术优先于锚点命中 → fail（一票否决不因锚点豁免）", "证据不足，虽提及 chg-x-A-root", []string{"chg-x-A-root"}, "fail"},
		{"pending 子串命中英文回退文案", "conclusion is still PENDING", []string{"chg-x"}, "fail"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st, _, ph := g3Of(tc.conclusion, tc.anchors)
			if st != tc.wantStatus {
				t.Errorf("g3Of = %q (phrases=%v), want %q", st, ph, tc.wantStatus)
			}
		})
	}
	t.Run("空结论不参与 G3", func(t *testing.T) {
		st, _, _ := g3Of("   ", []string{"anything"})
		if st != "" {
			t.Errorf("empty conclusion should skip G3, got %q", st)
		}
	})
}

func TestNormalizeAndTruncate(t *testing.T) {
	if got := normalizeAnchor("Prometheus://Nodes/N1 磁盘（dep-lo y_2）"); got != "prometheusnodesn1磁盘deploy2" {
		t.Errorf("normalizeAnchor = %q", got)
	}
	if got := truncateRunes("中文mixed结论abcdefghij", 6); !strings.HasSuffix(got, "…") ||
		strings.HasPrefix(got, "…") || len([]rune(got)) != 7 {
		t.Errorf("truncateRunes rune-unsafe: %q", got)
	}
	if got := truncateRunes("short", 10); got != "short" {
		t.Errorf("truncateRunes should pass through short strings, got %q", got)
	}
}

func TestBuildSummaryLLMModeGate(t *testing.T) {
	top1Ref := mockPrefix + "A-root"
	var us []unit
	for i := 0; i < 4; i++ {
		u := mockRCA(t, rcaBody(top1Ref, top1Ref, true, "done"), top1Ref, mockPrefix, mockNodes)
		u.EvidenceComplete = true
		us = append(us, u)
	}
	us = append(us, unit{Kind: "silence_guard", Status: "GUARD_PASS"})
	s := buildSummary(us, "20260101-000000", "rca-eval-t", false, 0.85)
	if !s.GatePassed || !s.Promotion.Pass || s.Mode != "llm" {
		t.Fatalf("all-green llm run must PASS, got gate=%v promo=%+v mode=%s", s.GatePassed, s.Promotion, s.Mode)
	}
	// 证据版路径：promotion 不适用，旧语义原样保留。
	s2 := buildSummary(us, "20260101-000000", "rca-eval-t", true, 0.85)
	if s2.Promotion.Applicable || !s2.GatePassed {
		t.Fatalf("evidence mode keeps legacy gate & not-applicable promotion, got %+v", s2.Promotion)
	}
}

func TestRenderReportPromotionSection(t *testing.T) {
	top1Ref := mockPrefix + "A-root"
	g := &goldenDoc{
		Source:      map[string]string{"playbook": "tools/faultinjector/main.go"},
		Maintenance: []string{"m1"},
		Segments: []goldenSegment{
			{Idx: 0, Code: "A", Name: "单节点磁盘故障", SrcLines: "56-60",
				DurationSec: 300, ExpectedNew: 1,
				Clusters: []goldenCluster{{ID: "A-n1", Nodes: []string{"n1"}}},
				Changes:  []goldenChange{{LogicalID: "A-root", Role: "root", Node: "n1"}}},
			{Idx: 1, Code: "C", Name: "静默", SrcLines: "1", Silence: true},
		},
	}
	ab := &answerbook{Segments: []abSegment{
		{Scenario: "A", Name: "s", DurationSec: 300, ExpectedNew: 1, Start: time.Now(), End: time.Now().Add(time.Minute)},
		{Scenario: "C", Name: "静默", DurationSec: 300, ExpectedNew: 0, Start: time.Now().Add(time.Minute), End: time.Now().Add(2 * time.Minute)},
	}}
	var us []unit
	for i := 0; i < 4; i++ {
		body := rcaBody(top1Ref, top1Ref, true, "done")
		if i == 3 {
			body = rcaBody(top1Ref, "网关超时", true, "done") // 锚点未命中 → suspect
		}
		u := mockRCA(t, body, top1Ref, mockPrefix, mockNodes)
		u.EvidenceComplete = true
		us = append(us, u)
	}
	us = append(us, unit{Kind: "silence_guard", SegIdx: 1, Status: "GUARD_PASS"})

	s := buildSummary(us, "20260101-000000", "rca-eval-t", false, 0.85)
	md := renderReport(s, us, g, ab, false)
	for _, want := range []string{"§转正判定", "G1 归因不回退", "G2 结论产出", "G3 结论-证据一致性",
		"存疑清单", "PASS——可转正", "G1 4/4=100%"} {
		if !strings.Contains(md, want) {
			t.Errorf("LLM 报告缺少 %q", want)
		}
	}
	if strings.Contains(md, "OPS_LLM_API_KEY") || strings.Contains(md, "sk-") {
		t.Error("报告不得出现 key 相关字样")
	}
	// 证据版形态：三门禁显式不适用。
	sE := buildSummary(us, "20260101-000000", "rca-eval-t", true, 0.85)
	mdE := renderReport(sE, us, g, ab, true)
	if !strings.Contains(mdE, "本轮不适用") || strings.Contains(mdE, "总判定：PASS") {
		t.Error("证据版报告须标注转正判定不适用且不出 PASS")
	}
}
