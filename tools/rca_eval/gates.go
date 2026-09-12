// gates.go W12 LLM 转正三门禁（G1/G2/G3）判定核。
//
// 背景：0129078 交付了 --no-llm 证据版基线（top1/top3 + evidence + guards +
// foreign_refs tripwire）；ADR-015 起 OPS_LLM_* 可接线（未配置时 conclude 恒
// pending）。本轮把"带 LLM 的跑分"升级为**转正判定**：三道门禁各自独立计数、
// 报告分节呈现，总判定 PASS 才可对外转正。
//
// 三道门禁（LLM 模式专用；证据版基线不适用、报告中显式标注）：
//
//	G1 归因不回退   LLM 只该改写"叙述"，不该动规则链归因——root_causes top1
//	                 仍须命中 golden。计数：命中数/全部 RCA 单元，≥阈值（85%）。
//	G2 结论产出     llm_used=true ∧ conclusion 非空 ∧ steps 中 conclude=done。
//	                 配置了 LLM 就必须出结论：100% 是硬线（1 例 pending 即 FAIL）。
//	G3 结论-证据一致性（启发式，不接第二个 LLM 当裁判）：
//	                 - 硬性失败：conclusion 出现与"已产出结论"矛盾的话术
//	                   （pending / 证据不足 / 无法定因 …）——fail-open 回退语
//	                   混进正式结论即编排与呈现脱节；
//	                 - 锚点命中：conclusion 文本（去分隔符、折大小写后）必须
//	                   包含首要 root_cause 的可识别锚点（其 ref 或对应
//	                   node_key）——防"结论说东道西"；
//	                 - 锚点未命中但无矛盾话术 → **存疑清单**（列 conclusion
//	                   摘要 + 对应 ref + 锚点集），人工终审，不自动判死。
//
// 总判定：G1 全部 ≥85% ∧ G2 100% ∧ G3 无硬性失败 ∧（单元全部跑通 ∧ 静默
// 守护全过）→ PASS=可转正。
//
// 纯函数纪律：本文件不碰 PG/HTTP/flag/env——unit 字段在 scorePhase 里
// 填充（hydrateUnit），evalPromotion 只做聚合，故表驱动测试可直接构造
// mock /rca 响应（JSON 反序列化进 rcaResp）复现全部判定分支，零外网调用。
package main

import (
	"strconv"
	"strings"
	"unicode"
)

// ===================== 文本启发式 =====================

// normalizeAnchor 锚点归一化：折小写 + 剔除一切非字母数字（含中文之外
// 的分隔符 `-` `_` `/` `:` `.` 空格，以及中英文标点）。比对双方都过这里，
// 于是 "prometheus://nodes/n1" ≡ "Prometheus_Nodes_N1" ≡ "prometheus节点n1"
// 里内嵌的 "nodes/n1"（去分隔符后包含匹配）。中文字符按 unicode.IsLetter
// 保留——结论正文常是中文，不能连正文一起被归一化洗掉。
func normalizeAnchor(s string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(s) {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// contradictionPhrases 硬性矛盾话术清单：conclusion 若宣称"没结论/证据不足/
// 待定"，与"G2 已产出结论、G1 已归因命中"直接矛盾（fail-open 回退文案漏进
// 正式结论、或上游把 pending 文本当结论透传）。中英双列，ASCII 项大小写
// 不敏感（在归一化文本上匹配，故这里也写归一化形态）。
var contradictionPhrases = []string{
	"证据不足", "无法定因", "无法确定根因", "无法确认根因", "无法得出结论",
	"不能得出结论", "暂无结论", "尚未生成结论", "结论未生成", "待定",
	"insufficientevidence", "cannotconclude", "cantconclude",
	"unabletodetermine", "noconclusion", "inconclusive", "pending",
}

// findContradictions 返回 conclusion 中命中的矛盾话术（空 = 无硬性失败）。
func findContradictions(conclusion string) []string {
	norm := normalizeAnchor(conclusion)
	var hits []string
	for _, p := range contradictionPhrases {
		if strings.Contains(norm, p) {
			hits = append(hits, p)
		}
	}
	return hits
}

// anchorHit 锚点包含匹配：任一锚点归一化后非空且是归一化 conclusion 的
// 子串即命中；同时返回命中的原始锚点（供报告溯源）。
func anchorHit(conclusion string, anchors []string) (string, bool) {
	norm := normalizeAnchor(conclusion)
	for _, a := range anchors {
		na := normalizeAnchor(a)
		if na != "" && strings.Contains(norm, na) {
			return a, true
		}
	}
	return "", false
}

// truncateRunes 按 rune 安全截断（结论文本可能是中英混排，字节切会碎码点），
// 超限补省略号——报告与 JSONL 只存摘要，判分用全文。
func truncateRunes(s string, max int) string {
	rs := []rune(s)
	if len(rs) <= max {
		return s
	}
	return string(rs[:max]) + "…"
}

// ===================== unit 填充（与判分解耦的纯映射） =====================

// hydrateUnit 把一次 GET /rca 解析结果里 LLM 门禁需要的字段填进 unit。
// 与证据版基线字段（Top1/Top3/Steps*）并行填充，不改写既有判定。
// logicalNode 映射 golden 变更 logical_id → 全 node_key，用于从 top1 ref
// 反查其锚点节点（ref 形如 chg-<run>-<logical_id>）。
func hydrateUnit(u *unit, rr *rcaResp, myPrefix string, logicalNode map[string]string) {
	u.LLMUsed = rr.LLMUsed
	for _, s := range rr.Steps {
		if s.Name == "conclude" && s.Status == "done" {
			u.ConcludeDone = true
		}
	}
	if rr.Conclusion != nil {
		u.Conclusion = *rr.Conclusion
	}
	// G3 锚点集：首要 root_cause 的 ref + 其 golden 登记的 node_key
	//（域外/他逻辑变更反查不到 node 就只剩 ref——命中不了自然进存疑）。
	if len(u.RootRefs) > 0 {
		ref := u.RootRefs[0]
		u.G3Anchors = []string{ref}
		if logical, ok := strings.CutPrefix(ref, myPrefix); ok {
			if nk, ok := logicalNode[logical]; ok && nk != "" {
				u.G3Anchors = append(u.G3Anchors, nk)
			}
		}
	}
	// 单元素一致性：G3 状态在评测现场即算好（纯文本启发式，不依赖外部服务）。
	u.G3Status, u.G3Matched, u.G3Phrases = g3Of(u.Conclusion, u.G3Anchors)
}

// g3Of 单单元 G3 三态：fail（矛盾话术，硬性）> pass（锚点命中）>
// suspect（无矛盾但锚点未命中——人工终审）。空结论不参与 G3（G2 已挂）。
func g3Of(conclusion string, anchors []string) (status, matched string, phrases []string) {
	if strings.TrimSpace(conclusion) == "" {
		return "", "", nil
	}
	if ph := findContradictions(conclusion); len(ph) > 0 {
		return "fail", "", ph
	}
	if m, ok := anchorHit(conclusion, anchors); ok {
		return "pass", m, nil
	}
	return "suspect", "", nil
}

// ===================== 三门禁聚合 =====================

// promotion W12 转正判定（LLM 模式）：三门禁各自独立计数 + 存疑清单。
// 证据版基线（--no-llm）不出此判定（Applicable=false），报告明示不适用。
type promotion struct {
	Applicable bool     `json:"applicable"`
	Threshold  float64  `json:"threshold"`
	GuardNotes []string `json:"guard_notes,omitempty"` // 门禁外的总判定否决项（单元未跑全/守护 FAIL）

	G1Total int     `json:"g1_total"`
	G1Hits  int     `json:"g1_hits"`
	G1Rate  float64 `json:"g1_rate"`
	G1Pass  bool    `json:"g1_pass"`

	G2Total int  `json:"g2_total"`
	G2Pass  int  `json:"g2_pass"`
	G2Pass_ bool `json:"g2_pass_all"` // 100% 硬线；避开与计数同名的序列化混乱

	G3Total   int  `json:"g3_total"`
	G3Pass    int  `json:"g3_pass"`
	G3Suspect int  `json:"g3_suspect"`
	G3Fail    int  `json:"g3_fail"`
	G3Pass_   bool `json:"g3_pass_all"` // 无硬性失败即可（存疑不判死）

	Suspects  []suspectItem  `json:"suspects,omitempty"`
	HardFails []hardFailItem `json:"hard_fails,omitempty"`
	G2Fails   []g2FailItem   `json:"g2_fails,omitempty"`

	Pass bool `json:"pass"`
}

type suspectItem struct {
	SegCode    string   `json:"seg_code"`
	ClusterID  string   `json:"cluster_id"`
	Ref        string   `json:"ref"`
	Anchors    []string `json:"anchors"`
	Conclusion string   `json:"conclusion"` // 摘要（rune 截断）
}

type hardFailItem struct {
	SegCode    string   `json:"seg_code"`
	ClusterID  string   `json:"cluster_id"`
	Ref        string   `json:"ref"`
	Phrases    []string `json:"phrases"`
	Conclusion string   `json:"conclusion"`
}

type g2FailItem struct {
	SegCode   string   `json:"seg_code"`
	ClusterID string   `json:"cluster_id"`
	Status    string   `json:"status"`
	Missing   []string `json:"missing"` // llm_used / conclusion非空 / conclude=done 之缺项
}

// evalPromotion 汇总三门禁。units 为全部评测单元（含静默守护）。
// noLLM=true 时返回 Not-applicable 结构——证据版基线不配谈转正。
func evalPromotion(units []unit, noLLM bool, thr float64, guardsTotal, guardsPass int) promotion {
	p := promotion{Applicable: !noLLM, Threshold: thr}
	if noLLM {
		return p
	}
	for _, u := range units {
		if u.Kind == "silence_guard" {
			continue
		}
		// G1：分母 = 全部 RCA 计划单元（未跑通 = 无从命中，不许从分母消失）。
		p.G1Total++
		if u.Top1 {
			p.G1Hits++
		}
		// G2：配置了 LLM 就必须出结论——同样全单元分母，100% 硬线。
		p.G2Total++
		var missing []string
		if !u.LLMUsed {
			missing = append(missing, "llm_used=false")
		}
		if strings.TrimSpace(u.Conclusion) == "" {
			missing = append(missing, "conclusion为空")
		}
		if !u.ConcludeDone {
			missing = append(missing, "conclude步未done")
		}
		if len(missing) == 0 {
			p.G2Pass++
		} else {
			p.G2Fails = append(p.G2Fails, g2FailItem{
				SegCode: u.SegCode, ClusterID: u.ClusterID, Status: u.Status, Missing: missing})
		}
		// G3：只对"有结论"的单元评估（无结论者 G2 已挂，不重复计）。
		switch u.G3Status {
		case "pass":
			p.G3Total++
			p.G3Pass++
		case "suspect":
			p.G3Total++
			p.G3Suspect++
			ref := ""
			if len(u.RootRefs) > 0 {
				ref = u.RootRefs[0]
			}
			p.Suspects = append(p.Suspects, suspectItem{
				SegCode: u.SegCode, ClusterID: u.ClusterID, Ref: ref,
				Anchors: u.G3Anchors, Conclusion: truncateRunes(u.Conclusion, 200)})
		case "fail":
			p.G3Total++
			p.G3Fail++
			ref := ""
			if len(u.RootRefs) > 0 {
				ref = u.RootRefs[0]
			}
			p.HardFails = append(p.HardFails, hardFailItem{
				SegCode: u.SegCode, ClusterID: u.ClusterID, Ref: ref,
				Phrases: u.G3Phrases, Conclusion: truncateRunes(u.Conclusion, 200)})
		}
	}
	if p.G1Total > 0 {
		p.G1Rate = float64(p.G1Hits) / float64(p.G1Total)
	}
	p.G1Pass = p.G1Total > 0 && p.G1Rate >= p.Threshold
	p.G2Pass_ = p.G2Total > 0 && p.G2Pass == p.G2Total
	p.G3Pass_ = p.G3Fail == 0
	if guardsTotal != guardsPass {
		p.GuardNotes = append(p.GuardNotes,
			"静默守护未全过（"+strconv.Itoa(guardsPass)+"/"+strconv.Itoa(guardsTotal)+"）——防误报是转正前置，非三门禁之一但一票否决")
	}
	p.Pass = p.G1Pass && p.G2Pass_ && p.G3Pass_ && len(p.GuardNotes) == 0
	return p
}
