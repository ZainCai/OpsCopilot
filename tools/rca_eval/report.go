// report.go 产物落盘：JSONL（逐评测单元）、summary.json、report.md 与
// 控制台摘要。报告结构对齐 docs/reviews 的评审文档惯例；"≥85% 转正门禁"
// 独立成章——证据版（--no-llm）基线在报告顶部与门禁章双重标注形态。
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func writeOutputs(s summary, units []unit, outDir, repDir string, g *goldenDoc, ab *answerbook, noLLM bool) {
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		fatal(2, "mkdir %s: %v", outDir, err)
	}
	jsonl := filepath.Join(outDir, "rca-eval-"+s.RunID+".jsonl")
	f, err := os.Create(jsonl)
	if err != nil {
		fatal(2, "create jsonl: %v", err)
	}
	w := bufio.NewWriter(f)
	for _, u := range units {
		b, _ := json.Marshal(u)
		w.Write(b)
		w.WriteByte('\n')
	}
	w.Flush()
	f.Close()

	sumRaw, _ := json.MarshalIndent(s, "", "  ")
	sumPath := filepath.Join(outDir, "rca-eval-"+s.RunID+"-summary.json")
	if err := os.WriteFile(sumPath, sumRaw, 0o644); err != nil {
		fatal(2, "write summary: %v", err)
	}
	fmt.Printf("\nartifacts: %s\n           %s\n", jsonl, sumPath)

	if repDir == "" {
		return
	}
	if err := os.MkdirAll(repDir, 0o755); err != nil {
		fatal(2, "mkdir %s: %v", repDir, err)
	}
	md := renderReport(s, units, g, ab, noLLM)
	mdPath := filepath.Join(repDir, "report.md")
	if err := os.WriteFile(mdPath, []byte(md+"\n"), 0o644); err != nil {
		fatal(2, "write report: %v", err)
	}
	if err := os.WriteFile(filepath.Join(repDir, "summary.json"), sumRaw, 0o644); err != nil {
		fatal(2, "write report summary: %v", err)
	}
	fmt.Printf("report:    %s\n", mdPath)
}

func renderReport(s summary, units []unit, g *goldenDoc, ab *answerbook, noLLM bool) string {
	var b strings.Builder
	modeLine := "证据版基线（`--no-llm`：LLM 未接线，conclude 恒 pending——ADR-003 禁止伪 RCA）"
	if !noLLM {
		modeLine = "LLM 版（OPS_LLM_* 已接线，conclude 应 done）"
	}
	fmt.Fprintf(&b, "# RCA 评测集报告——%s\n\n", modeLine)
	fmt.Fprintf(&b, "- run：`%s`　租户：`%s`　生成：%s\n", s.RunID, s.Tenant, s.GeneratedAt.Format(time.RFC3339))
	fmt.Fprintf(&b, "- golden：`%s`（逐场景抄自 %s）\n", "tools/rca_eval/golden.json", g.Source["playbook"])
	fmt.Fprintf(&b, "- 剧本缩放：answerbook 每段 %ds（golden 原始 %ds）；评测周期锚 %s ~ %s\n",
		ab.Segments[0].DurationSec, g.Segments[0].DurationSec,
		ab.Segments[0].Start.Format("15:04:05"), ab.Segments[len(g.Segments)-1].End.Format("15:04:05"))
	fmt.Fprintf(&b, "- **形态声明**：%s。本报告命中率是**规则+证据链**的归因上限参考，不是 LLM 结论质量，"+
		"**不单独构成转正依据**（见 §6）。\n\n", modeLine)

	b.WriteString("## 0. TL;DR\n\n")
	fmt.Fprintf(&b, "| 指标 | 值 |\n|---|---|\n")
	fmt.Fprintf(&b, "| RCA 评测单元（golden 6 段中 3 个故障段 → %d 个簇单元） | %d 跑通 / %d 计划 |\n", s.UnitsTotal, s.UnitsCompleted, s.UnitsTotal)
	fmt.Fprintf(&b, "| **top-1 命中率** | **%.0f%%**（%d/%d，门禁线 %.0f%%） |\n", s.Top1Rate*100, s.Top1Hits, s.UnitsCompleted, s.Threshold*100)
	fmt.Fprintf(&b, "| top-3 命中率 | %.0f%%（%d/%d） |\n", s.Top3Rate*100, s.Top3Hits, s.UnitsCompleted)
	fmt.Fprintf(&b, "| 证据完整率 | %.0f%%（%d/%d） |\n", s.EvidenceRate*100, s.EvidenceOK, s.UnitsCompleted)
	fmt.Fprintf(&b, "| 静默守护（防误报） | %d/%d 通过 |\n", s.GuardsPass, s.GuardsTotal)
	fmt.Fprintf(&b, "| GET /rca 延迟 P50/P95 | %dms / %dms |\n", s.RCAHTTPMS.P50, s.RCAHTTPMS.P95)
	fmt.Fprintf(&b, "| 出口判定 | %s |\n", map[bool]string{true: "PASS", false: "FAIL（基线如实暴露，见 §5）"}[s.GatePassed])
	if !noLLM {
		p := s.Promotion
		fmt.Fprintf(&b, "| **§7 转正三门禁** | G1 %d/%d=%.0f%% ∧ G2 %d/%d ∧ G3 fail=%d suspect=%d → **%s** |\n",
			p.G1Hits, p.G1Total, p.G1Rate*100, p.G2Pass, p.G2Total, p.G3Fail, p.G3Suspect,
			map[bool]string{true: "PASS 可转正", false: "FAIL 不得转正"}[p.Pass])
	}
	b.WriteString("\n")

	b.WriteString("## 1. 管线与口径\n\n")
	b.WriteString("```\ngolden 段起点前注入变更(POST /api/v1/changes, occurred_at 钉在段前)\n" +
		"  → faultinjector 剧本告警被 app 拉取：ProcessAlerts 成簇(影子判决落 alert_event)\n" +
		"  → 拉取链路入队建单(ingest_queue → UpsertExternal, origin=prometheus)\n" +
		"  → 评测器轮询 PG 等簇/单成（alert_event.cluster_key + incident.source_ref=promFingerprint(labels)）\n" +
		"  → 生产自动挂簇 OPS_AUTOATTACH（W10-6，评测接线 enforce+on）验证挂上；未开开关回退按 AttachCluster 语义直写桥接（见 §5）\n" +
		"  → GET /api/v1/incidents/{id}/rca → root_causes[].ref 对照 golden 根因变更\n" +
		"  → 直读 incident_audit(action='rca') 校验 #4 审计落库形态\n```\n\n")
	b.WriteString("- **top-1**：`root_causes[0].ref == 期望根因变更 id`；**top-3**：期望 id 出现在 `root_causes` 前 3 位。\n" +
		"- **证据完整**（单元素，全真才算）：故障域 == golden 域 ∧ 取证节点/变更数非空达标 ∧\n" +
		"  六步形态正确（证据版：5 done + conclude pending + conclusion=null + llm_used=false）∧\n" +
		"  审计行存在。\n" +
		"- 延迟三档：`detect_lag`（段起点→事件创建 T0，含采集/队列节拍）、`rca_http`（REST 端到端）、\n" +
		"  `audit_duration_ms`（编排层自报，取直读审计值——两者之差即 REST 链路开销）。\n\n")

	b.WriteString("## 2. 六场景 × top-1 / top-3 命中矩阵\n\n")
	b.WriteString("| 段 | 场景（golden→main.go 行号） | 评测单元 | top-1 | top-3 | 证据完整 | 备注 |\n|---|---|---|---|---|---|---|\n")
	for _, seg := range g.Segments {
		if seg.Silence {
			gs := unitsFor(units, func(u unit) bool { return u.Kind == "silence_guard" && u.SegIdx == seg.Idx })
			st := "—"
			if len(gs) > 0 {
				st = gs[0].Status
			}
			fmt.Fprintf(&b, "| %d | %s %s（L%s） | 静默守护 | — | — | — | %s |\n",
				seg.Idx, seg.Code, seg.Name, seg.SrcLines, st)
			continue
		}
		for _, u := range unitsFor(units, func(u unit) bool { return u.Kind == "rca" && u.SegIdx == seg.Idx }) {
			mark := func(ok bool) string { return map[bool]string{true: "✅", false: "❌"}[ok] }
			hit1, hit3, ev := "—", "—", "—"
			if u.Status == "OK" {
				hit1, hit3, ev = mark(u.Top1), mark(u.Top3), mark(u.EvidenceComplete)
			}
			note := u.Status
			if len(u.Notes) > 0 {
				note += "；" + strings.Join(u.Notes, "；")
			}
			fmt.Fprintf(&b, "| %d | %s %s（L%s） | %s（%v） | %s | %s | %s | %s |\n",
				seg.Idx, seg.Code, seg.Name, seg.SrcLines, u.ClusterID, u.ExpectedDomain, hit1, hit3, ev, note)
		}
	}
	b.WriteString("\n")

	b.WriteString("## 3. 延迟分布\n\n| 指标 | n | min | P50 | P95 | max |\n|---|---|---|---|---|---|\n")
	fmt.Fprintf(&b, "| detect_lag(ms) | %d | %d | %d | %d | %d |\n", s.DetectLagMS.N, s.DetectLagMS.Min, s.DetectLagMS.P50, s.DetectLagMS.P95, s.DetectLagMS.Max)
	fmt.Fprintf(&b, "| rca_http(ms) | %d | %d | %d | %d | %d |\n", s.RCAHTTPMS.N, s.RCAHTTPMS.Min, s.RCAHTTPMS.P50, s.RCAHTTPMS.P95, s.RCAHTTPMS.Max)
	fmt.Fprintf(&b, "| audit_duration(ms) | %d | %d | %d | %d | %d |\n\n", s.AuditDurMS.N, s.AuditDurMS.Min, s.AuditDurMS.P50, s.AuditDurMS.P95, s.AuditDurMS.Max)

	b.WriteString("## 4. 逐单元明细\n\n| 段 | 簇单元 | 事件 | 期望根因 | 实际 root_causes 序 | detect_lag | rca_http | 状态 |\n|---|---|---|---|---|---|---|---|\n")
	for _, u := range units {
		if u.Kind != "rca" {
			continue
		}
		fmt.Fprintf(&b, "| %s | %s | %s | %s | %v | %ds | %dms | %s |\n",
			u.SegCode, u.ClusterID, u.IncidentID, u.ExpectedChange, u.RootRefs,
			u.DetectLagMS/1000, u.RCAHTTPMS, u.Status)
	}
	b.WriteString("\n")

	b.WriteString("## 5. 暴露的问题\n\n")
	if len(s.Findings) == 0 {
		b.WriteString("- （本轮无未通过项）\n")
	}
	for _, f := range s.Findings {
		b.WriteString("- " + f + "\n")
	}
	b.WriteString("\n**结构性口径（W10-6 起）**：\n\n")
	b.WriteString("- 簇→事件生产自动挂簇已落地：`OPS_AUTOATTACH=on` + `OPS_NOISE_MODE=enforce` 时 new-incident\n")
	b.WriteString("  判决联动建单+挂簇+`attach_cluster` 审计（评测接线默认如此）。本评测**优先验证生产路径**；\n")
	b.WriteString("  桥接直写仅作未开开关 app 的回退——单元 note 出现\"回退桥接\"即本轮生产挂簇路径未被验证。\n")
	b.WriteString("- 一簇一事件（`idx_incident_cluster_unique`）：多指纹共簇仅**首单**持故障域；其余指纹判决走\n")
	b.WriteString("  cluster-merge 不再触发挂簇，其自建事件无域——处置口径 = 人工 `MergeInto` 归并到首单\n")
	b.WriteString("  （L2 只提示不自动级联，见 `docs/M2执行排期-W9到W11.md` W10-6 注记）；RCA 只能逐簇一单。\n")
	b.WriteString("- 变更证据链已按装配租户隔离：`PGChangeStore.LoadSince` 回放恒带 `tenant_id` 过滤\n")
	b.WriteString("  （评测 0129078 的跨 run 泄漏实锤后收口，#4 遗留的\"M3 补\"提前兑现）——`foreign_refs`\n")
	b.WriteString("  探测保留为回归防线：本轮 findings 再出现跨 run 泄漏即过滤失守/回滚，转正前必须查。\n\n")

	b.WriteString("## 6. ≥85% 转正门禁（W12）\n\n")
	fmt.Fprintf(&b, "- 门禁语义：**LLM 对外转正**要求带 LLM 结论的评测集准确率 ≥%0.f%%（二期池文档 / ADR-015），\n", s.Threshold*100)
	fmt.Fprintf(&b, "  完整判定见 §7 三门禁（G1 归因不回退 ∧ G2 结论产出 ∧ G3 结论-证据一致性）。\n")
	if noLLM {
		b.WriteString("- 本轮为**证据版基线**（本机 LLM 未配置，`--no-llm` 跑分并在报告标注）：conclude 恒 pending、\n")
		b.WriteString("  conclusion=null 属预期正确形态；**转正判定不适用本轮**。\n")
		fmt.Fprintf(&b, "- 规则链参考读数：top-1 %.0f%% / top-3 %.0f%%（%.0f%% 线）——证据链与归因排序先行达标，\n",
			s.Top1Rate*100, s.Top3Rate*100, s.Threshold*100)
		b.WriteString("  LLM 接线后同集重跑（`run_rca_eval.sh --with-llm` + .env 配 OPS_LLM_*）才是转正证据。\n")
	} else {
		fmt.Fprintf(&b, "- 本轮带 LLM 跑分：top-1 %.0f%% → %s。\n", s.Top1Rate*100,
			map[bool]string{true: "达门禁线", false: "未达门禁线，不得转正"}[s.Promotion.G1Pass])
	}
	b.WriteString("\n## 7. §转正判定（W12 · LLM 三门禁）\n\n")
	if noLLM {
		b.WriteString("**本轮不适用**——证据版基线（`--no-llm`）：LLM 未接线，conclude 恒 pending 是预期形态，\n" +
			"三门禁无任何一分可判；转正判定只在带 LLM 跑分（`--with-llm`）时生效。\n\n")
	} else {
		p := s.Promotion
		mark := func(ok bool) string { return map[bool]string{true: "✅ PASS", false: "❌ FAIL"}[ok] }
		b.WriteString("三门禁各自独立计数（G1/G2 分母 = 全部 RCA 计划单元——未跑通的单元在分母里挂账，" +
			"不存在\"报错即出局\"刷通过率的通道）：\n\n")
		fmt.Fprintf(&b, "| 门禁 | 定义 | 读数 | 判定 |\n|---|---|---|---|\n")
		fmt.Fprintf(&b, "| **G1 归因不回退** | LLM 只改写叙述，root_causes top1 仍须命中 golden | %d/%d = %.0f%%（线 %.0f%%） | %s |\n",
			p.G1Hits, p.G1Total, p.G1Rate*100, p.Threshold*100, mark(p.G1Pass))
		fmt.Fprintf(&b, "| **G2 结论产出** | llm_used=true ∧ conclusion 非空 ∧ steps[conclude]=done（配了 LLM 就必须出结论） | %d/%d（须 100%%） | %s |\n",
			p.G2Pass, p.G2Total, mark(p.G2Pass_))
		fmt.Fprintf(&b, "| **G3 结论-证据一致性**（启发式，不接第二个 LLM 当裁判） | 矛盾话术（证据不足/待定/pending…）= 硬性失败；conclusion 去分隔符折大小写后须含首要 root_cause 锚点（ref 或 node_key），未命中仅入存疑清单人工终审 | pass %d / suspect %d / **fail %d**（须 0） | %s |\n",
			p.G3Pass, p.G3Suspect, p.G3Fail, mark(p.G3Pass_))
		b.WriteString("\n")
		if len(p.G2Fails) > 0 {
			b.WriteString("**G2 缺项明细**：\n\n| 段 | 单元 | status | 缺项 |\n|---|---|---|---|\n")
			for _, g := range p.G2Fails {
				fmt.Fprintf(&b, "| %s | %s | %s | %v |\n", g.SegCode, g.ClusterID, g.Status, g.Missing)
			}
			b.WriteString("\n")
		}
		if len(p.HardFails) > 0 {
			b.WriteString("**G3 硬性失败（一票否决转正）**：\n\n| 段 | 单元 | ref | 命中话术 | conclusion 摘要 |\n|---|---|---|---|---|\n")
			for _, h := range p.HardFails {
				fmt.Fprintf(&b, "| %s | %s | %s | %v | %s |\n", h.SegCode, h.ClusterID, h.Ref, h.Phrases, h.Conclusion)
			}
			b.WriteString("\n")
		}
		if len(p.Suspects) > 0 {
			b.WriteString("**G3 存疑清单（锚点未命中、无矛盾话术——不自动判死，人工终审）**：\n\n" +
				"| 段 | 单元 | 首要 root_cause ref | 期望锚点 | conclusion 摘要 | 终审 |\n|---|---|---|---|---|---|\n")
			for _, su := range p.Suspects {
				fmt.Fprintf(&b, "| %s | %s | %s | %v | %s | ☐ |\n",
					su.SegCode, su.ClusterID, su.Ref, su.Anchors, su.Conclusion)
			}
			b.WriteString("\n存疑 ≠ 失败：启发式只保证\"结论提到了被归因的变更/节点\"，语义对错由上表逐条人核。\n\n")
		} else {
			b.WriteString("G3 存疑清单：空（所有结论都锚点命中）。\n\n")
		}
		for _, n := range p.GuardNotes {
			b.WriteString("- 否决项：" + n + "\n")
		}
		fmt.Fprintf(&b, "**总判定：%s**（规则：G1 ≥%.0f%% ∧ G2 100%% ∧ G3 无硬性失败 ∧ 静默守护全过）\n\n",
			map[bool]string{true: "PASS——可转正", false: "FAIL——不得转正"}[p.Pass], p.Threshold*100)
	}
	b.WriteString("## 8. golden 维护协议\n\n")
	for _, m := range g.Maintenance {
		b.WriteString("- " + m + "\n")
	}
	b.WriteString("- promFingerprint 算法双定义：`tools/rca_eval/main.go` 与\n")
	b.WriteString("  `cmd/opscopilot/pull_alerts.go`——改标签构成时以 INCIDENT_MISS 暴露，两处必须同步。\n\n")

	b.WriteString("## 9. 复跑\n\n```\nbash scripts/run_rca_eval.sh                    # 默认证据版基线（--no-llm, scale=0.1）\nbash scripts/run_rca_eval.sh --with-llm         # LLM 转正跑分（OPS_LLM_* 配在 .env，未配置拒跑不降级）\nbash scripts/run_rca_eval.sh stop               # 清理残留进程（pidfile 兜底）\n```\n")
	return b.String()
}

func unitsFor(units []unit, pred func(unit) bool) []unit {
	var out []unit
	for _, u := range units {
		if pred(u) {
			out = append(out, u)
		}
	}
	return out
}

func printConsole(s summary, units []unit) {
	fmt.Printf("\n==== RCA 评测汇总（%s，run=%s）====\n", s.Mode, s.RunID)
	for _, u := range units {
		if u.Kind == "silence_guard" {
			fmt.Printf("guard seg%d %-12s verdicts=%d new_incidents=%d\n", u.SegIdx, u.Status, u.ShadowAlerts, u.NewIncidents)
		} else {
			fmt.Printf("unit  seg%d %-8s %-28s top1=%-5v top3=%-5v evid=%-5v status=%s\n",
				u.SegIdx, u.SegCode, u.ClusterID, u.Top1, u.Top3, u.EvidenceComplete, u.Status)
		}
	}
	fmt.Printf("top1=%.0f%% top3=%.0f%% evidence=%.0f%% guards=%d/%d gate=%v\n",
		s.Top1Rate*100, s.Top3Rate*100, s.EvidenceRate*100, s.GuardsPass, s.GuardsTotal, s.GatePassed)
	if s.Promotion.Applicable {
		p := s.Promotion
		fmt.Printf("§转正判定 G1 %d/%d=%.0f%%(≥%.0f%%) G2 %d/%d(=100%%) G3 fail=%d suspect=%d → %s\n",
			p.G1Hits, p.G1Total, p.G1Rate*100, p.Threshold*100, p.G2Pass, p.G2Total, p.G3Fail, p.G3Suspect,
			map[bool]string{true: "PASS 可转正", false: "FAIL 不得转正"}[p.Pass])
	}
	for _, f := range s.Findings {
		fmt.Println("FINDING:", f)
	}
}
