// session_flow.go 复盘会话编排流程（原 rca_session.go 拆分，2026-09-14）。
// AddTurn / ReadSession / generateAnswer / buildPromptDoc / refreshHot /
// safeMeta / envelopeTurns / clipActor / actorOrSession / shortErr。
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"opscopilot/internal/incident"
	"opscopilot/internal/llmgw"
	"opscopilot/internal/rca"
	"opscopilot/internal/sessionstore"
)

// AddTurn 追加一轮用户追问并尽力产出 assistant 回答（全链见文件头 fail-open
// 纪律）。错误分类：ErrRCAIncidentNotFound（404）/ ErrSessionTurnEmpty（400）/
// context 超时（504）/ 其余真相层故障（500，细节只进日志）。
func (s *SessionOrchestrator) AddTurn(ctx context.Context, incidentID, actor, question string) (*SessionResult, error) {
	if _, err := s.incidents.Get(incidentID); err != nil {
		if errors.Is(err, incident.ErrNotFound) {
			return nil, fmt.Errorf("%w: %s", ErrRCAIncidentNotFound, incidentID)
		}
		return nil, err
	}
	question = strings.TrimSpace(question)
	if question == "" {
		return nil, ErrSessionTurnEmpty
	}
	if len([]rune(question)) > maxSessionTurnChars {
		return nil, fmt.Errorf("%w: content exceeds %d characters", ErrSessionTurnEmpty, maxSessionTurnChars)
	}
	actor = clipActor(actor)

	userTurn, err := s.truth.appendTurn(ctx, incidentID, actor, sessionstore.RoleUser, question, nil)
	if err != nil {
		return nil, fmt.Errorf("session: append user turn: %w", err)
	}
	s.refreshHot(ctx, incidentID)

	res := &SessionResult{AssistantStatus: "pending", Persistence: s.truth.persistence()}

	// finish 用真相层全量快照回显整会话（POST 与 GET 视图一致——"追加并
	// 返回该 incident 的复盘会话"；快照失败回退本次已知轮次，不吞成功）。
	finish := func(fallback ...SessionTurn) (*SessionResult, error) {
		if turns, meta, serr := s.truth.snapshot(ctx, incidentID); serr == nil {
			res.Turns, res.Meta = turns, meta
		} else {
			s.logf("WARNING: session snapshot echo %s: %v (fallback view)", incidentID, serr)
			res.Turns, res.Meta = fallback, s.safeMeta(ctx, incidentID)
		}
		return res, nil
	}

	if s.chat == nil {
		// LLM 未配置：与 RCA conclude 注入 nil Summarizer 同款——不发请求、
		// 不计请求指标（没有请求可计），user 轮已在真相层。
		return finish(userTurn)
	}

	answer, meta, cerr := s.generateAnswer(ctx, incidentID, actor, userTurn)
	if cerr != nil {
		// fail-open：assistant 轮不落库不伪造——pending 是显式契约语义。
		s.logf("WARNING: session assistant degraded for %s: %v (user turn %d persisted, pending returned)",
			incidentID, cerr, userTurn.Seq)
		return finish(userTurn)
	}
	asstTurn, err := s.truth.appendTurn(ctx, incidentID, "system:llm", sessionstore.RoleAssistant, answer, meta)
	if err != nil {
		// 回答已产出但真相写失败：宁可整体 pending（不拿没落库的回答冒充
		// 会话历史——"响应里有、真相里没有"是最难排查的分叉）。
		s.logf("WARNING: session: append assistant turn %s: %v (answer discarded, pending)", incidentID, err)
		return finish(userTurn)
	}
	s.refreshHot(ctx, incidentID)
	res.AssistantStatus = "answered"
	return finish(userTurn, asstTurn)
}

// ReadSession 读会话（拍板①懒恢复）：热态命中即用；Redis 无 key/坏信封/
// 客户端故障 → 从 PG 真相重建并回填（回填失败只 log，读仍成功）。
func (s *SessionOrchestrator) ReadSession(ctx context.Context, incidentID string) (*SessionResult, error) {
	if _, err := s.incidents.Get(incidentID); err != nil {
		if errors.Is(err, incident.ErrNotFound) {
			return nil, fmt.Errorf("%w: %s", ErrRCAIncidentNotFound, incidentID)
		}
		return nil, err
	}
	res := &SessionResult{Persistence: s.truth.persistence(), AssistantStatus: ""}
	key := sessionstore.SessionKey(s.tenant, incidentID)

	if s.hot != nil {
		if env, err := s.hot.LoadEnvelope(ctx, key); err == nil {
			res.Turns = envelopeTurns(env)
			res.Meta = s.safeMeta(ctx, incidentID) // 元信息恒走 PG 真相
			return res, nil
		} else if !errors.Is(err, sessionstore.ErrNotFound) {
			s.logf("WARNING: session hot load %s: %v (rebuilding from truth)", key, err)
		}
	}
	// 懒恢复路径（拍板①）：TTL 蒸发/Redis 清空/异代信封 → PG 重建。
	turns, meta, err := s.truth.snapshot(ctx, incidentID)
	if err != nil {
		return nil, fmt.Errorf("session: snapshot: %w", err)
	}
	res.Turns, res.Meta = turns, meta
	s.refreshHot(ctx, incidentID)
	return res, nil
}

// generateAnswer 组 prompt（RCA findings + 历史 + 追问）→ llmgw Chat →
// 截断渲染 + llm_meta。一切失败（含 RCA 取证彻底不可用且 LLM 也失败）都
// 返回 error 走 pending；指标在此统一记账（ok|timeout|error）。
func (s *SessionOrchestrator) generateAnswer(ctx context.Context, incidentID, actor string,
	userTurn SessionTurn) (string, map[string]any, error) {
	start := time.Now()

	prompt, note := s.buildPromptDoc(ctx, incidentID, userTurn)
	payload, err := json.Marshal(prompt)
	if err != nil { // 纯字符串 DTO 不可序列化 = 编程错误，同样 fail-open
		s.m.CountSessionLLM("error")
		return "", nil, fmt.Errorf("marshal prompt: %w", err)
	}
	content, stats, cerr := s.chat.Chat(ctx, []llmgw.Message{
		{Role: "system", Content: sessionSystemPrompt},
		{Role: "user", Content: string(payload)},
	}, llmTemperature)
	outcome := outcomeOf(cerr)
	text := strings.TrimSpace(content)
	if cerr == nil && text == "" {
		outcome = "error" // "200 但空应答"同样计入降级面（对齐 llm_summarizer 纪律）
	}
	s.m.CountSessionLLM(outcome)
	s.m.ObserveSessionLLMLatency(time.Since(start))
	if outcome != "ok" {
		errForLog := cerr
		if errForLog == nil {
			errForLog = errors.New("empty assistant content")
		}
		s.logf("WARNING: session llm %s: %v (model=%s prompt_chars=%d prompt_hash=%s status=%d resp_bytes=%d duration_ms=%d)",
			incidentID, errForLog, stats.Model, stats.PromptChars, stats.PromptHash,
			stats.HTTPStatus, stats.RespBytes, time.Since(start).Milliseconds())
		return "", nil, fmt.Errorf("llm degraded (%s): %w", outcome, errForLog)
	}
	s.logf("session answer ok: incident=%s model=%s prompt_chars=%d prompt_hash=%s resp_bytes=%d answer_chars=%d duration_ms=%d",
		incidentID, stats.Model, stats.PromptChars, stats.PromptHash, stats.RespBytes,
		len([]rune(text)), time.Since(start).Milliseconds())
	// llm_meta 只存哈希/长度/模型（拍板②；llmgw.Stats 脱敏字段集，不含正文/key）。
	meta := map[string]any{
		"model":        stats.Model,
		"prompt_hash":  stats.PromptHash,
		"prompt_chars": stats.PromptChars,
		"resp_bytes":   stats.RespBytes,
		"http_status":  stats.HTTPStatus,
		"note":         note, // "rca_evidence_unavailable" 之类短标记（非正文）
	}
	return truncate(text, sessionAnswerCharLimit), meta, nil
}

// sessionPromptDoc assistant 轮的证据信封：RCA 报告（截断版 findings，
// OPS_RCA_MAX_FINDINGS 口径——Report.Findings 在编排器里已裁）+ 复盘历史 +
// 本轮提问。llmFinding/llmEvidenceDoc 同族但语义不同（这里是"给人答疑"不是
// "产出结论 JSON"），不直接复用 conclude 的信封结构。
type sessionPromptDoc struct {
	IncidentID        string           `json:"incident_id"`
	T0                string           `json:"t0,omitempty"`
	Window            string           `json:"window,omitempty"`
	AlertedNodes      []string         `json:"alerted_nodes,omitempty"`
	Findings          []llmFinding     `json:"findings,omitempty"`
	FindingsTruncated int              `json:"findings_truncated,omitempty"`
	RootCauses        []llmFinding     `json:"root_causes,omitempty"`
	Conclusion        string           `json:"conclusion,omitempty"`
	Steps             map[string]int   `json:"steps,omitempty"`
	EvidenceNote      string           `json:"evidence_note,omitempty"`
	History           []llmHistoryItem `json:"history"`
}

type llmHistoryItem struct {
	Role    string `json:"role"`
	At      string `json:"at"`
	Content string `json:"content"`
}

// buildPromptDoc 取证 + 历史组装。RCA 分析失败**不阻断提问**：findings 置空
// 并带 evidence_note（如实告诉模型"这次没拿到证据链"）——模型按系统提示的
// "证据不足须明说"纪律作答，这就是复盘会话版的"宁缺毋滥"。
func (s *SessionOrchestrator) buildPromptDoc(ctx context.Context, incidentID string,
	userTurn SessionTurn) (sessionPromptDoc, string) {
	doc := sessionPromptDoc{IncidentID: incidentID}
	note := ""
	res, err := s.analyzer.Analyze(ctx, incidentID, clipActor(actorOrSession(userTurn.CreatedBy)))
	switch {
	case err != nil:
		note = fmt.Sprintf("RCA 证据获取失败（%v）——回答须明示无法依据证据链", shortErr(err))
		doc.EvidenceNote = note
		s.logf("WARNING: session rca evidence for %s: %v (prompt without findings)", incidentID, err)
	case res != nil && res.Report != nil:
		rep := res.Report
		doc.T0 = rep.Input.T0.UTC().Format(time.RFC3339Nano)
		doc.Window = rep.Input.Window.String()
		doc.AlertedNodes = rep.Input.AlertedNodes
		fs := rep.Findings // 编排器已按 OPS_RCA_MAX_FINDINGS 截断（#6 同源）
		doc.FindingsTruncated = rep.FindingsTruncated
		for _, f := range fs {
			v := llmFinding{Step: f.Step, Summary: f.Summary, Confidence: f.Confidence, Ref: f.Ref}
			doc.Findings = append(doc.Findings, v)
			if f.Step == rca.StepConclude {
				doc.Conclusion = f.Summary
			}
		}
		for _, f := range rep.RootCauses {
			doc.RootCauses = append(doc.RootCauses, llmFinding{Step: f.Step, Summary: f.Summary,
				Confidence: f.Confidence, Ref: f.Ref})
		}
		done, pending, failed := 0, 0, 0
		for _, sr := range rep.Steps {
			switch sr.Status {
			case rca.StatusDone:
				done++
			case rca.StatusPending:
				pending++
			case rca.StatusFailed:
				failed++
			}
		}
		doc.Steps = map[string]int{"done": done, "pending": pending, "failed": failed}
	}

	// 复盘上下文：本会话既有轮次（真相层全量）裁到最近 sessionHistoryTurns
	// 条，本轮提问已落真相、天然在列尾——不再重复塞 question 字段。
	turns, _, err := s.truth.snapshot(ctx, incidentID)
	if err != nil {
		s.logf("WARNING: session snapshot for prompt %s: %v (history dropped)", incidentID, err)
	}
	if len(turns) > sessionHistoryTurns {
		turns = turns[len(turns)-sessionHistoryTurns:]
	}
	for _, t := range turns {
		doc.History = append(doc.History, llmHistoryItem{Role: t.Role,
			At: t.CreatedAt.UTC().Format(time.RFC3339), Content: t.Content})
	}
	return doc, note
}

// refreshHot 用真相层全量重建热态信封（覆盖写即续 TTL——拍板①"活跃会话
// 靠 PG 真相 + 覆盖写续命"）。Redis 故障只 log：真相已持久，热态缺席
// 只影响读放大，不影响正确性。
func (s *SessionOrchestrator) refreshHot(ctx context.Context, incidentID string) {
	if s.hot == nil {
		return
	}
	turns, _, err := s.truth.snapshot(ctx, incidentID)
	if err != nil {
		s.logf("WARNING: session hot refresh snapshot %s: %v", incidentID, err)
		return
	}
	key := sessionstore.SessionKey(s.tenant, incidentID)
	env := sessionstore.Envelope{SessionID: key}
	for _, t := range turns {
		env.Turns = append(env.Turns, sessionstore.Turn{Seq: t.Seq, Role: t.Role,
			Content: t.Content, CreatedBy: t.CreatedBy, CreatedAt: t.CreatedAt})
	}
	if err := s.hot.SaveEnvelope(ctx, key, env); err != nil {
		s.logf("WARNING: session hot refresh save %s: %v (truth unaffected)", key, err)
	}
}

// safeMeta 读会话元信息，失败退化为空值（读路径不因元信息断头）。
func (s *SessionOrchestrator) safeMeta(ctx context.Context, incidentID string) SessionMeta {
	meta, err := s.truth.meta(ctx, incidentID)
	if err != nil {
		s.logf("WARNING: session meta %s: %v", incidentID, err)
		return SessionMeta{}
	}
	return meta
}

// envelopeTurns 热态信封 → 真相 DTO（llm_meta 不随热态往返：视图契约不含它）。
func envelopeTurns(env sessionstore.Envelope) []SessionTurn {
	out := make([]SessionTurn, 0, len(env.Turns))
	for _, t := range env.Turns {
		out = append(out, SessionTurn{Seq: t.Seq, Role: t.Role, Content: t.Content,
			CreatedBy: t.CreatedBy, CreatedAt: t.CreatedAt})
	}
	return out
}

// clipActor created_by/audit actor 的统一卫生钳制（对齐 rca_orchestrator 的
// 64 字节日截纪律；rune 安全版）。
func clipActor(a string) string {
	a = strings.TrimSpace(a)
	r := []rune(a)
	if len(r) > 64 {
		a = string(r[:64])
	}
	return a
}

// actorOrSession 空 actor 归一为 "session"（RCA 审计留痕的可辨识来源）。
func actorOrSession(a string) string {
	if a == "" {
		return "session"
	}
	return a
}

// shortErr 错误一句话摘要进 prompt note：只保留类型/包装文案首行并钳 120 rune
// （note 是给模型的运维事实，不是堆栈）。
func shortErr(err error) string {
	msg := err.Error()
	if i := strings.IndexByte(msg, '\n'); i >= 0 {
		msg = msg[:i]
	}
	r := []rune(msg)
	if len(r) > 120 {
		msg = string(r[:120]) + "…"
	}
	return msg
}
