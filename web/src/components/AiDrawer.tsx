import { useCallback, useEffect, useRef, useState } from "react";
import { ApiError, getJSON, postJSON } from "../api/client";
import { ENDPOINTS } from "../api/endpoints";
import { getActor, setActor } from "../api/token";
import type { SessionResponse } from "../api/types";
import { fmtTime } from "../lib/format";

/**
 * AI 复盘抽屉（W11-5 · 只读版）。端点 GET/POST /api/v1/incidents/{id}/rca/session
 * （rest_rca_session.go，二期 #7 S2）。
 *
 * 边界（验收项）：纯问答展示——**不放任何"执行"按钮**。ADR-003 的单一执行
 * 出口锁在后端（会话只读 RCA 证据 + 产出文本轮），前端连伪装入口都不给。
 *
 * 优雅降级探测策略（自定，注释即口径）：
 *  - 事件详情打开时对目标 incident 发一次 GET session（幂等只读，热态命中
 *    即返，不打扰 LLM）；
 *  - 200 → 能力在线，缓存到模块级（页面生命周期内只探一次）；
 *  - 503（OPS_SESSION=off / 未装配）→ 能力离线，永久缓存 → 抽屉入口隐藏；
 *  - 401（配了 Token 但没带对）与其他错误 → 不缓存、入口保持可见但点击后
 *    在抽屉内明示错误——"探测失败"≠"功能关闭"，不把网络抖动做成静默隐藏。
 */
type ProbeState = "unknown" | "on" | "off";
let probe: ProbeState = "unknown";

export function sessionEntryVisible(): boolean {
  return probe !== "off";
}

/** 只读探测一次；返回值 = 入口是否可用（unknown 期间也放行，见头注释）。 */
export async function probeSession(incidentId: string): Promise<boolean> {
  if (probe === "off") return false;
  if (probe === "on") return true;
  try {
    await getJSON<SessionResponse>(ENDPOINTS.incidentRcaSession(incidentId));
    probe = "on";
    return true;
  } catch (e) {
    if (e instanceof ApiError && e.status === 503) probe = "off";
    return !(e instanceof ApiError && e.status === 503);
  }
}

/** 抽屉本体（挂载即拉会话；由父级在 probe 通过后渲染）。 */
export function AiDrawer({ incidentId, onClose }: { incidentId: string; onClose: () => void }): React.ReactElement {
  const [ses, setSes] = useState<SessionResponse | null>(null);
  const [pendingLast, setPendingLast] = useState(false); // POST 回显 assistant_status=pending
  const [loading, setLoading] = useState(false);
  const [sending, setSending] = useState(false);
  const [err, setErr] = useState("");
  const [draft, setDraft] = useState("");
  const listRef = useRef<HTMLDivElement | null>(null);

  const load = useCallback(async () => {
    setLoading(true);
    setErr("");
    try {
      const data = await getJSON<SessionResponse>(ENDPOINTS.incidentRcaSession(incidentId));
      setSes({ ...data, turns: data.turns ?? [] });
      setPendingLast(false);
    } catch (e) {
      if (e instanceof ApiError && e.status === 503) { probe = "off"; onClose(); return; } // 打开后被关闸：收起入口
      setErr(e instanceof ApiError ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, [incidentId, onClose]);

  useEffect(() => { void load(); }, [load]);

  // 新轮追加后滚到底（气泡流式阅读方向）
  useEffect(() => {
    const el = listRef.current;
    if (el) el.scrollTop = el.scrollHeight;
  }, [ses, pendingLast]);

  async function ask(): Promise<void> {
    const content = draft.trim();
    if (!content || sending) return;
    let actor = getActor();
    if (!actor) {
      actor = window.prompt("提问人工号（会话 created_by 身份钩子）：") ?? "";
      if (!actor) return;
      setActor(actor);
    }
    setSending(true);
    setErr("");
    try {
      // 契约字段以 Go 侧为准：{content, actor}（DisallowUnknownFields，
      // 写成 message/author 会 400——见 rest_rca_session.go sessionPostReq）。
      const data = await postJSON<SessionResponse>(ENDPOINTS.incidentRcaSession(incidentId), { content, actor });
      setSes({ ...data, turns: data.turns ?? [] });
      setPendingLast(data.assistant_status === "pending");
      setDraft("");
    } catch (e) {
      setErr(e instanceof ApiError ? e.message + (e.needsToken ? "（需写 Token）" : "") : String(e));
    } finally {
      setSending(false);
    }
  }

  const turns = ses?.turns ?? [];
  return (
    <div className="drawer-mask" onClick={onClose} role="presentation">
      <aside className="drawer" onClick={(e) => e.stopPropagation()} role="dialog" aria-label="AI 复盘问答">
        <header className="drawer-h">
          <div>
            <div className="drawer-t">AI 复盘问答</div>
            <div className="panel-sub mono">{incidentId}{ses ? ` · ${ses.turns.length} 轮 · 真相层 ${ses.persistence}` : ""}</div>
          </div>
          <span style={{ marginLeft: "auto", display: "flex", gap: 8, alignItems: "center" }}>
            <button type="button" className="btn btn--sm" onClick={() => void load()} title="重读会话（Redis 热态缺失时后端自动从 PG 懒恢复）">刷新</button>
            <button type="button" className="btn btn--sm" onClick={onClose}>收起 ✕</button>
          </span>
        </header>

        <div className="drawer-b" ref={listRef}>
          {loading && !ses ? <span className="panel-sub"><span className="spin" />加载会话…</span> : null}
          {err ? <div className="banner err">会话失败：{err}</div> : null}
          {ses && turns.length === 0 && !pendingLast
            ? (
              <div className="empty" style={{ padding: "26px 10px" }}>
                <div className="e-t">还没有问答轮次</div>
                <div className="e-d">提问会先落库（user 轮进真相层），助手回答由 LLM 基于该事件的 RCA 证据产出——只问答，不执行。</div>
              </div>
            )
            : null}
          {turns.map((t) => (
            <div key={t.seq} className={`chat-row ${t.role}`}>
              <div className="chat-meta mono">
                {t.role === "user" ? (t.created_by || "用户") : "EagleOps"} · {fmtTime(t.created_at)} · #{t.seq}
              </div>
              <div className="chat-bubble">{t.content}</div>
            </div>
          ))}
          {pendingLast
            ? (
              <div className="chat-row assistant">
                <div className="chat-meta mono">EagleOps · <span className="chip chip--acked">pending</span></div>
                <div className="chat-bubble pending">
                  未生成回答——LLM 未配置或本轮调用失败（宁 pending 不假答）。你的提问已落库，配置就绪后重新提问即可。
                </div>
              </div>
            )
            : null}
        </div>

        <footer className="drawer-f">
          <textarea
            className="chat-input" rows={2} placeholder="就这份 RCA 提问，如「变更假设被验证了吗？」"
            value={draft}
            onChange={(e) => setDraft(e.target.value)}
            onKeyDown={(e) => { if (e.key === "Enter" && (e.ctrlKey || e.metaKey)) void ask(); }}
          />
          <div className="drawer-f-row">
            <span className="panel-sub">只读问答 · 无执行入口（ADR-003 单出口在后端锁定）· Ctrl+Enter 发送</span>
            <button type="button" className="btn btn--acc btn--sm" disabled={!draft.trim() || sending} onClick={() => void ask()}>
              {sending ? <span className="spin" /> : null}提问
            </button>
          </div>
        </footer>
      </aside>
    </div>
  );
}
