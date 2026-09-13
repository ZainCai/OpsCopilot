import { useCallback, useEffect, useState } from "react";
import { ApiError, delJSON, getJSON, postJSON } from "../api/client";
import { ENDPOINTS } from "../api/endpoints";
import { getActor, setActor } from "../api/token";
import type {
  IncidentRunbooksResponse, Runbook, RunbookCreateResponse, RunbookExecution,
  RunbookExecutionsResponse, RunbookMountView, RunbooksResponse,
} from "../api/types";
import { fmtTime } from "../lib/format";

/**
 * 事件详情 · Runbook 区块（W11-4 · F-12 记录版）。契约：docs/前端契约-runbook.md
 * （E1–E7，rest_runbook.go）。总口径照抄契约 §0，前端只做呈现：
 *  - **只记不执行**：没有"让系统替你跑手册"的按钮，执行相关文案一律"记录"语义，
 *    响应里契约恒带 auto_execution:false——页面上明示，防止误读成"执行器"；
 *  - 执行记录**不是审计证据**（不进 incident_audit 哈希链），展示面不标"审计级"，
 *    合规取证仍走审计轨迹区块；
 *  - 降级：无 DSN 时 store 不接线、端点显式 503（不伪装"没有手册"），区块变灰
 *    提示"需持久化后端"，与 RCA/session 端点降级表现同口径（RcaPanel 头注释）。
 * E3 一次带回正文 + 执行计数（挂载视图键是 runbook_id、无 id 键，契约 §2）。
 */

/** 适用范围呈现（"" = 不限，契约 §2 scope 口径）。 */
function scopeText(m: { scope_severity?: string; scope_service?: string }): string {
  const sev = m.scope_severity?.trim() || "不限级别";
  const svc = m.scope_service?.trim() || "不限服务";
  return `${sev} · ${svc}`;
}

/** refs 元素自由（string 或 object，契约 §2）——string 直显，object 压成单行 JSON。 */
function refText(r: unknown): string {
  return typeof r === "string" ? r : JSON.stringify(r);
}

/** 取操作人身份（executed_by / mounted_by 身份钩子），同事件详情的 prompt 口径。 */
function askActor(label: string): string {
  const cached = getActor();
  if (cached) return cached;
  const a = (window.prompt(label) ?? "").trim();
  if (a) setActor(a);
  return a;
}

function errText(e: unknown): string {
  return e instanceof ApiError
    ? e.message + (e.needsToken ? "（写操作需填 Token）" : "")
    : String(e);
}

export function RunbookSection({ id }: { id: string }): React.ReactElement {
  const [mounts, setMounts] = useState<RunbookMountView[] | null>(null);
  const [off, setOff] = useState(false); // 503 = store 未接线（无 DSN）
  const [err, setErr] = useState("");
  const [busy, setBusy] = useState(false);

  // 挂载选择器（E1 库列表懒加载；空库时内联最小创建表单 E2→E4）
  const [picker, setPicker] = useState(false);
  const [lib, setLib] = useState<Runbook[] | null>(null);
  const [picked, setPicked] = useState("");
  const [newTitle, setNewTitle] = useState("");
  const [newContent, setNewContent] = useState("");

  // 记录一次执行（E7 表单，result + refs 必填/可省，executed_by 身份钩子）
  const [recFor, setRecFor] = useState("");
  const [execBy, setExecBy] = useState(getActor());
  const [result, setResult] = useState("");
  const [refsText, setRefsText] = useState("");

  // 执行历史（E6 展开时懒拉一次；append-only 旧→新）
  const [hist, setHist] = useState<Record<string, RunbookExecution[]>>({});

  const loadMounts = useCallback(async () => {
    setErr("");
    try {
      const data = await getJSON<IncidentRunbooksResponse>(ENDPOINTS.incidentRunbooks(id));
      setMounts(data.runbooks ?? []);
      setOff(false);
    } catch (e) {
      if (e instanceof ApiError && e.status === 503) { setOff(true); return; }
      setErr(errText(e));
    }
  }, [id]);

  useEffect(() => {
    setMounts(null);
    setHist({});
    setPicker(false);
    setRecFor("");
    void loadMounts();
  }, [loadMounts]);

  async function loadHistory(rid: string): Promise<void> {
    try {
      const data = await getJSON<RunbookExecutionsResponse>(ENDPOINTS.incidentRunbookExecutions(id, rid));
      setHist((p) => ({ ...p, [rid]: data.executions ?? [] }));
    } catch (e) {
      setErr(errText(e));
    }
  }

  async function toggleHistory(rid: string, open: boolean): Promise<void> {
    if (open && !hist[rid]) void loadHistory(rid);
  }

  async function openPicker(): Promise<void> {
    setPicker((v) => !v);
    if (picker || lib) return;
    try {
      const data = await getJSON<RunbooksResponse>(ENDPOINTS.runbooks);
      setLib(data.runbooks ?? []);
    } catch (e) {
      if (e instanceof ApiError && e.status === 503) { setOff(true); return; }
      setErr(errText(e));
    }
  }

  async function mount(rid: string): Promise<boolean> {
    const by = askActor("挂载人（mounted_by 身份钩子）：");
    if (!by) return false;
    try {
      // E4 契约体仅 runbook_id + mounted_by（DisallowUnknownFields）。幂等：重挂同码同形。
      await postJSON(ENDPOINTS.incidentRunbooks(id), { runbook_id: rid, mounted_by: by });
      return true;
    } catch (e) {
      setErr("挂载失败：" + errText(e));
      return false;
    }
  }

  async function submitMount(): Promise<void> {
    if (!picked) { setErr("先在库里选一本手册"); return; }
    setBusy(true);
    setErr("");
    const ok = await mount(picked);
    if (ok) { setPicker(false); setPicked(""); void loadMounts(); }
    setBusy(false);
  }

  async function createAndMount(): Promise<void> {
    if (!newTitle.trim()) { setErr("手册标题必填（契约 E2：title 必填）"); return; }
    const by = askActor("建册人工号（created_by 身份钩子）：");
    if (!by) return;
    setBusy(true);
    setErr("");
    try {
      // E2 最小表单：id 省略（服务端 RBK-<时间戳>）、scope 留"不限"；正文系统不解析不执行。
      const created = await postJSON<RunbookCreateResponse>(ENDPOINTS.runbooks, { title: newTitle.trim(), content: newContent });
      setLib((p) => (p ? [created.runbook, ...p] : p));
      const ok = await mount(created.runbook.id);
      if (ok) { setPicker(false); setNewTitle(""); setNewContent(""); void loadMounts(); }
    } catch (e) {
      setErr("建册失败：" + errText(e));
    } finally {
      setBusy(false);
    }
  }

  async function unmount(m: RunbookMountView): Promise<void> {
    // 契约 E5：解挂只断挂载关系，执行历史保留、重挂后计数续算——确认框必须说清。
    if (!window.confirm(`确认把「${m.title}」从本事件解挂？\n只解除挂载关系：执行历史不删除（重挂后 execution_count 继续累计）。`)) return;
    setBusy(true);
    setErr("");
    try {
      await delJSON(ENDPOINTS.incidentRunbook(id, m.runbook_id));
      await loadMounts();
    } catch (e) {
      setErr("解挂失败：" + errText(e));
    } finally {
      setBusy(false);
    }
  }

  async function submitExecution(rid: string): Promise<void> {
    const by = execBy.trim() || askActor("执行人工号（executed_by 身份钩子）：");
    if (!by) return;
    if (!result.trim()) { setErr("result 必填（做了什么 / 结果如何，契约 E7）"); return; }
    setBusy(true);
    setErr("");
    try {
      // E7 契约体仅 executed_by/result/refs；refs 必须 JSON 数组（每行一条 string 引用）；
      // 不传 seq/executed_at（服务端发号打点，传了即未知字段 400）。
      const refs = refsText.split("\n").map((s) => s.trim()).filter(Boolean);
      const body: Record<string, unknown> = { executed_by: by, result: result.trim() };
      if (refs.length) body.refs = refs;
      await postJSON(ENDPOINTS.incidentRunbookExecutions(id, rid), body);
      setExecBy(by);
      setResult("");
      setRefsText("");
      setRecFor("");
      setHist((p) => { const n = { ...p }; delete n[rid]; return n; }); // 历史重拉，服务端排序为准
      await loadMounts();
    } catch (e) {
      setErr("记录执行失败：" + errText(e));
    } finally {
      setBusy(false);
    }
  }

  const mountedIds = new Set((mounts ?? []).map((m) => m.runbook_id));

  return (
    <div className={`sec${off ? " rb-off" : ""}`}>
      <div className="sec-h flex-row items-center gap-8">
        处置手册 · Runbook（只记不执行）
        {!off && mounts
          ? (
            <button type="button" className="btn btn--sm" disabled={busy} onClick={() => void openPicker()}>
              {picker ? "收起" : "挂载手册"}
            </button>
          )
          : null}
        <span className="panel-sub">
          auto_execution:false · 台账非审计证据（取证看审计轨迹）
        </span>
      </div>

      {off
        ? (
          <div className="banner warn">
            该功能需持久化后端：runbook store 未接线（无 DB DSN）时端点恒 503——
            与 RCA/session 降级口径一致，这里明示不可用而非伪装"没有手册"。
          </div>
        )
        : null}
      {err ? <div className="banner err mb-6">Runbook：{err}</div> : null}

      {/* 挂载选择器：E1 库列表 → 选 + E4 挂载；空库时给 E2 内联最小创建（title+content） */}
      {picker && !off
        ? (
          <div className="rb-picker">
            {lib === null
              ? <span className="panel-sub"><span className="spin" />加载手册库…</span>
              : lib.length === 0
                ? (
                  <>
                    <div className="faint mb-6">
                      手册库为空。可内联创建一本最小手册（标题 + markdown 正文；正文系统不解析、不执行，id 与适用范围走服务端默认）并立即挂载到本事件。
                    </div>
                    <div className="form-row form-narrow">
                      <span className="muted">标题</span>
                      <input value={newTitle} onChange={(e) => setNewTitle(e.target.value)} placeholder="如：磁盘清理手册" />
                      <span className="muted">正文（md）</span>
                      <textarea className="chat-input" rows={4} value={newContent} onChange={(e) => setNewContent(e.target.value)} placeholder="# 磁盘满&#10;1. df -h&#10;2. 清理 3 天前日志" />
                    </div>
                    <div className="acts">
                      <button type="button" className="btn btn--acc btn--sm" disabled={busy} onClick={() => void createAndMount()}>
                        {busy ? <span className="spin" /> : null}创建并挂载
                      </button>
                      <button type="button" className="btn btn--sm" onClick={() => setPicker(false)}>取消</button>
                    </div>
                  </>
                )
                : (
                  <>
                    <div className="form-row form-row-3">
                      <span className="muted">库内手册</span>
                      <select className="select" value={picked} onChange={(e) => setPicked(e.target.value)}>
                        <option value="">选择手册…（{lib.length} 本，新→旧）</option>
                        {lib.map((r) => (
                          <option key={r.id} value={r.id} disabled={mountedIds.has(r.id)}>
                            {r.title}（{r.id}） · {scopeText(r)}{mountedIds.has(r.id) ? " ·已挂载" : ""}
                          </option>
                        ))}
                      </select>
                      <span className="push-right flex-row items-center gap-6">
                        <button type="button" className="btn btn--acc btn--sm" disabled={busy || !picked} onClick={() => void submitMount()}>
                          {busy ? <span className="spin" /> : null}挂载所选
                        </button>
                        <button type="button" className="btn btn--sm" onClick={() => setPicker(false)}>取消</button>
                      </span>
                    </div>
                    <div className="faint my-4">挂载幂等：重复提交不产生第二条、不刷新首挂留痕（契约 E4），可安全重试。</div>
                  </>
                )}
          </div>
        )
        : null}

      {!off && !mounts && !err ? <span className="panel-sub"><span className="spin" />加载挂载手册…</span> : null}
      {!off && mounts && mounts.length === 0 && !picker
        ? <div className="faint">本事件未挂载手册。点「挂载手册」从库里选一本（库空时可内联创建）。</div>
        : null}

      {(mounts ?? []).map((m) => {
        const rows = hist[m.runbook_id]; // E6 懒拉缓存；undefined = 本会话未展开过
        return (
          <div key={m.runbook_id}>
          <div className="cand">
            <span className="mono">{m.runbook_id}</span>
            <span className="fw-600">{m.title}</span>
            <span className="chip" title="适用范围（scope_severity · scope_service，空 = 不限）">{scopeText(m)}</span>
            <span
              className={`chip ${m.execution_count > 0 ? "chip--open" : ""}`}
              title="该挂载点已记录的执行条数（execution_count；解挂历史保留、重挂续算）"
            >
              执行 {m.execution_count}
            </span>
            <span className="faint">挂载人 {m.mounted_by || "—"} · {fmtTime(m.mounted_at)}</span>
            <span className="push-right flex-row items-center gap-6">
              <button
                type="button" className="btn btn--sm" disabled={busy}
                title="人事后手填一条执行流水（只记录，系统不代跑）"
                onClick={() => { setRecFor((v) => (v === m.runbook_id ? "" : m.runbook_id)); setErr(""); }}
              >
                记录一次执行
              </button>
              <button type="button" className="btn btn--sm" disabled={busy} onClick={() => void unmount(m)}>解挂</button>
            </span>
          </div>

          {recFor === m.runbook_id
            ? (
              <div className="rb-exec">
                <div className="form-row form-narrow">
                  <span className="muted">执行人</span>
                  <input value={execBy} onChange={(e) => setExecBy(e.target.value.trim())} placeholder="executed_by 身份钩子，如 zhangsan" />
                  <span className="muted">结果</span>
                  <textarea
                    className="chat-input" rows={2} value={result} onChange={(e) => setResult(e.target.value)}
                    placeholder="做了什么 / 结果如何，如：清理3天前日志，水位回到62%（必填，≤8192B）"
                  />
                  <span className="muted">引用 refs</span>
                  <textarea
                    className="chat-input" rows={2} value={refsText} onChange={(e) => setRefsText(e.target.value)}
                    placeholder="可选，每行一条（URL 或说明）；落库恒为 JSON 数组"
                  />
                </div>
                <div className="acts">
                  <button type="button" className="btn btn--acc btn--sm" disabled={busy} onClick={() => void submitExecution(m.runbook_id)}>
                    {busy ? <span className="spin" /> : null}保存执行记录
                  </button>
                  <button type="button" className="btn btn--sm" onClick={() => setRecFor("")}>取消</button>
                  <span className="panel-sub">只记不执行（auto_execution:false）：seq/时间由服务端发号打点，事后手填、可补记。</span>
                </div>
              </div>
            )
            : null}

          <details
            className="rca-findings"
            onToggle={(e) => void toggleHistory(m.runbook_id, e.currentTarget.open)}
          >
            <summary>手册正文 · 执行历史{rows ? `（${rows.length}）` : `（已记 ${m.execution_count}，展开拉取）`}</summary>
            <pre className="rb-content">{m.content?.trim() || "（无正文）"}</pre>
            <div className="faint my-4">
              执行历史 append-only · 旧→新（服务端 IDENTITY 发号，只读不可改）。再强调一次：只记不执行，本页不代跑手册。
            </div>
            {!rows
              ? <div className="faint">展开后从 E6 拉取（旧→新）。</div>
              : rows.length === 0
                ? <div className="faint">该挂载点暂无执行记录。</div>
                : rows.map((x) => (
                  <div className="rca-find" key={x.seq}>
                    <span className="mono t-brand">#{x.seq}</span>
                    <span className="faint mono">{fmtTime(x.executed_at)}</span>
                    <span className="mono">{x.executed_by}</span>
                    <span className="rca-find-sum">{x.result}</span>
                    {(x.refs ?? []).length > 0
                      ? (
                        <span className="faint mono" title={(x.refs ?? []).map(refText).join("\n")}>
                          ·{(x.refs ?? []).map(refText).map((s) => (s.length > 24 ? `${s.slice(0, 23)}…` : s)).join(" ,")}
                        </span>
                      )
                      : null}
                  </div>
                ))}
          </details>
          </div>
        );
      })}
    </div>
  );
}
