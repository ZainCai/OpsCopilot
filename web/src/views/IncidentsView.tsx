import { useCallback, useEffect, useRef, useState } from "react";
import { ApiError, getJSON, postJSON, probeAuth } from "../api/client";
import { ENDPOINTS } from "../api/endpoints";
import { openIncidentStream, type StreamStatus } from "../api/sse";
import { getToken, setActor, setToken } from "../api/token";
import type {
  AuditResponse, DuplicatesResponse, Incident, IncidentStats, IncidentsResponse,
  TimelineResponse,
} from "../api/types";
import { Panel, Banner, Loading } from "../components/Panel";
import { DataTable, type Column } from "../components/DataTable";
import { OriginBadge, SevBadge, StateChip } from "../components/Badges";
import { PageHead } from "../components/Layout";
import { Seg, StatCard } from "../components/Stat";
import { AuditTimeline, MixedTimeline } from "../components/Timeline";
import { fmtTime, fmtDurationSec, isZeroTime, timeAgoText } from "../lib/format";

/** 事件状态机（后端 rest_incidents.go 保证合法性，前端只展示允许转移）。 */
const NEXT_STATES: Record<string, string[]> = {
  open: ["acked", "mitigated", "resolved"],
  acked: ["mitigated", "resolved"],
  mitigated: ["resolved"],
  resolved: [],
};
const STATE_VERB: Record<string, string> = { acked: "确认", mitigated: "缓解", resolved: "解决" };

type StateFilter = "active" | "resolved" | "";
type StreamChip = { cls: string; label: string; title: string };

const STREAM_STATES: Record<StreamStatus | "off", StreamChip> = {
  live: { cls: "ok", label: "实时推送", title: "SSE 实时推送已连接——事件变更即时显示" },
  connecting: { cls: "warn", label: "连接中", title: "正在建立 SSE 长连接" },
  reconnecting: { cls: "bad", label: "重连中", title: "实时推送断开，正在自动重连…" },
  poll: { cls: "warn", label: "轮询降级", title: "实时推送不可用，已降级为 30s 轮询" },
  off: { cls: "warn", label: "未连接", title: "当前视图未订阅实时推送" },
};

/**
 * 视图二 · 事件（实装，W9 双链路）。
 * 端点：GET /incidents（服务端过滤 + 游标分页 + stats）、GET /incidents/{id}、
 * GET /{id}/duplicates、GET /{id}/audit、GET /{id}/timeline（W10-1 混合时间线）、
 * POST /incidents（人工建单）、POST /{id}/transition、POST /{id}/merge、
 * GET /auth/status、SSE /events/stream。
 */
export function IncidentsView({ onConn }: { onConn: (ok: boolean) => void }): React.ReactElement {
  const [state, setState] = useState<StateFilter>("active");
  const [origin, setOrigin] = useState("");
  const [rows, setRows] = useState<Incident[]>([]);
  const [nextCursor, setNextCursor] = useState("");
  const [stats, setStats] = useState<IncidentStats | null>(null);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState("");
  const [detailId, setDetailId] = useState<string | null>(null);
  const [stream, setStream] = useState<StreamChip>(STREAM_STATES.off);
  const [authReady, setAuthReady] = useState(false); // write_authorized → 隐藏 Token 框
  const [tokenDraft, setTokenDraft] = useState(getToken());
  const reloadTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  const load = useCallback(async (cursor = "") => {
    setLoading(true);
    setErr("");
    try {
      const q: Record<string, string | number> = { limit: 200 };
      if (state) q.state = state;
      if (origin) q.origin = origin;
      if (cursor) q.cursor = cursor;
      const data = await getJSON<IncidentsResponse>(ENDPOINTS.incidents, q);
      setRows((prev) => (cursor ? prev.concat(data.incidents ?? []) : data.incidents ?? []));
      setNextCursor(data.next_cursor ?? "");
      setStats(data.stats ?? null);
      onConn(true);
    } catch (e) {
      setErr(e instanceof ApiError ? e.message : String(e));
      onConn(false);
    } finally {
      setLoading(false);
    }
  }, [state, origin, onConn]);

  // SSE：进入本视图订阅，离开即断开（长连接资源卫生）；
  // 突发合并——300ms 内的多次变更折成一次刷新。
  const streamStatus = useRef<StreamStatus>("connecting");
  useEffect(() => {
    const handle = openIncidentStream(
      () => {
        if (reloadTimer.current) return;
        reloadTimer.current = setTimeout(() => {
          reloadTimer.current = null;
          void load();
        }, 300);
      },
      (s) => {
        streamStatus.current = s;
        setStream(STREAM_STATES[s]);
      },
    );
    // SSE 不可用/降级期轮询兜底（与内嵌 console 同口径）
    const poll = setInterval(() => {
      if (streamStatus.current !== "live") void load();
    }, 30_000);
    return () => {
      handle.close();
      clearInterval(poll);
      if (reloadTimer.current) clearTimeout(reloadTimer.current);
    };
  }, [load]);

  useEffect(() => {
    probeAuth()
      .then((s) => setAuthReady(s.write_authorized))
      .catch(() => setAuthReady(false));
  }, []);

  const columns: Column<Incident>[] = [
    {
      key: "id", label: "事件 ID",
      render: (i) => (
        <span className="mono">{i.id}{i.merged_into ? <span className="faint" title={`已合并到 ${i.merged_into}`}> ⇥</span> : null}</span>
      ),
    },
    { key: "origin", label: "来源", render: (i) => <OriginBadge o={i.origin} /> },
    { key: "severity", label: "严重级", render: (i) => <SevBadge s={i.severity} /> },
    { key: "state", label: "状态", render: (i) => <StateChip s={i.state} /> },
    { key: "title", label: "标题", render: (i) => i.title || "-" },
    { key: "clusters", label: "关联簇", className: "num", render: (i) => <span className="mono">{(i.cluster_keys ?? []).length}</span> },
    { key: "by", label: "建单人", render: (i) => i.created_by || "—" },
    { key: "created", label: "创建时间", render: (i) => <span className="time" title={fmtTime(i.created_at)}>{timeAgoText(i.created_at)}</span> },
  ];

  const st = stats ?? { active: 0, resolved: 0, manual: 0, external: 0 };
  return (
    <>
      <PageHead
        title="事件"
        desc="人工建单 ∥ 外部自动导入 · 同一张表 · L2 疑似重复只提示不自动合并"
        right={
          <>
            {!authReady ? (
              <input
                className="tok-input" type="password" placeholder="写操作 Token" autoComplete="off"
                title="写操作（建单/流转/合并）需携带共享密钥；留空则读操作可用、写操作将 401"
                value={tokenDraft}
                onChange={(e) => { setTokenDraft(e.target.value.trim()); setToken(e.target.value.trim()); }}
              />
            ) : (
              <span className="panel-sub">写权限：已就绪（反代注入或未启用鉴权）</span>
            )}
            <span className={`env-chip ${stream.cls}`} title={stream.title}>
              <span className="dot" /><span>{stream.label}</span>
            </span>
            <Seg<StateFilter>
              options={[{ v: "active", label: "活跃" }, { v: "resolved", label: "已解决" }, { v: "", label: "全部" }]}
              value={state} onChange={(v) => setState(v)}
            />
          </>
        }
      />

      <div className="kpi-grid">
        <StatCard label="活跃事件" value={st.active} unit="单" sub={`${st.active + st.resolved} 单总量`} color="var(--brand)" />
        <StatCard label="已解决" value={st.resolved} unit="单" sub="含被合并单" color="var(--ok)" />
        <StatCard label="人工单" value={st.manual} unit="单" sub="链路 B · 界面建单" color="var(--brand)" />
        <StatCard label="外部单" value={st.external} unit="单" sub="链路 A · 自动导入" color="var(--crit)" />
      </div>

      <Panel
        title="事件列表"
        sub={`${rows.length} 单（当前过滤）`}
        actions={
          <>
            <select className="select" value={origin} onChange={(e) => setOrigin(e.target.value)}>
              <option value="">全部来源</option>
              <option value="manual">人工（manual）</option>
              <option value="alertmanager">Alertmanager</option>
              <option value="webhook">Webhook</option>
              <option value="prometheus">Prometheus（拉取）</option>
            </select>
            <button type="button" className="btn btn--acc btn--sm" onClick={() => setDetailId("__create__")}>+ 人工建单</button>
          </>
        }
        banner={
          <>
            {loading ? <Loading text="加载事件…" /> : null}
            {err ? <Banner kind="err">加载失败：{err}</Banner> : null}
          </>
        }
      >
        <DataTable
          columns={columns} rows={rows} rowKey={(i) => i.id}
          onRowClick={(i) => setDetailId(i.id)}
          selectedKey={detailId ?? undefined}
          empty={{
            title: state === "active" ? "当前无活跃事件" : "当前过滤条件下没有事件",
            desc: "外部告警经入队 worker 建单，或点右上角「+ 人工建单」手动录入。",
          }}
        />
        {nextCursor ? (
          <div style={{ padding: 10, textAlign: "center" }}>
            <button type="button" className="btn btn--sm" onClick={() => void load(nextCursor)}>加载更多</button>
          </div>
        ) : null}
      </Panel>

      {detailId === "__create__"
        ? <CreateForm onDone={() => { setDetailId(null); void load(); }} />
        : detailId ? <IncidentDetail id={detailId} onChanged={() => void load()} /> : null}
    </>
  );
}

// ---------- 人工建单（POST /api/v1/incidents） ----------
function CreateForm({ onDone }: { onDone: () => void }): React.ReactElement {
  const [title, setTitle] = useState("");
  const [severity, setSeverity] = useState("critical");
  const [id, setId] = useState("");
  const [by, setBy] = useState("");
  const [hint, setHint] = useState("");

  async function submit(): Promise<void> {
    if (!title.trim()) { setHint("标题必填"); return; }
    if (!by.trim()) { setHint("建单人工号必填（审计）"); return; }
    setHint("提交中…");
    const body: Record<string, string> = { title: title.trim(), severity, created_by: by.trim() };
    if (id.trim()) body.id = id.trim();
    try {
      const inc = await postJSON<Incident>(ENDPOINTS.incidents, body);
      setHint(`已建单 ${inc.id}`);
      setActor(by.trim());
      setTimeout(onDone, 400);
    } catch (e) {
      setHint("建单失败：" + (e instanceof ApiError ? e.message + (e.needsToken ? "（写操作需填 Token）" : "") : String(e)));
    }
  }

  return (
    <div className="panel-b" style={{ borderTop: "1px solid var(--line-soft)" }}>
      <div className="form-row" style={{ maxWidth: 580 }}>
        <span className="muted">标题</span><input value={title} onChange={(e) => setTitle(e.target.value)} placeholder="如：n1 磁盘使用率 96%" />
        <span className="muted">严重级</span>
        <select value={severity} onChange={(e) => setSeverity(e.target.value)}>
          <option>critical</option><option>warning</option><option>info</option>
        </select>
        <span className="muted">事件 ID（可选）</span><input value={id} onChange={(e) => setId(e.target.value)} placeholder="留空自动生成" />
        <span className="muted">建单人工号</span><input value={by} onChange={(e) => setBy(e.target.value)} placeholder="审计必填，如 zhangsan" />
      </div>
      <div className="acts">
        <button type="button" className="btn btn--acc btn--sm" onClick={() => void submit()}>提交建单</button>
        <button type="button" className="btn btn--sm" onClick={onDone}>取消</button>
        <span className="panel-sub">{hint}</span>
      </div>
    </div>
  );
}

// ---------- 事件详情：GET /{id} + /duplicates + /audit；转移/合并写操作 ----------
function IncidentDetail({ id, onChanged }: { id: string; onChanged: () => void }): React.ReactElement {
  const [inc, setInc] = useState<Incident | null>(null);
  const [cands, setCands] = useState<DuplicatesResponse["candidates"]>([]);
  const [audit, setAudit] = useState<AuditResponse["entries"]>([]);
  const [err, setErr] = useState("");
  const [tlRefresh, setTlRefresh] = useState(0); // 写操作成功后强制重拉混合时间线

  const reload = useCallback(async () => {
    setErr("");
    try {
      const [a, b, c] = await Promise.all([
        getJSON<Incident>(ENDPOINTS.incident(id)),
        getJSON<DuplicatesResponse>(ENDPOINTS.incidentDuplicates(id)).catch(() => ({ incident_id: id, candidates: [], count: 0 })),
        getJSON<AuditResponse>(ENDPOINTS.incidentAudit(id)).catch(() => ({ incident_id: id, entries: [], count: 0 })),
      ]);
      setInc(a);
      setCands(b.candidates ?? []);
      setAudit(c.entries ?? []);
    } catch (e) {
      setErr(e instanceof ApiError ? e.message : String(e));
    }
  }, [id]);

  useEffect(() => { void reload(); }, [reload]);

  async function transition(to: string): Promise<void> {
    const actor = window.prompt("操作人工号（审计）：") ?? "";
    if (!actor) return;
    setActor(actor);
    try {
      await postJSON(ENDPOINTS.incidentTransition(id), { to, actor });
      await reload();
      setTlRefresh((n) => n + 1);
      onChanged();
    } catch (e) {
      window.alert("流转失败：" + (e instanceof ApiError ? e.message + (e.needsToken ? "（写操作需填 Token）" : "") : String(e)));
    }
  }

  async function merge(srcId: string, targetId: string): Promise<void> {
    if (!window.confirm(`确认把 ${srcId} 合并到 ${targetId}？\n被合并单将置为已解决，原单保留并记审计。`)) return;
    const actor = window.prompt("操作人工号（审计）：") ?? "";
    if (!actor) return;
    setActor(actor);
    try {
      await postJSON(ENDPOINTS.incidentMerge(srcId), { target_id: targetId, actor });
      await reload();
      setTlRefresh((n) => n + 1);
      onChanged();
    } catch (e) {
      window.alert("合并失败：" + (e instanceof ApiError ? e.message + (e.needsToken ? "（写操作需填 Token）" : "") : String(e)));
    }
  }

  if (err) return <div className="detail" style={{ margin: 0 }}>加载失败：{err}</div>;
  if (!inc) return <div className="detail" style={{ margin: 0 }}><span className="spin" />加载事件 {id}…</div>;

  const kv: [string, React.ReactNode][] = [
    ["来源", (inc.origin ?? "-") + (inc.source_ref ? ` · ${inc.source_ref}` : "")],
    ["状态", inc.state], ["严重级", inc.severity || "-"], ["建单人", inc.created_by || "—"],
    ["关联簇", (inc.cluster_keys ?? []).join(", ") || "—"], ["去重键", inc.dedup_key || "—"],
    ["合并到", inc.merged_into || "—"], ["外部关单", inc.auto_close_policy || "auto"],
    ["创建", fmtTime(inc.created_at)], ["确认", isZeroTime(inc.acked_at) ? "未确认" : fmtTime(inc.acked_at)],
    ["更新", fmtTime(inc.updated_at)],
  ];
  const nexts = NEXT_STATES[inc.state] ?? [];
  return (
    <div className="detail" style={{ margin: 0 }}>
      <div style={{ fontWeight: 600, marginBottom: 6 }}>事件详情 · {inc.id}</div>
      <div className="kv" style={{ display: "grid", gridTemplateColumns: "84px 1fr", gap: "4px 10px" }}>
        {kv.map(([k, v]) => (
          <span key={k} style={{ display: "contents" }}>
            <span className="faint">{k}</span>
            <span className="mono" style={{ fontSize: 11.5 }}>{v}</span>
          </span>
        ))}
      </div>
      <SLABar inc={inc} />
      <div className="acts">
        {nexts.length > 0
          ? nexts.map((to) => (
            <button key={to} type="button" className={`btn btn--sm${to === "resolved" ? " btn--acc" : ""}`} onClick={() => void transition(to)}>
              {STATE_VERB[to]}
            </button>
          ))
          : <span className="panel-sub">已解决，无可用操作</span>}
      </div>
      <TimelineSection id={id} refresh={tlRefresh} />
      <div className="sec">
        <div className="sec-h">疑似重复（L2 · 只提示不自动合并）</div>
        {cands.length === 0
          ? <div className="faint">无相似候选</div>
          : cands.map((c) => (
            <div className="cand" key={c.incident_id}>
              <span className="score">{c.score}</span>
              <span className="mono">{c.incident_id}</span>
              <span>{c.title}</span>
              <span className="why">{(c.reasons ?? []).join(" / ")}</span>
              <span style={{ marginLeft: "auto", display: "flex", gap: 6 }}>
                <button type="button" className="btn btn--sm" title="把本单并入该候选" onClick={() => void merge(id, c.incident_id)}>并入它</button>
                <button type="button" className="btn btn--sm" title="把该候选并入本单" onClick={() => void merge(c.incident_id, id)}>并来本单</button>
              </span>
            </div>
          ))}
      </div>
      <div className="sec">
        <div className="sec-h">审计轨迹（{audit.length}）</div>
        <AuditTimeline entries={audit} />
      </div>
    </div>
  );
}

// ---------- SLA 时钟（W10-2/F-04：只记录与展示，不自动升级） ----------
// 数据全部来自 GET 视图派生字段（rest_sla.go）：前端零计算口径——只把
// sla_remaining_seconds/sla_breached 画成进度条与徽标。进度 = 已耗时/目标
// 时长；终态单的剩余/超时冻结在闭环时刻（后端保证），所以已解决单不会随
// 墙钟越拖越红。

function SLABar({ inc }: { inc: Incident }): React.ReactElement | null {
  if (!inc.sla_deadline || !inc.created_at || isZeroTime(inc.created_at)) return null;
  const total = (new Date(inc.sla_deadline).getTime() - new Date(inc.created_at).getTime()) / 1000;
  if (!isFinite(total) || total <= 0) return null;
  const remaining = inc.sla_remaining_seconds ?? 0;
  const breached = inc.sla_breached === true;
  const terminal = inc.state === "resolved";
  const pct = Math.min(100, Math.max(0, ((total - remaining) / total) * 100));
  return (
    <div className="sla-bar" title={`SLA 目标 ${fmtDurationSec(total)} · 截止 ${fmtTime(inc.sla_deadline)}${terminal ? " · 终态冻结" : ""}`}>
      <div className="sla-line">
        <span className="faint">SLA</span>
        {inc.sla_minutes ? <span className="sla-tag">自定义 {inc.sla_minutes}m</span> : <span className="sla-tag">按级默认</span>}
        <span className={`sla-state${breached ? " bad" : ""}`}>
          {breached ? `超时 ${fmtDurationSec(remaining)}` : `剩余 ${fmtDurationSec(remaining)}`}
          {terminal ? (breached ? "（闭环时已超）" : "（按时闭环）") : ""}
        </span>
      </div>
      <div className="sla-track">
        <div className={`sla-fill${breached ? " bad" : ""}`} style={{ width: `${pct}%` }} />
      </div>
    </div>
  );
}

// ---------- 混合时间线（GET /{id}/timeline，W10-1/F-03） ----------
// 后端把 alert_event ∥ change_record ∥ incident_audit 三源按时间归并并
// 分页（升序 + 同刻稳定序）；partial=true 表示有依赖源缺席（如无 DSN），
// 已拉到的照常展示——降级口径见 cmd/opscopilot/rest_timeline.go 文件头。

const TL_PAGE_SIZE = 50;

function TimelineSection({ id, refresh }: { id: string; refresh: number }): React.ReactElement {
  const [tl, setTl] = useState<TimelineResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState("");

  const load = useCallback(async (cursor: string) => {
    setLoading(true);
    setErr("");
    try {
      const data = await getJSON<TimelineResponse>(
        ENDPOINTS.incidentTimeline(id),
        { limit: TL_PAGE_SIZE, cursor: cursor || undefined },
      );
      // 翻页追加：升序契约下新条目只会出现在尾部，直接 concat 即保持单调。
      setTl((prev) => (cursor && prev ? { ...data, items: prev.items.concat(data.items ?? []) } : data));
    } catch (e) {
      setErr(e instanceof ApiError ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, [id]);

  useEffect(() => {
    setTl(null);
    void load("");
  }, [load, refresh]);

  const missing = tl?.missing ?? {};
  return (
    <div className="sec">
      <div className="sec-h">混合时间线{tl ? `（告警进出 ∥ 变更 ∥ 处置 · ${tl.total}）` : ""}</div>
      {tl?.partial
        ? (
          <div className="banner warn" style={{ marginBottom: 6 }}>
            部分源降级：{Object.entries(missing).map(([k, v]) => `${k} — ${v}`).join("；") || "未知原因"}
          </div>
        )
        : null}
      {!tl && loading ? <span className="panel-sub"><span className="spin" />加载时间线…</span> : null}
      {err ? <div className="banner err">时间线加载失败：{err}</div> : null}
      {tl ? <MixedTimeline items={tl.items} /> : null}
      {tl?.next_cursor
        ? (
          <button type="button" className="btn btn--sm" disabled={loading} onClick={() => void load(tl.next_cursor)}>
            {loading ? "加载中…" : "加载更多"}
          </button>
        )
        : null}
    </div>
  );
}
