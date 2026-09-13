import { useCallback, useEffect, useRef, useState } from "react";
import { ApiError, getJSON } from "../api/client";
import { ENDPOINTS } from "../api/endpoints";
import type { AuditGlobalEntry, AuditListResponse } from "../api/types";
import { PageHead } from "../components/Layout";
import { Panel, Banner, Loading } from "../components/Panel";
import { DataTable, type Column } from "../components/DataTable";
import { fmtTime, timeAgoText } from "../lib/format";

/**
 * 视图六 · 审计（W12 审计解锁包实装）。
 * 端点：GET /api/v1/audit（全局检索，actor/action/时间窗过滤 + keyset 游标，
 * 最新优先）。单事件全量 detail 仍在事件详情（GET /incidents/{id}/audit）；
 * 本页每行的"事件 ID"深链 #/incidents?id=<id> 直达选中详情。
 * 降级口径同族：503 "not wired" 显式横幅；persistence=memory 如实提示易失。
 */

/** 动作封闭集（与后端 cmd/audit.go AuditAction + incident_audit CHECK 同源；
 * 本期不扩——下拉里没有的，后端也一定 400。 */
const ACTIONS: { v: string; label: string }[] = [
  { v: "", label: "全部动作" },
  { v: "create", label: "建单 create" },
  { v: "transition", label: "流转 transition" },
  { v: "attach_cluster", label: "挂簇 attach_cluster" },
  { v: "merge", label: "合并 merge" },
  { v: "external_recovery_ignored", label: "忽略外部恢复" },
  { v: "rate_limited", label: "限流 rate_limited" },
  { v: "ingest_failed", label: "摄取失败" },
  { v: "rca", label: "根因分析 rca" },
];

/** 动作 → 徽章色（Timeline.tsx AUDIT_DOT 同口径：成功处置=ok ·
 * 需留意=warn · 摄取失败=crit · 其余默认 chip）。 */
const ACTION_CHIP: Record<string, string> = {
  create: "chip--ok", transition: "chip--ok", attach_cluster: "chip--ok", rca: "chip--ok",
  merge: "chip--warn", external_recovery_ignored: "chip--warn", rate_limited: "chip--warn",
  ingest_failed: "chip--crit",
};

/** 时间窗快捷档（since=now−档，半开区间 [since, +∞)；all = 不传 since）。 */
const WINDOWS: { v: string; label: string; ms: number }[] = [
  { v: "24h", label: "24h", ms: 24 * 3600_000 },
  { v: "7d", label: "7d", ms: 7 * 24 * 3600_000 },
  { v: "", label: "全部", ms: 0 },
];

export function AuditView(): React.ReactElement {
  const [actor, setActor] = useState("");
  const [action, setAction] = useState("");
  const [win, setWin] = useState("24h");
  const [rows, setRows] = useState<AuditGlobalEntry[]>([]);
  const [meta, setMeta] = useState<AuditListResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState("");
  const [errCode, setErrCode] = useState(0);
  // 过滤器切换与"加载更多"可能交错——请求序号防旧响应覆盖新页（不重不漏
  // 的前端口径：只接受最后一次意图的回包）。
  const seq = useRef(0);

  const load = useCallback(async (cursor: string) => {
    const my = ++seq.current;
    setLoading(true);
    setErr("");
    try {
      const q: Record<string, string | number | undefined> = { limit: 200 };
      if (actor) q.actor = actor;
      if (action) q.action = action;
      const w = WINDOWS.find((x) => x.v === win);
      if (w && w.ms > 0) q.since = new Date(Date.now() - w.ms).toISOString();
      if (cursor) q.cursor = cursor;
      const data = await getJSON<AuditListResponse>(ENDPOINTS.audit, q);
      if (my !== seq.current) return; // 已有更新的意图在途，丢弃
      setRows((prev) => (cursor ? prev.concat(data.entries ?? []) : data.entries ?? []));
      setMeta(data);
    } catch (e) {
      if (my !== seq.current) return;
      setErr(e instanceof ApiError ? e.message : String(e));
      setErrCode(e instanceof ApiError ? e.status : 0);
      setMeta(null);
      if (!cursor) setRows([]); // 翻页失败保留已加载行，首屏失败清空
    } finally {
      if (my === seq.current) setLoading(false);
    }
  }, [actor, action, win]);

  // 过滤条件变化即回第一页（与事件列表 mount-load/切换即拉取同口径）。
  useEffect(() => { void load(""); }, [load]);

  const columns: Column<AuditGlobalEntry>[] = [
    {
      key: "time", label: "时间",
      render: (e) => <span className="time" title={fmtTime(e.occurred_at)}>{timeAgoText(e.occurred_at)}</span>,
    },
    {
      key: "action", label: "动作",
      render: (e) => (
        <span className={`chip ${ACTION_CHIP[e.action] ?? ""}`} title={`action=${e.action}`}>{e.action}</span>
      ),
    },
    { key: "actor", label: "操作者", render: (e) => e.actor || "—" },
    { key: "summary", label: "摘要", render: (e) => e.summary || "—" },
    {
      key: "incident", label: "事件",
      // 深链：#/incidents?id=<id>（IncidentsView mount 读取 focusId 并选中该
      // 单详情——含已解决/被过滤掉的单，详情面板按 id 独立拉取）。
      render: (e) => (
        <a className="mono" href={`#/incidents?id=${encodeURIComponent(e.incident_id)}`}>{e.incident_id}</a>
      ),
    },
  ];

  return (
    <>
      <PageHead
        title="审计"
        desc="人工与自动动作统一留痕 · 最新优先 · 独立实例/哈希链审计属 M3（ADR-005）"
      />
      <Panel
        title="审计轨迹"
        sub={meta ? `${rows.length} 条（当前过滤）` : "—"}
        actions={
          <>
            <input
              className="tok-input" placeholder="操作者 actor" autoComplete="off"
              value={actor}
              onChange={(e) => setActor(e.target.value.trim())}
            />
            <select className="select" value={action} onChange={(e) => setAction(e.target.value)}>
              {ACTIONS.map((a) => <option key={a.v} value={a.v}>{a.label}</option>)}
            </select>
            <span className="seg" role="group" aria-label="时间窗">
              {WINDOWS.map((w) => (
                <button
                  key={w.v || "all"} type="button" className={`seg-i${win === w.v ? " on" : ""}`}
                  onClick={() => setWin(w.v)}
                >
                  {w.label}
                </button>
              ))}
            </span>
          </>
        }
        banner={
          <>
            {loading && rows.length === 0 ? <Loading text="加载审计轨迹…" /> : null}
            {err
              ? (
                <Banner kind="err">
                  {errCode === 503 || err.includes("not wired")
                    ? "审计后端未接线（503）——本部署形态不提供审计面，非查询失败。"
                    : `加载失败：${err}`}
                </Banner>
              )
              : null}
            {meta?.persistence === "memory"
              ? (
                <Banner kind="warn">
                  内存镜像（无 DSN）：重启即丢{meta.partial_hint ? ` · ${meta.partial_hint}` : ""}
                </Banner>
              )
              : null}
          </>
        }
      >
        <DataTable
          columns={columns} rows={rows}
          rowKey={(e, i) => `${e.occurred_at}-${e.incident_id}-${i}`}
          empty={{
            title: "当前过滤条件下没有审计记录",
            desc: "审计覆盖建单/流转/合并/挂簇/限流/RCA 等动作；切换时间窗或清空 actor 试试。",
          }}
        />
        {meta?.next_cursor
          ? (
            <div className="p-12 text-center">
              <button type="button" className="btn btn--sm" disabled={loading} onClick={() => void load(meta.next_cursor)}>
                {loading ? "加载中…" : "加载更多"}
              </button>
            </div>
          )
          : null}
      </Panel>
    </>
  );
}
