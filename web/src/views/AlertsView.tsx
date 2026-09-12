import { useCallback, useEffect, useState } from "react";
import { ApiError, getJSON } from "../api/client";
import { ENDPOINTS } from "../api/endpoints";
import type { AlertsResponse, ShadowAlert } from "../api/types";
import { Panel, Banner, Loading } from "../components/Panel";
import { DataTable, type Column } from "../components/DataTable";
import { ReasonBadge, SevBadge, levelToSev } from "../components/Badges";
import { PageHead } from "../components/Layout";
import { clampText, fmtTime, timeAgoText } from "../lib/format";

/**
 * 视图一 · 告警中心（实装）。
 * 对齐内嵌 console 的 alerts 视图：影子判决流，GET /api/v1/alerts?limit=200。
 * 行点击 → 结构化详情（摘要/结论/指标阈值/影响/建议措施）。
 */
export function AlertsView({ onConn }: { onConn: (ok: boolean) => void }): React.ReactElement {
  const [alerts, setAlerts] = useState<ShadowAlert[]>([]);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState<string>("");
  const [selected, setSelected] = useState<{ a: ShadowAlert; key: string } | null>(null);

  const load = useCallback(async () => {
    setLoading(true);
    setErr("");
    try {
      const data = await getJSON<AlertsResponse>(ENDPOINTS.alerts, { limit: 200 });
      setAlerts(data.alerts ?? []);
      onConn(true);
    } catch (e) {
      const msg = e instanceof ApiError
        ? e.message + (e.needsDb ? "（需 OPS_DB_DSN）" : "")
        : String(e);
      setErr(msg);
      onConn(false);
    } finally {
      setLoading(false);
    }
  }, [onConn]);

  useEffect(() => {
    void load();
  }, [load]);

  const columns: Column<ShadowAlert>[] = [
    { key: "level", label: "级别", render: (a) => <SevBadge s={levelToSev(a.level)} /> },
    { key: "title", label: "标题", render: (a) => a.title || "-" },
    { key: "service", label: "服务/模块", render: (a) => <span className="mono">{clampText(a.service, 34)}</span> },
    { key: "error_code", label: "错误码", render: (a) => <span className="mono">{a.error_code || "—"}</span> },
    { key: "time", label: "时间", render: (a) => <span className="time" title={fmtTime(a.occurred_at)}>{timeAgoText(a.occurred_at)}</span> },
  ];

  return (
    <>
      <PageHead
        title="告警中心"
        desc="影子判决流 · 与降噪评估同源（alert_event）· 最新 200 条"
        right={<button type="button" className="btn btn--sm" onClick={() => void load()}>刷新</button>}
      />
      {loading ? <Loading text="加载告警…" /> : null}
      {err ? <Banner kind="err">加载失败：{err}</Banner> : null}
      <Panel title="影子判决流" sub={`${alerts.length} 条（最新）`}>
        <DataTable
          columns={columns}
          rows={alerts}
          rowKey={(a, i) => `${a.fingerprint}-${i}`}
          onRowClick={(a, i) => setSelected({ a, key: `${a.fingerprint}-${i}` })}
          selectedKey={selected?.key}
          empty={{
            title: "暂无影子判决",
            desc: "降噪引擎影子模式下每次告警处理都会在此留痕（需 OPS_DB_DSN）",
          }}
        />
      </Panel>
      {selected ? <AlertDetail a={selected.a} /> : null}
    </>
  );
}

function KV({ k, children }: { k: string; children: React.ReactNode }): React.ReactElement {
  return (
    <div className="kv" style={{ display: "grid", gridTemplateColumns: "84px 1fr", gap: "4px 10px" }}>
      <span className="k faint">{k}</span>
      <span className="v" style={{ fontFamily: "var(--font-mono)", fontSize: 11.5, overflowWrap: "anywhere" }}>{children}</span>
    </div>
  );
}

/** 详情分区：结构化排障视图（对齐原型 incident.jsx 的证据分区）。 */
function AlertDetail({ a }: { a: ShadowAlert }): React.ReactElement {
  const steps = (a.steps ?? []).map((s, i) => <li key={i}>{s}</li>);
  return (
    <div className="detail" style={{ margin: 0 }}>
      <div style={{ fontWeight: 600, marginBottom: 6 }}>
        {a.level || "一般"} · {a.title || "-"} <span className="faint mono">{a.error_code ?? ""}</span>
      </div>
      <KV k="简要摘要">{a.summary || "—"}</KV>
      <KV k="结论"><ReasonBadge r={a.reason} /></KV>
      <KV k="指标与阈值"><span className="mono">{a.metric || "—"}</span> <span className="faint">阈值</span> <span className="mono">{a.threshold || "—"}</span></KV>
      <KV k="影响范围">{a.impact || "—"}</KV>
      <KV k="触发时间"><span className="time">{timeAgoText(a.occurred_at)}</span> <span className="faint mono">{a.occurred_at}</span></KV>
      <KV k="关联簇">{a.cluster_key ? <span className="mono">{a.cluster_key}</span> : <span className="faint">—</span>}</KV>
      <KV k="建议措施">{steps.length > 0 ? <ol style={{ margin: 0, paddingLeft: 18 }}>{steps}</ol> : "—"}</KV>
      <KV k="指纹">{a.fingerprint}</KV>
    </div>
  );
}
