import { useCallback, useEffect, useState } from "react";
import { ApiError, getJSON } from "../api/client";
import { ENDPOINTS } from "../api/endpoints";
import type { AlertCluster, ClustersResponse, KPIResponse, TopologyResponse } from "../api/types";
import { Panel, Banner, Loading } from "../components/Panel";
import { DataTable, type Column } from "../components/DataTable";
import { SevBadge, StateChip } from "../components/Badges";
import { PageHead } from "../components/Layout";
import { Seg, StatCard } from "../components/Stat";
import { clampText, fmtDurationSec, fmtTime, timeAgoText } from "../lib/format";

/**
 * 视图 · 降噪总览（实装，对齐内嵌 console 的 view-overview）。
 * 端点：GET /api/v1/clusters?state=active|resolved|all&limit=（列表）；
 * GET /api/v1/clusters/{key}（簇详情 = ClusterRecord 全量字段）；
 * GET /api/v1/topology（KPI「拓扑规模」只取 nodes/edges 计数）。
 * 内嵌页 overview 中的拓扑图面板按计划拆到 TopologyView（骨架）单独迁移。
 */
type StateFilter = "active" | "resolved" | "all";

interface Kpi {
  active: number | null;
  resolved: number | null;
  nodes: number | null;
  edges: number | null;
}

export function OverviewView({ onConn }: { onConn: (ok: boolean) => void }): React.ReactElement {
  const [state, setState] = useState<StateFilter>("active");
  const [clusters, setClusters] = useState<AlertCluster[]>([]);
  const [truncated, setTruncated] = useState(false);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState("");
  const [detailKey, setDetailKey] = useState<string | null>(null);
  const [kpi, setKpi] = useState<Kpi>({ active: null, resolved: null, nodes: null, edges: null });
  const [opsKpi, setOpsKpi] = useState<KPIResponse | null>(null); // 运维 KPI 行（W10-3 F-07）

  const load = useCallback(async () => {
    setLoading(true);
    setErr("");
    try {
      const data = await getJSON<ClustersResponse>(ENDPOINTS.clusters, { state, limit: 500 });
      setClusters(data.clusters ?? []);
      setTruncated(data.truncated ?? false);
      onConn(true);
    } catch (e) {
      setErr(e instanceof ApiError ? e.message : String(e));
      onConn(false);
    } finally {
      setLoading(false);
    }
  }, [state, onConn]);

  useEffect(() => {
    void load();
  }, [load]);

  // KPI：两个簇计数各拉一次（limit=1 只要 count）；拓扑规模拉一次全量列表；
  // 运维 KPI（MTTA/MTTR/吞吐，GET /kpis 默认窗）再拉一次——失败置 null 显示
  // 占位破折号，不阻断整页（端点未接线 503 也算失败面）。
  useEffect(() => {
    let dead = false;
    void Promise.all([
      getJSON<ClustersResponse>(ENDPOINTS.clusters, { state: "active", limit: 1 }).catch(() => null),
      getJSON<ClustersResponse>(ENDPOINTS.clusters, { state: "resolved", limit: 1 }).catch(() => null),
      getJSON<TopologyResponse>(ENDPOINTS.topology).catch(() => null),
      getJSON<KPIResponse>(ENDPOINTS.kpis).catch(() => null),
    ]).then(([a, r, t, k]) => {
      if (dead) return;
      setKpi({
        active: a?.count ?? null,
        resolved: r?.count ?? null,
        nodes: t?.nodes?.length ?? null,
        edges: t?.edges?.length ?? null,
      });
      setOpsKpi(k);
    });
    return () => { dead = true; };
  }, []);

  const totalAlerts = clusters.reduce((s, c) => s + (c.alert_count ?? 0), 0);

  const columns: Column<AlertCluster>[] = [
    { key: "key", label: "cluster_key", render: (c) => <span className="mono">{clampText(c.cluster_key, 28)}</span> },
    { key: "state", label: "状态", render: (c) => <StateChip s={c.state} /> },
    { key: "severity", label: "严重级", render: (c) => <SevBadge s={c.severity} /> },
    { key: "alerts", label: "告警数", className: "num", render: (c) => c.alert_count },
    { key: "summary", label: "摘要", render: (c) => clampText(c.summary, 30) },
    { key: "fp", label: "指纹", className: "num", render: (c) => (c.fingerprints ?? []).length },
    { key: "nodes", label: "节点", className: "num", render: (c) => (c.node_keys ?? []).length },
    { key: "last", label: "最近告警", render: (c) => <span className="time" title={fmtTime(c.last_seen_at)}>{timeAgoText(c.last_seen_at)}</span> },
  ];

  return (
    <>
      <PageHead
        title="降噪总览"
        desc="影子模式判决 · 只标注不拦截 · 点击簇行查看详情"
        right={
          <>
            <Seg<StateFilter>
              options={[{ v: "active", label: "活跃" }, { v: "resolved", label: "已解决" }, { v: "all", label: "全部" }]}
              value={state}
              onChange={(v) => { setDetailKey(null); setState(v); }}
            />
            <button type="button" className="btn btn--sm" onClick={() => void load()}>刷新</button>
          </>
        }
      />

      {loading && clusters.length === 0 ? <Loading text="加载簇…" /> : null}
      {err ? <Banner kind="err">加载失败：{err}（OPS_NOISE_SHADOW=off 或未接 DB 时返回 503）</Banner> : null}
      {truncated ? <Banner kind="warn">簇数量超过拉取上限（500），列表与「告警总量」为截断后口径</Banner> : null}

      <div className="kpi-grid">
        <StatCard label="活跃簇" value={kpi.active ?? "—"} unit="簇" sub={state === "active" ? `本页 ${clusters.length} 个` : "open + acked"} color="var(--brand)" />
        <StatCard label="已解决" value={kpi.resolved ?? "—"} unit="簇" sub="静默期自动收敛" color="var(--ok)" />
        <StatCard label="告警总量" value={totalAlerts} unit="条" sub="当前过滤下簇内累计" />
        <StatCard label="拓扑规模" value={kpi.nodes ?? "—"} unit="节点" sub={kpi.edges != null ? `${kpi.edges} 边` : "边数未刷新"} />
      </div>

      {/* 运维 KPI 行（W10-3 F-07）：GET /api/v1/kpis 真实读数（替换占位）。
          口径：窗口内新建事件队列的 MTTA/MTTR（平均闭环与 MTTR 同口径合并
          报告）；samples=0 显示破折号——"无数据"不是"零耗时"。 */}
      <div className="sec-h" style={{ margin: "14px 0 8px", fontSize: 11.5, fontWeight: 600, color: "var(--text-2)" }}>
        运维 KPI · 窗口 {opsKpi?.window ?? "—"}{opsKpi ? `（自 ${fmtTime(opsKpi.window_start)}）` : ""}
      </div>
      <div className="kpi-grid">
        <StatCard
          label="MTTA 平均确认"
          value={opsKpi && opsKpi.stats.acked_samples > 0 ? fmtDurationSec(opsKpi.stats.mtta_seconds) : "—"}
          unit="时长" sub={opsKpi ? `样本 ${opsKpi.stats.acked_samples} 单（窗口内新建）` : "GET /kpis 未就绪"} color="var(--brand)"
        />
        <StatCard
          label="MTTR · 平均闭环"
          value={opsKpi && opsKpi.stats.resolved_samples > 0 ? fmtDurationSec(opsKpi.stats.mttr_seconds) : "—"}
          unit="时长" sub={opsKpi ? `样本 ${opsKpi.stats.resolved_samples} 单（含被合并）` : "GET /kpis 未就绪"} color="var(--ok)"
        />
        <StatCard label="新建事件" value={opsKpi?.stats.created_count ?? "—"} unit="单" sub="窗口吞吐 · 建单" />
        <StatCard label="闭环事件" value={opsKpi?.stats.resolved_count ?? "—"} unit="单" sub="窗口吞吐 · 解决（含合并）" />
      </div>

      <Panel title="告警簇" sub={`${clusters.length} 个（${state === "active" ? "活跃" : state === "resolved" ? "已解决" : "全部"}）`}>
        <DataTable
          columns={columns}
          rows={clusters}
          rowKey={(c) => c.cluster_key}
          onRowClick={(c) => setDetailKey((k) => (k === c.cluster_key ? null : c.cluster_key))}
          selectedKey={detailKey ?? undefined}
          empty={{
            title: state === "active" ? "当前无活跃簇" : "当前过滤下没有簇",
            desc: "影子模式下每条告警都会归簇（需 OPS_DB_DSN 且降噪影子模式开启）。",
          }}
        />
        {detailKey ? <ClusterDetail clusterKey={detailKey} /> : null}
      </Panel>
    </>
  );
}

/** 簇详情：GET /api/v1/clusters/{key}。 */
function ClusterDetail({ clusterKey }: { clusterKey: string }): React.ReactElement {
  const [c, setC] = useState<AlertCluster | null>(null);
  const [err, setErr] = useState("");

  useEffect(() => {
    let dead = false;
    getJSON<AlertCluster>(ENDPOINTS.cluster(clusterKey))
      .then((d) => { if (!dead) setC(d); })
      .catch((e: unknown) => { if (!dead) setErr(e instanceof ApiError ? e.message : String(e)); });
    return () => { dead = true; };
  }, [clusterKey]);

  if (err) return <div className="detail" style={{ margin: 0 }}>详情加载失败：{err}</div>;
  if (!c) return <div className="detail" style={{ margin: 0 }}><span className="spin" />加载簇 {clusterKey}…</div>;

  const rows: [string, React.ReactNode][] = [
    ["cluster_key", <span className="mono">{c.cluster_key}</span>],
    ["状态", <StateChip s={c.state} />],
    ["严重级", <SevBadge s={c.severity} />],
    ["告警数", c.alert_count],
    ["摘要", c.summary || "—"],
    ["首次出现", <span className="time">{fmtTime(c.first_seen_at)}</span>],
    ["最近出现", <span className="time">{fmtTime(c.last_seen_at)}</span>],
    ["指纹", <span className="mono">{(c.fingerprints ?? []).join(", ") || "—"}</span>],
    ["节点", <span className="mono">{(c.node_keys ?? []).join(", ") || "—"}</span>],
  ];
  return (
    <div className="detail" style={{ margin: 0 }}>
      <div style={{ fontWeight: 600, marginBottom: 6 }}>簇详情</div>
      <div style={{ display: "grid", gridTemplateColumns: "84px 1fr", gap: "4px 10px" }}>
        {rows.map(([k, v]) => (
          <span key={k} style={{ display: "contents" }}>
            <span className="faint">{k}</span>
            <span style={{ fontSize: 11.5, overflowWrap: "anywhere" }}>{v}</span>
          </span>
        ))}
      </div>
    </div>
  );
}
