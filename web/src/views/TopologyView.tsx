import { useCallback, useEffect, useMemo, useState } from "react";
import { ApiError, getJSON } from "../api/client";
import { ENDPOINTS } from "../api/endpoints";
import type { ChangeRecord, ClustersResponse, TopoEdge, TopoNode, TopologyResponse } from "../api/types";
import { PageHead } from "../components/Layout";
import { Panel, Banner, Loading } from "../components/Panel";
import { useRcaHighlight, clearRcaHighlight } from "../lib/rcaShare";
import { clampText, fmtTime } from "../lib/format";

/**
 * 视图四 · 拓扑画布（W11-3 实装，替换占位骨架）。
 *
 * 自绘 SVG + HTML 混合：边/底层连线走 SVG（viewBox 缩放），节点卡走
 * foreignObject 内嵌 HTML div（复用 base.css 徽标口径，省掉 SVG 里手写
 * 排版）。零图库：布局为「按 node_type 分层环形」——每个类型一圈、圈上
 * 均分、逐圈旋转偏移防径向重叠。
 *
 * 性能口径（刻意不过度优化）：数据规模小（回环/灰度 ≤ 数百节点），布局
 * 一次 useMemo 同步计算、hover 高亮 O(E) 重描边、全量重渲染；不引力导向、
 * 不做四叉树命中/虚拟滚动。超过 AGG_THRESHOLD 300 节点时退化为类型分桶
 * 聚合表（与冻结 console 的 AGG_THRESHOLD 同口径）。
 *
 * 根因链路高亮（W11-3 验收项）：事件详情跑过 RCA 后结果登记在
 * lib/rcaShare（内存单例），这里消费——root_cause 节点红色环、其一跳邻域
 * 边虚线；无 RCA 结果时给提示而非空环。
 */
const AGG_THRESHOLD = 300; // 与 console.html 同口径：超限分桶不画散点
const W = 1240;
const H = 780;

/** 节点健康计数徽标的口径：拓扑契约（pb.TopologyNode）本身无健康字段，
 *  「健康」取活跃簇挂靠数（GET /clusters?state=active 的 node_keys 反查）——
 *  >0 = 有活跃告警簇挂在该节点（crit 徽标），0 = 正常（ok 徽标）；
 *  OPS_NOISE_SHADOW=off 时簇端点 503 → 徽标退化为「—」，不阻断画图。 */
interface LayoutPos { x: number; y: number }

function ringLayout(nodes: TopoNode[]): Map<string, LayoutPos> {
  const pos = new Map<string, LayoutPos>();
  const byType = new Map<string, TopoNode[]>();
  for (const n of nodes) {
    if (pos.has(n.node_key)) continue; // 防御：重复 node_key 只画第一枚
    const t = n.node_type || "node";
    const arr = byType.get(t);
    if (arr) arr.push(n); else byType.set(t, [n]);
  }
  const types = [...byType.keys()].sort();
  const cx = W / 2;
  const cy = H / 2;
  const rMax = Math.min(cx, cy) - 90;
  const rMin = Math.min(150, rMax * 0.55);
  types.forEach((t, ti) => {
    const list = byType.get(t) ?? [];
    const r = types.length === 1 ? Math.min(240, rMax) : rMin + ((rMax - rMin) * ti) / (types.length - 1);
    list.forEach((n, i) => {
      const a = (2 * Math.PI * i) / list.length - Math.PI / 2 + ti * 0.4;
      pos.set(n.node_key, { x: cx + r * Math.cos(a), y: cy + r * Math.sin(a) });
    });
  });
  return pos;
}

/** 置信度 → 语义色类（ADR-007 同源口径：high=ok、medium=warn、low=灰；
 *  圆点走 .dot--*，行内文字走 .t-* utility，替代上一轮的内联 color）。 */
const CONF_DOT: Record<string, string> = { high: "ok", medium: "warn", low: "idle" };
const CONF_TEXT: Record<string, string> = { high: "t-ok", medium: "t-warn", low: "t-idle" };

export function TopologyView(): React.ReactElement {
  const [topo, setTopo] = useState<TopologyResponse | null>(null);
  const [clusterOf, setClusterOf] = useState<Map<string, number> | null>(null);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState("");
  const [hovered, setHovered] = useState<{ key: string; x: number; y: number } | null>(null);
  const [selected, setSelected] = useState<string | null>(null);
  const [hlEnabled, setHlEnabled] = useState(true);

  const rca = useRcaHighlight();

  const load = useCallback(async () => {
    setLoading(true);
    setErr("");
    try {
      const [t, c] = await Promise.all([
        getJSON<TopologyResponse>(ENDPOINTS.topology),
        getJSON<ClustersResponse>(ENDPOINTS.clusters, { state: "active", limit: 1000 }).catch(() => null), // 503 → 健康徽标降级
      ]);
      setTopo(t);
      const counts = new Map<string, number>();
      if (c) {
        for (const cl of c.clusters ?? []) {
          for (const k of cl.node_keys ?? []) counts.set(k, (counts.get(k) ?? 0) + 1);
        }
      }
      setClusterOf(c ? counts : null);
    } catch (e) {
      setErr(e instanceof ApiError ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { void load(); }, [load]);

  const nodes = useMemo(() => topo?.nodes ?? [], [topo]);
  const edges = useMemo(() => topo?.edges ?? [], [topo]);
  const pos = useMemo(() => ringLayout(nodes), [nodes]);
  const nodeByKey = useMemo(() => new Map(nodes.map((n) => [n.node_key, n])), [nodes]);

  // 邻接表（hover 高亮 + 一跳虚线的双重用途；O(E) 一遍构建）
  const adj = useMemo(() => {
    const m = new Map<string, string[]>();
    for (const e of edges) {
      (m.get(e.src_key) ?? m.set(e.src_key, []).get(e.src_key)!).push(e.dst_key);
      (m.get(e.dst_key) ?? m.set(e.dst_key, []).get(e.dst_key)!).push(e.src_key);
    }
    return m;
  }, [edges]);

  const rootSet = useMemo(() => new Set(rca?.rootNodes ?? []), [rca]);
  const hop1Set = useMemo(() => {
    const s = new Set<string>();
    if (!hlEnabled) return s;
    for (const r of rootSet) for (const nb of adj.get(r) ?? []) if (!rootSet.has(nb)) s.add(nb);
    return s;
  }, [adj, rootSet, hlEnabled]);
  const rootEdge = useCallback((e: TopoEdge) => hlEnabled && (rootSet.has(e.src_key) || rootSet.has(e.dst_key)), [hlEnabled, rootSet]);

  const aggMode = nodes.length > AGG_THRESHOLD;
  const status = topo ? `${nodes.length} 节点 / ${edges.length} 边${topo.resolved_as_of ? ` · as_of ${fmtTime(topo.resolved_as_of)}` : ""}` : "";

  return (
    <>
      <PageHead
        title="拓扑"
        desc="观测事实（high）/ 聚合声明（medium）/ low 不进因果推理（ADR-007）· 自绘分层环形画布 · 根因链路高亮"
        right={
          <>
            <span className="panel-sub">{status}</span>
            <button type="button" className="btn btn--sm" onClick={() => void load()}>刷新</button>
          </>
        }
      />
      {loading && !topo ? <Loading text="加载拓扑…" /> : null}
      {err ? <Banner kind="err">拓扑加载失败：{err}</Banner> : null}

      {/* 根因链路高亮横幅（消费 rcaShare 登记；关掉可纯看拓扑） */}
      {rca
        ? (
          <div className="banner rootc">
            <span className="badge-root">根因链路</span>
            <span className="mono">{rca.incidentId}</span>
            <span className="faint">{rca.title ? clampText(rca.title, 30) : ""} · 根因节点 {rootSet.size} 个（红环）· 一跳邻域虚线 · 登记于 {new Date(rca.at).toLocaleTimeString()}</span>
            <span className="push-right flex-row gap-6">
              <button type="button" className="btn btn--sm" onClick={() => setHlEnabled((v) => !v)}>{hlEnabled ? "暂停高亮" : "恢复高亮"}</button>
              <button type="button" className="btn btn--sm" onClick={() => { clearRcaHighlight(); setHlEnabled(true); }}>清除</button>
            </span>
          </div>
        )
        : nodes.length > 0
          ? <div className="banner warn">尚无 RCA 根因结果可高亮——去事件详情点「RCA 分析」跑一次，回来即见红环链路。</div>
          : null}

      <Panel title="拓扑画布" sub={aggMode ? `节点数 ${nodes.length} 超渲染阈值 ${AGG_THRESHOLD}——类型分桶聚合（同 console 口径）` : "hover 节点看详情与邻接边 · 点击节点看 24h 变更"}>
        {!topo && !loading ? <div className="faint">尚未拉取。</div> : null}
        {topo && nodes.length === 0
          ? (
            <div className="empty">
              <div className="e-t">拓扑为空</div>
              <div className="e-d">等第一轮发现进图——连接器接入后 30s 内出现。</div>
            </div>
          )
          : null}

        {aggMode
          ? <AggregateTable nodes={nodes} />
          : nodes.length > 0
            ? (
              <div className="topo-wrap" onMouseLeave={() => setHovered(null)}>
                <svg viewBox={`0 0 ${W} ${H}`} className="topo-svg">
                  {edges.map((e, i) => {
                    const s = pos.get(e.src_key);
                    const d = pos.get(e.dst_key);
                    if (!s || !d) return null; // 悬空边（端点未在本轮快照）跳过，同 console 口径
                    const hot = hovered != null && (e.src_key === hovered.key || e.dst_key === hovered.key);
                    return (
                      <line
                        key={i}
                        x1={s.x} y1={s.y} x2={d.x} y2={d.y}
                        className={`topo-edge${hot ? " hot" : ""}${rootEdge(e) ? " root" : ""}`}
                      />
                    );
                  })}
                  {nodes.map((n) => {
                    const p = pos.get(n.node_key);
                    if (!p) return null;
                    const isRoot = hlEnabled && rootSet.has(n.node_key);
                    const isHop = hlEnabled && hop1Set.has(n.node_key);
                    return (
                      <foreignObject key={n.node_key} x={p.x - 92} y={p.y - 30} width={184} height={64} className="topo-fo">
                        <div
                          className={`topo-node${hovered?.key === n.node_key ? " hov" : ""}${selected === n.node_key ? " sel" : ""}${isRoot ? " root" : ""}${isHop ? " hop" : ""}`}
                          onMouseMove={(ev) => {
                            const box = (ev.currentTarget.closest(".topo-wrap") as HTMLElement | null)?.getBoundingClientRect();
                            setHovered({ key: n.node_key, x: box ? ev.clientX - box.left : 0, y: box ? ev.clientY - box.top : 0 });
                          }}
                          onMouseEnter={() => setSelected(null)}
                          onClick={() => setSelected((k) => (k === n.node_key ? null : n.node_key))}
                        >
                          <span className={`topo-dot dot--${CONF_DOT[n.confidence ?? "low"] ?? CONF_DOT.low}`} title={`confidence=${n.confidence ?? "low"}`} />
                          {isRoot ? <span className="badge-root">根因</span> : null}
                          <span className="topo-k mono" title={n.node_key}>{clampText(n.node_key, 22)}</span>
                          <span className="topo-t faint">{n.node_type || "node"}</span>
                          <span className={`topo-h${(clusterOf?.get(n.node_key) ?? 0) > 0 ? " bad" : ""}`}>
                            {clusterOf == null ? "健康—" : (clusterOf.get(n.node_key) ?? 0) > 0 ? `${clusterOf.get(n.node_key)} 告警簇` : "正常"}
                          </span>
                        </div>
                      </foreignObject>
                    );
                  })}
                </svg>
                {/* 图例（原型 TopoCanvas 口径 styles.css:420-424 / topo-canvas.jsx:269-288，按我们的连线语义裁剪） */}
                <div className="topo-legend mt-10">
                  <span className="lg-i"><span className="dot dot--ok" />置信 high（观测事实）</span>
                  <span className="lg-i"><span className="dot dot--warn" />medium（聚合声明）</span>
                  <span className="lg-i"><span className="dot dot--idle" />low（不进因果推理）</span>
                  <span className="lg-i"><span className="lg-line" />调用边</span>
                  <span className="lg-i"><span className="lg-line dash" />根因传播链路</span>
                  <span className="lg-i"><span className="badge-root">根因</span>根因节点（红环）</span>
                </div>
                {hovered
                  ? (
                    <div
                      className="topo-tip"
                      /* left/top 为跟随鼠标的运行时定位值，无静态类可收敛——保留内联 */
                      style={{ left: hovered.x + 14, top: hovered.y + 10 }}
                    >
                      {(() => {
                        const n = nodeByKey.get(hovered.key);
                        if (!n) return null;
                        return (
                          <>
                            <div className="tt-t mono">{n.node_key}</div>
                            <div>类型 {n.node_type || "node"} · 置信 <b className={CONF_TEXT[n.confidence ?? "low"] ?? CONF_TEXT.low}>{n.confidence ?? "low"}</b> · 邻居 {(adj.get(n.node_key) ?? []).length}</div>
                            <div className="faint">来源 {n.source || "—"} · 有效 {fmtTime(n.valid_from)} → {n.valid_to && !n.valid_to.startsWith("9999") ? fmtTime(n.valid_to) : "现在"}</div>
                            <div className="faint">点击查看 24h 变更</div>
                          </>
                        );
                      })()}
                    </div>
                  )
                  : null}
              </div>
            )
            : null}

        {selected ? <NodeDetail nodeKey={selected} neighbors={adj.get(selected) ?? []} /> : null}
      </Panel>
    </>
  );
}

/** 超限聚合表：按 node_type 分桶 + 置信度分布（复刻 console renderAggregate 口径）。 */
function AggregateTable({ nodes }: { nodes: TopoNode[] }): React.ReactElement {
  const rows = useMemo(() => {
    const m = new Map<string, { count: number; high: number; medium: number; low: number }>();
    for (const n of nodes) {
      const t = n.node_type || "unknown";
      const cur = m.get(t) ?? { count: 0, high: 0, medium: 0, low: 0 };
      cur.count++;
      const lvl = n.confidence === "high" || n.confidence === "medium" ? n.confidence : "low";
      cur[lvl]++;
      m.set(t, cur);
    }
    return [...m.entries()].sort(([a], [b]) => a.localeCompare(b));
  }, [nodes]);
  return (
    <div className="tbl-wrap">
      <table className="tbl">
        <thead><tr><th>node_type</th><th className="num">节点数</th><th className="num">high</th><th className="num">medium</th><th className="num">low</th></tr></thead>
        <tbody>
          {rows.map(([t, v]) => (
            <tr key={t}>
              <td className="mono">{t}</td>
              <td className="num mono">{v.count}</td>
              <td className="num mono t-ok">{v.high}</td>
              <td className="num mono t-warn">{v.medium}</td>
              <td className="num mono faint">{v.low}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

/** 节点详情：邻域计数 + 24h 变更（GET /changes，同 console showNodeDetail 口径）。 */
function NodeDetail({ nodeKey, neighbors }: { nodeKey: string; neighbors: string[] }): React.ReactElement {
  const [changes, setChanges] = useState<ChangeRecord[] | null>(null);
  const [err, setErr] = useState("");

  useEffect(() => {
    let dead = false;
    setChanges(null);
    setErr("");
    getJSON<{ changes?: ChangeRecord[] }>(ENDPOINTS.changes, {
      node_key: nodeKey,
      window_start: new Date(Date.now() - 24 * 3600 * 1000).toISOString(),
    })
      .then((d) => { if (!dead) setChanges(d.changes ?? []); })
      .catch((e: unknown) => { if (!dead) setErr(e instanceof ApiError ? e.message : String(e)); });
    return () => { dead = true; };
  }, [nodeKey]);

  return (
    <div className="detail b-top">
      <div className="sub-t">节点 <span className="mono">{nodeKey}</span></div>
      <div className="faint mb-6">邻居 <b className="mono">{neighbors.length}</b>：{clampText(neighbors.join("、"), 120) || "—"}</div>
      {err ? <div className="banner err">变更查询失败：{err}</div> : null}
      {!changes && !err ? <span className="panel-sub"><span className="spin" />加载 24h 变更…</span> : null}
      {changes && changes.length === 0 ? <div className="faint">24h 内无变更</div> : null}
      {(changes ?? []).map((c, i) => (
        <div className="rca-find" key={`${c.id ?? i}`}>
          <span className="sev sev--info">{c.change_type}</span>
          <span className="mono faint">{fmtTime(c.occurred_at)}</span>
          <span className="rca-find-sum">{c.actor || c.source || ""} {c.summary || ""}</span>
          {c.confidence ? <span className="faint">conf={c.confidence}</span> : null}
        </div>
      ))}
    </div>
  );
}
