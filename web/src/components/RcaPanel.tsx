import { useCallback, useState } from "react";
import { ApiError, getJSON } from "../api/client";
import { ENDPOINTS } from "../api/endpoints";
import { getActor } from "../api/token";
import type { Confidence, RcaFinding, RcaResponse, RcaStepStatus } from "../api/types";
import { navigate } from "../router/hashRouter";
import { setRcaHighlight } from "../lib/rcaShare";
import { fmtTime } from "../lib/format";

/** 六步流水线展示序（internal/rca/steps.go Step* 封闭集合，ADR-014 决策 2）。 */
const STEP_LABEL: Record<string, string> = {
  collect: "证据采集", hypothesize: "假设生成", verify: "假设验证",
  attribution: "根因归属", conclude: "LLM 结论", recommend: "处置建议",
};
const STEP_ICON: Record<RcaStepStatus, string> = { done: "✓", pending: "…", failed: "✕" };

/**
 * ADR-007 三段置信度视觉：high / medium / low 与拓扑、时间线同源口径——
 * 观测事实 = high、聚合声明 = medium、low 不进因果推理（root_cause 只会
 * 从 attribution+high 里挑，见 rca.go rootCauses()）。这里画成 3 格条：
 * 填充格数即档位（3/2/1），不做任何数值化"置信度百分比"的臆造。
 */
const CONF_LEVEL: Record<Confidence | string, number> = { high: 3, medium: 2, low: 1 };

function ConfBar({ c }: { c: string }): React.ReactElement {
  const n = CONF_LEVEL[c] ?? 1;
  const cls = c === "high" ? "ok" : c === "medium" ? "warn" : "low";
  return (
    <span className={`conf ${cls}`} title={`confidence=${c}（ADR-007：high=观测事实 / medium=聚合声明 / low=不进因果推理）`} aria-label={c}>
      {[1, 2, 3].map((i) => <span key={i} className="conf-seg" data-on={i <= n ? "1" : "0"} />)}
      <span className="conf-t">{c}</span>
    </span>
  );
}

function FindingLine({ f }: { f: RcaFinding }): React.ReactElement {
  return (
    <div className="rca-find">
      <ConfBar c={f.confidence} />
      <span className="rca-step-tag">{STEP_LABEL[f.step] ?? f.step}</span>
      <span className="rca-find-sum">{f.summary}</span>
      {(f.node_keys ?? []).length > 0
        ? <span className="mono faint"> ·{(f.node_keys ?? []).join(" ,")}</span>
        : null}
      {f.ref ? <span className="mono faint" title="关联证据标识（跨步引用链）"> ·#{f.ref}</span> : null}
    </div>
  );
}

/**
 * 事件详情 · RCA 区块（W11-2）。端点 GET /api/v1/incidents/{id}/rca?actor=
 * （rest_rca.go，Token 门禁与写路径同口径——分析会读全量证据链并写审计，
 * 不是免费读端点，故按钮显式触发、进详情不自动跑）。
 * 降级口径全部照后端契约呈现：503=组件未装配、504=超时、200+conclusion
 * null=「跑过但没结论」（ADR-003 禁伪 RCA），绝不装作无结论。
 */
export function RcaSection({ id }: { id: string }): React.ReactElement {
  const [res, setRes] = useState<RcaResponse | null>(null);
  const [showingAll, setShowingAll] = useState(false);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState("");

  const run = useCallback(async (all: boolean) => {
    setLoading(true);
    setErr("");
    try {
      const q: Record<string, string | number | undefined> = {};
      const actor = getActor();
      if (actor) q.actor = actor; // 审计留痕身份（handler 原样带入 RCA_AUDIT）
      if (all) q.all = "1"; // ?all=1：Token 门禁内的一次性全量回显（#6）
      const data = await getJSON<RcaResponse>(ENDPOINTS.incidentRca(id), q);
      setRes(data);
      setShowingAll(all);
      if (!all) {
        // 登记给拓扑视图（W11-3 根因链路高亮）；root_nodes 取各条 root_cause 的
        // node_keys 并集。全量视图不改登记——根因集与 findings 截断无关。
        const rootNodes = [...new Set((data.root_causes ?? []).flatMap((f) => f.node_keys ?? []))];
        setRcaHighlight({ incidentId: data.incident_id || id, title: data.title, rootNodes, at: Date.now() });
      }
    } catch (e) {
      setErr(e instanceof ApiError ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, [id]);

  return (
    <div className="sec">
      <div className="sec-h">
        根因分析 · RCA
        <button
          type="button" className="btn btn--sm" style={{ marginLeft: 10 }}
          disabled={loading} onClick={() => void run(false)}
        >
          {loading ? <span className="spin" /> : null}
          {res ? "重新分析" : "RCA 分析"}
        </button>
        {res && !loading
          ? (
            <button type="button" className="btn btn--sm" style={{ marginLeft: 6 }} onClick={() => navigate("topology")}>
              在拓扑中看根因链路 →
            </button>
          )
          : null}
      </div>

      {err
        ? (
          <div className="banner err">
            RCA 失败：{err}
            {err.includes("OPS_RCA=off") ? "（后端未启用按需根因分析，装配后重试）" : null}
            {err.includes("timed out") ? "（分析超时 504——增大 OPS_RCA_TIMEOUT 或缩小证据窗）" : null}
          </div>
        )
        : null}
      {!res && !err && !loading ? <div className="faint">未运行。分析按请求重跑（读全量证据链 + 写审计），点击按钮触发。</div> : null}
      {loading && !res ? <span className="panel-sub"><span className="spin" />分析中（同步跑六步流水线，秒级）…</span> : null}

      {res ? (
        <>
          {/* 六步状态条：done ✓ / pending … / failed ✕（步骤级失败不毁整单报告） */}
          <div className="rca-steps">
            {(res.steps ?? []).map((s) => (
              <span
                key={s.name} className={`rca-step st-${s.status}`}
                title={`${s.name} · ${s.status} · ${s.duration_ms}ms${s.error ? ` · ${s.error}` : ""}`}
              >
                <b className="rca-step-i">{STEP_ICON[s.status] ?? "?"}</b>
                <span>{STEP_LABEL[s.name] ?? s.name}</span>
                <span className="faint mono">{s.duration_ms}ms</span>
              </span>
            ))}
          </div>

          <div className="rca-meta">
            证据：拓扑 <b className="mono">{res.evidence?.nodes ?? 0}</b> 节点 /
            <b className="mono"> {res.evidence?.edges ?? 0}</b> 边 /
            <b className="mono"> {res.evidence?.changes ?? 0}</b> 变更 ·
            T0 <span className="mono">{fmtTime(res.t0)}</span> · 窗 <span className="mono">{res.window || "—"}</span> ·
            真相层 <span className="mono">{res.persistence}</span>
            {(res.cluster_keys ?? []).length > 0 ? <> · 簇 <span className="mono">{res.cluster_keys?.join(", ")}</span></> : null}
          </div>
          {(res.alerted_nodes ?? []).length > 0
            ? <div className="rca-meta faint">故障域节点：{res.alerted_nodes?.join("、")}</div>
            : <div className="rca-meta faint">故障域节点：无（无簇或簇已淘汰——各步按「证据不足」降级，不臆造假设）</div>}

          {/* 根因列表（attribution + high 才进这里，rca.go 硬口径） */}
          <div className="rca-h">根因标注（{(res.root_causes ?? []).length}）</div>
          {(res.root_causes ?? []).length === 0
            ? <div className="faint">未标注根因——证据不足或归属步未产出（是运维事实，不是渲染缺失）。</div>
            : (res.root_causes ?? []).map((f, i) => <FindingLine key={i} f={f} />)}

          {/* findings 折叠（truncated 时明示裁了多少、给全量开关） */}
          <details className="rca-findings">
            <summary>
              全部发现 findings（{(res.findings ?? []).length}）
              {res.truncated ? <span className="chip rca-trunc" title="OPS_RCA_MAX_FINDINGS 按置信度+时序截断（与审计同源）">已截断 {res.findings_truncated} 条</span> : null}
            </summary>
            {showingAll
              ? <div className="faint" style={{ margin: "4px 0" }}>当前为全量视图（?all=1）：默认口径下少了 {res.findings_truncated} 条。</div>
              : null}
            {(res.findings ?? []).length === 0
              ? <div className="faint">无 findings</div>
              : (res.findings ?? []).map((f, i) => <FindingLine key={i} f={f} />)}
            {!showingAll && res.truncated && !loading
              ? (
                <button type="button" className="btn btn--sm" style={{ marginTop: 6 }} onClick={() => void run(true)}>
                  拉取全量（?all=1 · 仍在 Token 门禁内）
                </button>
              )
              : null}
          </details>

          {/* 结论区：conclusion=null 是契约层面的显式"没有结论"，明示不装作 */}
          {res.conclusion != null
            ? (
              <div className="rca-conc">
                <div className="rca-h">LLM 结论 {res.llm_used ? <span className="chip chip--open">LLM</span> : null}</div>
                <div style={{ whiteSpace: "pre-wrap" }}>{res.conclusion}</div>
              </div>
            )
            : (
              <div className="banner warn" style={{ marginTop: 8 }}>
                结论待 LLM 配置（conclude pending）——llm-gateway 未接线或本轮未产出，
                ADR-003 禁止伪 RCA：这里明示「没有结论」，不代表分析没跑。
              </div>
            )}
        </>
      ) : null}
    </div>
  );
}
