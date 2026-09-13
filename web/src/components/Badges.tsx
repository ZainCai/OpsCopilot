import type { ReactNode } from "react";
import type { IncidentOrigin, IncidentState } from "../api/types";

/** 严重级徽标（原型 SevBadge 形态：底纹 + 同色文字）。 */
export function SevBadge({ s }: { s?: string }): ReactNode {
  const norm = s === "critical" || s === "warning" || s === "info" ? s : "info";
  return <span className={`sev sev--${norm}`}>{(s || "-").toUpperCase()}</span>;
}

/** 事件/簇状态芯片。 */
export function StateChip({ s }: { s?: IncidentState | string }): ReactNode {
  if (!s) return <span className="chip">-</span>;
  return <span className={`chip chip--${s}`}>{s}</span>;
}

const ORIGIN_LABEL: Record<IncidentOrigin | string, string> = {
  manual: "人工", alertmanager: "AM", webhook: "Webhook",
  prometheus: "Prom", azure: "Azure", pull: "拉取", api: "API",
};

/** 双链路来源徽标。 */
export function OriginBadge({ o }: { o?: IncidentOrigin | string }): ReactNode {
  const key = o || "api";
  return <span className={`og og--${key}`}>{ORIGIN_LABEL[key] ?? key ?? "-"}</span>;
}

/** 严重级圆点（原型 StatusDot 口径 styles.css:210-214：critical→crit、warning→warn、其余 idle）。 */
export function SevDot({ s }: { s?: string }): ReactNode {
  const cls = s === "critical" ? "crit" : s === "warning" ? "warn" : "idle";
  return <span className={`dot dot--${cls}`} title={s ? `severity=${s}` : "severity=n/a"} />;
}

/** 影子判决结论徽章（告警中心）：判决语义 → .chip--* 色板
 *  （新增成单=info 提示 · 并簇=warn 需留意 · 窗口去重=ok 已静默）。 */
const REASON_BADGE: Record<string, [string, string]> = {
  "new-incident": ["新增", "chip--info"],
  "cluster-merge": ["并簇", "chip--warn"],
  "dedup-window": ["窗口去重", "chip--ok"],
};
export function ReasonBadge({ r }: { r?: string }): ReactNode {
  const m = r ? REASON_BADGE[r] : undefined;
  if (!m) return <span className="chip">{r ?? "-"}</span>;
  return (
    <>
      <span className={`chip ${m[1]}`}>{m[0]}</span>{" "}
      <span className="faint mono">{r}</span>
    </>
  );
}

/** 中文告警级别 → 严重级映射（rest_alerts.go 的 level 字段）。 */
export function levelToSev(level?: string): string {
  return level === "紧急" ? "critical" : level === "重要" ? "warning" : "info";
}
