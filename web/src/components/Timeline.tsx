import type { ReactNode } from "react";
import type { AuditEntry, TimelineItem, TimelineKind } from "../api/types";
import { fmtTime } from "../lib/format";
import { SevBadge } from "./Badges";

function detailStr(d?: Record<string, string>): string {
  if (!d) return "";
  try {
    return Object.entries(d).map(([k, v]) => `${k}=${v}`).join(" ");
  } catch {
    return "";
  }
}

/** 审计动作 → 圆点语义色（cmd/opscopilot/audit.go AuditAction 封闭集合同口径：
 *  成功处置=ok · 合并/忽略/限流等需留意=warn · 摄取失败=crit · 其余=idle）。 */
const AUDIT_DOT: Record<string, string> = {
  create: "ok", transition: "ok", attach_cluster: "ok", rca: "ok",
  merge: "warn", external_recovery_ignored: "warn", rate_limited: "warn",
  ingest_failed: "crit",
};

/**
 * 审计轨迹时间线（对应 GET /api/v1/incidents/{id}/audit）。
 * 第二轮换皮：条目行改原型 .tl-item/.tl-t/.tl-dot（styles.css:351-354），
 * 时刻列定宽 mono，dot 按动作语义着色。
 */
export function AuditTimeline({ entries }: { entries: AuditEntry[] }): ReactNode {
  if (entries.length === 0) return <div className="faint">暂无审计记录</div>;
  return (
    <ul className="tl">
      {entries.map((e, i) => (
        <li className="tl-item" key={`${e.occurred_at ?? ""}-${i}`}>
          <span className="tl-t">{fmtTime(e.occurred_at)}</span>
          <span className={`tl-dot dot--${AUDIT_DOT[e.action ?? ""] ?? "idle"}`} title={`action=${e.action}`} />
          <div className="flex-1">
            <span className="sev sev--info">{e.action}</span>{" "}
            <b>{e.actor || "-"}</b>
            {detailStr(e.detail) ? <span className="faint"> {detailStr(e.detail)}</span> : null}
          </div>
        </li>
      ))}
    </ul>
  );
}

const KIND_LABEL: Record<TimelineKind, string> = {
  alert_in: "告警进",
  alert_out: "告警出",
  change: "变更",
  action: "处置",
};

/** 时间线 kind → 圆点语义色（告警进=crit · 变更=warn · 处置=ok · 告警出/恢复=idle） */
const KIND_DOT: Record<TimelineKind, string> = {
  alert_in: "crit", change: "warn", action: "ok", alert_out: "idle",
};

/** 时间线来源徽标（kind 封闭集合，后端 rest_timeline.go 保证）。 */
export function TimelineKindBadge({ k }: { k: TimelineKind }): ReactNode {
  return <span className={`tl-kind k-${k}`}>{KIND_LABEL[k] ?? k}</span>;
}

/**
 * 事件混合时间线（GET /{id}/timeline，W10-1/F-03）：
 * 告警进出 ∥ 变更 ∥ 处置动作，后端已按 ts 升序 + 同刻稳定序归并，
 * 前端只渲染——顺序即契约。行视觉同审计（.tl-item + kind 语义 dot）。
 */
export function MixedTimeline({ items }: { items: TimelineItem[] }): ReactNode {
  if (items.length === 0) {
    return <div className="faint">三源暂无可归并记录（无关联簇/无变更/无处置）</div>;
  }
  return (
    <ul className="tl">
      {items.map((e, i) => (
        <li className="tl-item" key={`${e.source_id}-${i}`}>
          <span className="tl-t">{fmtTime(e.ts)}</span>
          <span className={`tl-dot dot--${KIND_DOT[e.kind] ?? "idle"}`} title={KIND_LABEL[e.kind] ?? e.kind} />
          <div className="flex-1 flex-row flex-wrap items-center gap-6">
            <TimelineKindBadge k={e.kind} />
            {e.severity ? <SevBadge s={e.severity} /> : null}
            <span>{e.summary || e.source_id}</span>
            {e.confidence ? <span className="faint mono">conf={e.confidence}</span> : null}
          </div>
        </li>
      ))}
    </ul>
  );
}
