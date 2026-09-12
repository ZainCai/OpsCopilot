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

/** 审计轨迹时间线（对应 GET /api/v1/incidents/{id}/audit）。 */
export function AuditTimeline({ entries }: { entries: AuditEntry[] }): ReactNode {
  if (entries.length === 0) return <div className="faint">暂无审计记录</div>;
  return (
    <ul className="tl">
      {entries.map((e, i) => (
        <li key={`${e.occurred_at ?? ""}-${i}`}>
          <span className="sev sev--info">{e.action}</span>
          <span className="tl-meta">{fmtTime(e.occurred_at)}</span>
          <div>
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

/** 时间线来源徽标（kind 封闭集合，后端 rest_timeline.go 保证）。 */
export function TimelineKindBadge({ k }: { k: TimelineKind }): ReactNode {
  return <span className={`tl-kind k-${k}`}>{KIND_LABEL[k] ?? k}</span>;
}

/**
 * 事件混合时间线（GET /api/v1/incidents/{id}/timeline，W10-1/F-03）：
 * 告警进出 ∥ 变更 ∥ 处置动作，后端已按 ts 升序 + 同刻稳定序归并，
 * 前端只渲染——顺序即契约。
 */
export function MixedTimeline({ items }: { items: TimelineItem[] }): ReactNode {
  if (items.length === 0) {
    return <div className="faint">三源暂无可归并记录（无关联簇/无变更/无处置）</div>;
  }
  return (
    <ul className="tl">
      {items.map((e, i) => (
        <li key={`${e.source_id}-${i}`}>
          <TimelineKindBadge k={e.kind} />
          {e.severity ? <SevBadge s={e.severity} /> : null}
          <span className="tl-meta">{fmtTime(e.ts)}</span>
          <div>
            {e.summary || e.source_id}
            {e.confidence ? <span className="faint"> · conf={e.confidence}</span> : null}
          </div>
        </li>
      ))}
    </ul>
  );
}
