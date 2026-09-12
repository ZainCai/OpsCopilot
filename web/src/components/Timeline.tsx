import type { ReactNode } from "react";
import type { AuditEntry } from "../api/types";
import { fmtTime } from "../lib/format";

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
