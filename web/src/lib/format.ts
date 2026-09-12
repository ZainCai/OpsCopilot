/** 通用呈现辅助（与内嵌 console 的 timeAgo/文案口径一致）。 */

export function timeAgoText(iso?: string): string {
  if (!iso) return "—";
  const diff = (Date.now() - new Date(iso).getTime()) / 1000;
  if (diff < 60) return `${Math.max(0, Math.floor(diff))}s 前`;
  if (diff < 3600) return `${Math.floor(diff / 60)}m 前`;
  if (diff < 86400) return `${Math.floor(diff / 3600)}h 前`;
  return `${Math.floor(diff / 86400)}d 前`;
}

export function fmtTime(iso?: string): string {
  if (!iso || isZeroTime(iso)) return "—";
  return iso.replace("T", " ").slice(0, 19);
}

export const clampText = (s: string | undefined, n: number): string =>
  !s ? "—" : s.length > n ? `${s.slice(0, n - 1)}…` : s;

/** Go time.Time 零值序列化成 "0001-01-01T…"（从未发生，如未 ack）。 */
export const isZeroTime = (iso?: string): boolean => !iso || iso.startsWith("0001-01-01");

/** 秒数 → 人类可读时长（d/h/m/s 递减取两级；负数取绝对值，语义由调用方表述）。 */
export function fmtDurationSec(secs: number): string {
  const s = Math.abs(secs);
  if (!isFinite(s)) return "—";
  const d = Math.floor(s / 86400);
  const h = Math.floor((s % 86400) / 3600);
  const m = Math.floor((s % 3600) / 60);
  if (d > 0) return `${d}d ${h}h`;
  if (h > 0) return `${h}h ${m}m`;
  if (m > 0) return `${m}m`;
  return `${Math.floor(s)}s`;
}
