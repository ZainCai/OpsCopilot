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
  if (!iso) return "—";
  return iso.replace("T", " ").slice(0, 19);
}

export const clampText = (s: string | undefined, n: number): string =>
  !s ? "—" : s.length > n ? `${s.slice(0, n - 1)}…` : s;
