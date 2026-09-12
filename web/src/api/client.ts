import { apiUrl } from "./base";
import { ENDPOINTS, TOKEN_HEADER } from "./endpoints";
import { getToken } from "./token";

/** API 错误：保留 status 供 UI 区分 401/503 等语义。 */
export class ApiError extends Error {
  readonly status: number;
  constructor(status: number, message: string) {
    super(message);
    this.name = "ApiError";
    this.status = status;
  }
  /** 后端未接 DB 的降级口径（503 "not wired"）。 */
  get needsDb(): boolean {
    return this.message.includes("not wired");
  }
  /** 写操作缺 Token 的提示（401 unauthorized）。 */
  get needsToken(): boolean {
    return this.message.includes("unauthorized");
  }
}

type Query = Record<string, string | number | undefined | null>;

export function withQuery(path: string, q?: Query): string {
  if (!q) return path;
  const p = new URLSearchParams();
  for (const [k, v] of Object.entries(q)) {
    if (v !== undefined && v !== null && String(v) !== "") p.set(k, String(v));
  }
  const s = p.toString();
  return s ? `${path}?${s}` : path;
}

async function parseError(resp: Response): Promise<never> {
  const body = (await resp.json().catch(() => ({}))) as { error?: string };
  throw new ApiError(resp.status, body.error || `${resp.status} ${resp.statusText}`);
}

function authHeaders(): Record<string, string> {
  const t = getToken();
  return t ? { [TOKEN_HEADER]: t } : {};
}

/** GET 读路径（默认无鉴权；同源或经 vite 代理）。 */
export async function getJSON<T>(path: string, q?: Query): Promise<T> {
  const resp = await fetch(apiUrl(withQuery(path, q)), { headers: authHeaders() });
  if (!resp.ok) await parseError(resp);
  return (await resp.json()) as T;
}

/** POST 写路径：自动携带 X-OpsCopilot-Token；空响应返回 undefined。 */
export async function postJSON<T>(path: string, body: unknown): Promise<T> {
  const resp = await fetch(apiUrl(path), {
    method: "POST",
    headers: { "Content-Type": "application/json", ...authHeaders() },
    body: JSON.stringify(body),
  });
  if (!resp.ok) await parseError(resp);
  return (await resp.json().catch(() => undefined)) as T;
}

/** DELETE 写路径（渠道删除）。 */
export async function delJSON<T>(path: string): Promise<T> {
  const resp = await fetch(apiUrl(path), { method: "DELETE", headers: authHeaders() });
  if (!resp.ok) await parseError(resp);
  return (await resp.json().catch(() => undefined)) as T;
}

/** 鉴权探测：反代注入密钥（或未启用鉴权）时写权限自动就绪。 */
export async function probeAuth() {
  const { authStatus } = ENDPOINTS;
  type S = import("./types").AuthStatus;
  return getJSON<S>(authStatus);
}
