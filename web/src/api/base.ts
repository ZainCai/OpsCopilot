/**
 * API base 与 Token 的 Vite env 注入口（优化方案 #9）：
 *  - VITE_OPS_API_BASE：REST/SSE base URL。留空 = 同源相对路径——开发期由
 *    vite 代理把 /api 转发到后端（vite.config.ts），生产同源托管（推荐）。
 *    仅当独立部署前端且后端显式配了 OPS_CORS_ORIGIN 白名单时才填绝对 URL。
 *  - VITE_OPS_TOKEN：写操作共享密钥（X-OpsCopilot-Token）的初始值。
 *    注意：VITE_ 变量会打进构建产物，只适合内网个人调试；正式部署留空，
 *    改用界面输入（sessionStorage，关页即清）或反代注入该头。
 */
function trimBase(url: string | undefined): string {
  const s = (url ?? "").trim();
  return s.endsWith("/") ? s.slice(0, -1) : s;
}

const apiBase = trimBase(import.meta.env.VITE_OPS_API_BASE);

/** 给端点路径（/api/v1/...）拼上 base URL；同源口径下原样返回。 */
export function apiUrl(path: string): string {
  return apiBase ? `${apiBase}${path}` : path;
}

/** env 注入的 Token（token.ts 在 sessionStorage 为空时兜底取它）。 */
export function envToken(): string {
  return (import.meta.env.VITE_OPS_TOKEN ?? "").trim();
}
