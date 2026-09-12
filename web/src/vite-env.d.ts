/// <reference types="vite/client" />

/** Vite env 契约（web/.env.example 有样例）——只有 VITE_ 前缀会进包。 */
interface ImportMetaEnv {
  /** REST/SSE base URL：留空 = 同源（开发期走 vite 代理）。 */
  readonly VITE_OPS_API_BASE?: string;
  /**
   * 写操作共享密钥（X-OpsCopilot-Token 的值）。
   * 仅内网个人调试用：会打进构建产物，生产别设——改由 sessionStorage
   * 手输或反代注入（见 docs/adr/ADR-013 第 4 条）。
   */
  readonly VITE_OPS_TOKEN?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
