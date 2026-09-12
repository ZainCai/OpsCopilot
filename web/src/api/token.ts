/**
 * 写操作 Token 与操作人：与内嵌 console 同口径存 sessionStorage
 * （关页即清，少持久暴露）。sessionStorage 为空时兜底取 VITE_OPS_TOKEN
 * （仅内网调试注入，见 .env.example 的警告）。生产推荐反代注入
 * X-OpsCopilot-Token——届时 /api/v1/auth/status 探测到 write_authorized，
 * 界面隐藏输入框。
 */
import { envToken } from "./base";

const KEY_TOKEN = "ops_token";
const KEY_ACTOR = "ops_actor";

export function getToken(): string {
  try {
    return sessionStorage.getItem(KEY_TOKEN) || envToken();
  } catch {
    return envToken();
  }
}

export function setToken(t: string): void {
  try {
    sessionStorage.setItem(KEY_TOKEN, t);
  } catch {
    /* 隐私模式等场景静默 */
  }
}

export function getActor(): string {
  try {
    return sessionStorage.getItem(KEY_ACTOR) ?? "";
  } catch {
    return "";
  }
}

export function setActor(a: string): void {
  try {
    sessionStorage.setItem(KEY_ACTOR, a);
  } catch {
    /* ignore */
  }
}
