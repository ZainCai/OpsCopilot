import { defineConfig, type ProxyOptions } from "vite";
import react from "@vitejs/plugin-react";

/**
 * 开发代理：把 /api 转发到本地 opscopilot 后端。
 *
 * 端口口径（读 .env.example 核实）：后端默认监听 127.0.0.1:8080
 * （OPS_LISTEN_ADDR 默认值；docker-compose 亦映射 8080）。
 * 需要覆盖时设环境变量 VITE_API_PROXY=http://127.0.0.1:8081 等。
 *
 * 为什么代理够用而不用配 CORS：后端 CORS 门禁（D2 决策，internal/config
 * ValidateCORSOrigin）默认不设 OPS_CORS_ORIGIN = 仅同源放行；vite dev
 * server 代理让浏览器视角始终是同源（5173 → 代理 → 8080），无需后端为
 * 开发放开任何跨源头，也不碰 "*"（被明确禁止）。详见 docs/adr/ADR-013。
 */
// 不依赖 @types/node：经 globalThis 收窄读取 VITE_API_PROXY
const nodeProcess = (
  globalThis as { process?: { env?: Record<string, string | undefined> } }
).process;
const proxyTarget = nodeProcess?.env?.VITE_API_PROXY || "http://127.0.0.1:8080";

/**
 * /api 代理定义（server 与 preview 共用同一份）：dev 走 server.proxy，
 * 走查/CI 跑构建产物走 preview.proxy——`vite preview` 同样由 VITE_API_PROXY
 * 决定转发目标，浏览器视角仍是同源，后端 CORS 门禁不用动。
 * 用工厂函数各取一份对象，避免两种 server 共享可变配置对象。
 */
const apiProxy = (): Record<string, ProxyOptions> => ({
  "/api": {
    target: proxyTarget,
    changeOrigin: true,
    // SSE（/api/v1/events/stream）长连接：禁缓冲、禁超时，否则实时推送被掐断
    ws: false,
    configure: (proxy) => {
      proxy.on("proxyRes", (proxyRes) => {
        const ct = proxyRes.headers["content-type"];
        if (typeof ct === "string" && ct.includes("text/event-stream")) {
          proxyRes.headers["cache-control"] = "no-cache";
          proxyRes.headers["connection"] = "keep-alive";
        }
      });
    },
  },
});

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: apiProxy(),
  },
  preview: {
    port: 5173,
    proxy: apiProxy(),
  },
  build: {
    outDir: "dist",
    sourcemap: true,
  },
});
