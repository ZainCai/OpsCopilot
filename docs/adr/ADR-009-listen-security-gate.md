- 状态：已接受
- 日期：2026-09-11
- 关联：D1（用户决策）、S1（写路径准入）、ADR-006（部署）

## 背景

`OPS_WEBHOOK_TOKEN` 为空时所有写端点（建单/流转/合并/变更 webhook/ingest）完全无鉴权，此前只在启动日志打一行 WARNING——运维很容易漏看。读路径的口径是"loopback-only + 前置鉴权反代"（D2），但代码层没有任何强制。

## 决策

**监听地址与写密钥的组合在启动期 fail-fast 校验（`checkListenSecurity`）：**

1. 监听地址**非回环**（`0.0.0.0`、具体网卡 IP、`:8080` 全接口通配、空主机名）且未配置 `OPS_WEBHOOK_TOKEN` → **启动失败**。
2. 逃生门：`OPS_ALLOW_UNAUTHENTICATED=on` 显式豁免——仅限本机联调，禁止生产。
3. 读路径跨源（D2）：`Access-Control-Allow-Origin` 默认**不返回**（仅同源）；配置 `OPS_CORS_ORIGIN` 才放行，`*` 与非法形态**启动失败**。

## 后果

- 好处：不可能"带着无鉴权写路径"上线；错误配置在启动期即暴露而非运行期出事。
- 代价：`0.0.0.0` 本地联调必须显式设 token 或豁免开关（一次性成本）。
- 交叉检查提醒：**通配监听的判定必须覆盖 `:8080` 与空主机名**——Go 的 `http.Server` 对二者都会绑全部接口（第七轮 H1 曾因此绕过门禁）；新增监听相关逻辑须同步改 `cmd/opscopilot/listen_guard.go` 及其测试。

## 替代方案（否决理由）

- 只加强提示（/healthz 暴露鉴权状态）：靠人看日志，迟早漏。
- 读路径也要求 token：破坏"打开控制台即用"的核心体验，且与 D2 的反代方案重复。

## 注记

- **Host 校验已代码化（09-29）**：背景章"读路径的口径是 loopback-only，但代码层没有任何强制"这一缺口已补上（第十一轮 P1-1）。回环监听时新增 Host 头白名单中间件 `hostGuard`（`cmd/opscopilot/rest_gateway.go`，接线单点在 `newHTTPServer`）：`r.Host` 必须落在 `{127.0.0.1, localhost, [::1]}` × {配置端口, 缺端口形态} 之内，否则 403 固定文案；覆盖全部路由（REST 读端点、SSE、`/console`、换皮 UI 静态资源、`/metrics`、`/healthz` 与全部写路径），无豁免。防的是 **DNS rebinding**：默认部署（回环监听 + 空 token）下，受害者浏览器里的恶意网页把自家域名解析到 `127.0.0.1`，请求对监听方是"本机连接"、对浏览器是"同源"，CORS 拦不住——但浏览器不允许脚本伪造 Host，故 rebinding 请求必带真实域名，白名单即可挡下。非回环监听**不启用**此校验：该形态已由本 ADR 的启动期门禁强制 token，且前置反代/自定义域名会让 Host 白名单变成误杀源。回环判定复用 `listen_guard.go` 的 `isLoopbackHost`（两处判定不漂移）；新增监听相关逻辑仍须同步改 `listen_guard.go` 及其测试。
