# ADR-013: 前端独立工程（web/）——内嵌页冻结、后端只留 REST/SSE

- **状态**：已接受
- **日期**：2026-09-12
- **关联**：优化方案 #9（前端分离）、ADR-009（监听×鉴权×CORS 门禁 D1/D2）、
  `cmd/opscopilot/console.html`（go:embed 内嵌页）、`console_source_sample/`（EagleOps 原型皮肤种子）

## 背景

后端长期以 `go:embed` 携带单文件 `console.html`：升级即换页、无组件化、
无类型、无法工程化演进；功能对齐 EagleOps 原型的诉求（多视图、组件、构建
流水线）在单文件模式下已到天花板。根目录 `console_source_sample/` 是妙答
导出的 React JSX 原型（设计种子），与本仓库无构建集成。

## 决策

1. **内嵌 `console.html` 冻结**：不删除、不改功能——它仍是二进制自带的
   最小可用控制台（离线评估/冒烟场景依赖它），**只修 bug**。所有新页面、
   新交互一律进独立工程。
2. **新建 `web/` 独立前端工程**：Vite 5 + React 18 + TypeScript（strict +
   noUncheckedIndexedAccess），**零重型 UI 库、零 react-router**（手写 hash
   路由）；皮肤从 console.html 现网 CSS 抽为 `src/styles/tokens.css` +
   `base.css`，类名口径与内嵌页一致。
3. **后端职责收缩为 REST/SSE 提供方**：`/api/v1/*`（见
   `cmd/opscopilot/rest_*.go` 的 `Register()`）是唯一契约面；端点常量表在
   `web/src/api/endpoints.ts` 镜像一份，漂移时以 Go 侧为准。
4. **鉴权与 CORS 现状不变，靠开发代理衔接**：
   - 后端 CORS 门禁（ADR-009 / D2）：`OPS_CORS_ORIGIN` 默认不设 = 仅同源
     放行；`*` 在 config 装载期直接启动失败（防"任意网页跨源读事件/审计"）。
   - 开发期 vite dev server 把 `/api` 代理到 `http://127.0.0.1:8080`
     （`.env.example` 核实：`OPS_LISTEN_ADDR` 默认值即 8080），浏览器视角
     始终同源——**代理天然消化了跨源问题，门禁一行不用动**。这正是
     "默认仅同源 + 显式白名单"策略与独立前端并存的正解。
   - 写路径共享密钥 `X-OpsCopilot-Token` 原样沿用：前端 `api/client.ts`
     统一注入（sessionStorage 存 Token，关页即清）；生产推荐反代鉴权后注入
     该头，`GET /api/v1/auth/status` 探测到 `write_authorized` 时界面自动
     隐藏 Token 输入框——与内嵌页同语义。
   - SSE `/api/v1/events/stream`（事件名 `incident`）经代理透传
     （vite 配置对 `text/event-stream` 禁缓冲）；前端封装带断线重连：
     EventSource 原生抖动自恢复，连接进入 CLOSED 后指数退避重建
     （1s→2s→5s→10s 封顶），降级期 UI 提示"轮询降级"并由 30s 轮询兜底。
5. **生产构建产物托管二选一**：起步推荐后端加静态目录挂载（同源、零 CORS
   变更）；确需独立部署时才显式配 `OPS_CORS_ORIGIN=https://<前端源>`。
   go:embed 通道在 web/ 达到功能超集前不移除。

## 迁移节奏

- 新页面/新交互**只进 web/**；内嵌页仅修 bug（安全/数据错误类）。
- web/ 视图实装顺序：告警中心、事件（本批已完成）→ 降噪总览/拓扑 →
  通知渠道 → 全局审计（依赖后端新增列表端点）。
- web/ 覆盖内嵌页全部功能并通过验收后，另立 ADR 讨论移除 go:embed。

## 被否方案

- **继续扩写 console.html**：单文件已 1200+ 行，无组件/类型/构建，
  维护成本随功能超线性上升——正是 #9 要消除的。
- **把原型 JSX（console_source_sample）原样搬进仓库**：依赖妙搭平台全局
  运行时（DS/Icon/cx）与 CDN React UMD，无法本地构建；只作皮肤参照。
- **引入 react-router/重型组件库**：M2 规模六个视图的手写 hash 路由足够，
  内网依赖面越小越稳（同仓 Go 侧亦"算子前置"取向，ADR-002 精神）。

## 交叉检查提醒

- 与 ADR-009：web/ 独立部署形态落地时，需复核监听门禁（非回环暴露必须
  前置鉴权反代）——前端工程本身不引入新的未鉴权暴露面；
- 与 ADR-005：全局审计视图等待后端列表端点，届时审计仍走分离存储
  （append-only + 哈希链），前端不得旁路；
- 端点表（web/src/api/endpoints.ts）与 rest_*.go Register() 成对维护：
  改路由的 PR 若不同步端点表，验收不过。
