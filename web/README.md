# OpsCopilot 独立前端（web/）

优化方案 #9「前端分离」的落地工程：**Vite + React + TypeScript**，只消费后端
REST/SSE（`/api/v1/*`），不再往二进制里嵌页面逻辑。决策记录见
[`docs/adr/ADR-013-前端独立工程.md`](../docs/adr/ADR-013-前端独立工程.md)。

## 与内嵌 console.html 的关系（冻结说明）

- `cmd/opscopilot/console.html`（go:embed 进二进制）**已拍板冻结**：
  只修 bug，不加功能；新页面一律进本工程。
- 本工程的皮肤（tokens.css / base.css）复刻自内嵌页现网 CSS，类名口径一致，
  保证两版并存期间视觉统一；`console_source_sample/`（飞书妙答导出的 EagleOps
  原型 JSX）是设计种子，仅作参照、不参与构建。

## 开发环境

前置：Node ≥ 18（建议 20 LTS）+ 一个跑着的 opscopilot 后端。

```bash
# 1. 起后端（仓库根目录）
go run ./cmd/opscopilot            # 默认监听 127.0.0.1:8080（OPS_LISTEN_ADDR）

# 2. 起前端
cd web
npm install
npm run dev                        # http://127.0.0.1:5173
```

- 开发代理：`vite.config.ts` 把 `/api` 转发到 `http://127.0.0.1:8080`
  （读 `.env.example` 核实：后端默认端口是 **8080**，非 8081）。
  后端换端口时设 `VITE_API_PROXY=http://127.0.0.1:8081` 再 `npm run dev`。
- **不需要配 CORS**：代理让浏览器视角始终同源，后端的 CORS 门禁
  （D2 决策：`OPS_CORS_ORIGIN` 默认不设 = 仅同源）保持不动。
- **Vite env（`.env.example` → 复制为 `.env.local`，后者不入库）**：
  - `VITE_OPS_API_BASE`：REST/SSE base URL。留空 = 同源相对路径（推荐：开发走
    代理、生产后端托管 dist/）；仅前端独立部署且后端显式配了
    `OPS_CORS_ORIGIN` 白名单时才填绝对 URL（`src/api/base.ts` 统一拼接）。
  - `VITE_OPS_TOKEN`：写操作密钥 `X-OpsCopilot-Token` 的初始值（`src/api/token.ts`
    在 sessionStorage 为空时兜底读取）。**警告**：VITE_ 变量会打进构建产物，
    只适合内网个人调试；正式部署留空，改界面输入或反代注入。
  - `VITE_API_PROXY`：仅开发期，vite 代理目标覆盖。
- 写操作（建单/流转/合并/渠道）需共享密钥：在「事件」页右上 Token 框输入
  `OPS_WEBHOOK_TOKEN` 的值（存 sessionStorage，关页即清）；若部署由反代注入
  `X-OpsCopilot-Token` 或未启用鉴权，`GET /api/v1/auth/status` 探测到后
  输入框自动隐藏。

## 构建与部署

```bash
npm run build       # tsc --noEmit + vite build → web/dist/
```

产物 `web/dist/` 为纯静态文件（base 路径 `/`），两种托管方式任选：

1. **后端静态目录托管**（推荐起步）：给 opscopilot 加一个静态目录挂载
   （如 `OPS_CONSOLE_DIR` 指到 dist/，替换现 go:embed 通道）——同源、零 CORS 配置；
2. **独立部署**：任意静态服务器/内网 Nginx；此时才需要显式配
   `OPS_CORS_ORIGIN=https://<前端源>`（禁止 `*`，config 层直接拒绝）。

## 结构

```
src/
├── api/            # base.ts（VITE_OPS_API_BASE/VITE_OPS_TOKEN）/ client.ts（fetch+Token）
│                   # sse.ts（重连封装）/ endpoints.ts（端点常量表）/ token.ts / types.ts
├── components/     # Layout / DataTable / Panel / Stat+Seg / Badges / Timeline
├── router/         # 手写 hash 路由（少依赖，不引 react-router）
├── views/          # 见下方迁移计划表
├── styles/         # tokens.css（设计令牌，抽自 console.html）+ base.css
└── vite-env.d.ts   # VITE_ 变量类型声明
```

## 视图迁移计划（对齐冻结的 console.html 与 EagleOps 原型）

| web/ 视图（路由） | 对应内嵌页视图 / 原型页 | 状态 |
|---|---|---|
| `#/overview` 降噪总览 | view-overview（簇表+KPI+详情；拓扑图部分拆给 TopologyView） | ✅ 已迁移（GET /clusters、/clusters/{key}、/topology 计数） |
| `#/alerts` 告警中心 | view-alerts / 原型 pages/alerts.jsx | ✅ 已迁移（GET /alerts + 结构化详情） |
| `#/incidents` 事件 | view-events / 原型 pages/incidents.jsx+incident.jsx | ✅ 已迁移（列表/游标/详情/建单/流转/合并/SSE+轮询降级） |
| `#/topology` 拓扑 | view-overview 的拓扑图面板 / 原型 pages/topology.jsx | ⬜ 占位（待：环形 SVG、节点点击邻域/时间切片、24h 变更） |
| `#/settings` 设置·通知渠道 | view-settings / 原型 pages/settings.jsx | ⬜ 占位（端点已备：GET/POST /notify/channels、enabled、DELETE） |
| `#/audit` 全局审计 | （内嵌页无；原型无） | ⏸ 阻塞（待后端全局审计列表端点，ADR-005；单事件审计已在事件详情实装） |
| — RCA 根因分析 | 原型 pages/rca.jsx | ⬜ 二期候选（原型独有，后端暂无端点） |
| — 自动修复 | 原型 pages/remediation.jsx | ⬜ 二期候选（同上） |
| — 仪表盘 | 原型 pages/dashboard.jsx | ➡ 由 #/overview 承载（不双开） |

端点契约与冻结声明详见 [`docs/前端分离说明-冻结console.md`](../docs/前端分离说明-冻结console.md)。

## 构建状态

✅ **已验证通过**（2026-09-12，本机便携 Node 环境）：

- **Node**：v22.23.2（win-x64 便携 zip，npmmirror 镜像下载），解压于
  `C:\Users\<用户>\.qwenworkcn\tools\node\node-v22.23.2-win-x64\`（本机工具目录，
  不入库）；npm 随包为 10.9.8。
- **registry**：项目内 `web/.npmrc` 指向 `https://registry.npmmirror.com`
  （仅镜像地址、无密钥，随仓库入库；不动全局 npm 配置）。
- **结果**：`npm install`（68 包）→ `npm run build`（= `tsc --noEmit` +
  `vite build`，产物 `dist/` 约 170 kB JS / gzip 55 kB）→ `npx tsc --noEmit`
  三项全部一次通过，零类型/构建错误。

复跑命令（Git Bash，绝对路径调用，无需永久 PATH）：

```bash
export PATH="$HOME/.qwenworkcn/tools/node/node-v22.23.2-win-x64:$PATH"
cd opscopilot/web
npm install && npm run build && npx tsc --noEmit
```

如需长期使用，可自行把 node 目录追加进用户 PATH（PowerShell）：

```powershell
[Environment]::SetEnvironmentVariable('Path', $env:Path + ';C:\Users\蔡\.qwenworkcn\tools\node\node-v22.23.2-win-x64', 'User')
```
