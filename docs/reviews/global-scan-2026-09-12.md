# 全局扫描报告 — 2026-09-12

范围：全仓 Go 后端（70 文件 / 16.8k 行非测试）+ 独立前端 `web/`（28 文件，未提交）+ CI / 脚本 / migrations。
方式：代理通道限流（429，当日 20:53 恢复），转人工逐文件扫描。机械检查与逐条 Read 复核全部本地完成。

## 扫描覆盖与验证结果（先说查过什么、没查出什么）

| 检查项 | 结果 |
|---|---|
| `gofmt -l` / `go vet ./...` | 零输出，干净 |
| `go test ./...` 全量 | 全绿（22 个包，含 cmd/integration 假环境） |
| `scripts/check_module_boundaries.py` | 通过，无跨模块内部 import |
| 写路径鉴权 | **全覆盖**：POST incidents / transition / merge、notify CRUD、alertmanager+change webhook 全部走 `authorized()`（恒时比较，change_webhook.go:144-148）+ `MaxBytesReader` + `requireJSON`（CSRF 免预检阻挡） |
| SQL 注入 | 无 `Sprintf` 拼 SQL；抽查 rest_alerts.go（LIMIT 钳制 1000 + 全参数化）、pg_store / change_pg / noise_store_pg 均参数化 |
| 资源泄漏 | 9 处 `Query(ctx)` 全部配对 `rows.Close()`；`time.After` 无循环内滥用；SSE handler 退出条件完备（写失败 / ctx.Done / hub 关闭三路） |
| 状态机 | transition 走 `incident.Store.Transition`，转 acked 自动置 `manual_only` 在 Store 层兜底，无绕过路径 |
| 监听门禁 | listen_guard.go 覆盖 `:8080` 通配写法（H1 修复在位），loopback 判断含 IPv6 |
| CORS | 默认不同源不回 ACAO；`SetCORSOrigin` 走 `ValidateCORSOrigin` fail-fast，拒绝 `"*"` |
| 前端 token | sessionStorage（关页即清）、不进 URL / 日志；SSE 用 EventSource 原生无 token query |
| 前端 XSS | `dangerouslySetInnerHTML` / `innerHTML` / `eval` 全仓零命中 |
| 前端 React 卫生 | IncidentsView SSE handle/poll timer/reload timer 三路 cleanup 完备（81-104 行）；hashRouter 监听器成对移除；tsconfig strict 全家桶 |
| Dockerfile / compose | 存在（`.dockerignore` 也在） |
| 依赖 | go.mod 无未使用 require 迹象；无 TODO/FIXME 遗留 |

## 发现（按梯队）

### 高危
**无。** 本轮未发现可被利用的注入、鉴权绕过、数据竞争或泄漏。

### 第二梯队（中危，建议尽快修）

1. **CI 不覆盖前端 web/ —— TS 回归无门禁**
   - 问题：`.github/workflows/ci.yml` 三个 job 全是 Go；`web/` 有 `typecheck`/`build` 脚本（web/package.json:9-11）但 CI 从不执行。TS 类型错误、构建破坏只能靠本地自觉。
   - 根因：web/ 是本轮新拆分工程（ADR-013），CI 未跟上。
   - 改法：ci.yml 加一个 job（setup-node@v4 + node 20 + `npm ci` + `npm run typecheck` + `npm run build`）。
   - 验证：CI 绿 + 人为注入一个类型错误确认能拦住。

2. **backup_drill.sh 对账结论恒为「通过」——漂移被静默吞掉**
   - 问题：scripts/backup_drill.sh:63 `ok=1` 之后四条对账差值的分支全是 `|| echo note`，**从不置 `ok=0`**；74-78 行据此永远输出「对账通过」。行数/时间戳不一致实际只是打了一行 note，结论栏却写通过，与脚本自身注释「需人工判读」矛盾——演练的意义就是把不一致顶到结论里。
   - 根因：写脚本时把 note 当成了通过。
   - 改法：任一 note 触发时 `ok=0`；结论措辞区分「一致 / 有漂移待人工判读」。
   - 验证：人为在还原库删一行再跑，结论必须非「通过」。

### 第三梯队（低危 / 顺手项）

3. **leader.go 注释与实现矛盾**：Run 主循环注释（cmd/opscopilot/leader.go:183-185）写「复检 = 同连接重跑 try-lock：true 恒成立」，但 `probe()` 实际执行 `SELECT 1`（leader.go:78、262-274）——实现是对的（引用计数护栏，sqlLeaderProbe 注释讲得很清楚），注释是旧设计残留。纯文案修正，别让人误改回 try-lock。

4. **backup_drill.sh 清理残留**：
   - 二次还原（--disable-triggers）成功路径：`$CDUMP.err2`（backup_drill.sh:49 创建）未在 line 52 清理，容器 /tmp 留垃圾；
   - 二次还原失败 exit 3 路径（line 50）：临时库 `RESTORE_DB`（line 48 刚重建）不 DROP 就退出，残留临时库。
   - 改法：err2 并入清理列表；失败分支先 DROP 再 exit。

### 已知项（此前已入台账，本轮确认仍在，不重复开票）

- OverviewView 是骨架 TODO（web/src/views/OverviewView.tsx:30-34），簇表格 / KPI 待迁移——W9 迁移计划内。
- connectors.go `Config.TenantID` 硬编码 `"default"`——W10 建议顺手项。
- Clusterer resolved 簇常驻内存——W10 建议顺手项。

## 复核说明（防误报）

- 「无 Dockerfile」初判为**误报**，复核确认存在（Dockerfile / docker-compose.yaml / .dockerignore），已剔除。
- 代理限流导致本轮无代理广度扫；作为补偿，机械检查（gofmt/vet/test/boundaries）全量真跑，关键路径（路由、鉴权、SQL、SSE、leader、前端 API 层）逐文件人工读过，报告里的行号均为 Read 确认过的。
