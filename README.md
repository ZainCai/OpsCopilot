# OpsCopilot（智能运维副驾驶）

多云中立、只读优先、按效果计费的 AI 运维副驾驶。当前基线：架构方案 v1.3；**M1 已出口**（影子降噪转正评估达标，见 `docs/M1出口验收报告-2026-09-11.md`）；**M2 进行中**（W9 转正+通知闭环 ✅ / W10 事件域补全进行中 / W11 RCA 收尾，排期见 `docs/M2执行排期-W9到W11.md`）。

## 目录结构

```
opscopilot/
├── cmd/opscopilot/          # all-in-one 入口：装配（assembly.go）+ 降噪引擎接线 + REST/SSE/控制台
├── internal/
│   ├── config/              # 全部 OPS_*/REDIS_* 的唯一装载与校验（#2 配置收敛）
│   ├── connector/           # 数据源连接器（prometheus 采集；azure 仅发现）
│   ├── topology/            # 时态拓扑 + 变更事件库（PG 真相源 + 启动回放）
│   ├── noise/               # 影子/强制降噪（Dedup + Clusterer + Verdict 落库）
│   ├── incident/            # 事件域（状态机 + MemStore/PGStore 双实现 + 簇关联）
│   ├── notify/              # 通知闸门与渠道（generic/feishu/wecom + console 兜底）
│   ├── rca/                 # RCA 六步流水线纯 DTO（编排/接线在 cmd 层，ADR-014）
│   ├── llmgw/               # llm-gateway 纯协议出口（ADR-003/015：全系统唯一 LLM 出口）
│   ├── sessionstore/        # 会话热态袋（告警实例 Redis；消费方 = RCA 复盘会话，#7 S1/S2）
│   ├── transport/           # 进程内 gRPC + 唯一 HTTP 发送器（P1-2 调用纪律核心件）
│   └── contracts/           # 跨模块 gRPC 契约唯一存放处（禁止反向依赖）
├── pkg/                     # 通用库（httpx/memguard/metrics/readonly；禁止依赖 internal/）
├── web/                     # 独立前端工程（Vite+React+TS，ADR-013；console.html 已冻结）
├── scripts/                 # 边界检查 / demo / 容量基线 / 评测 / 测试库重置等编排脚本
├── tools/                   # faultinjector（剧本注入器）、rca_eval（golden 评测集）、evaluate.py、loadtest 等
├── migrations/              # golang-migrate SQL（000001~000018）
├── docs/                    # ADR、排期、方案、配置清单、容量、验收/评测报告、历史审核
└── .github/workflows/ci.yml # CI：build/test + 边界检查 + Windows/Linux 冒烟
```

## 里程碑状态

| 阶段 | 状态 | 证据 |
|---|---|---|
| M1（骨架 + 影子降噪转正评估） | ✅ 已出口（2026-09-11） | `docs/M1出口验收报告-2026-09-11.md`；评估集 tools/evaluate.py 100% |
| W9 转正 + 通知闭环（M2 第一个里程碑） | ✅ 全部完成 | `docs/M2执行排期-W9到W11.md`（逐任务 ✅ + commit）、`docs/W9出口演练记录-enforce端到端.md` |
| 优化方案 12 项 + 二期池三波 | ✅ 12/12 收口（09-12） | `docs/OpsCopilot项目优化方案-2026-09-11.md`（进度行含提交号） |
| W10 事件域补全（进行中） | W10-6 簇→事件生产自动挂簇 ✅（`OPS_AUTOATTACH`，评测桥接收口）；余见排期表 | `docs/M2执行排期-W9到W11.md` W10 进度行 |
| W11 RCA + 交付收尾 | 按需 RCA（ADR-014）、llm-gateway conclude（ADR-015）、复盘会话 S1+S2（#7）、自动触发（#4）已先行落地 | 各 ADR + `docs/设计-sessionstore消费方与接线.md` |

## 本机环境说明（重要）

当前 Go 为 **64 位（windows/amd64）**，装在 `C:\Program Files\Go`，与私有化分发目标一致。

常用命令（Git Bash）：

```bash
export PATH="/c/Program Files/Go/bin:$PATH"       # 每次新开 shell 需执行
export GOPROXY=https://goproxy.cn,direct          # proxy.golang.org 不通，用国内镜像

# 全量校验（gofmt 无输出 + vet/build + 单测 + 模块边界）
gofmt -l cmd internal tools pkg && go build ./... && go vet ./... && go test ./... -count=1
py scripts/check_module_boundaries.py

# 本地数据库迁移（需先 cp .env.example .env）
migrate -path migrations -database "postgres://opscopilot:opscopilot@localhost:5432/opscopilot?sslmode=disable" up

# PG 集成测试门控（OPS_TEST_PG_DSN 未设则自动 skip；建库/重置）
bash scripts/reset_test_pg.sh    # 建独立 opscopilot_test 库并应用迁移（--wipe 彻底重建）
export OPS_TEST_PG_DSN='postgres://opscopilot:opscopilot@localhost:5432/opscopilot_test?sslmode=disable'
```

> 若想把 GOPROXY 固化：`go env -w GOPROXY=https://goproxy.cn,direct`（在原生终端执行）。
> `make` 在 Git Bash 缺省未装，改用上条等价的直接 `migrate` 命令（golang-migrate CLI 须以 `go install -tags 'postgres'` 安装，否则报 `unknown driver postgres`）。

## 运维接入

### 环境变量

全量 **64 个运行时 env 键**（`OPS_*` 62 + `REDIS_*` 2）的默认值、非法值行为与消费方逐条见 **`docs/配置清单-OpsEnv.md`**；`.env.example` 是运行时镜像（含每项注释）。常用面摘录：

| 变量 | 默认值 | 说明 |
|---|---|---|
| `REDIS_ALERT_ADDR` / `REDIS_CACHE_ADDR` | 无（必填） | 持久化告警实例 / 缓存实例（ADR-004 双实例物理隔离，缺一或同址拒绝启动） |
| `OPS_LISTEN_ADDR` | `127.0.0.1:8080` | HTTP 监听地址；**默认只绑回环**（S1），容器/对外部署需显式 `0.0.0.0:8080`（compose 已配） |
| `OPS_WEBHOOK_TOKEN` | 空（无鉴权） | 写端点共享密钥；非回环暴露前必须设置（D1 门禁） |
| `OPS_DB_DSN` | 空 = 内存降级 | TimescaleDB 真相源（事件/队列/审计/变更/渠道共池，D7 `OPS_DB_MAX_CONNS=16`） |
| `OPS_NOISE_MODE` | `shadow` | **ADR-011 转正模式**：`shadow` 只标注不拦截；`enforce` 真拦截 + 放行通知；非法值启动失败。回退 = 改回 shadow 重启 |
| `OPS_AUTOATTACH` | `off` | **W10-6 簇→事件生产自动挂簇**：`on`（须 enforce，Validate 强制）时 new-incident 判决自动建单 + `AttachCluster` 挂簇 + `attach_cluster` 审计；共簇冲突跳过计 `opscopilot_autoattach_total{outcome}`；建单幂等键与链路 A 同源不双建。详见 `.env.example` |
| `OPS_RCA` / `OPS_RCA_AUTO` | `on` / `off` | **ADR-014/二期池 #4**：按需根因分析 `GET /api/v1/incidents/{id}/rca`；`on` 时 critical 升级成功 → 异步自动 RCA（actor=auto） |
| `OPS_LLM_ENDPOINT` 等 `OPS_LLM_*` | 空 = 禁用 | **ADR-015**：llm-gateway（OpenAI-compatible 单出口）接线 RCA conclude 与复盘会话；密钥绝不入库/入日志；超时预算 LLM ≤ RCA − 2s |
| `OPS_SESSION` | `off` | **二期池 #7**：RCA 复盘会话 `GET/POST /api/v1/incidents/{id}/rca/session`（热态 alert-Redis + PG 真相 000018，懒恢复；LLM 未配 fail-open pending） |

其余：`OPS_INCIDENT_AUTOCREATE`、`OPS_PULL_ALERTS`、`OPS_ESCALATION*`、`OPS_LEADER_*`、`OPS_NOISE_SINK_*`、`OPS_MEMLIMIT_*`、`OPS_TOPOLOGY_EDGES` 等见 `.env.example` 注释与配置清单。

**双实例/多副本部署（ADR-012）**：多副本直接部署即可——PG advisory lock 选主，leader 跑拓扑采集/判决链路/拉取/归档，事件链路（webhook 入队、消费 worker、REST/SSE、通知、升级扫描）全实例常驻，认领租约 + 建单幂等保证 at-least-once 不重复；本实例 leader 态看 `/metrics` 的 `opscopilot_is_leader`。

### 变更事件 webhook

`POST /api/v1/changes`（Git/Jenkins/人工统一入口，变更事件是 RCA 证据链输入）：

```bash
curl -X POST http://127.0.0.1:8080/api/v1/changes \
  -H "Content-Type: application/json" \
  -H "X-OpsCopilot-Token: <OPS_WEBHOOK_TOKEN>" \
  -d '{"id":"<唯一幂等键>","node_key":"azure://vm/<vmId>","type":"deploy","source":"git","summary":"v1.2.3 上线"}'
```

状态码语义（调用方按此处理，勿盲目重试）：

- `200` 入库成功；**重复 ID 也返回 200**（响应带 `"duplicate": true` 与原记录）——幂等键的意义就是"重复提交 = 已经成功"，自动重试的发送方不会形成重试风暴；
- `400` 请求体/字段校验失败；`401` Token 缺失或不匹配；`405` 非 POST；`413` 请求体超 1MiB；`422` 关联节点不在拓扑图中。

### 按需 RCA 与复盘会话（ADR-014/015 / #7）

```bash
curl -s -H "X-OpsCopilot-Token: <key>" \
  'http://127.0.0.1:8080/api/v1/incidents/<id>/rca?actor=ops'          # 六步流水线（取证→假设→验证→归因→建议→conclude）
curl -s '.../rca?all=1'                                                 # 取未截断全量 findings（默认按 OPS_RCA_MAX_FINDINGS 截断）
curl -s -H "X-OpsCopilot-Token: <key>" '.../incidents/<id>/rca/session'  # 复盘会话（OPS_SESSION=on；POST 追加轮次）
```

- 只读按需分析，`OPS_RCA=off` 时端点显式 503；每次分析落 `incident_audit`（action=rca）；
- conclude 步是唯一 LLM 挂点：`OPS_LLM_*` 未配置时恒 pending（宁缺不假答，ADR-003 禁止伪 RCA）；LLM 故障 fail-open 回 pending，绝不 5xx。

### 通知渠道配置（W9-2）

`OPS_NOISE_MODE=enforce` 时，降噪判定为**新事件**的告警会经渠道发出通知（窗口重复 / 故障域并入不重复通知）。渠道配置存 DB（`notify_channel`，迁移 000012），**保存即热生效，无需重启**：

```bash
# 新增/更新渠道（kind：generic 自建 JSON | feishu 飞书机器人 | wecom 企业微信机器人）
# min_severity（W9-3 路由）：critical 只收严重 / warning 收告警及以上 / info 全收（默认）
curl -X POST http://127.0.0.1:8080/api/v1/notify/channels \
  -H "Content-Type: application/json" -H "X-OpsCopilot-Token: <OPS_WEBHOOK_TOKEN>" \
  -d '{"name":"ops-feishu","kind":"feishu","url":"https://open.feishu.cn/open-apis/bot/v2/hook/xxx","min_severity":"warning","enabled":true}'

curl http://127.0.0.1:8080/api/v1/notify/channels                       # 列表（读路径）
curl -X POST .../api/v1/notify/channels/ops-feishu/enabled -d '{"enabled":false}' -H 'Content-Type: application/json' -H 'X-OpsCopilot-Token: <key>'
curl -X DELETE .../api/v1/notify/channels/ops-feishu -H 'X-OpsCopilot-Token: <key>'
```

内置 `console` 兜底渠道（落服务日志）恒在且**不可覆盖/删除**——enforce 下零渠道意味着通知静默丢失，兜底至少留痕。装配启动时若 enforce 且零 webhook 渠道会打 WARNING。控制台「设置」页提供同样的能力（列表/新增/启停/删除）。

**严重级路由（W9-3）**：每个渠道可声明 `min_severity`（`critical` > `warning` > `info`），投递时按告警严重级逐渠道判定——"critical 进 IM、info 只留日志"由此表达。未知/空严重级按 `critical` 处理（失败模式必须是多通知，而非静默丢弃）。

### 值班升级（W9-3，最小版）

通知发出去 ≠ 有人接手。`OPS_ESCALATION=on` 启用后，后台每 `OPS_ESCALATION_INTERVAL`（默认 60s）扫描一次：`state=open`（未确认）且创建超过 `OPS_ESCALATION_AFTER`（默认 15m）的事件，再发一次 `[超时未响应]` 通知。**只升级一次**——台账 `incident_escalation`（迁移 000014）以 `(tenant_id, incident_id)` 主键幂等，多实例也不会重复发；发送失败自动回滚认领，下一轮重试。

- 已 `acked` 的事件不再催（人工已接手）；不做排班/轮岗/多级升级（M3）；
- 升级通知沿用事件自身严重级 → 自然走上面的渠道路由；
- 无 DB 时台账退化为内存（重启即丢、仅单实例正确），启动会打 WARNING。

### 指标与延迟打点（W9-4）

`GET /metrics` 暴露 Prometheus 文本格式指标（读路径无鉴权，与其它 GET 一致）。

```
opscopilot_alerts_processed_total                                    进入降噪引擎的告警数
opscopilot_noise_verdicts_total{reason="…"}                          按收敛原因分桶（new-incident/dedup-window/cluster-merge/unjudged/other）
opscopilot_alert_fired_to_verdict_seconds{_bucket,_sum,_count,_p50,_p95,_p99}
                                                                     告警发射 → 判决**生成**（投递落库队列；#8 异步落库后口径，见 docs/W9-4 §9）
opscopilot_alert_fired_to_notify_seconds{…}                          告警发射 → 通知送达完成（enforce + 放行才有样本）
opscopilot_alert_latency_skipped_total{stage="verdict"|"notify"}     因源侧未给发射时刻而未观测延迟的条数
opscopilot_noise_gate_{suppressed,dispatched}_total                  Gate 累计计数（镜像 notify_gate_stats）
opscopilot_noise_sink_queue / _drops_total{store} / opscopilot_noise_write_dropped_total
                                                                     判决异步落库队列水位 / 队列侧丢弃 / 写库失败丢弃（宁漏库存不丢通知）
opscopilot_autoattach_total{outcome="attached"|"conflict"|"skipped"}  W10-6 自动挂簇三出口计数（OPS_AUTOATTACH=on 才有样本）
opscopilot_rca_requests_total{outcome} / _rca_duration_seconds / _rca_autotrigger_dropped_total
                                                                     按需 RCA 请求/耗时/自动触发队满丢弃
opscopilot_llm_requests_total{outcome} / _session_llm_*              llm-gateway 与复盘会话出站请求（降级面可计数）
opscopilot_mem_entries{store} / _mem_evictions_total{store}          内存有界结构规模/淘汰（#6）
opscopilot_is_leader                                                 本实例 leader 态 0/1（ADR-012）
```

- **`_p50/_p95/_p99` 是本项目的扩展**：直方图额外保留 10000 个滑窗原始样本，分位按**最近秩法**在真实观测值上精确计算。无样本时渲染为 `NaN`。
- `alert_fired_at` 取上游 startsAt/activeAt；**缺失时不打点**，该情形记进 `opscopilot_alert_latency_skipped_total{stage}`。
- **对账恒等式**：`alerts_processed_total − skipped{stage="verdict"} == fired_to_verdict_seconds_count`（#8 后打点在判决生成时刻，落库丢弃另立账，见 docs/W9-4 §9）。
- 该差值含"源侧告警年龄"（采集节拍决定，默认 30s 轮询 → 上界 ≈30s）；隔离自身处理耗时看 `sum/count` 与源侧时刻之差。

```bash
curl -s http://127.0.0.1:8080/metrics | grep -E 'fired_to_(verdict|notify)_seconds_(count|p95)'
```

### 运维脚本入口

| 脚本 | 用途 |
|---|---|
| `bash scripts/reset_test_pg.sh [--wipe]` | 建/重置独立测试库 `opscopilot_test`（`OPS_TEST_PG_DSN` 门控集成测试的前置） |
| `bash scripts/run_demo.sh` | 一键演示（compose 栈 + 注入器 + app） |
| `bash scripts/run_rca_eval.sh [--with-llm]` | RCA 评测集（golden set）一键跑分——W12 转正门禁载体；评测接线走 `OPS_NOISE_MODE=enforce + OPS_AUTOATTACH=on` 生产挂簇路径，出 `docs/reviews/rca-eval-*/report.md` |
| `bash scripts/run_capacity_baseline.sh` | 容量基线复跑（阶梯拐点/双实例扩展性/备份对账），结论见 `docs/容量基线-2026-09-12.md` |
| `bash scripts/backup_drill.sh` | 备份恢复演练（M1 出口 RPO/RTO 证据） |
| `py scripts/check_module_boundaries.py` | internal 禁互 import 静态检查（P1-2，CI 已挂） |

### 前端工程（web/，ADR-013）

控制台已从单文件 `console.html`（已冻结）迁到 `web/`（Vite + React + TS，3 视图接 REST/SSE）。构建：`cd web && npm run build`（产物 `web/dist`，需 node v22+；开发 `npm run dev` 起 vite dev server）。

### 本地 compose 环境

`make env-up` 启动 TimescaleDB + 双 Redis + 应用。compose 已配置 `OPS_LISTEN_ADDR=0.0.0.0:8080`（容器隔离即安全边界）与 Redis healthcheck；应用容器为 distroless（无 shell），探活用 `GET /healthz` 由外部编排层负责。默认 DB 口令仅限本地开发（见 `.env.example` 注释）。

## 验证结果（2026-09-12，Go 1.27.1 windows/amd64）

```
go build ./... / go vet ./...                     → 通过
go test ./... -count=1（含 OPS_TEST_PG_DSN 门控的 PG 集成测试）→ 全绿（无 DSN 自动 skip）
py scripts/check_module_boundaries.py             → 通过：无跨模块内部 import
bash scripts/run_rca_eval.sh（证据版）             → 4/4 top1=100%，静默守护 3/3，挂簇走 OPS_AUTOATTACH 生产路径
```

## 架构决策记录

`docs/adr/` 存 ADR 全集（001 事件骨干 / 002 算子前置 / 003 LLM 单出口 / 004 Redis 双实例 / 005 审计分离 / 006 数据库部署 / 007 拓扑置信度 / 008 外部事件代 / 009 监听门禁 / 010 工单归档 / 011 enforce 转正 / 012 单 owner 水平扩展 / 013 前端独立工程 / 014 RCA 最小链路 / 015 llm-gateway 转正 conclude；完整索引见 `docs/adr/README.md`）。

**任何 P0 修订或影响其他决策的变更，先写 ADR 再改文档**——这是 v1.3 §5.2 的硬性流程，源于 C17 部署策略与数据层特性冲突的事故。每份 ADR 末尾的"交叉检查提醒"记录耦合项。

## 文档索引（docs/）

| 文档 | 内容 |
|---|---|
| `智能运维副驾驶技术架构方案_v1.3.md` | 总架构基线 |
| `功能点清单-M2候选.md` | F-xx 功能点全集与优先级 |
| `M1出口验收报告-2026-09-11.md` · `M2执行排期-W9到W11.md` | 出口验收与周排期（进度行含提交号） |
| `OpsCopilot项目优化方案-2026-09-11.md` | 12 项优化 + 二期池三波收口记录 |
| `配置清单-OpsEnv.md` | **64 个 env 键唯一装载表**（默认值/非法值行为/消费方）+ 测试/CLI 键附录 |
| `容量模型-三级估算.md` · `容量基线-2026-09-12.md` | 三级估算与实测基线（部署必查项标注） |
| `方案-双链路事件来源.md` | 事件双链路（外部导入 ∥ 人工建单）融合与去重方案 |
| `设计-sessionstore消费方与接线.md` | 复盘会话五个开放问题的拍板记录（#7 S1/S2 蓝图） |
| `W9出口演练记录-enforce端到端.md` · `W9-4-E2E延迟打点与P95实测.md` | enforce 端到端与延迟专项 |
| `前端分离说明-冻结console.md` | web/ 独立工程与 console.html 冻结说明 |
| `reviews/` | 历次全局审核报告 + rca-eval 评测报告（含转正门禁章） |
| `history/` | 早期堆在仓库根的审核报告归档（D11） |

## 版本状态

- 远端：`https://github.com/ZainCai/OpsCopilot.git`，分支 `main`；
- 提交身份：`ZainCai <zaincai@outlook.com>`；
- 里程碑轨迹：M1 第 0 周骨架（`0549778`）→ W1 环境底座 → W2~W6 连接器/拓扑/降噪/评估 → **M1 出口**（方案 B）→ W9 转正+通知闭环 → 二期池 12/12 + 波次三件（ADR-014/015、#7 会话、W10-6 自动挂簇）——逐任务提交号见 `docs/M2执行排期-W9到W11.md` 进度行；
- 授权：专有软件，保留所有权利（详见仓库 `LICENSE` 文件）。

## 已知限制

- 读路径无鉴权（GET 面暴露聚合数字与事件列表）：跨源默认仅同源（D2），生产部署建议反代加认证；游标 HMAC 留 M3 候选；
- 降噪队列落库为"队满丢弃"取向：极端洪峰/慢 DB 下 PG 判决流可缺条目（Redis/PG 镜像与内存态非强一致），丢失面唯一出口是 `opscopilot_noise_sink_drops_total{store}` + ERROR 日志——通知链路不受影响；
- 值班升级台账 / 事件 Store / 审计在无 DB 时退化内存：重启即丢、仅单实例正确（启动响亮 WARNING）；
- 多实例 per-item 建单限流、读路径鉴权、排班/多级升级、RCA 长任务化（异步 job）均为 M3 候选，当前刻意不做（见排期"并行不占关键路径"与 ADR-014 留桩说明）；
- 自动挂簇（W10-6）一簇一事件：多指纹共簇仅首单持故障域，其余事件处置口径 = 人工 `MergeInto` 归并（L2 只提示不自动级联），口径详见排期 W10-6 注记与评测报告 §5。
