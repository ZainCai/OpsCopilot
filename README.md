# OpsCopilot（智能运维副驾驶）

多云中立、只读优先、按效果计费的 AI 运维副驾驶。M1 实施基线：架构方案 v1.3。

## 目录结构

```
opscopilot/
├── cmd/opscopilot/          # all-in-one 入口
├── internal/
│   ├── config/              # 双 Redis 实例配置与启动期强制（含单元测试）
│   ├── sessionstore/         # 会话状态存储：强制绑定持久化告警实例（含单元测试）
│   ├── transport/            # 进程内 gRPC（net.Pipe）（P1-2 调用纪律核心件）
│   └── contracts/           # 跨模块 gRPC 契约唯一存放处（禁止反向依赖）
├── pkg/                     # 通用库（禁止依赖 internal/）
├── scripts/
│   └── check_module_boundaries.py  # 模块边界静态检查（P1-2，已验证）
├── tools/smoke/goplugin/    # T2 冒烟：go-plugin Windows 可用性（已通过）
├── docs/adr/                # ADR-001~006 + 索引（见 docs/adr/README.md）
└── .github/workflows/ci.yml # CI：build/test + 边界检查 + Windows/Linux 冒烟
```

## M1 第 0 周四条并行任务状态：全部完成

| 任务 | 状态 | 落点 |
|---|---|---|
| T1 TimescaleDB 许可对照落档 | ✅ 完成（法务程序性确认待办，见 O8） | `docs/adr/ADR-006-database-deployment.md` |
| T2 go-plugin Windows 冒烟 | ✅ **已通过**（windows/amd64，TCP 回环确认） | `tools/smoke/goplugin/`，结论见 ADR-006 |
| P1-1 会话状态落持久 Redis 实例 | ✅ 完成（启动期强制 + panic 守卫 + 单元测试） | `internal/config` + `internal/sessionstore` |
| P1-2 调用纪律守护 | ✅ 完成（静态检查三连验证 + 单元测试覆盖） | `scripts/check_module_boundaries.py` + `internal/transport` |

## W1 环境底座：已完成并本机验证

| 项 | 状态 | 说明 |
|---|---|---|
| 本地编排 `docker-compose.yaml` | ✅ | TimescaleDB 社区版 + Redis 双实例（AOF/noeviction 与 LRU 按 ADR-004 区分）+ app |
| 数据库 schema `migrations/000001` | ✅ | 时态拓扑（valid_from/valid_to + as_of 索引）、变更记录、告警簇（幂等键 + 直通标记）、告警事件、审计索引 |
| 跨模块契约 `internal/contracts` | ✅ | `topology.proto` + 生成的 `pb/topology.pb.go`（603 行）/ `pb/topology_grpc.pb.go`（169 行），`scripts/gen.sh` 含中文路径兜底 |
| `Dockerfile` / `.env.example` / `Makefile` | ✅ | 多阶段 distroless；迁移统一走 golang-migrate（ADR-006） |
| 本机验证 | ✅ | `docker compose up` 起 3 容器；`migrate up` 建 7 表 + timescaledb 扩展（alert_event 同步转 hypertable，见迁移 000003）；双 Redis `PONG` |

> W1 **产品功能尚未开工**：连接器（Prometheus + 阿里/AWS）、语义模型 v1（as_of）、降噪影子模式、告警簇 API + 控制台均未写。M1 出口标准（影子降噪准确率 >85%）远未达。

## 验证结果（2026-09-08，Go 1.27.1 windows/amd64）

```
go build ./...   → 通过（无输出）
go vet ./...     → 通过（无输出）
go test ./...    → ok opscopilot/internal/config / ok opscopilot/internal/sessionstore / ok opscopilot/internal/transport
模块边界检查      → 通过：无跨模块内部 import
go-plugin 冒烟    → PASS handshake + rpc call / PASS kill protocol / GO-PLUGIN windows/amd64 OK
骨架功能验证      → 双实例未配置 exit 1、同地址 exit 1、合法配置 exit 0（约定行为全部符合）
```

## 本机环境说明（重要）

当前 Go 为 **64 位（windows/amd64）**——已重装 `go1.27.1.windows-amd64`，装在 `C:\Program Files\Go`，与私有化分发目标一致。旧的 32 位残留 `C:\Program Files (x86)\Go` 建议删除，避免 `go` 命令混淆。

常用命令（Git Bash）：

```bash
export PATH="/c/Program Files/Go/bin:$PATH"       # 每次新开 shell 需执行
export GOPROXY=https://goproxy.cn,direct          # proxy.golang.org 不通，用国内镜像

# 全量校验
go build ./... && go vet ./... && go test ./...
python scripts/check_module_boundaries.py

# go-plugin 冒烟
cd tools/smoke/goplugin && go build -o bin/plugin.exe ./plugin && go run ./host

# 本地数据库迁移（需先 cp .env.example .env）
migrate -path migrations -database "postgres://opscopilot:opscopilot@localhost:5432/opscopilot?sslmode=disable" up
```

> 若想把 GOPROXY 固化：`go env -w GOPROXY=https://goproxy.cn,direct`（在原生终端执行，Git Bash 下可能因缺 %AppData% 报错）。
> `make` 在 Git Bash 缺省未装，`make migrate` 改用上条等价的直接 `migrate` 命令即可（golang-migrate CLI 须以 `go install -tags 'postgres'` 安装，否则报 `unknown driver postgres`）。

## 运维接入

### 环境变量

| 变量 | 默认值 | 说明 |
|---|---|---|
| `REDIS_ALERT_ADDR` | 无（必填） | 持久化告警实例地址（ADR-004，缺一拒绝启动） |
| `REDIS_CACHE_ADDR` | 无（必填） | 缓存实例地址，必须与 alert 物理分离 |
| `OPS_LISTEN_ADDR` | `127.0.0.1:8080` | HTTP 监听地址；**默认只绑回环**（S1），容器/对外部署需显式 `0.0.0.0:8080`（compose 已配） |
| `OPS_WEBHOOK_TOKEN` | 空（无鉴权） | 变更 webhook 共享密钥；非回环暴露前必须设置 |
| `OPS_ALLOW_UNAUTHENTICATED` | `off` | **D1 门禁豁免**：非回环监听且无 token 时启动失败；仅限本机联调显式开启 |
| `OPS_CORS_ORIGIN` | 空（仅同源） | **D2**：跨源放行白名单；`*` 与非法形态启动失败；`null` 供 file:// 调试 |
| `OPS_TENANT` | `default` | 租户 ID（事件/簇/审计/队列共用）；评估环境用独立租户隔离数据 |
| `OPS_DB_MAX_CONNS` | `16` | **D7**：事件 Store + 导入队列 + 审计共池上限 |
| `OPS_INCIDENT_RETENTION` | `90d` | **D8**：resolved 事件归档保留期（`90d`/`2160h`/`off`）；归档含事件+簇+审计 |
| `OPS_NOISE_WINDOW` | `10m` | 影子降噪去重/聚类窗口；**评估时须 < 静默段 300s**（如 `4m`），否则打分失真 |
| `OPS_NOISE_MODE` | `shadow` | **ADR-011 转正模式**：`shadow` 只标注不拦截（行为同 M1）；`enforce` 噪声判决真拦截 + 放行项发通知；非法值启动失败。回退 = 改回 shadow 重启 |

其余：`OPS_NOISE_SHADOW`、`OPS_INCIDENT_AUTOCREATE`、`OPS_PULL_ALERTS`、`OPS_PULL_INTERVAL`、`OPS_PROM_URL`、`OPS_TOPOLOGY_EDGES` 等见 `.env.example` 注释。

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
opscopilot_noise_sink_queue                                          判决异步落库队列水位（条数）
opscopilot_noise_sink_drops_total{store="pg"|"redis"|"other"}        队列侧丢弃的持久化任务（队满/停机/drain 超时——宁漏库存不丢通知）
opscopilot_noise_write_dropped_total                                 到达 DB 但写失败丢弃的判决数（镜像引擎计数）
```

- **`_p50/_p95/_p99` 是本项目的扩展**：Prometheus 官方直方图只能给桶内插值近似分位，而验收要拿 P95 当证据——所以直方图额外保留 10000 个滑窗原始样本，分位按**最近秩法**在真实观测值上精确计算（结果必是实测值，不插值）。无样本时渲染为 `NaN`。
- `alert_fired_at` 取上游的告警发射时刻：Alertmanager 用 `startsAt`，Prometheus `/api/v1/alerts` 用 `activeAt`。**缺失时不打点**（用合成时间打点等于自欺），该情形记进 `opscopilot_alert_latency_skipped_total{stage}`。
- **对账恒等式**：`alerts_processed_total − skipped{stage="verdict"} == fired_to_verdict_seconds_count`
  （#8 后打点在判决**生成**、入队之前，与落库结果无关——队列丢弃/写失败不再从该式扣减；
  落库侧账目独立：`入队数 = 实际落库 + write_dropped`，`未入队数 = sink_drops`）。
  notify 侧同理，但分母只含**被放行**的判决（被拦截的不发通知，自然无样本），且不扣 ErrNoChannel 一类的投递异常。若抓取瞬间读到不相等，多半是**快照非原子**——`alerts_processed_total` 在引擎锁内累加，而延迟观测发生在锁外的入队路径上；下一次抓取即一致。
- **判决落库的一致性边界**（队满丢弃取向，见 `OPS_NOISE_SINK_*`）：落库尽力而为——
  极端洪峰/慢 DB 下 PG 判决流可缺条目，Redis/PG 镜像与内存态判决**非强一致**；
  丢失面唯一出口是 `opscopilot_noise_sink_drops_total{store}` + ERROR 日志。
  通知链路（降噪 → Gate.Admit → 渠道送达）全程不读判决存储、不受落库拖累。
- 该差值含"源侧告警年龄"——告警真实发射到我们下一次采集之间的等待，量级由采集节拍决定（采集 30s → 上界 ≈30s）。要隔离 opscopilot 自身的处理耗时，看 `sum/count` 与源侧 `activeAt` 的差值，或在注入器侧把 `activeAt` 设为请求时刻（本项目 `tools/faultinjector` 即如此）。

```bash
curl -s http://127.0.0.1:8080/metrics | grep -E 'fired_to_(verdict|notify)_seconds_(count|p95)'
```

### 本地 compose 环境

`make env-up` 启动 TimescaleDB + 双 Redis + 应用。compose 已配置 `OPS_LISTEN_ADDR=0.0.0.0:8080`（容器隔离即安全边界）与 Redis healthcheck；应用容器为 distroless（无 shell），探活用 `GET /healthz` 由外部编排层负责。默认 DB 口令仅限本地开发（见 `.env.example` 注释）。

## 架构决策记录

`docs/adr/` 存 ADR 全集（001 事件骨干 / 002 算子前置 / 003 LLM 单出口 / 004 Redis 双实例 / 005 审计分离 / 006 数据库部署 / 007 拓扑置信度 / 008 外部事件代 / 009 监听门禁 / 010 工单归档 / 011 enforce 转正 / 012 单 owner 水平扩展 / 013 前端独立工程 / 014 RCA 最小链路；完整索引见 `docs/adr/README.md`）。

**任何 P0 修订或影响其他决策的变更，先写 ADR 再改文档**——这是 v1.3 §5.2 的硬性流程，源于 C17 部署策略与数据层特性冲突的事故。每份 ADR 末尾的"交叉检查提醒"记录耦合项。

## 预留未接线的组件（D9 决策 A：保留不删）

以下 `internal/` 包**带完整测试但尚未被 `cmd` 装配**，是 M2 的直接候选件——不要误读为"已生效"，也不要顺手删除：

| 包 | 行数 | 用途 | 计划接线阶段 |
|---|---|---|---|
| `internal/rca` | ~560 | 根因分析六步流水线（**优化方案 #12 最小链路已接线（ADR-014）**：取证/假设/验证/归因/建议为规则+证据版，REST `GET /api/v1/incidents/{id}/rca`；剩余：conclude 步接 llm-gateway 单出口、Escalation 自动触发二期） | 已接线（最小链路） |
| `internal/notify` | 164 | 通知闸门/渠道（**Gate 随 W9-1 enforce 接线、渠道随 W9-2、严重级路由随 W9-3**；剩余：值班排班 M3） | 已接线 |
| `internal/sessionstore` | 61 | 会话状态（Redis 会话存储） | W5+ |

历史审核报告在 `docs/reviews/`（原堆在仓库根，D11 决策 A 归档）；M2 候选清单与双链路方案在 `docs/`。


## 版本状态

- 远端：`https://github.com/ZainCai/OpsCopilot.git`，分支 `main` 已与 `origin/main` 同步（ahead 0）；
- 提交身份：`ZainCai <zaincai@outlook.com>`（已通过 `git filter-branch` 将历史 4 个提交改写对齐）；
- 历史提交：`0549778`(M1 第0周骨架) → `26fb640`/`45170d6`(ADR 索引) → `0e91c1b`(W1 环境准备) → `4dbdc39`(契约生成) → `b6e1a78`(编排修复：移除 initdb.d 双写)；
- 授权：专有软件，保留所有权利（详见仓库 `LICENSE` 文件）。

## 已知限制

- 跨模块 gRPC 契约已生成：`internal/contracts/proto/topology.proto` + 生成的 `pb/topology.pb.go` / `pb/topology_grpc.pb.go`；更多服务契约随 W1 主体开发补充；
- Redis 侧集成测试已就位（W5-2.4，O10 关闭）：sessionstore 读写路径经 miniredis 真实协议锁定（TTL 刷新/过期/销毁/键前缀隔离）；ClusterRecord 镜像同机制。sessionstore 接线 main 留待首个会话消费方（控制台登录等）出现时做——不做无人调用的接线；TimescaleDB 侧集成测试待 W5 pgx 引入后补。
- `alert_event` 已通过 `migrations/000003` 转为 TimescaleDB hypertable（1 天 chunk，7 天后压缩按 `tenant_id,fingerprint` 分段，30 天后自动保留删除）；W4 接 alert ingest 之前完成，避免生产数据量起来后转 hypertable 的写入停摆窗口；
- `change_record` 通过 `migrations/000002` 与内存 `ChangeEvent` 对齐：`event_id TEXT` 承载幂等键（DB 层 UNIQUE 防重复）+ `change_type` CHECK 与 `ChangeType` 封闭集合一致；W4 接 DB 时按 `change.go` 文件头注释实现列映射；
- CI 已随 `git push` 在 GitHub Actions 运行：`build-and-check`（build/test/边界检查）+ `goplugin-smoke-windows` + `goplugin-smoke-linux`（T2 双平台冒烟）；
- W1 主体功能（连接器 / 语义模型 / 降噪 / API）尚未开工，M1 出口标准（影子降噪准确率 >85%）未达；
- main 启动日志已明确当前 W3 阶段（"wired: topology + change webhook；not wired: Host.Run、gRPC SemanticModelServer、sessionstore、/metrics、alert pipeline"）——**不要把"骨架就绪"误读为"产品就绪"**（N1 修复）。
