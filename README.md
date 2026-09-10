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

### 本地 compose 环境

`make env-up` 启动 TimescaleDB + 双 Redis + 应用。compose 已配置 `OPS_LISTEN_ADDR=0.0.0.0:8080`（容器隔离即安全边界）与 Redis healthcheck；应用容器为 distroless（无 shell），探活用 `GET /healthz` 由外部编排层负责。默认 DB 口令仅限本地开发（见 `.env.example` 注释）。

## 架构决策记录

`docs/adr/` 存 6 份 ADR（ADR-001 事件骨干 / 002 算子前置 / 003 LLM 单出口 / 004 Redis 双实例 / 005 审计分离 / 006 数据库部署 + 关键外部依赖清单）。

**任何 P0 修订或影响其他决策的变更，先写 ADR 再改文档**——这是 v1.3 §5.2 的硬性流程，源于 C17 部署策略与数据层特性冲突的事故。每份 ADR 末尾的"交叉检查提醒"记录耦合项。

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
