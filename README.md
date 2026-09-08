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
├── docs/adr/                # ADR-006：数据库部署 + 关键外部依赖清单（T1）
└── .github/workflows/ci.yml # CI：build/test + 边界检查 + Windows 冒烟
```

## M1 第 0 周四条并行任务状态：全部完成

| 任务 | 状态 | 落点 |
|---|---|---|
| T1 TimescaleDB 许可对照落档 | ✅ 完成（法务程序性确认待办） | `docs/adr/ADR-006-database-deployment.md` |
| T2 go-plugin Windows 冒烟 | ✅ **已通过**（windows/386，TCP 回环确认） | `tools/smoke/goplugin/`，结论见 ADR-006 |
| P1-1 会话状态落持久 Redis 实例 | ✅ 完成（启动期强制 + panic 守卫 + 单元测试） | `internal/config` + `internal/sessionstore` |
| P1-2 调用纪律守护 | ✅ 完成（静态检查三连验证 + 单元测试覆盖） | `scripts/check_module_boundaries.py` + `internal/transport` |

## 验证结果（2026-09-08，Go 1.27.1 windows/386）

```
go build ./...   → 通过（无输出）
go vet ./...     → 通过（无输出）
go test ./...    → ok opscopilot/internal/config / ok opscopilot/internal/sessionstore
模块边界检查      → 通过：无跨模块内部 import
go-plugin 冒烟    → PASS handshake + rpc call / PASS kill protocol / GO-PLUGIN windows/386 OK
骨架功能验证      → 双实例未配置 exit 1、同地址 exit 1、合法配置 exit 0（约定行为全部符合）
```

## 本机环境说明（重要）

当前 Go 为 **32 位（windows/386）**——安装的是 `go1.27.1.windows-386.msi`，装在 `C:\Program Files (x86)\Go`。纯 Go 代码编译运行正常（本次全部验证基于此），但建议后续换装 **windows-amd64** 版本（32 位在 CGO、部分依赖和性能上有局限）。

常用命令（Git Bash）：

```bash
export PATH="/c/Program Files (x86)/Go/bin:$PATH"   # 每次新开 shell 需执行
export GOPROXY=https://goproxy.cn,direct            # proxy.golang.org 不通，用国内镜像

# 全量校验
go build ./... && go vet ./... && go test ./...
python scripts/check_module_boundaries.py

# go-plugin 冒烟
cd tools/smoke/goplugin && go build -o bin/plugin.exe ./plugin && go run ./host
```

> 若想把 GOPROXY 固化：`go env -w GOPROXY=https://goproxy.cn,direct`（在原生终端执行，Git Bash 下可能因缺 %AppData% 报错）。

## 已知限制

- `internal/contracts` 目前只有占位文件，跨模块 gRPC 契约（protobuf）待 M1 主体开发时生成；
- 尚无真实 Redis 集成测试（sessionstore 的单测只覆盖构造期角色守卫，未覆盖读写路径）；
- CI 尚未在真实仓库运行过（.github/workflows/ci.yml 已就位，待推送 GitHub 验证）。
