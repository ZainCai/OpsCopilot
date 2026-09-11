# Git 收尾与 W1 阻塞清单 —— 已收尾归档（2026-09-09）

> 原文档（2026-09-08）列出的本机执行项与 W1 阻塞项，截至 2026-09-09 已全部解除。本文档转为**收尾归档**，不再指导执行；最新计划见 `M1执行计划.md`。

## 一、原"待本机执行"项 —— 全部完成
- **远端推送**：已 `git push -u origin main`；`main` 与 `origin/main` 同步（ahead 0），最新提交 `eef2b49`。
- **Docker Desktop**：已装并运行；`docker compose up` 起 3 容器（TimescaleDB + 双 Redis），均 healthy。
- **protoc**：已装；`scripts/gen.sh` 生成 `internal/contracts/pb/topology.pb.go`(603) / `topology_grpc.pb.go`(169)。
- **Go 架构**：已换装 **1.27.1 windows/amd64**（装于 `C:\Program Files\Go`），私有化分发目标形态达成；旧 32 位 `C:\Program Files (x86)\Go` 建议删除。
- **提交身份**：已 `git filter-branch` 改写全部提交为 `ZainCai <zaincai@outlook.com>`，全局 git config 已对齐。
- **Git**：系统级 2.55.0.3 已装；临时垫片 `~/.local/bin/git.cmd` 已删除。

## 二、原"W1 真实阻塞"项 —— 全部解除
| 阻塞项 | 原状态 | 现状态 |
|---|---|---|
| Docker Desktop | 未装 | ✅ 已装并验证（3 容器 up + 双 Redis PONG） |
| protoc | 未装 | ✅ 已装，契约已生成 |
| Go 架构 | 32 位 windows/386 | ✅ 已换 64 位 windows/amd64 |
| 远端推送 | 沙箱阻断 | ✅ 本机推送完成并同步 |

## 三、遗留 O 项收尾进度
- ✅ **本次已关闭（7 项）**：O4（CI Run #3 三平台全绿）、O9（transport 测试）、O12（README 刷新）、O13（契约生成）、O14（pkg 目录）、O15（专有 LICENSE）、O16（边界脚本自测）。
- ⏳ **仍挂起（4 项，后续周次）**：O6（最小资源规格，W7）、O7（DB owner + Patroni，W8）、O8（法务确认，W8 前并行）、O10（sessionstore 集成测试，W5）。

## 四、当前可开工的前置条件
W1 基座已全部就位：**Docker/Redis 起得来、Go amd64、Git 已装、远端已推送、契约已生成、LICENSE 已落**。M1 主体（连接器 → 存储 → 降噪）可直接开工。

## 五、已知限制（写入 README，非阻塞）
- `alert_event` 表已建但**未转 hypertable**，TimescaleDB 时序特性（压缩/降采样/连续聚合）当前未使用——W1-W8 若不需时序能力，可保留普通表，待降噪评估后决定是否转换。
- `sessionstore` 仅有 panic 守卫单测，**无真实 Redis 集成测试**（对应 O10，排队到 W5）。
- CI 3 个 job 全绿，但 **Node.js 20 弃用告警 ×3**（非致命，后续升 setup-node 到 22 可消）。
