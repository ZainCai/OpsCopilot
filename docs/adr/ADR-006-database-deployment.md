# ADR-006：数据库部署策略与关键外部依赖清单

- 状态：已接受（法务确认收尾中）
- 日期：2026-09-08
- 决策人：架构评审（v1.3 基线）
- 关联：ADR-005（审计分离）、v1.3 C19、二轮评审 P0-3

## 背景

v1.2 C17 曾定"首选云托管 PG/Timescale（RDS PG 主备）"。核实发现：云厂商 RDS/Azure PG 仅提供 TimescaleDB **Apache-2 版**（仅含 hypertable 基础分区），不含社区版（TSL）的连续聚合、保留策略、压缩——而数据层"三级降采样（1m/5m/1h）"与"冷数据分层"完全依赖这些特性。原决策与数据层设计冲突（详见 v1.3 第 1 章根因分析）。

## 决策

**SaaS：自建 TimescaleDB 社区版（TSL）+ Patroni + etcd 流复制主备（1 主 1 备）。**

### 合规结论（依据官方许可文档，2026-08 核实）

| 场景 | TSL 条款 | 结论 |
|---|---|---|
| SaaS 运行 TimescaleDB 提供增值服务 | 明确允许 Value-Added Service/Product | ✅ 本产品（AI 运维副驾驶）合规 |
| 单机私有化版随 Docker Compose 分发未修改二进制 | 允许 distribute unmodified Binaries as part of Value Added Product | ✅ 合规，禁止修改后分发 |
| 修改 TimescaleDB 源码并对外提供 | 禁止 distribute modified Source/Binaries（除非属 VAP 的二进制） | ⚠️ 约束：仅允许内部修改（right to repair），本产品无此需求 |

**法务确认：待办**（仅程序性复核上表，无开放性风险预判）。

### 降级链（按顺序触发）

1. **首选**：自建 TSL 社区版 + Patroni 主备（本决策）；
2. **备选 A**：托管 RDS/Azure PG（Apache-2 版）——触发条件：团队 <3 人且无 PG 运维经验（C20 阈值）；代价：失去连续聚合/保留策略/压缩，需 PG 原生分区 + cron 任务替代；
3. **备选 B**：Tiger Cloud 托管——触发条件：自建运维成本超阈值且需保留社区版特性；代价：数据入第三方、成本最高。

## 关键外部依赖清单（本次 P0 的结构性根因修复）

> 任何选型/部署决策变更，必须先过这张表。

| 组件 | 版本/许可 | 我们依赖的特性 | 特性所在版本 | 部署约束 |
|---|---|---|---|---|
| TimescaleDB | 社区版 TSL（当前稳定 2.29.x） | 连续聚合（三级降采样）、保留策略（冷分层）、压缩 | 仅 TSL 社区版 | RDS/Azure PG 不可用；Patroni + etcd 主备 |
| PostgreSQL | 16/17（TimescaleDB 2.29 已移除 PG15 支持） | pgBackRest 备份、RLS | 全版本 | 降级路径的最终兜底 |
| Redis | ≥7，双实例 | Streams + 消费者组（告警）、AOF everysec、noeviction | 全版本 | 告警实例与缓存实例物理隔离 |
| go-plugin (HashiCorp) | ≥1.6 | 子进程隔离、RPC 握手 | 全版本 | **T2 冒烟已通过（2026-09-08，windows/386）**：握手 + RPC 调用 + kill 协议三项 PASS；日志确认 Windows 走 `network=tcp`（127.0.0.1 回环）而非 Unix socket，与预判一致 |
| golang-migrate | 最新 | 版本化迁移 + 跨版本拒启 | 全版本 | 无 |
| Patroni | ≥4 | 自动故障转移 | 全版本 | 需 etcd；季度演练（C20） |

## T2 冒烟结论（2026-09-08 执行）

环境：Go 1.27.1 windows/386（32 位，见 README 环境说明）。命令：`go build -o bin/plugin.exe ./plugin && go run ./host`。

```
PASS handshake + rpc call on windows/386
PASS kill protocol (clean plugin exit)
SMOKE RESULT: GO-PLUGIN windows/386 OK
```

- 插件地址日志 `network=tcp, address=127.0.0.1:10000` 证实 Windows 走 TCP 回环，符合预判；
- **结论：私有化单机版不必限制为 Linux**，子进程插件模式在 Windows 可用；P1-3 的"编译内置"降级路线转为可选优化而非必选。

## 后果

- 正面：保住三大成本控制特性；99.95% 告警链路目标有自建高可用支撑；
- 负面：新增 Patroni/etcd 运维面，需指定数据库 owner + 季度联合演练（C20）；
- 撤销条件：触发降级链任一条件时，重新决策并更新本 ADR。
