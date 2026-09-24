# OpsCopilot v1.0.0 Release Notes — 2026-09-15

> 归档发布（4C）。对应 commit 基线：`2ba5ee2`（tag `v1.0.0`）；远端正分支 `main`。
> 本项目为专有软件，保留所有权利（见 `LICENSE`）。

## 发布摘要

OpsCopilot 智能运维副驾驶完成 M1/M2/M3 三线收口与全局精简，CI 首跑确认全绿，作为 **v1.0.0** 归档发布。影子降噪、enforce 转正、事件域主工作台、RCA 六步链路、Runbook、升级闸门、CI 真库门禁全部落地并经过真库/并发/走查验证。

## 里程碑与验证证据

| 里程碑 | 结论 | 证据 |
|---|---|---|
| M1 影子降噪转正评估 | 准确率 36/36 段 100% | `docs/M1出口验收报告-2026-09-11.md` |
| M2 全链路（W9~W11） | 出口放行，3 条件闭环 | `docs/M2出口评审纪要-2026-09-13.md` |
| M3（round10 P2 遗留 15 项） | 三阶段全部完成 | `docs/reviews/round10-P2遗留-M3清单.md`（闭环回填节） |
| 全局精简 | 行为不变 + 逐项清单 | commit `e5c8637`（web 死类）/ `7be4abd`（poll 合并） |
| CI 首跑确认 | run #87 四 job 全绿，真库 pgstore 实跑 | `8e8932e`；§3 下方"CI 首跑" |

## 功能面（运维接入入口）

- 事件双链路：Alertmanager push + Prometheus pull 导入，限流折叠 + 幂等收敛（`/api/v1/ingest/*`）
- 噪声治理：shadow → enforce 转正，Gate 持久化计数，sink 异步写 PG（drops 指标化）
- 事件域主工作台：事件列表（游标分页）、混合时间线（mem/PG 契约体）、SLA 时钟、运维 KPI、全局审计检索、通知渠道 CRUD（generic/feishu/wecom）、值班升级
- RCA：六步最小链路（ADR-014）+ llm-gateway conclude（ADR-015），评测集 top-1 4/4 = 100%
- 自动挂簇/建单：`OPS_AUTOATTACH`（enforce 下直写收敛同单）；`OPS_INCIDENT_AUTOCREATE` 默认 **on**（ADR-016 段三已转正，2026-09-24 拍板；on 须随附 `OPS_INGEST_BATCH=500`/`OPS_INGEST_INTERVAL=1s` 调优，off = 只排队不建单的影子期语义保留）
- 升级闸门：`opscopilot upgrade` 兼容 PK/golang-migrate 双形状 + 备份恢复演练（RTO/RPO 分钟级）
- 前端：web/ 独立工程（ADR-013），5 视口 × 6 路由走查自动化纳入 CI

## 质量门禁（v1.0.0 基线）

- 本机：`gofmt -l` 无输出 / `go vet ./...` / `go build ./...` / `go test ./... -count=1`（含真库 22 包）
- CI（GitHub Actions）：build-and-check（gofmt/vet/build/真库 Test/`-race`/模块边界）+ go-plugin 冒烟 ×2 + web-walkthrough，run #87 全绿
- 安全：P0=0（监听×写密钥 fail-fast、写端点 401 先行、CORS 默认同源、SQL 注入零发现）；依赖已升级（grpc v1.83.2 / x/net v0.59.0）
- 容量：单实例 ≥1000 events/s 起步（P99 62→25ms 为实测拐点）；决策延迟 P95 随活跃告警线性；ADR-016 灰度带参实测 ≈160 ev/s

## 灰度与观察项（发布时点）

- **ADR-016 段二观察中（发布时点记录）**：观察窗 2026-09-13 21:05 → 09-20；中期（09-15）四项判据全绿（attached=146 / conflict=0 / pending=0 / drops=0），决策延迟 3420 样本 P99≈0.03s 无零星慢轮
- **段三转正（2026-09-24 更新）**：段二到期评估四项判据全绿（误建单=0 / conflict=0 / pending 恒 0 / burst 未触发、drops 恒 0），拍板出厂默认 off → on，代码/测试/文档同步已完成（commit `dc7583b` / `24280e8`）；容量基线硬关口因本机无 Docker 以「引用容量基线 §2.2 + 灰度活体复证」替代，详见 ADR-016 段三执行记录
- 观察项记录在案：双实例共享 PG P95 劣化 4.6×、决策延迟 P99 零星慢轮（容量基线标注，多实例部署必查）

## 已知限制（详见 README「已知限制」）

读路径无鉴权（生产建议反代认证）、降噪队列队满丢弃取向（通知链路不受影响）、无 DB 时内存退化单实例、排班/多级升级/RCA 异步化不在本版范围。

## 部署起点

- `docker compose up`（TimescaleDB + 双 Redis + 应用）或二进制 + `migrate` 迁移；`upgrade` 双形状可用
- 环境键：69 个（`OPS_*` 67 + `REDIS_*` 2），唯一装载表 `docs/配置清单-OpsEnv.md`；`.env.example` 为运行时镜像
- 部署必查：`OPS_INGEST_BATCH/INTERVAL`（on 时）、双实例 `OPS_INGEST_LEASE_DURATION`、NTP 同钟（KPI 窗口）、反代认证（读路径）

## 文档索引

`docs/` 全量索引见 README「文档索引」；ADR 001~017 索引见 `docs/adr/README.md`。
