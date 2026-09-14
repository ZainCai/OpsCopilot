# round10 P2 遗留 · M3 待办清单 — 2026-09-13

- 来源：`docs/reviews/全局代码审核报告-第十轮-2026-09-13.md`（P2 共 17 项，全部不阻塞出口）
- 本轮（M2 出口收口包）已顺手修掉 **2 项**：P2-D4（未跟踪存量库误拒启——随 P1-2 一并落地）、P2-D6（README/配置清单"68→69"两处统计行 + 排期 W10-2/W10-3 回填 + W11 六行收口 + 进度行刷新）
- **遗留 15 项**按下表进 M3 池；一行一项，带文件指针。审核报告"建议至少顺手修"中的 heldConn 锁外 Acquire、run_rca_eval trap 前移、redactDSN keyword 分支本轮**未修**（与 P1 无关，按出口收口范围纪律留 M3）。

| # | 原编号 | 项 | 文件指针 | 建议处置 |
|---|---|---|---|---|
| 1 | P2-A1 | `heldConn` 持 `l.mu` 期间执行 `pool.Acquire`（可阻塞至 max(retry,2s)），`IsLeader()`/`Stop()` 被串行化——锁内 IO 残留孤点 | `cmd/opscopilot/leader.go:277-295`（消费方 `assembly.go:278` gauge） | Acquire 移锁外，锁内只做钉住/换出 |
| 2 | P2-A2 | `persistedSig` check-then-act 正确性依赖"ProcessAlerts 串行调用"——注释声明但未强制（`noise.go:49-50` 甚至称不依赖宿主串行） | `cmd/opscopilot/noise_process.go:127,369-423`、`internal/noise/noise.go:49-50` | 前提升格为断言/硬契约，或后半程入锁 |
| 3 | P2-A3 | 双保险调 `StopVerdictWriter`：首次超时后二次 `stop()` 重复计入 sink_drops（保守上界不丢通知，注释自认） | `cmd/opscopilot/noise_writequeue.go:186-202`（+ `main.go`/`Assembly.Close` 两调用点） | 去重计数或在指标语义文档明示"上界口径" |
| 4 | P2-A4 | 租约 vs 慢批：默认 batch=20 × perItem≈15s ≈5min > lease=2min，慢批合法越租 → 他实例重领做重复劳动（结果幂等无害） | `internal/config/config.go:498-503`、`internal/noise/ingest_queue.go:170-224` | lease ≥ 批最坏时长，或处理中续租 |
| 5 | P2-A5 | 并发用例缺口：无"N 路并发 ProcessAlerts + 并发 StopVerdictWriter/SetVerdictSink(nil)"、无 memguard"淘汰进行中并发 gauge 求值"、无 leader heldConn 并发用例 | `cmd/opscopilot/leader_test.go`、`pkg/memguard`、`internal/noise` | M3 补针对性并发用例（与 CI race 兜底互补） |
| 6 | P2-B1 | verdict writer 生命周期只绑显式 `Stop()` 不挂 ctx——绕过 `Assembly.Close` 的嵌入式用法会泄 writer（现两条路径均已调 Stop） | `cmd/opscopilot/noise_writequeue.go:205/285` | 文件头注释补"Stop 是唯一退出点" |
| 7 | P2-B2 | `rca_auto` pending 有界但 `seen` map 无淘汰（上界=进程生命周期 critical 事件数，量级小） | `cmd/opscopilot/rca_auto.go:62/106` | 长值守卫对齐 memguard 做法 |
| 8 | P2-C1 | `run_rca_eval.sh` 含明文 key 的 env 文件创建后、`trap stop_all EXIT` 注册前存在失败路径残留（文件已被 gitignore，故 P2） | `scripts/run_rca_eval.sh:83/135` | 文件创建处即挂 `trap 'rm -f …' EXIT` |
| 9 | P2-C2 | `redactDSN` 只按 `@` 打码 URL 形态；keyword 形态 DSN（`password=…`）可原样进连接失败错误 → 启动 WARNING 日志 | `cmd/opscopilot/schema_gate.go`（redactDSN） | 补 `password=` 分支打码 |
| 10 | P2-C3 | upgrade 逻辑备份值字面量仅双写单引号，依赖会话 `standard_conforming_strings=on` | `cmd/opscopilot/upgrade_backup.go`（sqlTextLiteral/backupBeforeUpgrade） | 备份文件头注入 `SET standard_conforming_strings = on;` |
| 11 | P2-C4 | 信息项：runbook 写面不入 incident_audit 哈希链、session 正文不入审计、`actor/created_by` body 自报可冒充（均有明示注释/拍板） | `cmd/opscopilot/rest_runbook.go:60-92`、`rest_rca_session.go:70-80`、`docs/设计-sessionstore消费方与接线.md` | 二期真身份落地时收口为一条 ADR |
| 12 | P2-D1 | tools CLI 以字面量散读 OPS_DB_DSN/OPS_TENANT/OPS_WEBHOOK_TOKEN 等，绕过 `internal/config` 键名常量——改名静默失联 | `tools/verdicts/main.go:45-48`、`tools/loadtest/main.go:168`、`tools/rca_eval/main.go:299-303,1200` | 引 `config.Env*` 常量或纪律文档明列 CLI 豁免 |
| 13 | P2-D2 | 边界脚本盲区：`cmd/`、`tools/`、`scripts/migrate` 不在扫描面；规则 4 出口模块名单硬编码不随新包自扩；"cmd 禁散读 OPS_*"无 Go 侧源码测试钉 | `scripts/check_module_boundaries.py`（对照 llmgw/connector 既有同构测试） | 扩扫描面 + 名单自扩 + 补 cmd 散读钉测试 |
| 14 | P2-D3 | W10-1 时间线是唯一"双份单侧测"（mem 侧与 PG 侧各测各）——partial 降级/同刻序两实现可漂移 | `cmd/opscopilot/rest_timeline.go`、`rest_timeline_test.go`、`rest_timeline_e2e_pg_test.go` | 收敛为共用契约体（对齐 SLA/KPI 契约打法） |
| 15 | P2-D5 | 三条并列：a) PATCH `sla_minutes=0` 语义 keep-current → REST 面无法清除覆盖回按级默认；b) transition+SetSLA 两步非原子且审计先行，SetSLA 失败呈半成功态；c) KPI `since` 用 Go 进程钟而 `created_at` 用 DB now()，时钟偏移即窗口漂移（部署需 NTP 同钟） | a/b：`cmd/opscopilot/rest_incidents.go:212,229-246`；c：`cmd/opscopilot/rest_kpi.go:46-47` | a) REST 增显式 clear 语义；b) 合并事务或补审计字段；c) 文档注明同钟要求/改 DB 钟 |

> 编号沿用 round10 报告原名，便于回读上下文；"已修 2 项"（D4/D6）的证据见
> `docs/M2出口评审纪要-2026-09-13.md` §一 与本轮 fix(db)/docs 提交。
