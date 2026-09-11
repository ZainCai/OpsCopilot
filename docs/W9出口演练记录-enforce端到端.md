# W9 出口演练记录 —— enforce 模式端到端（降噪拦截 → 建单 → 通知送达）

> M2 排期 W9 出口标准：**"enforce 模式下端到端演练通过（注入器 4 场景 → 降噪
> 拦截 → 建单 → 通知送达）"**；对应里程碑 M2-M1（转正上线，通知闭环端到端通过）。
> 本文件是该演练的配置、证据与结论。

- 演练日期：2026-09-11
- 被测版本：`main` @ W9-5 后（`892a7cf` + `e4c15f4` + `6de037b`）
- 结论：**出口标准达成**（4 项证据全绿，评估 9/9 段 = 100%）

---

## 1. 为什么需要这次演练

W9-1~W9-5 各自的验收都在自己的交付里验证过，但它们拼起来才是 W9 的出口：
此前没有任何一次运行**同时**满足"降噪真拦截 + 外部告警真建单 + 通知真送达"。
W9-4 的延迟实测那次 `OPS_INCIDENT_AUTOCREATE=off`（判决照常、通知照常，但
**没有建单**）；W9-2/W9-3 的通知验证用的是装配级测试或人工建单。所以出口
演练必须把三个开关同时打开跑一遍真实链路。

## 2. 演练配置

| 项 | 值 | 说明 |
|---|---|---|
| 数据源 | `tools/faultinjector -scale 0.2 -warmup 10` | 4 场景剧本（A 磁盘 / C 静默 / B 交换机 / C / D 噪声 / C），每段 60s，周期 360s |
| 降噪 | `OPS_NOISE_MODE=enforce`，`OPS_NOISE_WINDOW=45s` | 45s 介于采集节拍（30s）与静默段（60s）之间：段内重复去重、段间簇正常 resolve（评估有效性前提） |
| 拓扑 | `OPS_PROM_URL` + `OPS_TOPOLOGY_EDGES=n1->n2,n2->n3` | 发现建节点，静态边建因果链——**B 场景 3 条告警必须靠这条边才能并成一簇**（连接器只产节点不产边） |
| 建单 | `OPS_PULL_ALERTS=on`（10s）+ `OPS_INCIDENT_AUTOCREATE=on` | 拉取侧与降噪链在采集处汇流前**分叉**：降噪走连接器链，建单走队列→worker 链（设计如此，见 `pull_alerts.go` 文件头） |
| 通知 | console 兜底 + **真实 HTTP 假渠道**（`kind=generic`，`min_severity=info`） | 假渠道是独立 HTTP 服务，逐条落盘投递载荷——"送达"不再是推断 |
| 租户 | `OPS_TENANT=w9drill` | 与历史评估数据完全隔离 |
| 持续 | ~14 分钟（1.3 个完整剧本周期） | 每 30s 一次 `/metrics` 快照可对账 |

## 3. 四段证据

### ① 注入器 4 场景 → 降噪拦截

```
opscopilot_alerts_processed_total 21
opscopilot_noise_verdicts_total{reason="new-incident"} 6
opscopilot_noise_verdicts_total{reason="cluster-merge"} 6
opscopilot_noise_verdicts_total{reason="dedup-window"} 9
opscopilot_noise_gate_dispatched_total 6     ← 放行
opscopilot_noise_gate_suppressed_total 15    ← 真拦截（6+15=21 ✓）
```

**21 条告警里 15 条被拦截（71%）**，只有 6 条新事件穿透到通知——降噪的价值
第一次以"真拦截"而不是"影子标注"呈现。

### ② 建单（外部自动导入 → 工单）

```
incident created from prometheus: prometheus:prom:c321d299... (HighDiskUsage)
incident created from prometheus: prometheus:prom:f55298e6... (InodeExhaustion)
...
GET /api/v1/incidents?origin=prometheus → count=7  stats={active:7, external:7}
```

7 条工单对应 7 个独立指纹（A 段 2 + B 段 3 + D 段 2），且**按
`(tenant, origin, source_ref)` 幂等**——同指纹重复推送只刷新不重开单
（第二个周期重复出现时 count 仍是 7）。

### ③ 通知送达（双渠道）

```
console 渠道:      6 条  [notify/console] [warning|critical] <title> — 新事件簇 …
HTTP 假渠道:       6 条  deliveries.jsonl
{"body":"新事件簇 c:fp-a-disk@… （降噪后首个告警）需要人工关注",
 "cluster_key":"c:fp-a-disk@39758666","severity":"warning",
 "source":"opscopilot","tenant_id":"w9drill","title":"HighDiskUsage"}
```

**放行 6 = console 6 = HTTP 渠道 6**，逐条对应；载荷含租户/簇键/严重级，
`min_severity=info` 路由生效（critical 与 warning 都送达）。

### ④ 延迟与自证（W9-4 打点在真实三链路并跑下仍然成立）

```
alert_fired_to_verdict_seconds  count=21  p50=0.346s  p95=0.477s
alert_fired_to_notify_seconds   count=6   p50=0.400s  p95=0.480s
alert_latency_skipped_total     0 / 0
```

恒等式在整场演练成立：`processed 21 == verdict 观测 21 + skipped 0`；
`notify 观测 6 == gate_dispatched 6`。**P95 0.48s，远低于 30s 红线**——
且这次是"降噪 + 建单 + 通知"三链路同时跑的数字，比 W9-4 单链路更接近真实负载。

## 4. 降噪准确率（W9-1 验收的复核）

W9-1 的验收是"**切 enforce 后注入器评估仍 100%，且拦截决策可回放**"。
本次演练即在 enforce 模式下运行，判决照常落库（`alert_event.source='shadow'`
两种模式都写，这是 ADR-011 定下的"enforce 首周与影子基线对比"的数据基础），
因此可以直接用 W6-3 的评估工具打分：

```
python tools/evaluate.py --answerbook http://127.0.0.1:19190/answerbook \
    --verdicts-file verdicts-w9drill.json        # tools/verdicts 导出，24 条

[PASS] A 09-11 23:19 verdicts=4 new=1 (期望 new=1)
[PASS] C 静默 09-11 23:20 无判决
[PASS] B 09-11 23:21 verdicts=6 new=1 (期望 new=1)
[PASS] C 静默 09-11 23:22 无判决
[PASS] D 09-11 23:23 verdicts=4 new=2 (期望 new=2)
[PASS] C 静默 09-11 23:24 无判决
[PASS] A 09-11 23:25 verdicts=4 new=1 (期望 new=1)
[PASS] C 静默 09-11 23:26 无判决
[PASS] B 09-11 23:27 verdicts=6 new=1 (期望 new=1)
## 结果: 9/9 段达标 = 100.0%
```

- **9/9 段 PASS = 100%**：A=1、B=1、D=2、C=0，与答案簿逐段一致；
- "拦截决策可回放"：判决行含 `cluster_key/fingerprint/reason`（JSONB payload），
  `tools/verdicts` 一条命令即可导出重放——本次评估就是一次回放。

## 5. 演练中确认的两个边界

1. **降噪链与建单链是两条独立的链**（设计如此）：降噪吃连接器采集链，
   建单吃队列→worker 链，两者在采集处分叉。因此"簇合并成 1 个新事件"
   与"建了 2 条工单"（A 段 2 个指纹）**并不矛盾**——前者是降噪视图，
   后者是工单视图。若要求"一簇一单"，那是 W10-4（autoCreate 转正决策）
   要拍板的事，不是本演练的缺陷。
2. **B 场景的并簇依赖静态拓扑边**：连接器只产节点不产边，没有
   `OPS_TOPOLOGY_EDGES=n1->n2,n2->n3` 时 B 段 3 条告警会各自成簇
   （评估会 FAIL）。评估环境必须带这条边；生产环境由云 API 发现补齐。

## 6. 结论

| W9 出口要求 | 证据 | 结论 |
|---|---|---|
| 注入器 4 场景 | A/C/B/C/D/C 完整播放 1.3 周期，评估 9 段 | ✅ |
| 降噪拦截 | 21 告警 → 15 拦截（dedup 9 + merge 6） | ✅ |
| 建单 | 7 条 `origin=prometheus` 工单，指纹幂等 | ✅ |
| 通知送达 | 6 放行 × 2 渠道，HTTP 渠道逐条落盘 | ✅ |
| （附带）W9-1 验收复核 | enforce 下评估 9/9 段 = 100%，判决可回放 | ✅ |
| （附带）M1 标准 3 复核 | verdict P95 0.477s / notify P95 0.480s | ✅ |

**M2-M1 里程碑（转正上线，通知闭环端到端通过）达成。**
