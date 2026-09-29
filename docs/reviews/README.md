# docs/reviews 归档约定

- **dated 证据归档策略（第十一轮 P3 定案）**：带日期后缀的证据目录/快照（`rca-eval-*`、`loadtest-*`、`web-walkthrough-*` 等）超过 **90 天**、或同一主题快照超过 **3 份**时，移入 `docs/history/`（保留最末 3 份在位供近线对照）；CI 产出类证据以 GitHub Actions artifact（保留 **14 天**）为权威副本，仓内快照只是落档便利，过期即可清。
