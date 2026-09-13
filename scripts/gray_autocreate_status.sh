#!/usr/bin/env bash
# gray_autocreate_status.sh —— ADR-016 灰度段一·一次性观察读数（人跑，绝不自启）。
#
# 配合 run_gray_autocreate.sh 使用：本脚本只读数、不拉起任何进程、不留任何
# 循环采集器（孤儿采样进程教训）。输出带时间戳，可整段粘进灰度观察记录。
#
# 用法：bash scripts/gray_autocreate_status.sh
#   可覆盖：GRAY_OPS_URL（默认 http://127.0.0.1:8090）/ GRAY_TENANT（默认 gray01）
#           GRAY_DB_DSN（默认开发 compose TimescaleDB）
#
# 读数口径：
#   /metrics：autoattach_total 三态（attached|conflict|skipped，段一硬验收 attached≥1）、
#             判决计数、sink 队列水位与 drops、opscopilot_is_leader；
#   PG：灰度租户 incident 新建数 / attach_cluster 审计行数 / 近 24h 小时分布 /
#       ingest_queue 两级水位（pending=入队侧积压）/ 限流折叠（rate_limited、burst:*）。
#   （ingest 队列水位在 DB 不在 /metrics——链路 A 队列本就落 ingest_queue 表。）
#
# PG 侧读数经一次性 go run 助手（源码生成于 .gray/pgstatus/，gitignored，
# 仓库 Go 侧零改动）。
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="$ROOT/.gray"
OPS_URL="${GRAY_OPS_URL:-http://127.0.0.1:8090}"
TENANT="${GRAY_TENANT:-gray01}"
DB_DSN="${GRAY_DB_DSN:-postgres://opscopilot:opscopilot@127.0.0.1:5432/opscopilot?sslmode=disable}"

echo "=== ADR-016 灰度段一读数 | $(date '+%F %T %z') | 实例 $OPS_URL 租户 $TENANT ==="

M="$(curl -s --noproxy '*' --max-time 5 "$OPS_URL/metrics" || true)"
if [ -z "$M" ]; then
  echo "[/metrics] 不可达（$OPS_URL）——实例没在跑？bash scripts/run_gray_autocreate.sh status"
else
  gm() { awk -v pat="$2" '$0 ~ pat {v=$NF} END {print (v==""?"-":v)}' <<< "$M"; }
  echo "[/metrics]"
  echo "  autoattach attached=$(gm _ 'outcome="attached"') conflict=$(gm _ 'outcome="conflict"') skipped=$(gm _ 'outcome="skipped"')   # 硬验收 attached>=1"
  echo "  leader            =$(gm _ '^opscopilot_is_leader')"
  echo "  alerts_processed  =$(gm _ '^opscopilot_alerts_processed_total')"
  echo "  verdicts          = new-incident $(gm _ 'reason="new-incident"') | dedup $(gm _ 'reason="dedup-window"') | cluster-merge $(gm _ 'reason="cluster-merge"')"
  drops=$(awk '/^opscopilot_noise_sink_drops_total/{s+=$NF} END {print s+0}' <<< "$M")
  echo "  sink_drops(全store合计)=$drops  sink_queue 水位=$(gm _ '^opscopilot_noise_sink_queue')  write_dropped=$(gm _ '^opscopilot_noise_write_dropped_total')"
fi

echo "[API] GET /api/v1/incidents（本实例=灰度租户视图）"
code=$(curl -s --noproxy '*' -o "$OUT/.inc.json" -w '%{http_code}' --max-time 5 "$OPS_URL/api/v1/incidents?limit=200" || true)
if [ "$code" = "200" ]; then
  cnt=$(grep -o '"count":[0-9]*' "$OUT/.inc.json" | head -1 | tr -dc '0-9')
  echo "  HTTP $code, count=${cnt:-0}"
else
  echo "  HTTP ${code:-失败}——未通？先看 run_gray_autocreate.sh status"
fi
rm -f "$OUT/.inc.json"

echo "[PG]（DSN=${DB_DSN%%@*}@…）"
mkdir -p "$OUT/pgstatus"
if [ ! -f "$OUT/pgstatus/main.go" ]; then
  cat > "$OUT/pgstatus/main.go" <<'EOF'
// 一次性灰度读数助手（.gray/ 下生成，gitignored；不属于仓库 Go 代码）。
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, os.Getenv("GRAY_DB_DSN"))
	if err != nil {
		fmt.Println("  PG 连接失败:", err)
		return
	}
	defer conn.Close(context.Background())
	t := os.Getenv("GRAY_TENANT")

	count := func(q string, args ...any) int {
		var n int
		if err := conn.QueryRow(ctx, q, args...).Scan(&n); err != nil {
			fmt.Println("  query err:", err)
		}
		return n
	}
	inc24 := count(`SELECT count(*) FROM incident WHERE tenant_id=$1 AND created_at >= now()-interval '24 hours'`, t)
	fmt.Printf("  incident(%s): 总=%d 近24h新建=%d\n", t,
		count(`SELECT count(*) FROM incident WHERE tenant_id=$1`, t), inc24)
	fmt.Printf("  attach_cluster 审计: 总=%d 近24h=%d\n",
		count(`SELECT count(*) FROM incident_audit WHERE tenant_id=$1 AND action='attach_cluster'`, t),
		count(`SELECT count(*) FROM incident_audit WHERE tenant_id=$1 AND action='attach_cluster' AND occurred_at >= now()-interval '24 hours'`, t))
	fmt.Printf("  create 审计: 总=%d 近24h=%d | rate_limited 近24h=%d | burst 聚合单=%d\n",
		count(`SELECT count(*) FROM incident_audit WHERE tenant_id=$1 AND action='create'`, t),
		count(`SELECT count(*) FROM incident_audit WHERE tenant_id=$1 AND action='create' AND occurred_at >= now()-interval '24 hours'`, t),
		count(`SELECT count(*) FROM incident_audit WHERE tenant_id=$1 AND action='rate_limited' AND occurred_at >= now()-interval '24 hours'`, t),
		count(`SELECT count(*) FROM incident WHERE tenant_id=$1 AND source_ref LIKE 'burst:%'`, t))
	fmt.Printf("  ingest_queue(%s): pending=%d | 近24h入队=%d | 近24h已消费=%d | 死信=%d\n", t,
		count(`SELECT count(*) FROM ingest_queue WHERE tenant_id=$1 AND processed_at IS NULL AND attempts<5`, t),
		count(`SELECT count(*) FROM ingest_queue WHERE tenant_id=$1 AND received_at >= now()-interval '24 hours'`, t),
		count(`SELECT count(*) FROM ingest_queue WHERE tenant_id=$1 AND processed_at IS NOT NULL AND processed_at >= now()-interval '24 hours'`, t),
		count(`SELECT count(*) FROM ingest_queue WHERE tenant_id=$1 AND processed_at IS NULL AND attempts>=5`, t))

	fmt.Println("  近24h incident 按小时分布:")
	rows, err := conn.Query(ctx, `
SELECT to_char(date_trunc('hour', created_at) AT TIME ZONE 'Asia/Shanghai','MM-DD HH24"h"') AS h, count(*)
FROM incident WHERE tenant_id=$1 AND created_at >= now()-interval '24 hours'
GROUP BY 1 ORDER BY 1`, t)
	if err != nil {
		fmt.Println("  query err:", err)
		return
	}
	any := false
	for rows.Next() {
		var h string
		var n int
		if err := rows.Scan(&h, &n); err == nil {
			fmt.Printf("    %s | %d\n", h, n)
			any = true
		}
	}
	rows.Close()
	if !any {
		fmt.Println("    （无）")
	}
}
EOF
fi
( cd "$ROOT" && GRAY_DB_DSN="$DB_DSN" GRAY_TENANT="$TENANT" go run ./.gray/pgstatus ) 2>"$OUT/pgstatus.err" | cat
[ -s "$OUT/pgstatus.err" ] && { echo "  [go run stderr]"; sed 's/^/    /' "$OUT/pgstatus.err"; }

echo "=== 读数结束 $(date '+%F %T %z') —— 整段粘贴进 ADR-016 灰度观察记录 ==="
