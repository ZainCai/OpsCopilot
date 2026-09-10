#!/usr/bin/env bash
# run_demo.sh W6-2 评估环境一键启动（本机 demo）：
#   故障注入器 + opscopilot（Redis 镜像 + TimescaleDB 真相源双写）。
# 前置：docker compose 栈在跑（opscopilot-db / redis-alert / redis-cache）。
#
# 用法：bash scripts/run_demo.sh [--fast]
#   --fast  场景时间缩放 0.1（联调用；默认真实 5min/场景）
#
# 访问：
#   控制台   http://127.0.0.1:8080/console
#   注入状态 http://127.0.0.1:19090/api/v1/status
#   答案簿   http://127.0.0.1:19090/answerbook
# 停止：taskkill //IM opscopilot.exe //F && taskkill //IM faultinjector.exe //F
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="$ROOT/.demo"
mkdir -p "$OUT"

SCALE=1.0
[ "${1:-}" = "--fast" ] && SCALE=0.1

echo "building..."
(cd "$ROOT" && go build -o "$OUT/faultinjector.exe" ./tools/faultinjector)
(cd "$ROOT" && go build -o "$OUT/opscopilot.exe" ./cmd/opscopilot)

echo "starting fault injector (scale=$SCALE)..."
"$OUT/faultinjector.exe" -addr 127.0.0.1:19090 -scale "$SCALE" -warmup 60 \
  > "$OUT/injector.log" 2>&1 &

echo "starting opscopilot..."
REDIS_ALERT_ADDR=127.0.0.1:6380 \
REDIS_CACHE_ADDR=127.0.0.1:6381 \
OPS_PROM_URL=http://127.0.0.1:19090 \
OPS_WEBHOOK_TOKEN=dev \
OPS_NOISE_WINDOW=10m \
OPS_TOPOLOGY_EDGES="n1->n2,n2->n3" \
OPS_DB_DSN="postgres://opscopilot:opscopilot@127.0.0.1:5432/opscopilot?sslmode=disable" \
OPS_LISTEN_ADDR=127.0.0.1:8080 \
"$OUT/opscopilot.exe" > "$OUT/opscopilot.log" 2>&1 &

sleep 3
echo "up. console: http://127.0.0.1:8080/console"
echo "logs: $OUT/"
