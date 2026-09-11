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
#
# 停止：bash scripts/run_demo.sh stop
# 注意：进程用 nohup 脱离终端——关闭终端窗口不会杀掉它们，
# 必须用 stop 子命令（或任务管理器）停止。
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="$ROOT/.demo"
mkdir -p "$OUT"
PIDFILE="$OUT/demo.pids"

stop_demo() {
  if [ -f "$PIDFILE" ]; then
    while read -r pid; do
      kill "$pid" 2>/dev/null || true
    done < "$PIDFILE"
    rm -f "$PIDFILE"
    echo "demo stopped (pids in $PIDFILE)"
  else
    echo "no pidfile — 若进程仍在，请用任务管理器结束 opscopilot.exe / faultinjector.exe"
  fi
}

if [ "${1:-}" = "stop" ]; then
  stop_demo
  exit 0
fi

SCALE=1.0
[ "${1:-}" = "--fast" ] && SCALE=0.1

# 环境变量透传（D12=B 评估用）：租户与降噪窗口可由调用方覆盖——
# 评估需要 窗口 < 静默段(300s)，否则上一段簇未 resolve、下一段告警并入
# 旧簇是正确降噪行为，却会被 evaluate.py 误判 FAIL（22.7% 假象的另一根源）。
export OPS_TENANT="${OPS_TENANT:-default}"
export OPS_NOISE_WINDOW="${OPS_NOISE_WINDOW:-10m}"

echo "building..."
(cd "$ROOT" && go build -o "$OUT/faultinjector.exe" ./tools/faultinjector)
(cd "$ROOT" && go build -o "$OUT/opscopilot.exe" ./cmd/opscopilot)

# 重复启动防线：端口被占 = 上一次的进程还在，直接复用并提示。
if [ -f "$PIDFILE" ]; then
  echo "WARNING: $PIDFILE exists — demo 可能在运行。先执行 bash scripts/run_demo.sh stop 再启动。"
fi

echo "starting fault injector (scale=$SCALE)..."
nohup "$OUT/faultinjector.exe" -addr 127.0.0.1:19090 -scale "$SCALE" -warmup 60 \
  > "$OUT/injector.log" 2>&1 &
echo $! >> "$PIDFILE"

echo "starting opscopilot..."
REDIS_ALERT_ADDR=127.0.0.1:6380 \
REDIS_CACHE_ADDR=127.0.0.1:6381 \
OPS_PROM_URL=http://127.0.0.1:19090 \
OPS_WEBHOOK_TOKEN=dev \
OPS_TOPOLOGY_EDGES="n1->n2,n2->n3" \
OPS_DB_DSN="postgres://opscopilot:opscopilot@127.0.0.1:5432/opscopilot?sslmode=disable" \
OPS_LISTEN_ADDR=127.0.0.1:8080 \
nohup "$OUT/opscopilot.exe" > "$OUT/opscopilot.log" 2>&1 &
echo $! >> "$PIDFILE"

disown -a 2>/dev/null || true

# 启动自检：等引擎就绪并探活，失败给排查指引。
ok=""
for _ in $(seq 1 10); do
  sleep 1
  code=$(curl -s -o /dev/null -w "%{http_code}" --noproxy '*' http://127.0.0.1:8080/healthz || true)
  [ "$code" = "200" ] && ok=1 && break
done
if [ -n "$ok" ]; then
  echo "UP. console: http://127.0.0.1:8080/console"
else
  echo "FAILED to become healthy — 排查顺序："
  echo "  1) docker compose 栈在跑吗（opscopilot-db / redis）？"
  echo "  2) 8080 被占？taskkill //IM opscopilot.exe //F 后重试"
  echo "  3) 日志：$OUT/opscopilot.log"
  exit 1
fi
echo "logs: $OUT/   stop: bash scripts/run_demo.sh stop"
echo "提示：若浏览器打不开控制台，检查代理软件是否拦截 127.0.0.1（本机此前出现过系统代理吞本地请求）"
