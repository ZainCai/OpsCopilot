#!/usr/bin/env bash
# run_rca_eval.sh — 二期池 #8 RCA 评测集（golden set）一键基线。
#
# 流程（编排范式抄 scripts/run_demo.sh + tools/evaluate.py 的门禁语义）：
#   compose 起栈 → build faultinjector/opscopilot/rca_eval → run_demo 式启动
#   （注入器 + 被测 app，独立租户、加速节拍）→ 跑 rca_eval（默认 --no-llm
#   证据版基线）→ 出 docs/reviews/rca-eval-<date>/report.md（含 ≥85% 转正
#   门禁章节）→ 退出前杀干净本次拉起的进程。
#
# 用法：
#   bash scripts/run_rca_eval.sh            # 证据版基线（LLM 未配即此形态）
#   bash scripts/run_rca_eval.sh --llm      # LLM 转正跑分（OPS_LLM_* 由 env 透传）
#   bash scripts/run_rca_eval.sh stop       # 只清理残留（pidfile 兜底）
#
# 可调 env：RCA_EVAL_SCALE(默认 0.1)  RCA_EVAL_WARMUP(默认 90)
#           OPS_TENANT(默认 rca-eval-<ts>，自动生成独立租户)
#           DOCKER_BIN(默认自动探测 Docker Desktop 路径)
#
# 进程纪律（第八轮教训：孤儿进程写脏数据）：nohup 起的两个进程一律写
# pidfile + trap EXIT 必杀 + stop 子命令兜底；评测退出后才允许任何补写。
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="$ROOT/.rca-eval"
PIDFILE="$OUT/run.pids"
mkdir -p "$OUT"

find_docker() {
  if command -v docker >/dev/null 2>&1; then echo docker; return; fi
  local cand="$LOCALAPPDATA/Programs/DockerDesktop/resources/bin/docker.exe"
  [ -x "$cand" ] && { echo "$cand"; return; }
  echo ""
}

port_open() { (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; }

stop_all() {
  if [ -f "$PIDFILE" ]; then
    while read -r pid; do kill "$pid" 2>/dev/null || true; done < "$PIDFILE"
    rm -f "$PIDFILE"
    echo "rca-eval processes stopped (pids from $PIDFILE)"
  fi
}

if [ "${1:-}" = "stop" ]; then stop_all; exit 0; fi

# ---- 模式 ----
MODE="--no-llm"
if [ "${1:-}" = "--llm" ]; then MODE="--llm"; fi

# ---- 端口冲突防线（run_demo 的孤儿进程/双实例教训）----
if port_open 8080 || port_open 19090; then
  echo "ERROR: 8080 或 19090 已被占用——先 bash scripts/run_demo.sh stop 或 stop 本脚本，再重试" >&2
  exit 2
fi

# ---- compose 起栈 ----
DOCKER_BIN="${DOCKER_BIN:-$(find_docker)}"
if [ -n "$DOCKER_BIN" ]; then
  echo "compose up (timescaledb + redis x2)..."
  "$DOCKER_BIN" compose -f "$ROOT/docker-compose.yaml" up -d timescaledb redis-alert redis-cache >/dev/null
else
  echo "WARN: 未找到 docker CLI，直接探测端口（栈若已在跑则复用）" >&2
fi
for p in 5432 6380 6381; do
  if ! port_open "$p"; then echo "ERROR: 127.0.0.1:$p 未监听，compose 栈没起来？" >&2; exit 2; fi
done

# ---- build（工具与被测 app 全部来自当前 HEAD，评测对象 = 本仓库代码）----
echo "building faultinjector / opscopilot / rca_eval..."
(cd "$ROOT" && go build -o "$OUT/faultinjector.exe" ./tools/faultinjector \
  && go build -o "$OUT/opscopilot.exe" ./cmd/opscopilot \
  && go build -o "$OUT/rca_eval.exe" ./tools/rca_eval)

# ---- 评测参数：独立租户 + 加速剧本 + 快节拍 ----
TS="$(date +%Y%m%d-%H%M%S)"
TENANT="${OPS_TENANT:-rca-eval-$TS}"
SCALE="${RCA_EVAL_SCALE:-0.1}"
WARMUP="${RCA_EVAL_WARMUP:-90}"
DSN="postgres://opscopilot:opscopilot@127.0.0.1:5432/opscopilot?sslmode=disable"
REPORT_DIR="docs/reviews/rca-eval-$(date +%F)"
[ -e "$ROOT/$REPORT_DIR/report.md" ] && REPORT_DIR="docs/reviews/rca-eval-$(date +%F)-$(date +%H%M)"
export OPS_TENANT="$TENANT"

echo "tenant=$TENANT scale=$SCALE warmup=${WARMUP}s mode=${MODE#--}"

# ---- 起进程（run_demo 式；trap 兜底杀干净）----
trap stop_all EXIT
: > "$PIDFILE"
echo "starting fault injector (addr 127.0.0.1:19090)..."
nohup "$OUT/faultinjector.exe" -addr 127.0.0.1:19090 -scale "$SCALE" -warmup "$WARMUP" \
  > "$OUT/injector.log" 2>&1 &
echo $! >> "$PIDFILE"

echo "starting opscopilot (eval wiring: pull+autocreate on, 10s 节拍, 25s 噪声窗)..."
REDIS_ALERT_ADDR=127.0.0.1:6380 \
REDIS_CACHE_ADDR=127.0.0.1:6381 \
OPS_PROM_URL=http://127.0.0.1:19090 \
OPS_WEBHOOK_TOKEN=dev \
OPS_TOPOLOGY_EDGES="n1->n2,n2->n3" \
OPS_DB_DSN="$DSN" \
OPS_LISTEN_ADDR=127.0.0.1:8080 \
OPS_PULL_ALERTS=on OPS_INCIDENT_AUTOCREATE=on \
OPS_CONNECTOR_INTERVAL=10s OPS_PULL_INTERVAL=10s \
OPS_NOISE_WINDOW=25s \
OPS_RCA=on \
nohup "$OUT/opscopilot.exe" > "$OUT/opscopilot.log" 2>&1 &
echo $! >> "$PIDFILE"
disown -a 2>/dev/null || true

ok=""
for _ in $(seq 1 15); do
  sleep 1
  code=$(curl -s -o /dev/null -w "%{http_code}" --noproxy '*' http://127.0.0.1:8080/healthz || true)
  [ "$code" = "200" ] && ok=1 && break
done
if [ -z "$ok" ]; then
  echo "ERROR: opscopilot 未就绪——排查 $OUT/opscopilot.log" >&2
  exit 2
fi
echo "app UP. 剧本一个周期 ≈ 180s（scale=$SCALE）+ warmup ${WARMUP}s + 判分等待，全程约 5-6 分钟，请勿中断"

# ---- 跑评测（评测器自己等剧本时间线，超时上限 25min）----
rc=0
(cd "$ROOT" && "$OUT/rca_eval.exe" \
  -golden tools/rca_eval/golden.json \
  -dsn "$DSN" -token dev -tenant "$TENANT" \
  -out-dir "$OUT" -report-dir "$ROOT/$REPORT_DIR" $MODE) || rc=$?

# ---- 先杀进程（trap 也会兜底），再报结果 ----
stop_all; trap - EXIT

echo
echo "eval exit=$rc  report: $REPORT_DIR/report.md"
exit "$rc"
