#!/usr/bin/env bash
# run_gray_autocreate.sh —— ADR-016 灰度段一环境起停管理（开发机 demo，租户 gray01）。
#
# 段一参数组（权威口径 docs/adr/ADR-016-autoCreate转正与灰度.md）：
#   OPS_INCIDENT_AUTOCREATE=on + OPS_INGEST_BATCH=500 + OPS_INGEST_INTERVAL=1s
#   + OPS_NOISE_MODE=enforce + OPS_AUTOATTACH=on + OPS_TENANT=gray01
#   其余键走 .env / 出厂默认；链路接法沿用 scripts/run_demo.sh 现状
#   （faultinjector 假 Prometheus 喂场景告警，链路 A push/pull 同队列汇流）。
#
# 用法：
#   bash scripts/run_gray_autocreate.sh start    # 起（build→detach 拉起注入器+灰度实例→探活→种子注入）
#   bash scripts/run_gray_autocreate.sh stop     # 停（pidfile 全清 + 按 .gray 路径兜底扫孤儿）
#   bash scripts/run_gray_autocreate.sh status   # 存活概览（读数请用 gray_autocreate_status.sh）
#   bash scripts/run_gray_autocreate.sh push     # 手动补一次链路 A webhook 样例注入（一次性，非循环）
#
# 纪律（吸取孤儿采样进程教训）：
#   - 本脚本**不带任何循环采集器**——观察读数走一次性脚本
#     scripts/gray_autocreate_status.sh（人跑、不自启）。
#   - 进程用 powershell Start-Process 脱离本会话存活，日志重定向落 .gray/；
#     pidfile 在 .gray/gray.pids，stop 先按 pid 杀、再按"可执行文件位于
#     .gray\ 下的 opscopilot.exe / faultinjector.exe"兜底清扫（不误伤 .demo 实例）。
#   - 端口与 demo 栈错开：灰度实例 127.0.0.1:8090，注入器 127.0.0.1:19091。
#   - 本机无 docker CLI：compose 栈（5432/6380/6381）由别处管理，本脚本只探活、
#     不可达即报错给出排查指引，绝不静默降级。
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="$ROOT/.gray"
PIDFILE="$OUT/gray.pids"

OPS_PORT="${GRAY_OPS_PORT:-8090}"
INJ_PORT="${GRAY_INJ_PORT:-19091}"
OPS_URL="http://127.0.0.1:$OPS_PORT"
INJ_URL="http://127.0.0.1:$INJ_PORT"
GRAY_TENANT="${GRAY_TENANT:-gray01}"
GRAY_TOKEN="${GRAY_WEBHOOK_TOKEN:-dev}"
DB_DSN="${GRAY_DB_DSN:-postgres://opscopilot:opscopilot@127.0.0.1:5432/opscopilot?sslmode=disable}"
INJ_SCALE="${GRAY_SCALE:-1.0}"
INJ_WARMUP="${GRAY_WARMUP:-60}"

mkdir -p "$OUT"
WINOUT="$(cygpath -w "$OUT")"

port_open() { (echo > "/dev/tcp/127.0.0.1/$1") 2>/dev/null; }

preflight() {
  local miss=""
  port_open 5432 || miss="$miss 5432(TimescaleDB)"
  port_open 6380 || miss="$miss 6380(redis-alert)"
  port_open 6381 || miss="$miss 6381(redis-cache)"
  if [ -n "$miss" ]; then
    echo "ERROR: compose 栈不可达：$miss" >&2
    echo "本机无 docker CLI，compose 栈由别处管理——请把缺的端口列给栈管理员" >&2
    echo "（docker compose -f docker-compose.yaml up -d），或修正 GRAY_* 端口覆盖后重试。" >&2
    exit 1
  fi
}

# ps_start <exe-win> <args-ps> <stdout-win> <stderr-win> <ps-env-语句>  → 打印新进程 pid
# 注意：powershell 的 stdout 绝不用 $(...) 管道捕获——被 detach 的子进程会继承
# 管道写端，bash 等 EOF 永久挂起（本脚本首版踩过）。pid 走 side 文件回传。
ps_start() {
  local pidf="$WINOUT\\last.pid"
  rm -f "$OUT/last.pid"
  local argpart=""
  [ -n "$2" ] && argpart=" -ArgumentList $2"
  powershell -NoProfile -Command "$5\$p = Start-Process -FilePath '$1'$argpart -WindowStyle Hidden -RedirectStandardOutput '$3' -RedirectStandardError '$4' -PassThru; \$p.Id | Out-File -Encoding ascii '$pidf'" </dev/null >/dev/null 2>&1
  tr -d '\r\n' < "$OUT/last.pid"
}

ps_alive() {
  powershell -NoProfile -Command "if (Get-Process -Id $1 -ErrorAction SilentlyContinue) { 'yes' } else { 'no' }" | tr -d '\r\n'
}

do_stop() {
  local n=0
  if [ -f "$PIDFILE" ]; then
    while read -r pid; do
      [ -z "${pid//[$'\r\n ']/}" ] && continue
      powershell -NoProfile -Command "Stop-Process -Id $pid -Force -ErrorAction SilentlyContinue" || true
      n=$((n + 1))
    done < <(tr -d '\r' < "$PIDFILE")
    rm -f "$PIDFILE"
  fi
  # 兜底清扫：任何"从 .gray\ 目录启动"的灰度进程（pidfile 丢了/被重启脚本漏记也不留孤儿）。
  # 只匹配 ExecutablePath 在 .gray\ 下的 opscopilot.exe / faultinjector.exe，绝不碰 .demo 实例。
  # 注意：set -euo pipefail 下管道非零会中止脚本——清扫失败必须 || true 兜住，
  # 否则 stop 半途而废（首版踩坑：Get-CimInstance 偶发非零 → 孤儿没杀还装死）。
  local orphans
  orphans="$( { powershell -NoProfile -Command "Get-CimInstance Win32_Process | Where-Object { (\$_.Name -eq 'opscopilot.exe' -or \$_.Name -eq 'faultinjector.exe') -and \$_.ExecutablePath -like '*.gray\\*' } | ForEach-Object { Stop-Process -Id \$_.ProcessId -Force -ErrorAction SilentlyContinue; \$_.ProcessId }" | tr -d '\r'; } || true )"
  [ -n "$orphans" ] && { echo "stop: 兜底清扫 .gray 孤儿进程: $(echo $orphans | tr '\n' ' ')"; n=$((n + 1)); }
  echo "gray stopped (pidfile 记录 ${n:-0} 组；日志留在 $OUT/)"
}

do_status() {
  echo "=== ADR-016 灰度段一 · 实例状态 $(date '+%F %T %z') ==="
  if [ -f "$PIDFILE" ]; then
    while read -r pid; do
      [ -z "${pid//[$'\r\n ']/}" ] && continue
      pid="${pid//[!0-9]/}"
      echo "  pid $pid: $(ps_alive "$pid")"
    done < <(tr -d '\r' < "$PIDFILE")
  else
    echo "  无 pidfile（$PIDFILE）——未在跑或未由本脚本启动"
  fi
  echo "  healthz  $OPS_URL : $(curl -s --noproxy '*' -o /dev/null -w '%{http_code}' --max-time 4 "$OPS_URL/healthz" || true)"
  echo "  注入器   $INJ_URL : $(curl -s --noproxy '*' -o /dev/null -w '%{http_code}' --max-time 4 "$INJ_URL/-/healthy" || true)"
  echo "  控制台   $OPS_URL/console    读数: bash scripts/gray_autocreate_status.sh"
}

seed_push() {
  # 链路 A webhook 一次性样例注入（run_demo 现有接法：X-OpsCopilot-Token 共享密钥）。
  # 幂等靠 (tenant,origin,source_ref) 待处理态唯一索引——同指纹重推不堆积；
  # 指纹带启动时戳：死信行（processed_at IS NULL 且 attempts>=5）同指纹会永久占位，
  # 换时戳保证重启后种子注入必入队。
  # **payload 必须走文件**：Git Bash 向原生 curl.exe 传参时按系统 ANSI 码页（GBK）
  # 转换命令行，中文 summary 到服务端已成非法 UTF-8、被 JSON 解码替换成 U+FFFD
  # （2026-09-13 灰度首跑实证）。临时文件由 bash 直写，UTF-8 字节全程不变。
  local now stamp payload
  now="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  stamp="$(date +%s)"
  payload="$(mktemp)"
  cat > "$payload" <<EOF
{"status":"firing","alerts":[
  {"labels":{"alertname":"GraySeedHighCPU","instance":"n1","job":"nodes","severity":"critical"},"annotations":{"summary":"灰度种子告警（链路A push 通路自检）"},"startsAt":"$now","fingerprint":"grayseed-$stamp-01"},
  {"labels":{"alertname":"GraySeedDiskFull","instance":"n2","job":"nodes","severity":"warning"},"annotations":{"summary":"灰度种子告警 2"},"startsAt":"$now","fingerprint":"grayseed-$stamp-02"}]}
EOF
  curl -s --noproxy '*' -X POST "$OPS_URL/api/v1/ingest/alertmanager" \
    -H "Content-Type: application/json" -H "X-OpsCopilot-Token: $GRAY_TOKEN" \
    --data-binary "@$payload" || true
  rm -f "$payload"
  echo ""
}

do_start() {
  preflight
  if [ -f "$PIDFILE" ] && [ -s "$PIDFILE" ]; then
    echo "ERROR: $PIDFILE 已存在——灰度环境可能在跑。先 bash scripts/run_gray_autocreate.sh stop" >&2
    exit 1
  fi
  port_open "$OPS_PORT" && { echo "ERROR: $OPS_PORT 已被占（上一次的灰度实例？先 stop）" >&2; exit 1; }

  echo "building → $OUT/ ..."
  (cd "$ROOT" && go build -o "$OUT/faultinjector.exe" ./tools/faultinjector)
  (cd "$ROOT" && go build -o "$OUT/opscopilot.exe" ./cmd/opscopilot)

  echo "starting faultinjector (fake prometheus, $INJ_URL, scale=$INJ_SCALE, warmup=${INJ_WARMUP}s)..."
  local inj_pid
  inj_pid="$(ps_start "$WINOUT\\faultinjector.exe" \
    "'-addr','127.0.0.1:$INJ_PORT','-scale','$INJ_SCALE','-warmup','$INJ_WARMUP'" \
    "$WINOUT\\injector.out.log" "$WINOUT\\injector.err.log" '')"
  [ -n "$inj_pid" ] || { echo "ERROR: faultinjector 拉起失败（检查 $OUT/injector.err.log）" >&2; exit 1; }
  echo "$inj_pid" > "$PIDFILE"

  echo "starting opscopilot gray instance ($OPS_URL, tenant=$GRAY_TENANT) ..."
  # 段一参数组（ADR-016）+ run_demo 同款链路接法；OPS_PULL_ALERTS=on 让场景告警
  # 走链路 A 拉取入队（source_ref=promFingerprint）与 push 在队列汇流。
  local psenv="\$env:OPS_TENANT='$GRAY_TENANT';\$env:OPS_INCIDENT_AUTOCREATE='on';\$env:OPS_INGEST_BATCH='500';\$env:OPS_INGEST_INTERVAL='1s';\$env:OPS_NOISE_MODE='enforce';\$env:OPS_AUTOATTACH='on';\$env:OPS_PULL_ALERTS='on';\$env:OPS_DB_DSN='$DB_DSN';\$env:REDIS_ALERT_ADDR='127.0.0.1:6380';\$env:REDIS_CACHE_ADDR='127.0.0.1:6381';\$env:OPS_PROM_URL='$INJ_URL';\$env:OPS_WEBHOOK_TOKEN='$GRAY_TOKEN';\$env:OPS_TOPOLOGY_EDGES='n1->n2,n2->n3';\$env:OPS_LISTEN_ADDR='127.0.0.1:$OPS_PORT';"
  local ops_pid
  ops_pid="$(ps_start "$WINOUT\\opscopilot.exe" "" \
    "$WINOUT\\opscopilot.out.log" "$WINOUT\\opscopilot.err.log" "$psenv")"
  [ -n "$ops_pid" ] || { echo "ERROR: opscopilot 拉起失败：tail $OUT/opscopilot.err.log / last.pid 未生成" >&2; exit 1; }
  echo "$ops_pid" >> "$PIDFILE"

  local ok=""
  for _ in $(seq 1 30); do
    sleep 1
    [ "$(curl -s --noproxy '*' -o /dev/null -w '%{http_code}' --max-time 3 "$OPS_URL/healthz" || true)" = "200" ] && { ok=1; break; }
  done
  if [ -z "$ok" ]; then
    echo "FAILED to become healthy — 排查：" >&2
    echo "  1) tail $OUT/opscopilot.err.log（enforce+autoattach 组合非法会拒启）" >&2
    echo "  2) compose 栈：bash 探活 5432/6380/6381" >&2
    echo "  3) 端口冲突：bash scripts/run_gray_autocreate.sh status" >&2
    exit 1
  fi

  echo "UP — 链路 A push 种子注入（入队→建单通路自检）："
  seed_push
  echo ""
  echo "灰度段一在跑：pidfile=$PIDFILE (injector $inj_pid / opscopilot $ops_pid)"
  echo "  控制台   $OPS_URL/console"
  echo "  读数     bash scripts/gray_autocreate_status.sh   （一次性，人跑）"
  echo "  停止     bash scripts/run_gray_autocreate.sh stop"
  echo "注意：注入器 warmup ${INJ_WARMUP}s + 场景节拍（连接器 30s 采集）——autoattach"
  echo "首单预计 start 后 2~3 分钟出现；链路 A 建单种子即时入队（batch=500/1s 消费）。"
}

case "${1:-}" in
  start)  do_start ;;
  stop)   do_stop ;;
  status) do_status ;;
  push)   seed_push ;;
  *)
    echo "用法: bash scripts/run_gray_autocreate.sh {start|stop|status|push}" >&2
    exit 2
    ;;
esac
