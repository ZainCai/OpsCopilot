#!/usr/bin/env bash
# run_capacity_baseline.sh —— 优化方案 #7：容量基线压测编排（可重复执行）。
#
# 起服务 → 阶梯压测 → 收指标 → 清理，一站式复跑容量基线全部矩阵
# （结果对照 docs/容量基线-2026-09-12.md 判读）：
#   M1  webhook 灌入阶梯（入队 + 消费两级水位）                     → §2
#   M2  判决链路规模梯度（假 Prometheus 源 10..4000 条/轮）          → §3
#   M3  双实例扩展性（leader 互斥 / 灌入近线性 / 消费认领）          → §4
#   M5  队满丢弃路径（小 sink 队列逼 drops，验证丢判决不丢通知）     → §5
#   M4  30 分钟浸泡（SOAK=1 才跑）                                   → §6
#   BK  备份恢复演练（复用 scripts/backup_drill.sh）                 → §7
#
# 用法（Git Bash；compose 栈没起会自动 up -d + migrate）：
#   bash scripts/run_capacity_baseline.sh                  # 默认 M1,M2,M3,M5,BK
#   MATS=M1 bash scripts/run_capacity_baseline.sh          # 只跑某矩阵（逗号分隔）
#   MATS=M4 SOAK=1 bash scripts/run_capacity_baseline.sh   # 浸泡（约 35 分钟）
#   LADDER="10,100,1000" GRAD="50 500" RAWDIR=/tmp/raw bash scripts/run_capacity_baseline.sh
#
# 前置：Go、Docker Desktop（PATH 兜底见下）、PowerShell（RSS 采样）、curl。
# 纪律：压测器 opsload 不入仓库——从 raw 快照现场编译；exe/日志全落仓库外
#       mktemp 目录（EXIT trap 清理）；仓库内只新增 RAWDIR 下的原始输出。
set -uo pipefail
export PATH="$PATH:/c/Users/蔡/AppData/Local/Programs/DockerDesktop/resources/bin"
export MSYS_NO_PATHCONV=1

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MATS="${MATS:-M1,M2,M3,M5,BK}"
SOAK="${SOAK:-0}"
RAWDIR="${RAWDIR:-$REPO/docs/reviews/loadtest-$(date +%Y-%m-%d)/raw}"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/opscap.XXXXXX")"
WORKM="$(cygpath -m "$WORK")"   # Go/Windows 侧看同一目录（/tmp 在两边映射不同，必须用盘符路径）
CT_DB=opscopilot-db; CT_RA=opscopilot-redis-alert; CT_RC=opscopilot-redis-cache
DBURL="postgres://opscopilot:opscopilot@127.0.0.1:5432/opscopilot?sslmode=disable"
TOKEN="lt7token"; FAKE=127.0.0.1:19191; SINKADDR=127.0.0.1:19200
LADDER="${LADDER:-10,50,200,500,1000,2000,4000,8000}"
GRAD="${GRAD:-10 50 200 1000 2000 4000}"
mkdir -p "$RAWDIR"

PIDS=()          # 本脚本拉起的所有子进程（含采样器/假源/sink）
log() { echo "[$(date +%H:%M:%S)] $*"; }
has() { [[ ",$MATS," == *",$1,"* ]]; }
psq() { docker exec "$CT_DB" psql -U opscopilot -d opscopilot -t -A -c "$1" 2>/dev/null | tr -d '\r'; }
g()   { awk -v p="$2" '$0 ~ "^"p"[ {]"{print $2; exit}' <<< "$1"; }
bump() { PIDS+=("$1"); }   # 登记 pid 以便 EXIT 清理
cleanup() {
  log "清理：杀掉本脚本拉起的全部进程并删除 $WORK"
  for p in "${PIDS[@]:-}"; do [ -n "$p" ] && kill "$p" 2>/dev/null; done
  sleep 1
  rm -rf "$WORK"
}
trap cleanup EXIT INT

# ---------- 0. 环境与构建（仓库外） ----------
docker compose -f "$REPO/docker-compose.yaml" up -d >/dev/null 2>&1
for _ in $(seq 1 30); do
  docker exec "$CT_DB" pg_isready -U opscopilot >/dev/null 2>&1 && break; sleep 1
done
if [ "${SKIP_MIGRATE:-0}" != 1 ]; then (cd "$REPO" && make migrate >/dev/null 2>&1 || true); fi
log "compose 栈就绪；在仓库外构建被测实例与压测器（$WORK）"
(cd "$REPO" && go build -o "$WORKM/opscopilot.exe" ./cmd/opscopilot) || { echo "app 编译失败"; exit 1; }
HARNESS="$REPO/docs/reviews/loadtest-2026-09-12/raw/opsload-harness.go.txt"
[ -f "$HARNESS" ] || { echo "缺压测器快照 $HARNESS（见容量基线文档附录）"; exit 1; }
mkdir -p "$WORK/opsload"
cp "$HARNESS" "$WORK/opsload/main.go"
printf 'module opsload\n\ngo 1.22\n' > "$WORK/opsload/go.mod"
(cd "$WORK/opsload" && go build -o "$WORKM/opsload.exe" .) || { echo "opsload 编译失败"; exit 1; }
OPSLOAD="$WORKM/opsload.exe"

# ---------- 通用积木 ----------
FAKEPID=""
fakesrc() { # fakesrc <alerts/轮> <critical/轮>
  [ -n "$FAKEPID" ] && kill "$FAKEPID" 2>/dev/null; sleep 1
  "$OPSLOAD" -mode=fakesrc -addr "$FAKE" -alerts "$1" -nodes 20 -critical "$2" \
    > "$WORK/fakesrc.log" 2>&1 & FAKEPID=$!; bump $FAKEPID; sleep 1
}
launch() { # launch <port> <tag> —— 压测口径参数集（LT7_* 可覆盖，含义见配置清单）
  local port="$1" tag="$2"
  OPS_LISTEN_ADDR="127.0.0.1:$port" OPS_DB_DSN="$DBURL" \
  REDIS_ALERT_ADDR=127.0.0.1:6380 REDIS_CACHE_ADDR=127.0.0.1:6381 \
  OPS_WEBHOOK_TOKEN="$TOKEN" OPS_TENANT=default \
  OPS_LEADER_ELECTION="${LT7_ELECTION:-on}" OPS_INCIDENT_AUTOCREATE="${LT7_AUTOCREATE:-on}" \
  OPS_INGEST_BATCH="${LT7_ING_BATCH:-500}" OPS_INGEST_INTERVAL="${LT7_ING_INTERVAL:-1s}" \
  OPS_NOISE_SINK_QUEUE="${LT7_SINK_QUEUE:-4096}" OPS_NOISE_SINK_BATCH="${LT7_SINK_BATCH:-200}" \
  OPS_NOISE_SINK_FLUSH="${LT7_SINK_FLUSH:-}" OPS_NOISE_MODE="${LT7_NOISE_MODE:-shadow}" \
  OPS_PROM_URL="${LT7_PROM_URL:-}" OPS_PULL_ALERTS="${LT7_PULL:-off}" OPS_TOPOLOGY_EDGES="${LT7_EDGES:-}" \
  nohup "$WORKM/opscopilot.exe" > "$WORK/app-$tag.log" 2>&1 &
  echo $! > "$WORK/instance-$tag.pid"; bump $!
  for _ in $(seq 1 15); do sleep 1
    [ "$(curl -s --noproxy '*' -o /dev/null -w '%{http_code}' "http://127.0.0.1:$port/healthz" || true)" = 200 ] && { log "实例 $tag UP :$port (pid $(cat "$WORK/instance-$tag.pid"))"; return 0; }
  done
  echo "实例 $tag 启动失败："; tail -20 "$WORK/app-$tag.log"; exit 1
}
kill_app() { [ -f "$WORK/instance-$1.pid" ] && { kill "$(cat "$WORK/instance-$1.pid")" 2>/dev/null; sleep 2; rm -f "$WORK/instance-$1.pid"; }; }
rss_mb() { powershell -NoProfile -Command '$p=Get-Process opscopilot -ErrorAction SilentlyContinue|select -First 1; if($p){[math]::Round($p.WorkingSet64/1MB,1)}else{0}' 2>/dev/null; }
sampler() { # sampler <url> <interval-sec> <tag> —— 周期抓 /metrics 水位 + PG 连接 + RSS
  local out="$RAWDIR/trace-$3.tsv"; : > "$out"
  printf 'ts\turl\tleader\talerts_processed\tsink_q\tsink_drops\tverdict_n\tv_p50\tv_p95\tv_p99\talert_event\tpg_conn\trss_mb\n' >> "$out"
  ( while :; do
      local M evt pgc rss
      evt=$(psq "SELECT count(*) FROM alert_event;"); pgc=$(psq "SELECT count(*) FROM pg_stat_activity WHERE datname='opscopilot';"); rss=$(rss_mb)
      M=$(curl -s --noproxy '*' --max-time 4 "$1/metrics" 2>/dev/null)
      printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$(date +%s)" "$1" \
        "$(g "$M" opscopilot_is_leader)" "$(g "$M" opscopilot_alerts_processed_total)" \
        "$(g "$M" opscopilot_noise_sink_queue)" \
        "$(awk '/^opscopilot_noise_sink_drops_total/{s+=$2}END{print s+0}' <<< "$M")" \
        "$(g "$M" opscopilot_alert_fired_to_verdict_seconds_count)" \
        "$(g "$M" opscopilot_alert_fired_to_verdict_seconds_p50)" \
        "$(g "$M" opscopilot_alert_fired_to_verdict_seconds_p95)" \
        "$(g "$M" opscopilot_alert_fired_to_verdict_seconds_p99)" \
        "$evt" "$pgc" "$rss" >> "$out"
      sleep "$2"
    done ) & bump $!
}

# ---------- M1：webhook 灌入阶梯（单实例，两级水位） ----------
run_m1() {
  log "== M1 灌入阶梯：$LADDER events/s × 60s/档（batch=25, conc=64）=="
  kill_app A; LT7_PROM_URL="" launch 8081 A
  sampler http://127.0.0.1:8081 8 m1-ladder
  "$OPSLOAD" -mode=ingest -base http://127.0.0.1:8081 -token "$TOKEN" \
    -steps "$LADDER" -step-dur 60s -cooldown 20s -batch 25 -conc 64 -prefix lt7c \
    > "$RAWDIR/ladder-current.jsonl" 2> "$RAWDIR/ladder-current.err"
  cat "$RAWDIR/ladder-current.jsonl"
  log "M1 入队侧完成（拐点看 jsonl 的 p95_ms/p99_ms 跳变与 err）。消费侧水位：灌入时观察 ingest_queue pending 排空——"
  log "  pending=$(psq "SELECT count(*) FROM ingest_queue WHERE processed_at IS NULL;")（默认批/间隔 vs 调优 500/1s 各跑一次对照，见文档 §2.2/§10-A）"
  docker exec "$CT_RA" redis-cli INFO memory | awk -F: '/used_memory:/{print "  redis-alert used_memory="$2" bytes"}'
  docker exec "$CT_RC" redis-cli INFO memory | awk -F: '/used_memory:/{print "  redis-cache used_memory="$2" bytes"}'
}

# ---------- M2：判决链路规模梯度 ----------
run_m2() {
  log "== M2 判决梯度：$GRAD 条/轮，每档重启清滑窗后跑约 4.5 个 30s 采集轮 =="
  : > "$RAWDIR/m2-gradient.log"
  for n in $GRAD; do
    crit=$((n / 10)); [ "$crit" -lt 5 ] && crit=5
    fakesrc "$n" "$crit"
    kill_app A                          # 重启实例 = 清空延迟滑窗（分位口径干净）
    LT7_PROM_URL="http://$FAKE" LT7_ING_BATCH=200 LT7_ING_INTERVAL=1s launch 8081 A
    b0=$(psq "SELECT count(*) FROM alert_event;")
    sleep 135
    curl -s --noproxy '*' http://127.0.0.1:8081/metrics > "$RAWDIR/m2-metrics-N$n.txt"
    b1=$(psq "SELECT count(*) FROM alert_event;")
    M="$(cat "$RAWDIR/m2-metrics-N$n.txt")"
    echo "N=$n verdict_p50=$(g "$M" opscopilot_alert_fired_to_verdict_seconds_p50) p95=$(g "$M" opscopilot_alert_fired_to_verdict_seconds_p95) p99=$(g "$M" opscopilot_alert_fired_to_verdict_seconds_p99) samples=$(g "$M" opscopilot_alert_fired_to_verdict_seconds_count) alert_event_delta=$((b1-b0)) sinkq=$(g "$M" opscopilot_noise_sink_queue) rssMB=$(rss_mb)" | tee -a "$RAWDIR/m2-gradient.log"
  done
  log "M2 提醒：轮末快照可能出现分位假回落（文档 §9-C），判读用 sum/count 均值+桶分布双确认。"
}

# ---------- M3：双实例扩展性（ADR-012） ----------
run_m3() {
  log "== M3 双实例：A(8081)=leader 判决+事件，B(8082)=仅事件 =="
  kill_app A; kill_app B
  fakesrc 100 10
  LT7_PROM_URL="http://$FAKE" launch 8081 A; sleep 4; launch 8082 B; sleep 5
  local MA MB
  MA=$(curl -s --noproxy '*' http://127.0.0.1:8081/metrics); MB=$(curl -s --noproxy '*' http://127.0.0.1:8082/metrics)
  echo "3a A is_leader=$(g "$MA" opscopilot_is_leader) B is_leader=$(g "$MB" opscopilot_is_leader)（之和应=1）"
  echo "   B alerts_processed=$(g "$MB" opscopilot_alerts_processed_total)（恒 0 = 判决不随实例扩展，ADR-012 预期）"
  echo "3b 双实例各 4000 ev/s 并发灌入 60s（对照 M1 单实例同档：TPS 应近 2×、P95 会因共享 PG 争抢劣化）…"
  "$OPSLOAD" -mode=ingest -base http://127.0.0.1:8081 -token "$TOKEN" -steps 4000 -step-dur 60s -cooldown 1s -batch 25 -conc 64 -prefix lt7dA > "$RAWDIR/dual-A.jsonl" 2>&1 & local PA=$!; bump $PA
  "$OPSLOAD" -mode=ingest -base http://127.0.0.1:8082 -token "$TOKEN" -steps 4000 -step-dur 60s -cooldown 1s -batch 25 -conc 64 -prefix lt7dB > "$RAWDIR/dual-B.jsonl" 2>&1 & local PB=$!; bump $PB
  wait $PA; wait $PB
  echo "A: $(cat "$RAWDIR/dual-A.jsonl")"
  echo "B: $(cat "$RAWDIR/dual-B.jsonl")"
  echo "3c 消费认领：灌 6000 条积压后双 worker 排空（应 ≈ 单实例速率 ×2、无重复建单）"
  "$OPSLOAD" -mode=ingest -base http://127.0.0.1:8081 -token "$TOKEN" -steps 200 -step-dur 30s -cooldown 1s -batch 20 -conc 32 -prefix lt7dC > "$RAWDIR/dual-C.jsonl" 2>&1
  local t0=$(date +%s) p
  while :; do
    p=$(psq "SELECT count(*) FROM ingest_queue WHERE source_ref LIKE 'lt7dC%' AND processed_at IS NULL;")
    [ "${p:-0}" = "0" ] && break
    [ $(( $(date +%s) - t0 )) -gt 200 ] && break
    sleep 3
  done
  echo "   lt7dC 排空用时 $(( $(date +%s) - t0 ))s，残量=$p（应 0；重复建单判据：incident 中 lt7dC/burst 单无膨胀）"
}

# ---------- M5：队满丢弃路径（丢判决不丢通知） ----------
run_m5() {
  log "== M5 sink 队列 64 + enforce + 假源 3000/轮，3 个采集轮 =="
  kill_app A; kill_app B
  "$OPSLOAD" -mode=sink -addr "$SINKADDR" > "$WORK/sink.log" 2>&1 & bump $!; sleep 1
  LT7_PROM_URL="http://$FAKE" LT7_NOISE_MODE=enforce LT7_SINK_QUEUE=64 LT7_SINK_BATCH=200 LT7_SINK_FLUSH=8s launch 8081 A
  fakesrc 3000 60
  curl -s --noproxy '*' -X POST http://127.0.0.1:8081/api/v1/notify/channels \
    -H 'Content-Type: application/json' -H "X-OpsCopilot-Token: $TOKEN" \
    -d "{\"name\":\"drill\",\"kind\":\"generic\",\"url\":\"http://$SINKADDR/notify\",\"min_severity\":\"critical\"}" >/dev/null
  sleep 95
  local M; M=$(curl -s --noproxy '*' http://127.0.0.1:8081/metrics); echo "$M" > "$RAWDIR/m5-metrics.txt"
  echo "drops{pg}=$(awk '/^opscopilot_noise_sink_drops_total\{store="pg"\}/{print $2}' <<< "$M")（>0 即队满丢判决路径打通）"
  echo "fired→notify 观测=$(g "$M" opscopilot_alert_fired_to_notify_seconds_count) / 接收端 $(curl -s --noproxy '*' "http://$SINKADDR/stats")（通知不丢判据）"
}

# ---------- M4：30 分钟浸泡 ----------
run_m4() {
  log "== M4 浸泡：ingest 100 ev/s × 30min + 判决 100 条/轮混跑 =="
  kill_app A; kill_app B
  fakesrc 100 10
  LT7_PROM_URL="http://$FAKE" launch 8081 A
  sampler http://127.0.0.1:8081 10 m4-soak
  "$OPSLOAD" -mode=ingest -base http://127.0.0.1:8081 -token "$TOKEN" -steps 100 -step-dur 30m -batch 25 -conc 64 -prefix lt7soak > "$RAWDIR/soak.jsonl" 2>&1
  cat "$RAWDIR/soak.jsonl"
  log "浸泡判读：trace-m4-soak.tsv 中 sink_q/sink_drops/rss 30min 走势全平 = 无泄漏（文档 §6 口径）"
}

# ---------- BK：备份恢复演练 ----------
run_bk() { log "== BK 备份恢复演练（incident/change_record/alert_event 对账）=="; bash "$REPO/scripts/backup_drill.sh" 2>&1 | tee "$RAWDIR/backup-drill.log"; }

# ---------- 主流程 ----------
has M1 && run_m1
has M2 && run_m2
has M3 && run_m3
has M5 && run_m5
[ "$SOAK" = 1 ] && has M4 && run_m4
has BK && run_bk
log "完成。原始输出目录：$RAWDIR（exe/日志随临时目录 $WORK 已由 trap 删除）"
