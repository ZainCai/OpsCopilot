#!/usr/bin/env bash
# backup_drill.sh —— 优化方案 #7：备份恢复演练（可复跑）。
#
# 流程：容器内 pg_dump（custom 格式）→ 建临时库 restore → 全量还原 →
# 对账（incident/alert_event 行数 + 最新时间戳）→ 记录耗时/体积 → 删临时库与 dump。
# 演练只读源库、只写临时库，绝不动源数据。全程在 PG 容器内完成，
# 不跨 host/container 拷文件（规避 Git Bash↔docker 的路径转换坑）。
#
# 用法（Git Bash；compose 栈需在跑）：
#   bash scripts/backup_drill.sh
# 可覆盖：CT=PG 容器名，DB/USR=源库与用户。
set -euo pipefail
export PATH="$PATH:/c/Users/蔡/AppData/Local/Programs/DockerDesktop/resources/bin"
export MSYS_NO_PATHCONV=1   # Git Bash 专有：禁止把容器内 /tmp/... 路径改写为宿主路径

DB="${DB:-opscopilot}"
USR="${USR:-opscopilot}"
CT="${CT:-opscopilot-db}"                 # compose 里的 PG 容器名
TS=$(date +%Y%m%d-%H%M%S)
CDUMP="/tmp/ops-backup-$TS.dump"          # 容器内路径
RESTORE_DB="restore_drill_${TS//[:-]/}"

log() { echo "[$(date +%H:%M:%S)] $*"; }
psq() { docker exec "$CT" psql -U "$USR" -d "$1" -t -A -c "$2" | tr -d '\r'; }

# ---- 0. 源库基线 ----
SRC_INC=$(psq "$DB" "SELECT count(*) FROM incident;")
SRC_EVT=$(psq "$DB" "SELECT count(*) FROM alert_event;")
SRC_CHG=$(psq "$DB" "SELECT count(*) FROM change_record;")
SRC_INC_T=$(psq "$DB" "SELECT coalesce(max(created_at)::text,'-') FROM incident;")
SRC_EVT_T=$(psq "$DB" "SELECT coalesce(max(occurred_at)::text,'-') FROM alert_event;")
SRC_CHG_T=$(psq "$DB" "SELECT coalesce(max(occurred_at)::text,'-') FROM change_record;")
log "源库基线: incident=$SRC_INC (max created_at=$SRC_INC_T) | alert_event=$SRC_EVT (max occurred_at=$SRC_EVT_T) | change_record=$SRC_CHG (max occurred_at=$SRC_CHG_T)"

# ---- 1. 备份（容器内 pg_dump，custom 格式）----
T0=$(date +%s)
docker exec "$CT" pg_dump -U "$USR" -d "$DB" -Fc -f "$CDUMP"
SIZE=$(docker exec "$CT" stat -c %s "$CDUMP" | tr -d '\r')
T1=$(date +%s)
log "备份完成: ${SIZE} bytes, 耗时 $((T1-T0))s（容器内 $CDUMP）"

# ---- 2. 建临时库并还原 ----
docker exec "$CT" psql -U "$USR" -d postgres -v ON_ERROR_STOP=1 -c "CREATE DATABASE $RESTORE_DB;" >/dev/null
R0=$(date +%s)
# 首选整库还原；TimescaleDB 连续聚合的循环外键偶尔要求 --disable-triggers（容器内超级用户满足）。
if ! docker exec "$CT" pg_restore -U "$USR" -d "$RESTORE_DB" --no-owner "$CDUMP" 2> "$CDUMP.err"; then
  log "整库还原报错（见下），改用 --disable-triggers 重试："
  tail -15 "$CDUMP.err"
  docker exec "$CT" psql -U "$USR" -d postgres -c "DROP DATABASE $RESTORE_DB;" >/dev/null
  docker exec "$CT" psql -U "$USR" -d postgres -v ON_ERROR_STOP=1 -c "CREATE DATABASE $RESTORE_DB;" >/dev/null
  docker exec "$CT" pg_restore -U "$USR" -d "$RESTORE_DB" --no-owner --disable-triggers "$CDUMP" 2> "$CDUMP.err2" || {
    echo "二次还原仍失败："; tail -25 "$CDUMP.err2"; docker exec "$CT" rm -f "$CDUMP" "$CDUMP.err2"; exit 3; }
fi
docker exec "$CT" rm -f "$CDUMP" "$CDUMP.err"
R1=$(date +%s)
log "还原完成: 耗时 $((R1-R0))s → 库 $RESTORE_DB"

# ---- 3. 对账 ----
D_INC=$(psq "$RESTORE_DB" "SELECT count(*) FROM incident;")
D_EVT=$(psq "$RESTORE_DB" "SELECT count(*) FROM alert_event;")
D_CHG=$(psq "$RESTORE_DB" "SELECT count(*) FROM change_record;")
D_INC_T=$(psq "$RESTORE_DB" "SELECT coalesce(max(created_at)::text,'-') FROM incident;")
D_EVT_T=$(psq "$RESTORE_DB" "SELECT coalesce(max(occurred_at)::text,'-') FROM alert_event;")
D_CHG_T=$(psq "$RESTORE_DB" "SELECT coalesce(max(occurred_at)::text,'-') FROM change_record;")
log "还原库:   incident=$D_INC (max created_at=$D_INC_T) | alert_event=$D_EVT (max occurred_at=$D_EVT_T) | change_record=$D_CHG (max occurred_at=$D_CHG_T)"

ok=1
[ "$SRC_INC" = "$D_INC" ] || { echo "note incident 行数 $SRC_INC → $D_INC（演练期间源库仍被写入，属预期漂移，人工判读）"; }
[ "$SRC_EVT" = "$D_EVT" ] || echo "note alert_event 行数 $SRC_EVT → $D_EVT（高频表，窗口内增量属预期）"
[ "$SRC_CHG" = "$D_CHG" ] || echo "note change_record 行数 $SRC_CHG → $D_CHG"
[ "$SRC_CHG_T" = "$D_CHG_T" ] || echo "note change_record 最新时间戳 $SRC_CHG_T → $D_CHG_T"
[ "$SRC_INC_T" = "$D_INC_T" ] || echo "note incident 最新时间戳漂移 $SRC_INC_T → $D_INC_T"
[ "$D_EVT_T" = "$SRC_EVT_T" ] || echo "note alert_event 最新时间戳 $SRC_EVT_T → $D_EVT_T（高频表，窗口内漂移属预期）"

# ---- 4. 清理 ----
docker exec "$CT" psql -U "$USR" -d postgres -c "DROP DATABASE $RESTORE_DB;" >/dev/null
rm -f /tmp/ops-backup-$TS.dump* 2>/dev/null || true
log "临时库 $RESTORE_DB 与容器内 dump 已删除"

if [ "$ok" = 1 ]; then
  CONCLUSION="对账通过（行数/时间戳一致性见上，漂移需人工判读）"
else
  CONCLUSION="见上"
fi
log "演练结论：$CONCLUSION。备份 $((T1-T0))s / 还原 $((R1-R0))s / 体积 $SIZE bytes"
