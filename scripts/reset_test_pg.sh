#!/usr/bin/env bash
# reset_test_pg.sh —— 二期池波一-1：测试 PG 数据隔离（建独立测试库 + 应用迁移）。
#
# 背景（为什么需要这个脚本）：
#   OPS_TEST_PG_DSN 此前直接复用开发库（DB_DSN 指向的 opscopilot），
#   incident 契约用例的翻页探针 walkAll（internal/incident/
#   store_contract_test.go，≤100 页硬上限）会翻遍过滤条件下的**全表**
#   （PG 共享库里其他前缀的行也算一页），开发库数据一旦累积超 ~200 行
#   （Limit 2 × 100 页），带 DSN 跑就红。d87d89a 后实测复现（旧开发库、
#   -count=2，稳定红 3 例）：
#     --- FAIL: TestContractUpsertExternalGenerations/pgstore  walk did not terminate within 100 pages
#     --- FAIL: TestContractListPage/pgstore                  walk did not terminate within 100 pages
#     --- FAIL: TestContractConcurrency/pgstore               walk did not terminate within 100 pages
#   切到本脚本新建的干净 opscopilot_test 后三例全绿（数据敏感断言，非真
#   bug；不改测试逻辑与生产代码）。go test ./cmd/... 在两侧均绿。
#
# 本脚本**不清开发库**：只在同实例上建独立测试库 opscopilot_test（不存在
# 则 CREATE DATABASE；--wipe 则 DROP (FORCE) + CREATE 彻底重建），然后用
# 纯 Go 助手 scripts/migrate 按序应用 migrations/*.up.sql（本机无 docker
# CLI / psql，pgx 直连 5432；幂等由各迁移自身 IF NOT EXISTS 保证）。
#
# 权限：compose 的 POSTGRES_USER=opscopilot 是集群超级用户，CREATE/DROP
# DATABASE 可行。若换到无建库权限的托管实例，本脚本会明确报错退出；退化
# 方案是"同库不同 schema"：CREATE SCHEMA ops_test + 会话
# search_path=ops_test,public 后应用迁移（timescaledb 扩展与
# create_hypertable 需按 schema 限定表名，alert_event 已在 000003 处理），
# 并把 OPS_TEST_PG_DSN 的 options=-c search_path 参数带上——本次实测未用到。
#
# 用法（Git Bash，compose 栈需在跑）：
#   bash scripts/reset_test_pg.sh            # 建库（已存在则只补应用迁移）
#   bash scripts/reset_test_pg.sh --wipe     # 删库重建（测试数据彻底清场）
# 完成后在 .env 里保留（本仓库已配好）：
#   OPS_TEST_PG_DSN=postgres://opscopilot:opscopilot@localhost:5432/opscopilot_test?sslmode=disable
# 注意：.env 改动不会自动进已开着的终端会话——重跑测试前需重新 source/export。
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

# 载入 .env（Git Bash 下兼容 CRLF；不存在则跳过），DB_DSN / OPS_TEST_PG_DSN 由它提供
if [ -f .env ]; then
  set +e; set -a; . <(sed 's/\r$//' .env); rc=$?; set +a; set -e
  [ $rc -eq 0 ] || echo "警告：.env 载入失败（忽略，用环境变量/默认值）" >&2
fi

WIPE=""
case "${1:-}" in
  "")      : ;;
  --wipe)  WIPE="--wipe" ;;
  *)       echo "用法: bash scripts/reset_test_pg.sh [--wipe]" >&2; exit 2 ;;
esac

# 目标 = 测试库 DSN。允许环境覆盖；默认把开发库名替换为 opscopilot_test。
TEST_DB="${OPS_TEST_DB:-opscopilot_test}"
if [ -n "${OPS_TEST_PG_DSN:-}" ]; then
  DSN="$OPS_TEST_PG_DSN"
else
  BASE="${DB_DSN:-postgres://opscopilot:opscopilot@localhost:5432/opscopilot?sslmode=disable}"
  after="${BASE##*/}"        # 最后一个 / 之后的内容（含可能的 ?query）
  name="${after%%\?*}"       # 原库名
  rest="${after#"$name"}"    # ?query 或空
  DSN="${BASE%"$after"}${TEST_DB}${rest}"
fi

echo "==> 目标测试库: ${DSN%%\?*}  ${WIPE:-（非破坏模式）}"
go run ./scripts/migrate -dsn "$DSN" -path migrations -create-db $WIPE

echo
echo "==> 完成。复验命令（注意先让新 OPS_TEST_PG_DSN 生效：source .env 或重新导出）："
echo "    export OPS_TEST_PG_DSN='${DSN}'"
echo "    go test ./internal/incident/... -count=2"
echo "    go test ./cmd/... -count=1"
