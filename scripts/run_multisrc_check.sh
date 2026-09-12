#!/usr/bin/env bash
# run_multisrc_check.sh —— #11 水平扩展"双实例同跑"手工验证入口（ADR-012 验收标准）。
#
# 用法（OPS_TEST_PG_DSN 须指向独立测试库，先跑 scripts/reset_test_pg.sh 建好）：
#   OPS_TEST_PG_DSN='postgres://opscopilot:opscopilot@localhost:5432/opscopilot_test?sslmode=disable' \
#     bash scripts/run_multisrc_check.sh
#
# 覆盖（同进程双 Assembly 共享一套 TimescaleDB，与双进程部署同语义——
# 认领租约/选主锁/台账全部落在 PG 会话/行级，跨进程与跨池同样互斥）：
#   (a) TestDualInstanceIngestExclusive  入队 N 条 → 双 worker 恰一处理、无重复建单
#   (a') TestDualIngestClaimDisjoint      认领租约层直证：同一行至多一个 owner
#   (b) TestLeaderElectionMutualExclusionAndTakeover  任意时刻恰一 leader；Stop 后限时接管
#   (b') TestLeaderOnPromoteGateBlocksFlip 簇恢复失败不开闸（宁漏勿杀）
#   (c) TestDualInstanceEscalationSingleClaim        同扫逾期单恰发一次升级通知
#       TestPGEscalationLedgerConcurrentClaim        并发双 Claim 恰一成功
#
# faultinjector 全链路双实例回放暂以本测试集为验收口径（同进程双池已覆盖
# 互斥原语）；需要跨进程演练时按下述手工步骤：
#   1) 终端 A：OPS_LISTEN_ADDR=127.0.0.1:8081 OPS_TOPOLOGY_EDGES="n1->n2" go run ./cmd/opscopilot
#   2) 终端 B：OPS_LISTEN_ADDR=127.0.0.1:8082 OPS_TOPOLOGY_EDGES="n1->n2" go run ./cmd/opscopilot
#   3) curl -s 127.0.0.1:8081/metrics | grep opscopilot_is_leader  （两实例之和恒 = 1）
#   4) 杀掉 leader 那个终端，≤15s 内另一实例该 gauge 翻 1（日志见 "leader: PROMOTED"）
#   5) 向任一端 POST /api/v1/ingest/alertmanager 灌告警，确认 incident 无重复、
#      两侧日志合计每条消息仅一次 "incident created"。
set -euo pipefail
cd "$(dirname "$0")/.."

if [ -z "${OPS_TEST_PG_DSN:-}" ]; then
  echo "ERROR: 需先 export OPS_TEST_PG_DSN（本机 compose TimescaleDB 即可）" >&2
  exit 1
fi

go test ./cmd/opscopilot -count=1 -v \
  -run 'TestDualInstance|TestDualIngest|TestLeaderElection|TestLeaderOnPromote|TestPGEscalationLedgerConcurrentClaim|TestClusterRestore|TestRunLeaderGated'

echo
echo "双实例集成全部通过 —— #11 验收标准 (a)(b)(c) 见上列测试注释。"
