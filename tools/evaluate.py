#!/usr/bin/env python
# evaluate.py W6-3 影子降噪准确率评估（M1 出口标准：>85%）。
#
# 原理：逐条影子判决（alert_event, source='shadow'）按 occurred_at
# 落入注入器 answerbook 的场景段，与该段的已知答案对照打分：
#   （期望值以注入器 answerbook 数据为准，本文件不复述——避免第二处定义漂移）
# 段满分条件：new-incident 数 == expected_new_incidents（簇形态由
# cluster_key 共享性隐式验证：段内去重后 (expected_groups 数) 簇）。
#
# 用法：
#   python tools/evaluate.py [--answerbook URL|file:///...] \
#       [--verdicts-file tools/verdicts 导出的 JSON] [--tenant default] \
#       [--cutoff ISO8601]
# 取数两选一：--verdicts-file（推荐，无 Docker 依赖）或 docker exec psql。
#
# 评估有效性前提（重要）：打分模型假设 **静默段时长 > 降噪窗口**
# （现行剧本每段 300s，故窗口须 < 5m，如 OPS_NOISE_WINDOW=4m）——否则
# 上一段簇未 resolve，下一段告警（含同节点）并入旧簇是**正确降噪行为**，
# 会被误判 FAIL（实测 22.7% 即此假象：--fast 把段缩到 30s 且未缩窗口）。
#
# 输出：控制台报告 + 每段的判定明细；退出码 0=达标(>85%)、1=不达标。
import argparse
import json
import subprocess
import sys
import urllib.request
from datetime import datetime, timedelta, timezone

CONTAINER = "opscopilot-db"
# docker 可执行文件：默认 PATH 查找；Windows 下可显式传 DockerDesktop 路径。
DOCKER = "docker"


def fetch_answerbook(url):
    with urllib.request.urlopen(url, timeout=10) as r:
        return json.load(r)


def fetch_verdicts(tenant="default"):
    """从 opscopilot-db 容器拉全部影子判决（CSV）。"""
    safe = tenant.replace("'", "''")  # 本地工具：仅防 operator 手滑，非安全边界
    sql = (
        "SELECT occurred_at, cluster_key, fingerprint, "
        f"payload->>'reason' AS reason FROM alert_event "
        f"WHERE source='shadow' AND tenant_id='{safe}' "
        "AND occurred_at > now() - interval '7 days' "
        "ORDER BY occurred_at"
    )
    out = subprocess.run(
        [DOCKER, "exec", CONTAINER, "psql", "-U", "opscopilot", "-d",
         "opscopilot", "-At", "-F", "\x01", "-c", sql],
        capture_output=True, text=True, timeout=30,
    )
    if out.returncode != 0:
        sys.exit("psql failed: " + out.stderr)
    rows = []
    for line in out.stdout.splitlines():
        if not line.strip():
            continue
        occurred, ckey, fp, reason = line.split("\x01")
        rows.append({
            "occurred_at": datetime.fromisoformat(occurred),
            "cluster_key": ckey,
            "fingerprint": fp,
            "reason": reason,
        })
    return rows


def load_verdicts_file(path):
    """从 tools/verdicts 导出的 JSON 读取影子判决（无 Docker 环境用）。"""
    with open(path, encoding="utf-8") as f:
        raw = json.load(f)
    rows = []
    for v in raw:
        rows.append({
            "occurred_at": parse_ts(v["occurred_at"]),
            "cluster_key": v.get("cluster_key", ""),
            "fingerprint": v.get("fingerprint", ""),
            "reason": v.get("reason", ""),
        })
    return rows


def parse_ts(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--answerbook", default="http://127.0.0.1:19090/answerbook")
    ap.add_argument("--threshold", type=float, default=0.85)
    ap.add_argument("--docker", default="docker", help="docker.exe path")
    ap.add_argument("--verdicts-file", default="",
                    help="从 tools/verdicts 导出的 JSON 读判决（无 Docker 环境用；"
                         "设置后跳过 docker exec 取数）")
    ap.add_argument("--tenant", default=os.environ.get("OPS_TENANT", "default"),
                    help="影子判决所属租户（docker 取数路径用；--verdicts-file 不受影响）")
    ap.add_argument("--cutoff", default="",
                    help="评估截止时刻（ISO8601）：start > cutoff 的段一律不计——"
                         "注入器停机后的空段不应参与打分（静默段会被误算 PASS）")
    args = ap.parse_args()

    global DOCKER
    DOCKER = args.docker
    book = fetch_answerbook(args.answerbook)
    verdicts = load_verdicts_file(args.verdicts_file) if args.verdicts_file else fetch_verdicts(args.tenant)
    cutoff = parse_ts(args.cutoff) if args.cutoff else None
    segments = book["segments"]
    now = datetime.now(timezone.utc)
    # 只评已完结的段（end <= now），且只看最近 7 天。
    horizon = now - timedelta(days=7)

    total = scored = passed = 0
    lines = ["# 影子降噪准确率评估报告", "",
             f"判决总数: {len(verdicts)}  可评段数: 待定  达标线: {args.threshold:.0%}", ""]

    for seg in segments:
        start, end = parse_ts(seg["start"]), parse_ts(seg["end"])
        if end > now or end < horizon:
            continue
        if cutoff and start > cutoff:
            continue  # 注入器已停机的时段：不打分（空静默段会虚增分数）
        sc = seg["scenario"]
        if sc == "C":  # 静默段：有判决即扣分
            total += 1
            hit = [v for v in verdicts if start <= v["occurred_at"] < end]
            if not hit:
                passed += 1
                scored += 1
                lines.append(f"[PASS] C 静默 {start:%m-%d %H:%M} 无判决")
            else:
                lines.append(
                    f"[FAIL] C 静默 {start:%m-%d %H:%M} 出现 {len(hit)} 条判决（误告警）")
            continue
        total += 1
        seg_v = [v for v in verdicts if start <= v["occurred_at"] < end]
        if len(seg_v) == 0:
            lines.append(f"[SKIP] {sc} {start:%m-%d %H:%M} 段内无判决（采集缺口?）")
            continue
        scored += 1
        new_incidents = sum(1 for v in seg_v if v["reason"] == "new-incident")
        clusters = {v["cluster_key"] for v in seg_v if v["cluster_key"]}
        ok = new_incidents == seg["expected_new_incidents"] and \
            len(clusters) <= seg["expected_new_incidents"]
        if ok:
            passed += 1
        lines.append(
            f"[{'PASS' if ok else 'FAIL'}] {sc} {start:%m-%d %H:%M} "
            f"verdicts={len(seg_v)} new={new_incidents} "
            f"(期望 new={seg['expected_new_incidents']})")

    rate = (passed / total) if total else 0.0
    lines += ["", f"## 结果: {passed}/{total} 段达标 = {rate:.1%}",
              "达标" if rate > args.threshold else "未达标（M1 出口 >85%）"]
    report = "\n".join(lines)
    print(report)
    with open("evaluation-report.md", "w", encoding="utf-8") as f:
        f.write(report + "\n")
    sys.exit(0 if rate > args.threshold else 1)


if __name__ == "__main__":
    main()
