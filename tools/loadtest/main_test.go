// main_test.go 纯逻辑单测（不连库、不起服务）：flag 校验 + 报告输出格式。
// 第十一轮 P2「tools/loadtest 零测试（影响容量基线可信度）」补位。
//
// 口径注记：本工具的落盘产物是 **markdown 报告**（buildReport → -out 路径）；
// docs/reviews/loadtest-*/raw/*.jsonl 的逐档 JSONL 读数出自
// scripts/run_capacity_baseline.sh 内嵌的 opsload harness，不在本包编译面内，
// 故此处锁的是 loadtest 自身的输出格式与达标判定的数据通路。
package main

import (
	"strings"
	"testing"
	"time"
)

func TestValidateFlags(t *testing.T) {
	if err := validateFlags(100, 500); err != nil {
		t.Fatalf("validateFlags(100,500) 意外报错：%v", err)
	}
	if err := validateFlags(1, 1); err != nil { // 最小合法值（n == conc）
		t.Fatalf("validateFlags(1,1) 意外报错：%v", err)
	}
	for _, c := range []struct{ conc, n int }{{0, 10}, {10, 0}, {-1, 10}, {10, -5}, {0, 0}} {
		if err := validateFlags(c.conc, c.n); err == nil {
			t.Fatalf("validateFlags(%d,%d) 期望报错，实得 nil", c.conc, c.n)
		}
	}
}

func TestBuildReportFormat(t *testing.T) {
	// 合成两条场景：一条达标、一条 P95 超阈（300ms 口径），检查表格行、
	// 分位数（最近秩法）与未达标结论的完整落盘格式。
	ok := &result{success: 10, failed: 0, rps: 123.4}
	ok.lat = []time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 30 * time.Millisecond}
	ok.p50, ok.p90, ok.p95, ok.p99 = ms(percentile(ok.lat, 50)), ms(percentile(ok.lat, 90)), ms(percentile(ok.lat, 95)), ms(percentile(ok.lat, 99))
	ok.max = ms(ok.lat[len(ok.lat)-1])
	bad := &result{success: 8, failed: 2, rps: 50, p50: 100, p90: 200, p95: 450.5, p99: 900, max: 1000}

	scenarios := []scenario{{"场景A 写路径", ok}, {"场景B 读路径", bad}}
	report := buildReport("http://127.0.0.1:8080", 10, 10, 300, scenarios, false,
		[]string{"场景B 读路径 有 2 个失败", "场景B 读路径 P95 450.5ms > 300ms"})

	for _, want := range []string{
		"# 事件面并发压测报告（验收口径③）",
		"- 目标：`http://127.0.0.1:8080`",
		"- 并发度：**10**，每场景请求数：**10**",
		"- 达标口径：**全部场景零失败** 且 **P95 ≤ 300 ms**",
		"| 场景 | 成功 | 失败 | RPS | P50 (ms) | P90 (ms) | P95 (ms) | P99 (ms) | Max (ms) |",
		"| 场景A 写路径 | 10 | 0 | 123.4 | 20.0 | 30.0 | 30.0 | 30.0 | 30.0 |",
		"| 场景B 读路径 | 8 | 2 | 50.0 | 100.0 | 200.0 | 450.5 | 900.0 | 1000.0 |",
		"## 结论\n\n**未达标** ——",
		"- 场景B 读路径 有 2 个失败",
		"- 场景B 读路径 P95 450.5ms > 300ms",
	} {
		if !strings.Contains(report, want) {
			t.Fatalf("报告缺少 %q；全文：\n%s", want, report)
		}
	}
	// 最近秩法核对：3 样本 P50 = 第 2 小（ceil(0.5*3)=2），P95/P99 钳到最大样本。
	if ok.p50 != 20 || ok.p95 != 30 || ok.p99 != 30 {
		t.Fatalf("percentile 最近秩法漂移：p50=%v p95=%v p99=%v", ok.p50, ok.p95, ok.p99)
	}
	// 达标分支：结论行翻转，无"未达标"字样。
	passReport := buildReport("http://x", 1, 1, 300, []scenario{{"场景A 写路径", ok}}, true, nil)
	if !strings.Contains(passReport, "**达标** —— 全部场景零失败") || strings.Contains(passReport, "未达标") {
		t.Fatalf("达标分支结论行错误：\n%s", passReport)
	}
}
