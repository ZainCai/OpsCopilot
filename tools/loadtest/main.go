// loadtest 事件面并发压测（W8 验收口径③）。
//
// 定位：对运行中的 opscopilot 打真接口，度量**人工建单（写路径）**与
// **事件列表（读路径）**在并发下的延迟分位与成功率，产出可复核的 markdown
// 报告，并以退出码表达是否达标（0=达标）。
//
// 用真接口而非直连 Store：验收要的是"端到端 HTTP 路径"的表现——含路由、
// 鉴权、JSON 编解码、审计写入与连接池。直连 Store 会掩盖这些成本。
//
// 用法：
//
//	go run ./tools/loadtest -base http://127.0.0.1:8080 -token <写密钥>
//	# 或先 go build -o loadtest.exe ./tools/loadtest
//
// 压测会真实建单（污染库）。加 -cleanup 并在 OPS_DB_DSN（或 -dsn）可用时，
// 结束时按"本次实际创建的 incident_id"精确删除（含审计行），不留残余——
// 只删本次记录的 ID，不做任何模式删除。
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// idPrefix 压测建单 ID 前缀（清理与辨识用）。
const idPrefix = "LOAD-"

func main() {
	var (
		base     = flag.String("base", "http://127.0.0.1:8080", "服务基址")
		token    = flag.String("token", "", "写路径共享密钥（空=未启用鉴权）")
		conc     = flag.Int("concurrency", 100, "并发度（同时在途请求数）")
		n        = flag.Int("n", 500, "每场景总请求数")
		p95ms    = flag.Float64("p95-ms", 300, "P95 达标阈值（毫秒）")
		out      = flag.String("out", "loadtest-report.md", "报告输出路径")
		actor    = flag.String("actor", "loadtest", "建单人（审计）")
		cleanup  = flag.Bool("cleanup", false, "结束按本次创建的 ID 精确清理（需 OPS_DB_DSN 或 -dsn）")
		dsn      = flag.String("dsn", "", "PG DSN（清理用；缺省读 OPS_DB_DSN）")
		tenantID = flag.String("tenant", "default", "租户（清理用）")
	)
	flag.Parse()

	if *conc <= 0 || *n <= 0 {
		fmt.Fprintln(os.Stderr, "concurrency 与 n 必须为正")
		os.Exit(2)
	}
	if *n < *conc {
		fmt.Fprintln(os.Stderr, "提示：n < concurrency，实际并发上不满")
	}

	client := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        *conc * 2,
			MaxIdleConnsPerHost: *conc * 2,
			MaxConnsPerHost:     *conc * 2,
			IdleConnTimeout:     60 * time.Second,
			DialContext:         (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		},
	}
	// Windows/部分环境 localhost 解析慢：先用一次探活建立连接，避免首批请求
	// 把 DNS/握手成本算进延迟分位。
	if _, err := client.Get(strings.TrimRight(*base, "/") + "/healthz"); err != nil {
		fmt.Fprintf(os.Stderr, "探活失败（%s/healthz）：%v\n", *base, err)
		os.Exit(2)
	}

	runTag := fmt.Sprintf("%d", time.Now().UnixNano())
	// 预先生成 ID 列表（build 回调在多个 worker goroutine 里跑，
	// 在其中 append 切片会构成数据竞争——先算好，回调只读）。
	createIDs := make([]string, *n)
	for i := range createIDs {
		createIDs[i] = fmt.Sprintf("%s%s-%d", idPrefix, runTag, i)
	}

	// ---- 场景 1：人工建单（写路径）----
	createRes := runScenario(*conc, *n, func(i int) *http.Request {
		id := createIDs[i]
		body, _ := json.Marshal(map[string]string{
			"id": id, "title": "loadtest incident " + id, "severity": "warning", "created_by": *actor,
		})
		req, _ := http.NewRequest(http.MethodPost, strings.TrimRight(*base, "/")+"/api/v1/incidents", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		if *token != "" {
			req.Header.Set("X-OpsCopilot-Token", *token)
		}
		return req
	}, client)

	// ---- 场景 2：事件列表（读路径）----
	listRes := runScenario(*conc, *n, func(int) *http.Request {
		req, _ := http.NewRequest(http.MethodGet, strings.TrimRight(*base, "/")+"/api/v1/incidents", nil)
		return req
	}, client)

	// ---- 场景 3：状态流转（写路径，第二类写操作）----
	transRes := runScenario(*conc, *n, func(i int) *http.Request {
		id := createIDs[i%len(createIDs)]
		body, _ := json.Marshal(map[string]string{"to": "acked", "actor": *actor})
		req, _ := http.NewRequest(http.MethodPost,
			strings.TrimRight(*base, "/")+"/api/v1/incidents/"+id+"/transition", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		if *token != "" {
			req.Header.Set("X-OpsCopilot-Token", *token)
		}
		return req
	}, client)

	scenarios := []scenario{
		{"人工建单 POST /api/v1/incidents", createRes},
		{"事件列表 GET /api/v1/incidents", listRes},
		{"状态流转 POST /api/v1/incidents/{id}/transition", transRes},
	}

	// ---- 控制台输出 ----
	fmt.Printf("压测目标 %s  并发 %d  每场景 %d 请求  P95 阈值 %.0fms\n\n", *base, *conc, *n, *p95ms)
	fmt.Printf("%-46s %8s %8s %9s %8s %8s %8s %8s\n", "场景", "成功", "失败", "RPS", "P50ms", "P95ms", "P99ms", "Maxms")
	for _, s := range scenarios {
		r := s.Res
		fmt.Printf("%-46s %8d %8d %9.1f %8.1f %8.1f %8.1f %8.1f\n",
			s.Name, r.success, r.failed, r.rps, r.p50, r.p95, r.p99, r.max)
	}

	pass := true
	var reasons []string
	for _, s := range scenarios {
		if s.Res.failed > 0 {
			pass = false
			reasons = append(reasons, fmt.Sprintf("%s 有 %d 个失败", s.Name, s.Res.failed))
		}
		if s.Res.p95 > *p95ms {
			pass = false
			reasons = append(reasons, fmt.Sprintf("%s P95 %.1fms > %.0fms", s.Name, s.Res.p95, *p95ms))
		}
	}
	if pass {
		fmt.Printf("\n结论：达标（全部场景零失败，P95 ≤ %.0fms）\n", *p95ms)
	} else {
		fmt.Printf("\n结论：未达标 —— %s\n", strings.Join(reasons, "；"))
	}

	// ---- 报告 ----
	report := buildReport(*base, *conc, *n, *p95ms, scenarios, pass, reasons)
	if err := os.WriteFile(*out, []byte(report), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "写报告失败：%v\n", err)
	} else {
		fmt.Printf("报告：%s\n", *out)
	}

	// ---- 清理 ----
	if *cleanup {
		target := *dsn
		if target == "" {
			target = os.Getenv("OPS_DB_DSN")
		}
		if target == "" {
			fmt.Fprintln(os.Stderr, "清理跳过：未提供 -dsn 且 OPS_DB_DSN 为空")
		} else if err := cleanupCreated(target, *tenantID, createIDs); err != nil {
			fmt.Fprintf(os.Stderr, "清理失败：%v\n", err)
		} else {
			fmt.Printf("清理：已删除本次创建的 %d 条事件（含审计）\n", len(createIDs))
		}
	}

	if !pass {
		os.Exit(1)
	}
}

// scenario 一个压测场景（名称 + 统计）。
type scenario struct {
	Name string
	Res  *result
}

// result 单场景统计。
type result struct {
	success, failed         int
	lat                     []time.Duration
	rps                     float64
	p50, p90, p95, p99, max float64
}

// runScenario 用 conc 个 worker 打 n 个请求（build 返回 nil 表示跳过该请求）。
func runScenario(conc, n int, build func(i int) *http.Request, client *http.Client) *result {
	res := &result{lat: make([]time.Duration, 0, n)}
	jobs := make(chan int)
	var mu sync.Mutex
	var wg sync.WaitGroup

	start := time.Now()
	for w := 0; w < conc; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				req := build(i)
				if req == nil {
					continue
				}
				t0 := time.Now()
				resp, err := client.Do(req)
				el := time.Since(t0)
				ok := err == nil
				if ok {
					// 读干响应体（不读会导致连接无法复用，放大延迟）。
					_, _ = io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
					// 2xx 视为成功；写路径 201/200，读路径 200。
					ok = resp.StatusCode >= 200 && resp.StatusCode < 300
				}
				mu.Lock()
				if ok {
					res.success++
					res.lat = append(res.lat, el)
				} else {
					res.failed++
				}
				mu.Unlock()
			}
		}()
	}
	for i := 0; i < n; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	elapsed := time.Since(start)

	if len(res.lat) > 0 {
		sort.Slice(res.lat, func(a, b int) bool { return res.lat[a] < res.lat[b] })
		res.p50 = ms(percentile(res.lat, 50))
		res.p90 = ms(percentile(res.lat, 90))
		res.p95 = ms(percentile(res.lat, 95))
		res.p99 = ms(percentile(res.lat, 99))
		res.max = ms(res.lat[len(res.lat)-1])
	}
	if elapsed > 0 {
		res.rps = float64(res.success+res.failed) / elapsed.Seconds()
	}
	return res
}

// percentile 最近秩法（p 为 0-100）。已排序切片。
func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	rank := int(math.Ceil(p/100*float64(len(sorted)))) - 1
	if rank < 0 {
		rank = 0
	}
	if rank >= len(sorted) {
		rank = len(sorted) - 1
	}
	return sorted[rank]
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000.0 }

// buildReport 生成 markdown 报告。
func buildReport(base string, conc, n int, p95ms float64, scenarios []scenario, pass bool, reasons []string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# 事件面并发压测报告（验收口径③）\n\n")
	fmt.Fprintf(&b, "- 目标：`%s`\n", base)
	fmt.Fprintf(&b, "- 并发度：**%d**，每场景请求数：**%d**\n", conc, n)
	fmt.Fprintf(&b, "- 达标口径：**全部场景零失败** 且 **P95 ≤ %.0f ms**\n", p95ms)
	fmt.Fprintf(&b, "- 生成时间：%s\n\n", time.Now().Format(time.RFC3339))

	b.WriteString("## 结果\n\n")
	b.WriteString("| 场景 | 成功 | 失败 | RPS | P50 (ms) | P90 (ms) | P95 (ms) | P99 (ms) | Max (ms) |\n")
	b.WriteString("|---|---:|---:|---:|---:|---:|---:|---:|---:|\n")
	for _, s := range scenarios {
		r := s.Res
		fmt.Fprintf(&b, "| %s | %d | %d | %.1f | %.1f | %.1f | %.1f | %.1f | %.1f |\n",
			s.Name, r.success, r.failed, r.rps, r.p50, r.p90, r.p95, r.p99, r.max)
	}
	b.WriteString("\n")
	if pass {
		b.WriteString("## 结论\n\n**达标** —— 全部场景零失败，P95 均在阈值内。\n")
	} else {
		b.WriteString("## 结论\n\n**未达标** ——\n\n")
		for _, r := range reasons {
			fmt.Fprintf(&b, "- %s\n", r)
		}
	}
	return b.String()
}

// cleanupCreated 精确删除本次压测创建的事件与审计（只按记录到的 ID）。
func cleanupCreated(dsn, tenant string, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer pool.Close()

	// 先删审计（无外键），再删事件（incident_cluster 由 ON DELETE CASCADE 带走）。
	if _, err := pool.Exec(ctx,
		`DELETE FROM incident_audit WHERE tenant_id=$1 AND incident_id = ANY($2)`, tenant, ids); err != nil {
		return fmt.Errorf("delete audit: %w", err)
	}
	tag, err := pool.Exec(ctx,
		`DELETE FROM incident WHERE tenant_id=$1 AND incident_id = ANY($2)`, tenant, ids)
	if err != nil {
		return fmt.Errorf("delete incident: %w", err)
	}
	fmt.Printf("清理：incident 删除 %d 行\n", tag.RowsAffected())
	return nil
}
