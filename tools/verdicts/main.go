// verdicts 导出影子判决（D12=B / W6-3 评估的数据源）。
//
// 为什么要有这个工具：tools/evaluate.py 原本用 `docker exec psql` 取数——
// 本机没有 Docker（TimescaleDB 是原生服务）。本工具用项目已有的 pgx 直连，
// 输出与 evaluate.py 打分所需字段一致的 JSON，供 `--verdicts-file` 消费。
//
// 用法：
//
//	OPS_DB_DSN=postgres://... go run ./tools/verdicts -out verdicts.json
//	tools/verdicts.exe -dsn postgres://... -since 168h -out verdicts.json
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// verdict 一条影子判决（与 evaluate.py 的打分字段一一对应）。
type verdict struct {
	OccurredAt  time.Time `json:"occurred_at"`
	ClusterKey  string    `json:"cluster_key"`
	Fingerprint string    `json:"fingerprint"`
	Reason      string    `json:"reason"`
}

// resolveTenant 租户取值口径：flag/环境值 > "default"。
// 空租户会让查询静默返回 0 行，进而让 evaluate.py 产生"静默段全 PASS"
// 的假 100%（第七轮 H3）——必须兜底。
func resolveTenant(v string) string {
	if t := strings.TrimSpace(v); t != "" {
		return t
	}
	return "default"
}

func main() {
	var (
		dsn    = flag.String("dsn", os.Getenv("OPS_DB_DSN"), "PG DSN（缺省读 OPS_DB_DSN）")
		since  = flag.Duration("since", 7*24*time.Hour, "只取最近 N 时长的判决")
		out    = flag.String("out", "", "输出文件（缺省 stdout）")
		tenant = flag.String("tenant", os.Getenv("OPS_TENANT"), "租户（缺省读 OPS_TENANT，再缺省 default）")
	)
	flag.Parse()
	*tenant = resolveTenant(*tenant) // 第七轮 H3：空租户会静默查 0 行（假 100%）
	if *dsn == "" {
		fmt.Fprintln(os.Stderr, "no DSN (set OPS_DB_DSN or -dsn)")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, *dsn)
	if err != nil {
		fmt.Fprintln(os.Stderr, "connect:", err)
		os.Exit(1)
	}
	defer pool.Close()

	rows, err := pool.Query(ctx, `
SELECT occurred_at, cluster_key, fingerprint, payload->>'reason'
FROM alert_event
WHERE source='shadow' AND tenant_id=$2 AND occurred_at > now() - $1::interval
ORDER BY occurred_at`, *since, *tenant)
	if err != nil {
		fmt.Fprintln(os.Stderr, "query:", err)
		os.Exit(1)
	}
	defer rows.Close()

	list := []verdict{}
	for rows.Next() {
		var v verdict
		if err := rows.Scan(&v.OccurredAt, &v.ClusterKey, &v.Fingerprint, &v.Reason); err != nil {
			fmt.Fprintln(os.Stderr, "scan:", err)
			os.Exit(1)
		}
		list = append(list, v)
	}
	if err := rows.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "rows:", err)
		os.Exit(1)
	}
	if len(list) == 0 {
		// 空结果往往意味着租户/窗口配错——静默输出会让 evaluate.py 产生
		// "静默段全 PASS"的假 100%（第七轮 H3），必须让操作者看见。
		fmt.Fprintf(os.Stderr, "WARNING: 0 verdicts for tenant %q since %v — check OPS_TENANT/window\n", *tenant, *since)
	}

	data, err := json.MarshalIndent(list, "", "  ")
	if err != nil {
		fmt.Fprintln(os.Stderr, "marshal:", err)
		os.Exit(1)
	}
	if *out == "" {
		os.Stdout.Write(data)
		fmt.Println()
		return
	}
	if err := os.WriteFile(*out, data, 0o644); err != nil {
		fmt.Fprintln(os.Stderr, "write:", err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "wrote %d verdicts to %s\n", len(list), *out)
}
