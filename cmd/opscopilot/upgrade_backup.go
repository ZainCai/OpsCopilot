// upgrade_backup.go 应用前逻辑备份（原 version.go 拆分，2026-09-14）。
// 逐表 SELECT 导出 INSERT 文本到 backups/pre-upgrade-<ts>.sql；
// alert_event 为 hypertable 限行导出（backupAlertEventLimit）。
package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// ---------- 应用前逻辑备份 ----------

// backupTables 固定备份清单：人的工单域 + 机器簇域 + 变更证据域 + runbook 三表 +
// 原始告警。**逻辑备份定位 = 开发/演示库**：跨表文本快照，够支撑
// "upgrade 前存档 → 出问题回滚" 的演练闭环，不承诺生产级完备性
// （无 sequence/catalog、无权限、alert_event 限量，见下）。
var backupTables = []string{
	"incident", "incident_cluster", "incident_audit",
	"change_record", "alert_cluster",
	"runbook", "incident_runbook", "runbook_execution_log",
	"alert_event",
}

// backupAlertEventLimit alert_event 是 hypertable，顶层 SELECT 即可读全量，
// 但量大时逻辑备份会把内存/磁盘撑爆——限量导出并在文件里注明（超出部分
// 属原始告警，可由 Redis 镜像/上游重放重建，不在回滚关键路径上）。
const backupAlertEventLimit = 100000

// backupFileHeader 备份文件头文本（P2-C3：注入 standard_conforming_strings
// 会话设置——sqlTextLiteral 只双写单引号，其正确性依赖该设置；恢复 = 在
// 空库上重放本文件，先设好，字面量规则不依赖恢复会话默认值）。
func backupFileHeader(ts time.Time) string {
	return fmt.Sprintf("-- opscopilot upgrade 应用前逻辑备份（%s UTC）\n"+
		"-- 定位：开发/演示库的文本级快照，非物理备份；恢复 = 在空库上重放本文件。\n"+
		"SET standard_conforming_strings = on;\n\n", ts.UTC().Format(time.RFC3339))
}

// backupBeforeUpgrade 逐表 SELECT 导出 INSERT 文本到 backups/pre-upgrade-<ts>.sql。
// 列一律 ::text 取回（NULL 保持 NULL）：一套字面量规则覆盖 JSONB/时间戳/数组，
// 不逐列做类型方言。表不存在（空库首装）跳过并注明。
func backupBeforeUpgrade(ctx context.Context, conn *pgx.Conn, dir string, ts time.Time) (string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("建备份目录 %s: %w", dir, err)
	}
	path := filepath.Join(dir, fmt.Sprintf("pre-upgrade-%s.sql", ts.UTC().Format("20060102T150405Z")))
	f, err := os.Create(path)
	if err != nil {
		return "", fmt.Errorf("创建备份文件: %w", err)
	}
	defer func() { _ = f.Close() }()
	w := bufio.NewWriter(f)

	fmt.Fprint(w, backupFileHeader(ts))

	for _, table := range backupTables {
		cols, err := backupColumns(ctx, conn, table)
		if err != nil {
			return "", fmt.Errorf("备份 %s 取列: %w", table, err)
		}
		if len(cols) == 0 {
			fmt.Fprintf(w, "\n-- skip: 表 %s 不存在（空库首装，无可备份）\n", table)
			continue
		}
		query := fmt.Sprintf("SELECT %s FROM %s", castColumns(cols), pgx.Identifier{table}.Sanitize())
		limited := false
		if table == "alert_event" {
			query = fmt.Sprintf("%s LIMIT %d", query, backupAlertEventLimit)
			limited = true
		}
		fmt.Fprintf(w, "\n-- ---- %s ----\n", table)
		n, truncated, err := dumpTable(ctx, conn, w, table, cols, query, limited)
		if err != nil {
			return "", fmt.Errorf("备份 %s: %w", table, err)
		}
		if truncated {
			fmt.Fprintf(w, "-- 注意：alert_event 为 hypertable，本次逻辑备份仅前 %d 行（限量即其开发环境定位的一部分）\n",
				backupAlertEventLimit)
		}
		fmt.Fprintf(w, "-- %s: %d 行\n", table, n)
	}
	if err := w.Flush(); err != nil {
		return "", fmt.Errorf("flush 备份文件: %w", err)
	}
	if err := f.Close(); err != nil {
		return "", fmt.Errorf("关闭备份文件: %w", err)
	}
	return path, nil
}

// backupColumns 返回表的列名（按序）；表不存在返回空切片（调用方决定跳过）。
// current_schema() 限定，避开 information_schema 里系统/扩展 schema 的同名表。
func backupColumns(ctx context.Context, conn *pgx.Conn, table string) ([]string, error) {
	var exists bool
	if err := conn.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, table).Scan(&exists); err != nil {
		return nil, err
	}
	if !exists {
		return nil, nil
	}
	rows, err := conn.Query(ctx,
		`SELECT column_name FROM information_schema.columns
		  WHERE table_name = $1 AND table_schema = current_schema()
		  ORDER BY ordinal_position`, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var cols []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return nil, err
		}
		cols = append(cols, c)
	}
	return cols, rows.Err()
}

func castColumns(cols []string) string {
	parts := make([]string, len(cols))
	for i, c := range cols {
		parts[i] = fmt.Sprintf("%s::text", pgx.Identifier{c}.Sanitize())
	}
	return strings.Join(parts, ", ")
}

// dumpTable 执行导出查询并把每行写成一条 INSERT（列名显式、值走 sqlTextLiteral）。
func dumpTable(ctx context.Context, conn *pgx.Conn, w io.Writer, table string, cols []string,
	query string, limited bool) (rowsN int, truncated bool, err error) {
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return 0, false, err
	}
	defer rows.Close()
	colList := make([]string, len(cols))
	for i, c := range cols {
		colList[i] = pgx.Identifier{c}.Sanitize()
	}
	insertPrefix := fmt.Sprintf("INSERT INTO %s (%s) VALUES (", pgx.Identifier{table}.Sanitize(), strings.Join(colList, ", "))

	values := make([]any, len(cols))
	targets := make([]any, len(cols))
	for i := range values {
		targets[i] = &values[i]
	}
	for rows.Next() {
		if err := rows.Scan(targets...); err != nil {
			return rowsN, truncated, err
		}
		parts := make([]string, len(values))
		for i, v := range values {
			parts[i] = sqlTextLiteral(v)
		}
		fmt.Fprintf(w, "%s%s);\n", insertPrefix, strings.Join(parts, ", "))
		rowsN++
	}
	if err := rows.Err(); err != nil {
		return rowsN, truncated, err
	}
	return rowsN, limited && rowsN >= backupAlertEventLimit, nil
}

// sqlTextLiteral 值 → SQL 字面量。备份查询所有列都 ::text 取回，故正常只会
// 命中 nil/string；[]byte 与兜底分支防驱动方言变化时静默丢数据。
// 单引号按 SQL 标准双写；其余原样进引号（::text 输出对 PG 字面量无损）。
// 依赖：备份文件头已注入 `SET standard_conforming_strings = on`（P2-C3）——
// 该会话设置下反斜杠是普通字符，双写单引号即完整转义。
func sqlTextLiteral(v any) string {
	switch x := v.(type) {
	case nil:
		return "NULL"
	case string:
		return "'" + strings.ReplaceAll(x, "'", "''") + "'"
	case []byte:
		return "'" + strings.ReplaceAll(string(x), "'", "''") + "'"
	default:
		return "'" + strings.ReplaceAll(fmt.Sprint(x), "'", "''") + "'"
	}
}
