// Command migrate 是一个零外部依赖（不需要 docker CLI / psql）的纯 Go
// 迁移助手：可选地创建/重建目标库，然后按文件名序把 migrations/*.up.sql
// 逐文件、逐语句应用到目标库。幂等由各迁移自身的 IF NOT EXISTS /
// if_not_exists 保证，可安全重复执行。
//
// 用途（二期池波一-1，测试 PG 数据隔离）：scripts/reset_test_pg.sh 用它
// 建独立测试库 opscopilot_test 并应用全部 schema，让 OPS_TEST_PG_DSN 门控
// 的集成测试不再打共享开发库——开发库数据累积会让契约用例的翻页探针
// （walkAll ≤100 页）偶发变红。
//
// 用法：
//
//	go run ./scripts/migrate -dsn <目标库DSN> [-path migrations] [-create-db] [-wipe]
//
// -create-db : 先在 -admin-db（默认 postgres 库）下 CREATE DATABASE（已存在则跳过）；
// -wipe      : 先 DROP DATABASE ... WITH (FORCE) 再 CREATE（隐含 -create-db）。
// 两者都通过改写 -dsn 的库名连到管理库执行，DSN 只写一遍。
//
// 权限说明：compose 的 POSTGRES_USER=opscopilot 即集群超级用户，可
// CREATE DATABASE；若换用无建库权限的账号，本工具会明确报错退出（提示
// 退化方案见 scripts/reset_test_pg.sh 头注释）。
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

func main() {
	log.SetPrefix("migrate: ")
	log.SetFlags(0)

	dsn := flag.String("dsn", "", "目标库 DSN（必填；迁移应用到这个库）")
	path := flag.String("path", "migrations", "*.up.sql 所在目录")
	createDB := flag.Bool("create-db", false, "目标库不存在则 CREATE DATABASE（经管理库执行）")
	wipe := flag.Bool("wipe", false, "DROP DATABASE (FORCE) 后重建（隐含 -create-db）")
	adminDB := flag.String("admin-db", "postgres", "执行建库/删库的管理库名")
	flag.Parse()

	if *dsn == "" {
		log.Fatal("-dsn 必填")
	}
	if err := run(*dsn, *path, *createDB || *wipe, *wipe, *adminDB); err != nil {
		log.Fatalf("失败: %v", err)
	}
}

func run(dsn, path string, ensure, dropFirst bool, adminDB string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return fmt.Errorf("解析 DSN: %w", err)
	}
	targetDB := cfg.Database

	if ensure {
		if err := ensureDatabase(ctx, cfg, targetDB, adminDB, dropFirst); err != nil {
			return err
		}
	}

	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return fmt.Errorf("连接目标库 %s: %w（若缺库请先加 -create-db）", targetDB, err)
	}
	defer func() { _ = conn.Close(ctx) }()

	files, err := filepath.Glob(filepath.Join(path, "*.up.sql"))
	if err != nil || len(files) == 0 {
		return fmt.Errorf("在 %s 下没有找到 *.up.sql: %w", path, err)
	}
	sort.Strings(files)

	total := 0
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			return fmt.Errorf("读取 %s: %w", f, err)
		}
		stmts := splitStatements(string(data))
		start := time.Now()
		for i, stmt := range stmts {
			if _, err := conn.Exec(ctx, stmt); err != nil {
				return fmt.Errorf("%s 第 %d/%d 条语句失败: %w\n语句: %.200s",
					filepath.Base(f), i+1, len(stmts), err, strings.TrimSpace(stmt))
			}
		}
		total += len(stmts)
		fmt.Printf("✔ %-46s %3d 条语句 (%s)\n", filepath.Base(f), len(stmts), time.Since(start).Round(time.Millisecond))
	}

	var ver string
	if err := conn.QueryRow(ctx, "SELECT extversion FROM pg_extension WHERE extname='timescaledb'").Scan(&ver); err == nil {
		fmt.Printf("迁移完成：%d 个文件 / %d 条语句；timescaledb 扩展 %s 就绪\n", len(files), total, ver)
	} else if !errors.Is(err, pgx.ErrNoRows) {
		fmt.Printf("迁移完成：%d 个文件 / %d 条语句；警告：未检测到 timescaledb 扩展（000003 hypertable 依赖它）\n", len(files), total)
	} else {
		fmt.Printf("迁移完成：%d 个文件 / %d 条语句；警告：timescaledb 扩展未安装（000003 会失败）\n", len(files), total)
	}
	return nil
}

// ensureDatabase 把 cfg 的库名换成管理库连接，按需 DROP(-FORCE)/CREATE 目标库。
func ensureDatabase(ctx context.Context, cfg *pgx.ConnConfig, targetDB, adminDB string, dropFirst bool) error {
	admin := cfg.Copy()
	admin.Database = adminDB
	conn, err := pgx.ConnectConfig(ctx, admin)
	if err != nil {
		return fmt.Errorf("连接管理库 %s 失败: %w", adminDB, err)
	}
	defer func() { _ = conn.Close(ctx) }()

	q := pgx.Identifier{targetDB}.Sanitize()
	if dropFirst {
		// WITH (FORCE)：踢掉还在用旧测试库的会话（pg 连接池未关等）。
		if _, err := conn.Exec(ctx, "DROP DATABASE IF EXISTS "+q+" WITH (FORCE)"); err != nil {
			return fmt.Errorf("DROP DATABASE %s 失败（权限不足？compose 的 opscopilot 是超级用户；"+
				"外部实例请给建库权限或退化为独立 schema 方案）: %w", targetDB, err)
		}
		fmt.Printf("✔ 已删除旧库 %s（--wipe）\n", targetDB)
	}

	var exists bool
	if err := conn.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname=$1)", targetDB).Scan(&exists); err != nil {
		return fmt.Errorf("查询库 %s 是否存在: %w", targetDB, err)
	}
	if exists {
		fmt.Printf("✔ 目标库 %s 已存在（跳过 CREATE；需要彻底重建请加 --wipe）\n", targetDB)
		return nil
	}
	if _, err := conn.Exec(ctx, "CREATE DATABASE "+q); err != nil {
		return fmt.Errorf("CREATE DATABASE %s 失败（权限不足？可退化方案：同库不同 schema + search_path，"+
			"见 scripts/reset_test_pg.sh 头注释）: %w", targetDB, err)
	}
	fmt.Printf("✔ 已创建目标库 %s\n", targetDB)
	return nil
}

// splitStatements 按顶层分号切分 SQL，正确处理：单引号/双引号字符串、
// -- 行注释、/* */ 块注释、$tag$ 美元引号（000003 的 DO $do$ 块内有分号）。
func splitStatements(sql string) []string {
	var out []string
	var cur strings.Builder
	n := len(sql)
	i := 0
	for i < n {
		c := sql[i]
		switch c {
		case '\'', '"':
			cur.WriteByte(c)
			i++
			for i < n {
				if sql[i] == c {
					if i+1 < n && sql[i+1] == c { // 连续引号转义（'' / ""）
						cur.WriteString(sql[i : i+2])
						i += 2
						continue
					}
					cur.WriteByte(c)
					i++
					break
				}
				if c == '\'' && sql[i] == '\\' && i+1 < n && (sql[i+1] == '\'' || sql[i+1] == '\\') {
					// standard_conforming_strings=on 时反斜杠不是转义符，但保守跳过。
					cur.WriteByte(sql[i])
					cur.WriteByte(sql[i+1])
					i += 2
					continue
				}
				cur.WriteByte(sql[i])
				i++
			}
		case '-':
			if i+1 < n && sql[i+1] == '-' {
				for i < n && sql[i] != '\n' {
					cur.WriteByte(sql[i])
					i++
				}
			} else {
				cur.WriteByte(c)
				i++
			}
		case '/':
			if i+1 < n && sql[i+1] == '*' {
				end := strings.Index(sql[i+2:], "*/")
				if end < 0 {
					cur.WriteString(sql[i:])
					i = n
				} else {
					cur.WriteString(sql[i : i+2+end+2])
					i += 2 + end + 2
				}
			} else {
				cur.WriteByte(c)
				i++
			}
		case '$':
			if tag, ok := dollarTag(sql[i:]); ok {
				end := strings.Index(sql[i+len(tag):], tag)
				if end < 0 {
					cur.WriteString(sql[i:])
					i = n
				} else {
					cur.WriteString(sql[i : i+len(tag)+end+len(tag)])
					i += len(tag)*2 + end
				}
			} else {
				cur.WriteByte(c)
				i++
			}
		case ';':
			if s := strings.TrimSpace(cur.String()); s != "" {
				out = append(out, s)
			}
			cur.Reset()
			i++
		default:
			cur.WriteByte(c)
			i++
		}
	}
	if s := strings.TrimSpace(cur.String()); s != "" {
		out = append(out, s)
	}
	return out
}

// dollarTag 识别 $tag$ / $$ 开标记（PG 规则：可跟字母下划线，不能以数字开头）。
func dollarTag(s string) (string, bool) {
	j := 1
	for j < len(s) && s[j] != '$' {
		c := s[j]
		ok := c == '_' || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
			(j > 1 && c >= '0' && c <= '9') // 首位不允许数字
		if !ok {
			return "", false
		}
		j++
	}
	if j >= len(s) || s[j] != '$' {
		return "", false
	}
	if j > 1 && s[1] >= '0' && s[1] <= '9' {
		return "", false
	}
	return s[:j+1], true
}
