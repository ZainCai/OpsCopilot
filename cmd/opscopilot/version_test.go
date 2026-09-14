// version_test.go W11-6 单测：常量防漂移、闸门矩阵（假 lookup）、清单解析、
// 参数分流、备份字面量与语句切分。真库演练在 upgrade_drill_pg_test.go。
package main

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestSchemaMaxVersionMatchesMigrationsDir 防漂移钉：SchemaMaxVersion 是手工
// 维护常量（embed 不了仓库外目录，见 version.go 文件头），新增迁移忘改常量
// 在这里变红，而不是等生产闸门误判。
func TestSchemaMaxVersionMatchesMigrationsDir(t *testing.T) {
	files, err := listMigrations(filepath.Join("..", "..", "migrations"))
	if err != nil {
		t.Fatalf("listMigrations: %v", err)
	}
	max := files[len(files)-1].version
	if max != SchemaMaxVersion {
		t.Errorf("migrations/ 最大编号 v%d != SchemaMaxVersion 常量 v%d —— 新增迁移后须同步 bump 常量（或回滚迁移文件）",
			max, SchemaMaxVersion)
	}
}

func TestListMigrationsSortedAndContiguous(t *testing.T) {
	files, err := listMigrations(filepath.Join("..", "..", "migrations"))
	if err != nil {
		t.Fatalf("listMigrations: %v", err)
	}
	if len(files) < 20 {
		t.Fatalf("expected >=20 migrations, got %d", len(files))
	}
	for i, f := range files {
		if want := i + 1; f.version != want {
			t.Fatalf("files[%d].version = %d, want %d（版本序列须从 1 连续递增）", i, f.version, want)
		}
	}
}

// TestStartupSchemaGate 决策矩阵（maxVersion=20，gap=3）：
//
//	相等            → 放行、不打日志
//	落后 1..3       → 放行 + WARNING
//	落后 4（>gap）  → 拒启
//	DB 更新（>20）  → 拒启（旧二进制配新库）
//	读 0 + 域表已在（未跟踪存量库，P2-D4）→ 放行 + WARNING 提示登记
//	lookup 报错     → 放行 + WARNING（DB 不可达不拦，降级哲学）
func TestStartupSchemaGate(t *testing.T) {
	cases := []struct {
		name    string
		dbVer   int
		legacy  bool // version==0 且域表存在（scripts/migrate/psql 建的未跟踪存量库）
		lookup  error
		wantErr bool
		wantLog string
	}{
		{"equal-pass-silent", 20, false, nil, false, ""},
		{"behind-1-warn", 19, false, nil, false, "BEHIND"},
		{"behind-equals-gap-warn", 17, false, nil, false, "BEHIND"},
		{"behind-gap-plus-1-reject", 16, false, nil, true, ""},
		{"fresh-empty-db-reject", 0, false, nil, true, ""},
		{"untracked-legacy-pass-warn", 0, true, nil, false, "untracked"},
		{"db-newer-reject", 21, false, nil, true, "旧二进制配新库"},
		{"lookup-error-pass-warn", 0, false, errors.New("dial refused"), false, "SKIPPED"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var logged []string
			lookup := func(ctx context.Context, dsn string) (schemaProbe, error) {
				if tc.lookup != nil {
					return schemaProbe{}, tc.lookup
				}
				return schemaProbe{version: tc.dbVer, legacyUntracked: tc.legacy}, nil
			}
			err := startupSchemaGate(context.Background(), "dsn://x", 20, 3, lookup,
				func(format string, args ...any) {
					logged = append(logged, fmt.Sprintf(format, args...))
				})
			if tc.wantErr && err == nil {
				t.Fatal("want reject error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("want pass, got %v", err)
			}
			joined := strings.Join(logged, "\n")
			if err != nil {
				joined += "\n" + err.Error() // 拒启案例的关键词在错误信息里
			}
			if tc.wantLog == "" && len(logged) != 0 {
				t.Fatalf("want silent, logged: %s", strings.Join(logged, "\n"))
			}
			if tc.wantLog != "" && !strings.Contains(joined, tc.wantLog) {
				t.Fatalf("log/err %q missing %q", joined, tc.wantLog)
			}
			if tc.wantErr && !strings.Contains(err.Error(), "upgrade") {
				t.Fatalf("reject error should name the way out (opscopilot upgrade): %v", err)
			}
		})
	}
}

func TestParseUpgradeArgs(t *testing.T) {
	// 默认无参 = server：与 W11-6 之前逐字节一致。
	up, dry, path, usage := parseUpgradeArgs(nil)
	if up || dry || path != "migrations" || usage != "" {
		t.Fatalf("no-arg must be plain server mode: %v %v %q %q", up, dry, path, usage)
	}
	// 未知首参同样落 server（旧行为：参数被忽略），不新增拒绝面。
	if up, _, _, _ := parseUpgradeArgs([]string{"--whatever"}); up {
		t.Fatal("unknown first arg must NOT be treated as upgrade")
	}
	up, dry, path, usage = parseUpgradeArgs([]string{"upgrade", "--dry-run"})
	if !up || !dry || path != "migrations" || usage != "" {
		t.Fatalf("upgrade --dry-run: %v %v %q %q", up, dry, path, usage)
	}
	up, dry, path, usage = parseUpgradeArgs([]string{"upgrade", "--path", "db/migrations"})
	if !up || dry || path != "db/migrations" || usage != "" {
		t.Fatalf("upgrade --path: %v %v %q %q", up, dry, path, usage)
	}
	if _, _, _, usage := parseUpgradeArgs([]string{"upgrade", "--nope"}); usage == "" {
		t.Fatal("unknown upgrade flag must produce usage error")
	}
}

// TestRunUpgradeRequiresDSN runUpgrade 的 env-free 入口守卫（不碰库）。
func TestRunUpgradeRequiresDSN(t *testing.T) {
	err := runUpgrade(upgradeOptions{})
	if err == nil || !strings.Contains(err.Error(), "OPS_DB_DSN") {
		t.Fatalf("empty DSN must fail pointing at OPS_DB_DSN, got %v", err)
	}
}

func TestSQLTextLiteral(t *testing.T) {
	if got := sqlTextLiteral(nil); got != "NULL" {
		t.Fatalf("nil: %s", got)
	}
	if got := sqlTextLiteral("O'Brien"); got != "'O''Brien'" {
		t.Fatalf("quote escape: %s", got)
	}
	if got := sqlTextLiteral([]byte("raw")); got != "'raw'" {
		t.Fatalf("bytes: %s", got)
	}
}

// TestBackupFileHeaderInjectsSCS P2-C3：备份文件头注入会话设置——
// sqlTextLiteral 双写单引号的正确性依赖 standard_conforming_strings=on，
// 恢复重放本文件先设好，不依赖恢复会话默认值。
func TestBackupFileHeaderInjectsSCS(t *testing.T) {
	h := backupFileHeader(time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC))
	if !strings.HasPrefix(h, "-- opscopilot upgrade 应用前逻辑备份") {
		t.Fatalf("header must start with the banner, got %q", h)
	}
	for _, want := range []string{
		"SET standard_conforming_strings = on;",
		"-- 定位：开发/演示库的文本级快照",
	} {
		if !strings.Contains(h, want) {
			t.Fatalf("header missing %q\n---\n%s", want, h)
		}
	}
}

// TestRedactDSN P2-C2：URL 与 keyword 两种 DSN 形态的口令都打码（此前
// keyword 形态可把 password= 原样带进连接失败错误 / 启动 WARNING 日志）。
func TestRedactDSN(t *testing.T) {
	cases := []struct{ in, want string }{
		{"postgres://opscopilot:secret@localhost:5432/opscopilot?sslmode=disable",
			"***@localhost:5432/opscopilot?sslmode=disable"},
		{"host=localhost port=5432 user=opscopilot password=secret sslmode=disable",
			"host=localhost port=5432 user=opscopilot password=***"},
		{"host=localhost password= sslmode=disable",
			"host=localhost password=***"},
		{"postgres://localhost/opscopilot", "postgres://localhost/opscopilot"},
		{"host=localhost user=opscopilot sslmode=disable", "host=localhost user=opscopilot sslmode=disable"},
	}
	for _, c := range cases {
		if got := redactDSN(c.in); got != c.want {
			t.Fatalf("redactDSN(%q) = %q, want %q", c.in, got, c.want)
		}
	}
	// 任何形态都不该出现明文口令段（含 URL query 里的 password=）。
	for _, in := range []string{
		"postgres://opscopilot:supersecret@h:5432/db",
		"host=h password=supersecret",
		"postgres://u:p@h/db?password=querysecret",
	} {
		if got := redactDSN(in); strings.Contains(got, "supersecret") || strings.Contains(got, "querysecret") {
			t.Fatalf("redactDSN leaked secret: %q -> %q", in, got)
		}
	}
}

// TestSplitStatementsDollarBlock upgrade 与 scripts/migrate 必须同规则切语句
// （000003 的 DO $do$ 块内分号不能被切开）——这里是复制版的回归钉。
func TestSplitStatementsDollarBlock(t *testing.T) {
	sql := "SELECT 1;\nDO $do$\nBEGIN\n  PERFORM 1;\nEND $do$;\n-- comment with ;\nSELECT 'a;b';"
	stmts := splitStatements(sql)
	if len(stmts) != 3 {
		t.Fatalf("want 3 statements, got %d: %#v", len(stmts), stmts)
	}
	if !strings.Contains(stmts[1], "PERFORM 1;") {
		t.Fatalf("dollar block was split apart: %q", stmts[1])
	}
	// 行注释黏在下一条语句头部一起下发（与 scripts/migrate 同律：注释里的
	// 分号、引号字面量里的分号都不切）——关键是没被劈成两条。
	if !strings.Contains(stmts[2], "SELECT 'a;b'") || !strings.Contains(stmts[2], "-- comment with ;") {
		t.Fatalf("comment/quoted-literal semicolon split: %q", stmts[2])
	}
}

func TestSchemaMaxVersionSane(t *testing.T) {
	if SchemaMaxVersion < 1 {
		t.Fatalf("SchemaMaxVersion must be positive, got %d", SchemaMaxVersion)
	}
	_ = strconv.Itoa(SchemaMaxVersion)
}
