// upgrade_drill_pg_test.go W11-6 真库演练（OPS_TEST_PG_DSN 门控，缺库先
// bash scripts/reset_test_pg.sh）：回退一版 → dry-run 检出 1 缺口且不写 →
// 实 upgrade 补齐 + 逻辑备份非空含 INSERT → 版本写超常量 → 闸门拒启。
// 回放验证简化为"备份文件非空且含 INSERT"（逻辑备份定位=开发环境，见
// version.go backupTables 注释）；跨版本拒启的分支矩阵在 version_test.go 用
// 假 lookup 穷举，这里只补"真读数链路"一条端到端。
package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestUpgradeDrillOnRealDB(t *testing.T) {
	dsn := os.Getenv("OPS_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("OPS_TEST_PG_DSN not set — upgrade drill skipped")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Skipf("OPS_TEST_PG_DSN unreachable — upgrade drill skipped: %v", err)
	}
	// 关闭注册在最前 → t.Cleanup 是 LIFO，数据清理（注册在后）先跑、连接最后关。
	// （写成 defer conn.Close 的话清理函数面对的是已关闭连接，静默失败把
	// schema_migrations 留在测试库里——第一轮演练实测踩中。）
	t.Cleanup(func() { _ = conn.Close(context.Background()) })

	// 演练起点：schema_migrations 由本测试全权管理（reset_test_pg.sh 不建它）。
	if _, err := conn.Exec(ctx, `DROP TABLE IF EXISTS schema_migrations`); err != nil {
		t.Fatalf("drop schema_migrations: %v", err)
	}
	incidentID := "w116-drill-" + time.Now().Format("20060102150405.000000000")
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer ccancel()
		if _, err := conn.Exec(cctx, `DELETE FROM incident WHERE tenant_id='default' AND incident_id=$1`, incidentID); err != nil {
			t.Errorf("cleanup seeded incident: %v", err)
		}
		if _, err := conn.Exec(cctx, `DROP TABLE IF EXISTS schema_migrations`); err != nil {
			t.Errorf("cleanup schema_migrations: %v", err)
		}
	})

	// 1) 未跟踪库（表缺席）读数 = 0——"空库视为 0"的实证。
	if v, err := querySchemaVersion(ctx, dsn); err != nil || v != 0 {
		t.Fatalf("untracked DB version = (%d, %v), want (0, nil)", v, err)
	}

	// 2) 种子数据：备份必须抓到 incident 行。
	if _, err := conn.Exec(ctx,
		`INSERT INTO tenant (id, name) VALUES ('default','default') ON CONFLICT (id) DO NOTHING`); err != nil {
		t.Fatalf("seed tenant: %v", err)
	}
	if _, err := conn.Exec(ctx,
		`INSERT INTO incident (tenant_id, incident_id, title, severity, state)
		 VALUES ('default', $1, 'W11-6 upgrade 演练单', 'warning', 'open')`, incidentID); err != nil {
		t.Fatalf("seed incident: %v", err)
	}

	// 3) 模拟"落后一版"：登记 1..SchemaMaxVersion-1。
	if _, err := conn.Exec(ctx, ensureSchemaMigrationsDDL); err != nil {
		t.Fatalf("create schema_migrations: %v", err)
	}
	for v := 1; v < SchemaMaxVersion; v++ {
		if _, err := conn.Exec(ctx,
			`INSERT INTO schema_migrations (version) VALUES ($1) ON CONFLICT DO NOTHING`, v); err != nil {
			t.Fatalf("seed version %d: %v", v, err)
		}
	}

	migrationsDir := filepath.Join("..", "..", "migrations")
	backupDir := t.TempDir()
	fixed := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

	// 4) dry-run：只列 1 个缺口，零写入（版本不动、无备份文件）。
	var dry bytes.Buffer
	if err := runUpgrade(upgradeOptions{
		DSN: dsn, MigrationsDir: migrationsDir, BackupDir: backupDir,
		DryRun: true, MaxVersion: SchemaMaxVersion, Out: &dry, Now: func() time.Time { return fixed },
	}); err != nil {
		t.Fatalf("dry-run: %v\n%s", err, dry.String())
	}
	t.Logf("dry-run 输出:\n%s", dry.String())
	out := dry.String()
	if !strings.Contains(out, "pending (1)") || !strings.Contains(out, "v"+strings.TrimSpace(itoa(SchemaMaxVersion))) {
		t.Fatalf("dry-run must list exactly 1 gap at v%d, got:\n%s", SchemaMaxVersion, out)
	}
	if v, err := readSchemaVersion(ctx, conn); err != nil || v != SchemaMaxVersion-1 {
		t.Fatalf("dry-run must not write: version = (%d, %v)", v, err)
	}
	if entries, _ := os.ReadDir(backupDir); len(entries) != 0 {
		t.Fatalf("dry-run must not take a backup, found %v", entries)
	}

	// 5) 实 upgrade：补齐到常量 + 备份非空且含种子行。
	var real bytes.Buffer
	if err := runUpgrade(upgradeOptions{
		DSN: dsn, MigrationsDir: migrationsDir, BackupDir: backupDir,
		DryRun: false, MaxVersion: SchemaMaxVersion, Out: &real, Now: func() time.Time { return fixed },
	}); err != nil {
		t.Fatalf("upgrade: %v\n%s", err, real.String())
	}
	t.Logf("upgrade 输出:\n%s", real.String())
	if v, err := readSchemaVersion(ctx, conn); err != nil || v != SchemaMaxVersion {
		t.Fatalf("after upgrade version = (%d, %v), want %d", v, err, SchemaMaxVersion)
	}
	backupPath := filepath.Join(backupDir, "pre-upgrade-20260102T030405Z.sql")
	data, err := os.ReadFile(backupPath)
	if err != nil {
		t.Fatalf("backup file missing at %s: %v", backupPath, err)
	}
	bak := string(data)
	if !strings.Contains(bak, `INSERT INTO "incident"`) || !strings.Contains(bak, incidentID) {
		t.Fatalf("backup must contain seeded incident INSERT (%s), got head:\n%.500s", incidentID, bak)
	}
	if !strings.Contains(real.String(), backupPath) {
		t.Fatalf("upgrade output should name the backup path")
	}

	// 6) 幂等续跑：无缺口即空转。
	var again bytes.Buffer
	if err := runUpgrade(upgradeOptions{
		DSN: dsn, MigrationsDir: migrationsDir, BackupDir: backupDir,
		MaxVersion: SchemaMaxVersion, Out: &again, Now: func() time.Time { return fixed },
	}); err != nil || !strings.Contains(again.String(), "no pending migrations") {
		t.Fatalf("second upgrade should no-op: (%v)\n%s", err, again.String())
	}

	// 7) 跨版本拒启（真 lookup）：库版本写超常量 → 闸门报错；回滚到相等 → 静默放行。
	newVer := SchemaMaxVersion + 1
	if _, err := conn.Exec(ctx, `INSERT INTO schema_migrations (version) VALUES ($1)`, newVer); err != nil {
		t.Fatalf("simulate newer DB: %v", err)
	}
	var logged []string
	gateLogf := func(format string, args ...any) {
		logged = append(logged, "LOG: "+fmt.Sprintf(format, args...))
	}
	err = startupSchemaGate(ctx, dsn, SchemaMaxVersion, 3, querySchemaVersion, gateLogf)
	if err == nil || !strings.Contains(err.Error(), "旧二进制配新库") {
		t.Fatalf("gate must reject DB-newer-than-binary with the recovery hint, got %v", err)
	}
	t.Logf("跨版本拒启实证: %v", err)
	if _, err := conn.Exec(ctx, `DELETE FROM schema_migrations WHERE version > $1`, SchemaMaxVersion); err != nil {
		t.Fatalf("restore version: %v", err)
	}
	if err := startupSchemaGate(ctx, dsn, SchemaMaxVersion, 3, querySchemaVersion, gateLogf); err != nil {
		t.Fatalf("aligned versions must pass the gate: %v", err)
	}
	if len(logged) != 0 {
		t.Fatalf("pass must be silent, logged: %v", logged)
	}
}

func itoa(n int) string { return strconv.Itoa(n) }
