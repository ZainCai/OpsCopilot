// upgrade.go `opscopilot upgrade` 子命令主体（原 version.go 拆分，2026-09-14）。
// 连库 -> 列缺口 -> 应用前逻辑备份 -> 逐迁移应用并登记版本；
// 表形状探测与兼容登记（P1-2）同在此文件。
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// schema_migrations 表由 upgrade 首次执行时创建（本仓库的迁移文件从不碰它，
// scripts/migrate 全量建库路径也不写它——表缺席 = 空库/未跟踪库，版本读 0）。
// 注意 CREATE TABLE IF NOT EXISTS 对**已存在**的表是 no-op：README/ADR-006 确立
// `migrate -path migrations … up`（golang-migrate）为主线通道，它建的表形状是
// (version bigint, dirty boolean NOT NULL) **无 PK 无默认值**——与本表形状不同、
// 且没有可供 ON CONFLICT 用的唯一约束。故登记语句必须按表形状探测分流，
// 见 registerVersion / schemaMigrationsShape。
const ensureSchemaMigrationsDDL = `CREATE TABLE IF NOT EXISTS schema_migrations (
    version    integer PRIMARY KEY,
    applied_at timestamptz NOT NULL DEFAULT now()
)`

// ---------- upgrade 子命令 ----------

// upgradeOptions runUpgrade 的显式输入（测试直接构造，不碰 env/cwd）。
type upgradeOptions struct {
	DSN           string
	MigrationsDir string // 默认 "migrations"
	BackupDir     string // 默认 "backups"
	DryRun        bool
	MaxVersion    int // SchemaMaxVersion（main 注入；测试可覆盖）
	Out           io.Writer
	Now           func() time.Time // 备份文件名时间戳（测试注入）
}

// runUpgrade 执行 `opscopilot upgrade` 的主体。--dry-run 只读：连库、读数、
// 列缺口，绝不建表/备份/应用。
func runUpgrade(opts upgradeOptions) error {
	if opts.DSN == "" {
		return fmt.Errorf("DSN 为空（upgrade 需要 OPS_DB_DSN；无库 = 无 schema 可升）")
	}
	out := opts.Out
	if out == nil {
		out = os.Stdout
	}
	if opts.MigrationsDir == "" {
		opts.MigrationsDir = "migrations"
	}
	if opts.BackupDir == "" {
		opts.BackupDir = "backups"
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}

	files, err := listMigrations(opts.MigrationsDir)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	conn, err := pgx.Connect(ctx, opts.DSN)
	if err != nil {
		return fmt.Errorf("连接 %s 失败: %w（DSN 来自 OPS_DB_DSN）", redactDSN(opts.DSN), err)
	}
	defer func() { _ = conn.Close(ctx) }()

	cur, err := readSchemaVersion(ctx, conn)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "schema_migrations 当前版本: %d；二进制 SchemaMaxVersion: %d\n", cur, opts.MaxVersion)
	if cur > opts.MaxVersion {
		return fmt.Errorf("DB 版本 %d 高于本二进制支持上限 %d —— 旧二进制拒绝升级新库（与启动闸门同律），"+
			"请换配套二进制", cur, opts.MaxVersion)
	}

	var pending []migrationFile
	for _, f := range files {
		if f.version > cur {
			pending = append(pending, f)
		}
	}
	if len(pending) == 0 {
		fmt.Fprintf(out, "no pending migrations (already at v%d) — 无需升级\n", cur)
		return nil
	}

	if opts.DryRun {
		fmt.Fprintf(out, "pending (%d) — dry-run，只列缺口不写入:\n", len(pending))
		for _, f := range pending {
			fmt.Fprintf(out, "  v%-4d %s\n", f.version, filepath.Base(f.path))
		}
		return nil
	}

	// 备份先于一切写入：失败即中止，此时 DB 尚未被动过（上面的读数全是 SELECT）。
	backupPath, err := backupBeforeUpgrade(ctx, conn, opts.BackupDir, now())
	if err != nil {
		return fmt.Errorf("upgrade 中止：应用前备份失败（DB 未被改动）: %w", err)
	}
	fmt.Fprintf(out, "逻辑备份已写入: %s\n", backupPath)

	if _, err := conn.Exec(ctx, ensureSchemaMigrationsDDL); err != nil {
		return fmt.Errorf("建 schema_migrations 跟踪表: %w", err)
	}
	// P1-2：表形状探测分流。IF NOT EXISTS 对已存在的表是 no-op——golang-migrate
	// 主线库留下来的是 (version bigint, dirty NOT NULL) 无 PK 形状，在它上面
	// ON CONFLICT (version) 必报 42P10、裸 INSERT (version) 撞 dirty NOT NULL，
	// 迁移半程退出（语句已落库、版本未登记）重跑同处死循环。登记语句按
	// 探测结果选形，两种形状都能走完整 upgrade 流程。
	shape, err := probeSchemaMigrationsShape(ctx, conn)
	if err != nil {
		return fmt.Errorf("探测 schema_migrations 形状: %w", err)
	}
	fmt.Fprintf(out, "schema_migrations 形状: %s\n", shape.describe())

	fmt.Fprintf(out, "开始应用 %d 个迁移（逐语句执行，与 scripts/migrate 同律；"+
		"各迁移自带 IF NOT EXISTS 幂等，中途失败修复后重跑 upgrade 即续传）:\n", len(pending))
	for _, f := range pending {
		data, err := os.ReadFile(f.path)
		if err != nil {
			return fmt.Errorf("读取 %s: %w", f.path, err)
		}
		stmts := splitStatements(string(data))
		start := time.Now()
		for i, stmt := range stmts {
			if _, err := conn.Exec(ctx, stmt); err != nil {
				return fmt.Errorf("%s 第 %d/%d 条语句失败: %w\n语句: %.200s\n"+
					"（版本未记录——修复后重跑 upgrade 会从 v%d 续apply）",
					filepath.Base(f.path), i+1, len(stmts), err, strings.TrimSpace(stmt), f.version-1)
			}
		}
		if err := registerVersion(ctx, conn, shape, f.version); err != nil {
			return fmt.Errorf("记录版本 %d: %w", f.version, err)
		}
		fmt.Fprintf(out, "  ✔ v%-4d %-46s %3d 条语句 (%s)\n",
			f.version, filepath.Base(f.path), len(stmts), time.Since(start).Round(time.Millisecond))
	}

	final, err := readSchemaVersion(ctx, conn)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "upgrade 完成: v%d -> v%d；备份: %s\n", cur, final, backupPath)
	return nil
}

// ---------- 跟踪表形状探测与兼容登记（P1-2） ----------

// schemaMigrationsShape schema_migrations 形状探测结果（登记语句的兼容开关）。
// 两个主流形状：
//
//	versionUniq  version 单列 PK/唯一约束——opscopilot 自建形状
//	             （ensureSchemaMigrationsDDL），可用 ON CONFLICT (version)；
//	hasDirty     有 dirty 列——golang-migrate 主线形状 (version bigint,
//	             dirty boolean NOT NULL) 且无 PK：ON CONFLICT 无约束可用、
//	             裸 INSERT 撞 NOT NULL，且 golang-migrate 约定"单行=当前版本"。
type schemaMigrationsShape struct {
	versionUniq bool
	hasDirty    bool
}

func (s schemaMigrationsShape) describe() string {
	switch {
	case s.versionUniq:
		return "自建形状（version 唯一约束可用 → ON CONFLICT 登记）"
	case s.hasDirty:
		return "golang-migrate 主线形状（无唯一约束、dirty NOT NULL → 单行 DELETE+INSERT 同事务登记）"
	default:
		return "无唯一约束、无 dirty 列（INSERT…SELECT WHERE NOT EXISTS 登记）"
	}
}

// probeSchemaMigrationsShape 只读探测（information_schema/pg_constraint，不改
// 任何数据）。调用前提：表已存在（upgrade 在建表/IF NOT EXISTS 之后才探测）。
func probeSchemaMigrationsShape(ctx context.Context, conn *pgx.Conn) (schemaMigrationsShape, error) {
	var sh schemaMigrationsShape
	// version 单列 PK/唯一约束存在性（ON CONFLICT (version) 的可用性判据）。
	if err := conn.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			  FROM pg_constraint c
			  JOIN pg_attribute a
			    ON a.attrelid = c.conrelid AND a.attnum = c.conkey[1]
			 WHERE c.conrelid = to_regclass('schema_migrations')
			   AND c.contype IN ('p', 'u')
			   AND cardinality(c.conkey) = 1
			   AND a.attname = 'version')`).Scan(&sh.versionUniq); err != nil {
		return sh, fmt.Errorf("probe version unique constraint: %w", err)
	}
	// dirty 列存在性（golang-migrate 形状判据；current_schema 限定，与备份取列同口径）。
	if err := conn.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			 WHERE table_name = 'schema_migrations'
			   AND table_schema = current_schema()
			   AND column_name = 'dirty')`).Scan(&sh.hasDirty); err != nil {
		return sh, fmt.Errorf("probe dirty column: %w", err)
	}
	return sh, nil
}

// registerVersion 登记"版本 N 已应用"（幂等：中途失败修复后重跑 upgrade 可
// 续传）。按探测到的表形状选形——这是对旧版"无条件 ON CONFLICT (version)
// DO NOTHING"只在自建 PK 形状上成立、在 golang-migrate 主线形状上报 42P10
// 的修复（P1-2：语句已落库、版本未登记的半程退出死循环）。
func registerVersion(ctx context.Context, conn *pgx.Conn, sh schemaMigrationsShape, version int) error {
	switch {
	case sh.versionUniq:
		// 自建形状：version 列自带唯一约束，ON CONFLICT 最短路径；
		// applied_at 走列默认值 now()。
		_, err := conn.Exec(ctx,
			`INSERT INTO schema_migrations (version) VALUES ($1) ON CONFLICT (version) DO NOTHING`,
			version)
		return err
	case sh.hasDirty:
		// golang-migrate 主线形状：无唯一约束可用（ON CONFLICT → 42P10），
		// dirty NOT NULL 无默认（裸 INSERT(version) 插不进）。且 golang-migrate
		// 的读侧约定是"全表只有一行 = 当前版本"（SELECT version, dirty LIMIT 1
		// 不带 ORDER BY）——追加多行会让它随机读到旧版本。故按 golang-migrate
		// 自身的语义重写台账：清掉整表再写唯一一行 (N, false)。false = 不脏，
		// 与"该版本迁移语句全部成功应用后才登记"的含义一致。两条语句必须同
		// 事务：拆开存在"DELETE 已提交、INSERT 失败"的半窗，MAX(version) 会
		// 倒退到更低版本，下次启动闸门按假读数误判。
		tx, err := conn.Begin(ctx)
		if err != nil {
			return fmt.Errorf("开登记事务: %w", err)
		}
		defer func() { _ = tx.Rollback(ctx) }() // Commit 成功后 Rollback 无害
		if _, err := tx.Exec(ctx, `DELETE FROM schema_migrations`); err != nil {
			return fmt.Errorf("清 golang-migrate 台账行: %w", err)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO schema_migrations (version, dirty) VALUES ($1, false)`, version); err != nil {
			return fmt.Errorf("写 golang-migrate 台账行: %w", err)
		}
		return tx.Commit(ctx)
	default:
		// 兜底形状（无唯一约束、无 dirty，如手建的裸表）：
		// INSERT…SELECT WHERE NOT EXISTS 不依赖任何约束即幂等；读-插在同一
		// 语句内完成，单语句本身即原子（无需显式事务）。
		_, err := conn.Exec(ctx, `
			INSERT INTO schema_migrations (version)
			SELECT $1 WHERE NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)`,
			version)
		return err
	}
}
