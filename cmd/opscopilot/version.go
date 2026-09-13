// version.go W11-6：schema 版本闸门（跨版本拒启）+ `opscopilot upgrade` 子命令。
//
// 两个入口共用同一套骨架（schema_migrations 读数 + migrations/*.up.sql 清单）：
//
//  1. server 启动（main.go 有 DSN 时）调 startupSchemaGate：
//     DB 版本 > 本二进制 SchemaMaxVersion → 拒启（旧二进制配新库是 schema
//     回写事故的高发区，宁拒勿猜）；DB 落后 ≤ OPS_MAX_VERSION_GAP（默认 3）
//     → 响亮 WARNING 继续起；落后更多 → 拒启。无 DSN 直接跳过校验——与
//     ADR-001 "库缺席只影响重建能力" 的内存降级哲学一致；DB 不可达同样只
//     WARNING 不拦（PG 连接失败在装配层本就是降级路径，闸门不升级这件事的
//     后果等级）。
//  2. `opscopilot upgrade [--dry-run]`：连 DSN → 对比 schema_migrations 与
//     migrations/*.up.sql 清单 → 应用前先导出逻辑备份（备份失败即中止，DB
//     不动）→ 缺口逐个应用并记录版本。--dry-run 全程只读。
//
// SchemaMaxVersion 是构建期常量而非 go:embed：cmd 目录与 migrations/ 不同根、
// 部署产物里也不保证有迁移目录，embed 只会把"仓库布局"焊进二进制。漂移防线
// 走 version_test.go 的断言（新增迁移忘改常量 → 测试红），这是该方案的全部
// 成本，也是它敢手工维护的前提。
package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// SchemaMaxVersion 本二进制期望的 schema 版本（= migrations/ 最大编号）。
// **手工维护**：新增 migrations/0000NN_*.up.sql 时必须同步 +1，
// TestSchemaMaxVersionMatchesMigrationsDir 会钉死这一点（忘改即红）。
const SchemaMaxVersion = 21

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

// schemaVersionSQL 当前版本读数。注意：**不能**用
// "CASE WHEN to_regclass(...) IS NULL THEN 0 ELSE (SELECT MAX(version) FROM
// schema_migrations)" 一条语句兜住表缺席——PG 在解析期就会因为引用的表不存在
// 直接抛 42P01，根本走不到运行时分支。所以先探存在性、再读数（两步都只读）。
const schemaVersionSQL = `SELECT COALESCE(MAX(version), 0)::bigint FROM schema_migrations`

const schemaMigrationsExistsSQL = `SELECT to_regclass('schema_migrations') IS NOT NULL`

// rowQuerier 收敛 *pgx.Conn / *pgxpool.Pool 的公共查询面（读数函数两处可用）。
type rowQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// schemaProbe 启动闸门读数：库内版本 + "未跟踪存量库"判别。
type schemaProbe struct {
	version int
	// legacyUntracked = 版本读 0 但域表（alert_event/incident/change_record
	// 任一）已存在：多半是 scripts/migrate 或 psql 通道建的全 schema 存量库
	// （两条通道都不维护 schema_migrations），**不是**新建空库。闸门据此放行
	// + 响亮提示跑 upgrade 登记，而不是按"落后 20 > gap"误拒启（P2-D4）。
	legacyUntracked bool
}

// schemaVersionLookup 可注入的闸门读数（startupSchemaGate 的单测用假 lookup，
// 不碰真库）。
type schemaVersionLookup func(ctx context.Context, dsn string) (schemaProbe, error)

// domainTablesAnySQL 域表存在性探测（三个建库通道都会建的核心表任在其一
// 即视为"存量库"；to_regclass 按 search_path 解析，与表存在性探测同口径）。
const domainTablesAnySQL = `SELECT (to_regclass('alert_event') IS NOT NULL)
    OR (to_regclass('incident') IS NOT NULL)
    OR (to_regclass('change_record') IS NOT NULL)`

// querySchemaVersion schemaVersionLookup 的默认实现：pgx 直连读数。
// 连接失败原样报错——是否致命由调用方定（server 闸门降级为 WARNING，
// upgrade 直接失败退出）。
func querySchemaVersion(ctx context.Context, dsn string) (schemaProbe, error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return schemaProbe{}, fmt.Errorf("connect %s: %w", redactDSN(dsn), err)
	}
	defer func() { _ = conn.Close(ctx) }()
	v, err := readSchemaVersion(ctx, conn)
	if err != nil {
		return schemaProbe{}, err
	}
	p := schemaProbe{version: v}
	if v == 0 {
		// 只在"读 0"时才多跑一次只读探测区分空库 vs 未跟踪存量库。
		if err := conn.QueryRow(ctx, domainTablesAnySQL).Scan(&p.legacyUntracked); err != nil {
			return schemaProbe{}, fmt.Errorf("probe domain tables (untracked-legacy detection): %w", err)
		}
	}
	return p, nil
}

func readSchemaVersion(ctx context.Context, q rowQuerier) (int, error) {
	var exists bool
	if err := q.QueryRow(ctx, schemaMigrationsExistsSQL).Scan(&exists); err != nil {
		return 0, fmt.Errorf("probe schema_migrations existence: %w", err)
	}
	if !exists {
		return 0, nil // 表不存在 = 空库/未跟踪库，版本 0
	}
	var v int64
	if err := q.QueryRow(ctx, schemaVersionSQL).Scan(&v); err != nil {
		return 0, fmt.Errorf("read schema_migrations version: %w", err)
	}
	return int(v), nil
}

// redactDSN DSN 可能含口令，日志/错误里只留"主机/库"段。
func redactDSN(dsn string) string {
	if i := strings.Index(dsn, "@"); i >= 0 {
		return "***@" + dsn[i+1:]
	}
	return dsn
}

// startupSchemaGate server 启动期的跨版本闸门（main.go 有 DSN 时调用）。
// 返回 error = 拒启；返回 nil 时可能已经通过 logf 打过响亮 WARNING。
// 决策矩阵（gap = cfg.DB.MaxVersionGap，默认 3）：
//
//	DB 版本 >  常量                          → 拒启（旧二进制配新库）
//	DB 版本 == 常量                          → 放行（静默）
//	DB 版本 == 0 且域表已在（未跟踪存量库） → 放行 + WARNING 提示跑 upgrade
//	                                          登记（scripts/migrate / psql 建的
//	                                          全 schema 库，"读 0"≠"落后 20"）
//	常量 − DB 版本 ≤ gap                    → 放行 + WARNING（提示跑 upgrade）
//	常量 − DB 版本 > gap（含真空库读 0）    → 拒启（落后太多，先 upgrade）
//	lookup 报错（DB 不可达）                → 放行 + WARNING（与装配层 PG 降级同取向）
func startupSchemaGate(ctx context.Context, dsn string, maxVersion, gap int,
	lookup schemaVersionLookup, logf func(format string, args ...any)) error {
	probe, err := lookup(ctx, dsn)
	if err != nil {
		logf("WARNING: schema version gate SKIPPED — schema_migrations unreadable (%v); "+
			"starting anyway (same degrade stance as the pg sink being unreachable)", err)
		return nil
	}
	dbVer := probe.version
	switch {
	case dbVer > maxVersion:
		return fmt.Errorf("schema too NEW: DB is at version %d but this binary only understands up to %d — "+
			"旧二进制配新库会按旧 schema 写表，拒启。"+
			"二选一：恢复升级前的备份（backups/pre-upgrade-*.sql），或换用配套该 schema 版本的 opscopilot 二进制",
			dbVer, maxVersion)
	case dbVer == maxVersion:
		return nil
	case dbVer == 0 && probe.legacyUntracked:
		// 未跟踪存量库（P2-D4）：schema 实际齐全，只是没有版本台账——拒启是
		// 误伤（"落后 20"是读数假象）。放行但响亮提示登记，登记本身零 schema
		// 改动（迁移幂等重放 + 写台账行）。
		logf("WARNING: schema_migrations missing/empty on a POPULATED db (domain tables exist) — "+
			"this looks like an untracked DB built by scripts/migrate or psql, NOT a fresh empty one; "+
			"starting anyway. Run `opscopilot upgrade` to register the current version "+
			"(all %d migrations are idempotent — closing this gap only writes tracker rows)", maxVersion)
		return nil
	case maxVersion-dbVer <= gap:
		logf("WARNING: DB schema v%d is %d version(s) BEHIND binary v%d (within OPS_MAX_VERSION_GAP=%d) — "+
			"running anyway; run `opscopilot upgrade` soon to close the gap",
			dbVer, maxVersion-dbVer, maxVersion, gap)
		return nil
	default:
		return fmt.Errorf("schema too OLD: DB is at version %d, binary wants %d — behind by %d versions, "+
			"which exceeds OPS_MAX_VERSION_GAP=%d. Run `opscopilot upgrade` (takes a pre-upgrade backup first), "+
			"or set OPS_MAX_VERSION_GAP to knowingly accept a wider gap",
			dbVer, maxVersion, maxVersion-dbVer, gap)
	}
}

// ---------- 迁移清单 ----------

type migrationFile struct {
	version int
	path    string
}

var upFileRE = regexp.MustCompile(`^(\d+)_.*\.up\.sql$`)

// listMigrations 读取目录下的 *.up.sql 并按版本号升序返回（upgrade 与漂移测试共用）。
func listMigrations(dir string) ([]migrationFile, error) {
	matches, err := filepath.Glob(filepath.Join(dir, "*.up.sql"))
	if err != nil {
		return nil, fmt.Errorf("glob %s: %w", dir, err)
	}
	var out []migrationFile
	seen := map[int]string{}
	for _, m := range matches {
		base := filepath.Base(m)
		g := upFileRE.FindStringSubmatch(base)
		if g == nil {
			return nil, fmt.Errorf("%s: 文件名不合 <数字>_<名字>.up.sql 约定，拒绝猜测版本", base)
		}
		v, err := strconv.Atoi(g[1])
		if err != nil || v <= 0 {
			return nil, fmt.Errorf("%s: 版本号非法: %v", base, err)
		}
		if dup, ok := seen[v]; ok {
			return nil, fmt.Errorf("duplicate migration version %d: %s vs %s", v, filepath.Base(dup), base)
		}
		seen[v] = m
		out = append(out, migrationFile{version: v, path: m})
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("在 %s 下没有找到 *.up.sql（在仓库根运行或用 --path 指定迁移目录）", dir)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].version < out[j].version })
	return out, nil
}

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

	fmt.Fprintf(w, "-- opscopilot upgrade 应用前逻辑备份（%s UTC）\n", ts.UTC().Format(time.RFC3339))
	fmt.Fprintf(w, "-- 定位：开发/演示库的文本级快照，非物理备份；恢复 = 在空库上重放本文件。\n")

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

// ---------- 语句切分 ----------
//
// SYNC：splitStatements / dollarTag 逐字复制自 scripts/migrate/main.go——
// upgrade 与建库工具必须按同一规则切语句，否则"migrate 能建、upgrade 不能补"
// 会成最难查的一对孪生 bug。scripts/migrate 是 package main（不可 import），
// 故以复制换一致；改动任何一份都要同步另一份（两边文件头都有本注记）。

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
