// schema_gate.go schema 版本门禁的读数与判定（原 version.go 拆分，2026-09-14）。
// server 启动跨版本门禁（startupSchemaGate）+ 版本读数（querySchemaVersion /
// readSchemaVersion）+ DSN 脱敏（redactDSN）。
package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

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

// redactDSN DSN 可能含口令，日志/错误里只留"主机/库"段。两种 DSN 形态
// 都打码（P2-C2：keyword 形态此前可把 password= 原样带进连接失败错误）：
//   - URL 形态（postgres://user:pass@host:port/db）→ 保留 @ 之后（主机/库）；
//   - keyword 形态（host=... password=xxx）→ password= 的值及其后全部省略
//     （key= 以空白分隔，值可能含特殊字符，保守截断）。
//
// 兜底：两种形态都识别不出时原样返回（不猜，避免把非口令段误伤）。
func redactDSN(dsn string) string {
	if i := strings.Index(dsn, "@"); i >= 0 {
		return "***@" + redactKeywordPassword(dsn[i+1:])
	}
	return redactKeywordPassword(dsn)
}

// redactKeywordPassword 对 keyword 形态 DSN 的 password= 段打码；无则原样
// 返回（@ 分支的剩余段也过一遍：URL query 里若带 password= 同样不漏）。
func redactKeywordPassword(s string) string {
	i := strings.Index(strings.ToLower(s), "password=")
	if i < 0 {
		return s
	}
	return s[:i] + "password=***"
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
