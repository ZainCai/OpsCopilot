// migration_000019_test.go 000019（incident SLA/acked_at 列）**up/down 往返**
// 集成测试（W10-2 验收项）。OPS_TEST_PG_DSN 门控，缺库 skip。
//
// 为什么开临时库而不是共享测试库：down 会真 DROP COLUMN——在共享库上跑会把
// 并行契约测试的腿打断。临时库只应用 000019 的依赖链（000001 建 tenant、
// 000004 建 incident），验证的是这一支迁移自身的成对可逆性：
// up 建列（默认值/可空语义正确）→ down 干净拆列 → 再 up（幂等可重放，
// 与 scripts/migrate 的"幂等由各迁移自身 IF NOT EXISTS 保证"约定一致）。
package incident

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// connectPG 短连接（迁移测试专用；每文件一把执行，避免池化连接拖住
// DROP DATABASE 的"无其他连接"前提）。
func connectPG(ctx context.Context, dsn string) (*pgx.Conn, error) {
	return pgx.Connect(ctx, dsn)
}

// applyMigrationFile 整文件一把执行（pgx 无参 Exec 走 simple protocol，
// 天然支持多语句）。本迁移与依赖链均无 $-quote，无语句切割歧义。
func applyMigrationFile(ctx context.Context, dsn, file string) error {
	conn, err := connectPG(ctx, dsn)
	if err != nil {
		return fmt.Errorf("connect %s: %w", file, err)
	}
	defer conn.Close(ctx)
	stmt, err := os.ReadFile(file)
	if err != nil {
		return fmt.Errorf("read %s: %w", file, err)
	}
	if _, err := conn.Exec(ctx, string(stmt)); err != nil {
		return fmt.Errorf("apply %s: %w", file, err)
	}
	return nil
}

func TestMigration000019UpDownload(t *testing.T) {
	dsn := os.Getenv("OPS_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("OPS_TEST_PG_DSN not set — migration roundtrip skipped")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// 临时库名：字母数字下划线（避免引号标识符在 CREATE/DROP DATABASE 里的歧义）。
	dbName := fmt.Sprintf("mig19_%d", time.Now().UnixNano())
	must := func(err error, what string) {
		t.Helper()
		if err != nil {
			t.Fatalf("%s: %v", what, err)
		}
	}

	admin, err := connectPG(ctx, dsn)
	must(err, "connect admin")
	t.Cleanup(func() {
		c, cerr := connectPG(context.Background(), dsn)
		if cerr == nil {
			_, _ = c.Exec(context.Background(), "DROP DATABASE IF EXISTS "+dbName+" WITH (FORCE)")
			c.Close(context.Background())
		}
	})
	_, err = admin.Exec(ctx, "CREATE DATABASE "+dbName)
	must(err, "create scratch db")
	_ = admin.Close(ctx)

	scratch := scratchDSN(t, dsn, dbName)
	// 依赖链（IF NOT EXISTS 幂等，可整体重放）。
	for _, f := range []string{
		"../../migrations/000001_init.up.sql",
		"../../migrations/000004_incident.up.sql",
	} {
		must(applyMigrationFile(ctx, scratch, f), "apply "+f)
	}

	colExists := func(col string) bool {
		conn, err := connectPG(ctx, scratch)
		must(err, "connect for probe")
		defer conn.Close(ctx)
		var n int
		err = conn.QueryRow(ctx, `SELECT count(*) FROM information_schema.columns
WHERE table_name='incident' AND column_name=$1`, col).Scan(&n)
		must(err, "probe "+col)
		return n > 0
	}

	must(applyMigrationFile(ctx, scratch, "../../migrations/000019_incident_sla.up.sql"), "up #1")
	if !colExists("sla_minutes") || !colExists("acked_at") {
		t.Fatal("up #1 must create sla_minutes and acked_at")
	}
	// 语义探针：sla_minutes NOT NULL DEFAULT 0；acked_at 可空。
	func() {
		conn, err := connectPG(ctx, scratch)
		must(err, "connect for semantics")
		defer conn.Close(ctx)
		if _, err := conn.Exec(ctx, `INSERT INTO tenant (id, name) VALUES ('default','default') ON CONFLICT DO NOTHING`); err != nil {
			t.Fatalf("seed tenant: %v", err)
		}
		if _, err := conn.Exec(ctx, `INSERT INTO incident (tenant_id, incident_id, title)
VALUES ('default','M19-SEED','seed')`); err != nil {
			t.Fatalf("seed insert: %v", err)
		}
		var sla int
		var acked *time.Time
		must(conn.QueryRow(ctx, `SELECT sla_minutes, acked_at FROM incident WHERE incident_id='M19-SEED'`).
			Scan(&sla, &acked), "seed probe")
		if sla != 0 || acked != nil {
			t.Fatalf("defaults wrong: sla=%d acked=%v, want 0/NULL", sla, acked)
		}
	}()

	must(applyMigrationFile(ctx, scratch, "../../migrations/000019_incident_sla.down.sql"), "down")
	if colExists("sla_minutes") || colExists("acked_at") {
		t.Fatal("down must drop both columns")
	}

	must(applyMigrationFile(ctx, scratch, "../../migrations/000019_incident_sla.up.sql"), "up #2 (replay)")
	if !colExists("sla_minutes") || !colExists("acked_at") {
		t.Fatal("up must be replayable (IF NOT EXISTS idempotency contract)")
	}
}

// scratchDSN 把测试库 DSN 的库名替换为 dbName（仅供本迁移测试的临时库使用）。
func scratchDSN(t *testing.T, dsn, dbName string) string {
	t.Helper()
	i := strings.LastIndex(dsn, "/")
	j := strings.Index(dsn[i+1:], "?")
	if i < 0 {
		t.Fatalf("unparsable dsn (no /): %s", redactDSN(dsn))
	}
	if j < 0 {
		return dsn[:i+1] + dbName
	}
	return dsn[:i+1] + dbName + dsn[i+1+j:]
}

func redactDSN(dsn string) string {
	if i := strings.Index(dsn, "@"); i > 0 {
		return "***@" + dsn[i+1:]
	}
	return dsn
}
