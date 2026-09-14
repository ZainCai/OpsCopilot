// version.go 保留：SchemaMaxVersion 常量 + 迁移文件清单（upgrade 与漂移测试共用）。
// schema 版本门禁见 schema_gate.go，upgrade 主体见 upgrade.go / upgrade_backup.go，
// 语句切分见 sqlsplit.go（2026-09-14 拆分，纯文件级重构零行为变化）。
package main

import (
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
)

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

// SchemaMaxVersion 本二进制期望的 schema 版本（= migrations/ 最大编号）。
// **手工维护**：新增 migrations/0000NN_*.up.sql 时必须同步 +1，
// TestSchemaMaxVersionMatchesMigrationsDir 会钉死这一点（忘改即红）。
const SchemaMaxVersion = 21

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
