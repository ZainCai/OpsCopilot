// boundary_test.go P2-D2：cmd/ 生产代码散读 OPS_* env 的源码级锁定（与
// scripts/check_module_boundaries.py 规则 5 互补，进 `go test ./...` 必跑面；
// 同构 internal/llmgw/boundary_test.go 的 walkGo 范式）。
//
// 键名必须经 internal/config 的 Env* 常量引用——字面量散读会让键改名
// 静默失联（服务读不到值、行为漂移且无编译期信号）。_test.go 豁免：
// OPS_TEST_PG_DSN 是集成测试环境专用键，本就在 config 域之外。
package main

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// repoRoot 从本包目录向上定位 go.mod（测试 cwd 即本包目录，双保险）。
func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 8; i++ {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		dir = filepath.Dir(dir)
	}
	t.Fatal("go.mod not found upward")
	return ""
}

func walkGo(t *testing.T, root string, fn func(path string, src []byte)) {
	t.Helper()
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			base := d.Name()
			// .gray/.demo 是 gitignore 的运行时产物目录（灰度/演示脚本输出，
			// 含临时 Go 助手，不属仓库正式代码，见 .gitignore）。
			if base == ".git" || base == "web" || base == "bin" || base == "vendor" ||
				base == ".gray" || base == ".demo" {
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		src, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		fn(path, src)
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
}

func rel(t *testing.T, root, path string) string {
	r, err := filepath.Rel(root, path)
	if err != nil {
		t.Fatal(err)
	}
	return filepath.ToSlash(r)
}

// TestCmdProdNoEnvLiteralRead cmd/opscopilot 生产文件（非 _test.go）不得
// `os.Getenv("OPS_...")` 散读——键名唯一出处是 internal/config 的 Env*
// 常量（P2-D2 规则 5；键改名在编译期即失联）。
func TestCmdProdNoEnvLiteralRead(t *testing.T) {
	root := repoRoot(t)
	var offenders []string
	walkGo(t, root, func(path string, src []byte) {
		if strings.HasSuffix(path, "_test.go") {
			return // OPS_TEST_PG_DSN 是集成测试环境专用键（config 域之外）
		}
		if strings.Contains(string(src), `os.Getenv("OPS_`) {
			offenders = append(offenders, rel(t, root, path))
		}
	})
	if len(offenders) > 0 {
		t.Fatalf("生产代码散读 OPS_* env（键改名静默失联，须 config.Env* 常量，见 check_module_boundaries.py 规则 5）：%v", offenders)
	}
}
