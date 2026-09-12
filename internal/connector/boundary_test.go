// boundary_test.go ADR-015 出口纪律的源码级锁定（照 internal/llmgw
// 先例同构；与 scripts/check_module_boundaries.py 规则 4 互补，
// 进 `go test ./...` 必跑面）：
//
//  1. connector 模块（含 azure/prometheus 子包）的生产代码禁止
//     import "net/http"——二期池波三出口迁移后，物理发送唯一经
//     internal/transport.HTTPClient（白名单放行的出口消费者），连接器
//     只保留 URL 拼装、分页与归一化；旧的 `*http.Client` 注入 +
//     自建兜底已删除，本测试防止第二条发送路径复活。
//  2. 测试文件不受约束（httptest 假服务端合法触网）。
package connector

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

// TestConnectorEgressHoldsNoSocket connector 全模块（含子包）生产文件
// 不得 import net/http（ADR-015 出口纪律：出站 IO 唯一经 transport）。
func TestConnectorEgressHoldsNoSocket(t *testing.T) {
	root := repoRoot(t)
	pkgDir := filepath.Join(root, "internal", "connector")
	var offenders []string
	err := filepath.WalkDir(pkgDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		src, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if strings.Contains(string(src), `"net/http"`) {
			rel, _ := filepath.Rel(root, path)
			offenders = append(offenders, filepath.ToSlash(rel))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if len(offenders) > 0 {
		t.Fatalf("internal/connector 生产代码持有 HTTP 栈（发送必须经 transport，ADR-015）：%v", offenders)
	}
}
