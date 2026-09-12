// boundary_test.go ADR-015 出口纪律的源码级锁定（与
// scripts/check_module_boundaries.py 互补，进 `go test ./...` 必跑面）：
//
//  1. llmgw 与 notify 的生产代码禁止 import "net/http"——出站模块只做
//     协议/载荷逻辑，物理发送唯一归 internal/transport（ADR-015 白名单：
//     边界脚本允许 internal 模块 import transport 作为出口依赖，
//     check_module_boundaries.py 规则 2/4 与本测试双向锁定）。
//  2. OpenAI chat 协议路径字面量 "chat/completions" 全仓只允许出现在
//     internal/llmgw——第二处出现 = 第二个 LLM 出口 = 违反 ADR-003。
//  3. llmgw 只允许被 cmd/ import——internal 兄弟模块不得直连 LLM 协议层
//     （结论生成必须经装配注入 Summarizer，ADR-014 挂点语义）。
package llmgw

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
			if base == ".git" || base == "web" || base == "bin" || base == "vendor" {
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

// TestEgressModulesHoldNoSocket llmgw/notify 生产文件不得 import net/http
// （ADR-015 出口纪律：出站 IO 唯一经 internal/transport）。
func TestEgressModulesHoldNoSocket(t *testing.T) {
	root := repoRoot(t)
	for _, mod := range []string{"llmgw", "notify"} {
		pkgDir := filepath.Join(root, "internal", mod)
		var offenders []string
		walkGo(t, pkgDir, func(path string, src []byte) {
			if strings.HasSuffix(path, "_test.go") {
				return // 测试文件合法触网（httptest 假网关）
			}
			if strings.Contains(string(src), `"net/http"`) {
				offenders = append(offenders, rel(t, root, path))
			}
		})
		if len(offenders) > 0 {
			t.Fatalf("internal/%s 生产代码持有 HTTP 栈（发送必须经 transport，ADR-015）：%v", mod, offenders)
		}
	}
}

// TestChatCompletionsLiteralUnique 全仓 "chat/completions" 字面量只出现在
// internal/llmgw（LLM 协议出口唯一性；测试文件同包内允许）。
func TestChatCompletionsLiteralUnique(t *testing.T) {
	root := repoRoot(t)
	var hits []string
	walkGo(t, root, func(path string, src []byte) {
		if !strings.Contains(string(src), "chat/completions") {
			return
		}
		r := rel(t, root, path)
		if !strings.HasPrefix(r, "internal/llmgw/") {
			hits = append(hits, r)
		}
	})
	if len(hits) > 0 {
		t.Fatalf(`"chat/completions" 出现在 internal/llmgw 之外（第二 LLM 出口，违反 ADR-003/015）：%v`, hits)
	}
}

// TestLLMGwImportedByCmdOnly 只有 cmd/ 允许 import llmgw（装配汇合点专属；
// internal 业务模块直连 LLM 协议层 = 绕开 Summarizer 挂点）。
func TestLLMGwImportedByCmdOnly(t *testing.T) {
	root := repoRoot(t)
	var hits []string
	walkGo(t, root, func(path string, src []byte) {
		r := rel(t, root, path)
		if strings.HasPrefix(r, "cmd/") || strings.HasPrefix(r, "internal/llmgw/") {
			return
		}
		if strings.Contains(string(src), `"opscopilot/internal/llmgw"`) {
			hits = append(hits, r)
		}
	})
	if len(hits) > 0 {
		t.Fatalf("llmgw 被 cmd 与自身之外的包 import（结论出口必须经装配注入 Summarizer，ADR-014/015）：%v", hits)
	}
}

// TestTransportHTTPClientConstructionPoints transport.NewHTTPClient /
// NewHTTPClientLimit（物理发送件的两种构造形态）的构造点全仓锁定在四类合法位置：
// cmd 装配（llmgw 的 Sender 注入）、
// internal/notify（渠道投递，白名单出口消费者）、internal/connector
// （数据源拉取，波三出口迁移的白名单消费者）、internal/transport 本包。
// 别处出现 = 有人绕开统一出口私搭发送路径。
func TestTransportHTTPClientIsOnlySender(t *testing.T) {
	root := repoRoot(t)
	var hits []string
	walkGo(t, root, func(path string, src []byte) {
		if strings.HasSuffix(path, "_test.go") {
			return // 规则只锁生产代码（本测试文件自身就含被扫描字面量）
		}
		r := rel(t, root, path)
		if strings.HasPrefix(r, "internal/transport/") || strings.HasPrefix(r, "internal/notify/") ||
			strings.HasPrefix(r, "internal/connector/") || strings.HasPrefix(r, "cmd/") {
			return
		}
		if strings.Contains(string(src), "transport.NewHTTPClient(") ||
			strings.Contains(string(src), "transport.NewHTTPClientLimit(") {
			hits = append(hits, r)
		}
	})
	if len(hits) > 0 {
		t.Fatalf("transport.NewHTTPClient 在合法构造点（cmd 装配 / notify / connector）之外被构造：%v", hits)
	}
}
