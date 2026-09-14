#!/usr/bin/env python3
"""模块边界静态检查（P1-2 调用纪律守护，CI 强制）。

规则（v1.3 §5.2）：
  1. internal/ 下每个子目录是一个模块；
  2. 模块 M 的代码只允许 import：
     - 标准库与外部依赖（不以 opscopilot/ 开头）
     - 自身 internal/M/... 与 internal/M 本包
     - 共享契约：internal/contracts（gRPC 生成代码）
     - 通用库：pkg/...（不依赖任何 internal 模块）
     - 跨模块调用必须走 gRPC client（通过 transport 包拨号）
     - **出口白名单例外（ADR-015）**：internal 模块允许 import
       internal/transport（出站 IO 统一承载层：gRPC 拨号 + HTTPClient
       发送器）；transport 自身不得反向依赖任何业务模块。
  3. internal/contracts 与 pkg/ 不得 import 任何 internal/<module>。
  4. （ADR-015 出口纪律）出口模块生产代码禁止 import net/http——物理发送
     唯一归 internal/transport（llmgw 经装配注入的 Sender=transport.NewHTTPClient；
     notify 直接以 transport.HTTPClient 为发送器；connector 的 azure/prometheus
     数据源拉取同挂 transport，二期池波三迁移删除了旧的裸 http.Client 注入
     兜底）。业务模块直持 http.Client = 第二个不受控出口。
     **名单自扩**：出口模块以 `internal/<模块>/.socket-egress` 目录标记文件
     声明——新增出口模块 = 放标记文件，脚本零改动即纳入；删除标记文件即
     解除（标记文件内容不解析，仅存在性生效）。
  5. （P2-D2）cmd/ 生产代码禁止散读 "OPS_*" env——必须经 internal/config
     的 Env* 键常量引用，键改名在编译期即失联（_test.go 豁免：
     OPS_TEST_PG_DSN 是集成测试环境专用键，本就在 config 域之外）。
  6. （P2-D2）tools/ 与 scripts/migrate 是独立 CLI 交付面：只允许 import
     internal/config（键常量/配置结构）与 internal/contracts、pkg/*——
     禁止 import 其余 internal 业务模块（业务逻辑必须走被测服务 HTTP/gRPC
     面，直连内部包 = 绕过对外契约）。

用法：python scripts/check_module_boundaries.py [项目根目录]
退出码：0=通过；1=存在违例。
"""
import re
import sys
from pathlib import Path

IMPORT_RE = re.compile(r'^\s*(?:import\s+)?(?:[_\w]+(?:\.[_\w]+)*\s+)?"(opscopilot/[^"]+)"')
STDLIB_NETHTTP_RE = re.compile(r'^\s*(?:import\s+)?(?:[_\w]+(?:\.[_\w]+)*\s+)?"net/http"')
ENV_GET_RE = re.compile(r'os\.Getenv\("OPS_')
# 规则 4 出口模块标记文件：出口模块在此目录放 .socket-egress 即纳入名单（自扩）。
SOCKET_EGRESS_MARK = ".socket-egress"
ALLOWED_SHARED = ("opscopilot/internal/contracts", "opscopilot/pkg")
# 规则 5/6 额外扫描面（相对仓库根）。cmd=装配汇合点（规则 5 散读钉）；
# tools/scripts/migrate=独立 CLI 交付面（规则 6 业务模块隔离）。
EXTRA_SCAN_ROOTS = ("cmd", "tools", "scripts/migrate")
# CLI 交付面允许 import 的 internal 包前缀。
CLI_ALLOWED_INTERNAL = ("opscopilot/internal/config", "opscopilot/internal/contracts")


def module_name(path: Path, internal_root: Path) -> str:
    return path.relative_to(internal_root).parts[0]


def socket_egress_modules(internal_root: Path) -> set[str]:
    """规则 4 名单自扩：扫描 internal/*/.socket-egress 标记文件。"""
    return {p.parent.name for p in internal_root.glob(f"*/{SOCKET_EGRESS_MARK}")}


def check(root: Path) -> list[str]:
    internal_root = root / "internal"
    violations: list[str] = []
    egress = socket_egress_modules(internal_root)

    if internal_root.exists():
        for go_file in sorted(internal_root.rglob("*.go")):
            rel = go_file.relative_to(root).as_posix()
            # contracts 模块自身只能被外部 import，不能依赖业务模块
            if go_file.parent == internal_root / "contracts":
                for line in go_file.read_text(encoding="utf-8").splitlines():
                    m = IMPORT_RE.match(line)
                    if m and m.group(1).startswith("opscopilot/internal/") \
                            and not m.group(1).startswith("opscopilot/internal/contracts"):
                        violations.append(f"{rel}: contracts 不得依赖业务模块: {m.group(1)}")
                continue

            mod = module_name(go_file, internal_root)
            text = go_file.read_text(encoding="utf-8")
            # 规则 4（ADR-015 出口纪律）：出口模块（.socket-egress 自扩名单）
            # 生产码 import net/http 必须被抓到，物理发送唯一归 internal/transport。
            if mod in egress and not go_file.name.endswith("_test.go"):
                for line in text.splitlines():
                    if STDLIB_NETHTTP_RE.match(line):
                        violations.append(f"{rel}: 【ADR-015 出口纪律】internal/{mod} 生产代码禁止 import net/http"
                                          f"（出站 IO 唯一经 internal/transport；llmgw 用装配注入的 Sender）")
            for line in text.splitlines():
                m = IMPORT_RE.match(line)
                if not m:
                    continue
                imp = m.group(1)
                if not imp.startswith("opscopilot/internal/"):
                    if imp.startswith("opscopilot/pkg"):
                        continue
                    continue
                target = imp.split("/")[2]
                # 白名单：自身模块、共享契约 contracts、出口承载层 transport（ADR-015）。
                if target == mod or target in ("contracts", "transport"):
                    continue
                violations.append(f"{rel}: 模块 {mod} 不得 import 模块 {target} 的内部包（跨模块调用须走 gRPC/transport）: {imp}")

        # pkg/ 不得反向依赖 internal/
        pkg_root = root / "pkg"
        if pkg_root.exists():
            for go_file in sorted(pkg_root.rglob("*.go")):
                rel = go_file.relative_to(root).as_posix()
                for line in go_file.read_text(encoding="utf-8").splitlines():
                    m = IMPORT_RE.match(line)
                    if m and m.group(1).startswith("opscopilot/internal/"):
                        violations.append(f"{rel}: pkg/ 不得依赖 internal/ 模块: {m.group(1)}")

    # 规则 5：cmd/ 生产代码禁散读 OPS_* env（须 config.Env* 常量）。
    cmd_root = root / "cmd"
    if cmd_root.exists():
        for go_file in sorted(cmd_root.rglob("*.go")):
            if go_file.name.endswith("_test.go"):
                continue  # OPS_TEST_PG_DSN 是测试环境专用键
            rel = go_file.relative_to(root).as_posix()
            for line in go_file.read_text(encoding="utf-8").splitlines():
                if ENV_GET_RE.search(line):
                    violations.append(f"{rel}: cmd/ 生产代码散读 OPS_* env（须经 internal/config 的 Env* 键常量，改名编译期失联）: {line.strip()}")

    # 规则 6：tools/ 与 scripts/migrate 只允许 config/contracts/pkg，禁业务模块。
    for cli_root_name in ("tools", "scripts/migrate"):
        cli_root = root / cli_root_name
        if not cli_root.exists():
            continue
        for go_file in sorted(cli_root.rglob("*.go")):
            rel = go_file.relative_to(root).as_posix()
            for line in go_file.read_text(encoding="utf-8").splitlines():
                m = IMPORT_RE.match(line)
                if not m:
                    continue
                imp = m.group(1)
                if imp.startswith("opscopilot/internal/") and not imp.startswith(CLI_ALLOWED_INTERNAL):
                    violations.append(f"{rel}: {cli_root_name} 不得 import internal 业务模块"
                                      f"（CLI 只允许 config/contracts；业务逻辑走被测服务对外面）: {imp}")
    return violations


def selftest() -> int:
    """自测（O16）：用临时样例树验证规则本身有效，避免规则改动后静默失效。"""
    import tempfile

    cases = [
        ("单行 import 违例应被抓到", "package a\n\nimport _ \"opscopilot/internal/b\"\n", 1),
        ("块内 import 违例应被抓到", "package a\n\nimport (\n\t\"fmt\"\n\t\"opscopilot/internal/b\"\n)\n\nvar _ = fmt.Sprint\n", 1),
        ("引用 contracts 应放行", "package a\n\nimport pb \"opscopilot/internal/contracts/pb\"\n\nvar _ = pb.X\n", 0),
        ("引用 pkg 应放行", "package a\n\nimport \"opscopilot/pkg/x\"\n", 0),
        ("自身模块应放行", "package a\n\nimport \"opscopilot/internal/a/sub\"\n", 0),
        ("引用 transport 出口层应放行（ADR-015 白名单）",
         "package a\n\nimport \"opscopilot/internal/transport\"\n\nvar _ = transport.DefaultHTTPMaxResponseBytes\n", 0),
        # 规则 5（P2-D2）：cmd/ 生产码散读 OPS_* 被抓；_test.go 豁免；无散读放行。
        ("cmd 生产码散读 OPS_* 应被抓到", "cmd", "opscopilot", "main.go",
         "package main\n\nimport \"os\"\n\nfunc main() { _ = os.Getenv(\"OPS_DB_DSN\") }\n", 1),
        ("cmd 测试文件散读 OPS_TEST_PG_DSN 应豁免", "cmd", "opscopilot", "x_test.go",
         "package main\n\nimport (\n\t\"os\"\n\t\"testing\"\n)\n\nfunc TestX(t *testing.T) { _ = os.Getenv(\"OPS_TEST_PG_DSN\") }\n", 0),
        ("cmd 生产码无散读应放行", "cmd", "opscopilot", "main.go",
         "package main\n\nfunc main() {}\n", 0),
        # 规则 6（P2-D2）：tools 禁业务模块、放行 config。
        ("tools import 业务模块应被抓到", "tools", "verdicts", "main.go",
         "package main\n\nimport \"opscopilot/internal/incident\"\n\nvar _ = incident.X\n", 1),
        ("tools import config 应放行", "tools", "verdicts", "main.go",
         "package main\n\nimport \"opscopilot/internal/config\"\n\nvar _ = config.EnvDBDSN\n", 0),
        # 规则 4 自扩（P2-D2）：.socket-egress 标记文件 = 纳入出口名单；
        # 无标记 = 不生效（名单不再硬编码）。
        ("无 .socket-egress 标记的模块 import net/http 应放行", "internal", "plain", "sample.go",
         "package plain\n\nimport (\n\t\"net/http\"\n\t\"strings\"\n)\n\nvar _ = strings.TrimSpace\nvar _ = http.MethodPost\n", 0),
    ]
    failed = 0
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        mod_dir = root / "internal" / "a"
        mod_dir.mkdir(parents=True)
        target = mod_dir / "sample.go"
        for item in cases:
            if len(item) == 3:
                name, content, expected = item
                where = mod_dir
                fname = "sample.go"
            else:
                name, area, sub, fname, content, expected = item
                where = root / area / sub
                where.mkdir(parents=True, exist_ok=True)
            (where / fname).write_text(content, encoding="utf-8")
            actual = 1 if check(root) else 0
            status = "PASS" if actual == expected else "FAIL"
            if actual != expected:
                failed += 1
            print(f"  [{status}] {name}")
            (where / fname).unlink()

        # 规则 4（ADR-015 出口纪律）自扩名单：先验证"无标记不生效"（名单不
        # 硬编码），再放标记文件 → 禁 net/http；测试文件与"不 import
        # net/http"的生产码放行。
        for m in ("llmgw", "notify", "connector"):
            mark_dir = root / "internal" / m
            mark_dir.mkdir(parents=True, exist_ok=True)
            content = (f'package {m}\n\nimport (\n\t"net/http"\n\t"strings"\n)\n\n'
                       f"var _ = strings.TrimSpace\nvar _ = http.MethodPost\n")
            f = mark_dir / "sample.go"
            f.write_text(content, encoding="utf-8")
            actual = 1 if check(root) else 0
            status = "PASS" if actual == 0 else "FAIL"
            if actual != 0:
                failed += 1
            print(f"  [{status}] {m} 无标记 import net/http 应放行（名单自扩，不硬编码）")
            f.unlink()
            (mark_dir / SOCKET_EGRESS_MARK).write_text(
                "出口模块标记（ADR-015）：生产代码禁 import net/http\n", encoding="utf-8")

        egress_rule = []
        for m in ("llmgw", "notify", "connector"):
            mark_dir = root / "internal" / m
            egress_rule += [
                (f"{m} 放 .socket-egress 标记后 import net/http 应被抓到", m, "sample.go",
                 f'package {m}\n\nimport (\n\t"net/http"\n\t"strings"\n)\n\nvar _ = strings.TrimSpace\nvar _ = http.MethodPost\n', 1),
                (f"{m} 标记生效时生产码不触网应放行", m, "sample.go",
                 f'package {m}\n\nimport "strings"\n\nvar _ = strings.TrimSpace\n', 0),
                (f"{m} 标记生效时测试文件不受规则 4 约束", m, "sample_test.go",
                 f'package {m}\n\nimport (\n\t"net/http"\n\t"testing"\n)\n\nfunc TestX(*testing.T) {{ _ = http.MethodPost }}\n', 0),
            ]
        for name, m, fname, content, expected in egress_rule:
            mod_dir = root / "internal" / m
            mod_dir.mkdir(exist_ok=True)
            (mod_dir / fname).write_text(content, encoding="utf-8")
            actual = 1 if check(root) else 0
            status = "PASS" if actual == expected else "FAIL"
            if actual != expected:
                failed += 1
            print(f"  [{status}] {name}")
            (mod_dir / fname).unlink()
    print("自测通过：规则有效。" if failed == 0 else f"自测失败：{failed} 个用例不符预期。")
    return 1 if failed else 0


def main() -> int:
    args = [a for a in sys.argv[1:] if not a.startswith("-")]
    flags = [a for a in sys.argv[1:] if a.startswith("-")]

    if "--selftest" in flags:
        print("模块边界检查 - 自测模式")
        return selftest()

    root = Path(args[0]).resolve() if args else Path(__file__).resolve().parent.parent
    violations = check(root)
    if violations:
        print("模块边界检查未通过：")
        for v in violations:
            print("  -", v)
        return 1
    print("模块边界检查通过：无跨模块内部 import。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
