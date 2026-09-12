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
  4. （ADR-015 出口纪律）llmgw、notify 与 connector 为纯逻辑层：生产代码
     禁止 import net/http——物理发送唯一归 internal/transport（llmgw 经装配
     注入的 Sender=transport.NewHTTPClient；notify 直接以 transport.HTTPClient
     为发送器；connector 的 azure/prometheus 数据源拉取同挂 transport，
     二期池波三迁移删除了旧的裸 http.Client 注入兜底）。业务模块直持
     http.Client = 第二个不受控出口。

用法：python scripts/check_module_boundaries.py [项目根目录]
退出码：0=通过；1=存在违例。
"""
import re
import sys
from pathlib import Path

IMPORT_RE = re.compile(r'^\s*(?:import\s+)?(?:[_\w]+(?:\.[_\w]+)*\s+)?"(opscopilot/[^"]+)"')
STDLIB_NETHTTP_RE = re.compile(r'^\s*(?:import\s+)?(?:[_\w]+(?:\.[_\w]+)*\s+)?"net/http"')
# 出口模块（生产码禁 net/http）：LLM 协议层、通知投递层、数据源连接器层。
NO_SOCKET_MODULES = ("llmgw", "notify", "connector")
ALLOWED_SHARED = ("opscopilot/internal/contracts", "opscopilot/pkg")


def module_name(path: Path, internal_root: Path) -> str:
    return path.relative_to(internal_root).parts[0]


def check(root: Path) -> list[str]:
    internal_root = root / "internal"
    violations: list[str] = []
    if not internal_root.exists():
        return violations

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
        # 规则 4（ADR-015 出口纪律）：出口模块（llmgw/notify/connector）生产码
        # import net/http 必须被抓到，物理发送唯一归 internal/transport。
        if mod in NO_SOCKET_MODULES and not go_file.name.endswith("_test.go"):
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
    ]
    failed = 0
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        mod_dir = root / "internal" / "a"
        mod_dir.mkdir(parents=True)
        target = mod_dir / "sample.go"
        for name, content, expected in cases:
            target.write_text(content, encoding="utf-8")
            actual = 1 if check(root) else 0
            status = "PASS" if actual == expected else "FAIL"
            if actual != expected:
                failed += 1
            print(f"  [{status}] {name}")
        # 规则 4（ADR-015 出口纪律）：出口模块（llmgw/notify/connector）生产码
        # import net/http 必须被抓到；测试文件与"不 import net/http"的生产码放行。
        # connector 为波三出口迁移新纳入名单——三模块同构校验，防规则名单静默失效。
        rule4 = []
        for m in ("llmgw", "notify", "connector"):
            rule4 += [
                (f"{m} 生产码 import net/http 应被抓到", m, "sample.go",
                 f'package {m}\n\nimport (\n\t"net/http"\n\t"strings"\n)\n\nvar _ = strings.TrimSpace\nvar _ = http.MethodPost\n', 1),
                (f"{m} 生产码不触网应放行", m, "sample.go",
                 f'package {m}\n\nimport "strings"\n\nvar _ = strings.TrimSpace\n', 0),
                (f"{m} 测试文件不受规则 4 约束", m, "sample_test.go",
                 f'package {m}\n\nimport (\n\t"net/http"\n\t"testing"\n)\n\nfunc TestX(*testing.T) {{ _ = http.MethodPost }}\n', 0),
            ]
        for name, m, fname, content, expected in rule4:
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
