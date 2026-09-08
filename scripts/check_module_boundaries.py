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
  3. internal/contracts 与 pkg/ 不得 import 任何 internal/<module>。

用法：python scripts/check_module_boundaries.py [项目根目录]
退出码：0=通过；1=存在违例。
"""
import re
import sys
from pathlib import Path

IMPORT_RE = re.compile(r'^\s*(?:import\s+)?(?:[_\w]+(?:\.[_\w]+)*\s+)?"(opscopilot/[^"]+)"')
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
        for line in go_file.read_text(encoding="utf-8").splitlines():
            m = IMPORT_RE.match(line)
            if not m:
                continue
            imp = m.group(1)
            if not imp.startswith("opscopilot/internal/"):
                if imp.startswith("opscopilot/pkg"):
                    continue
                continue
            target = imp.split("/")[2]
            if target == mod or target == "contracts":
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


def main() -> int:
    root = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else Path(__file__).resolve().parent.parent
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
