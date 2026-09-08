# T2 冒烟测试执行指引

## 前置

- Go 1.22+（本机当前未安装，装好后执行）
- Windows / Linux 各跑一遍（重点 Windows——go-plugin 在 Windows 上走 TCP 回环而非 Unix socket）

## 执行

```bash
cd tools/smoke/goplugin
go mod tidy   # 或在仓库根 go work use ./tools/smoke/goplugin
go run ./host
```

## 判定

- 输出三行 PASS + `SMOKE RESULT: GO-PLUGIN <os> OK` → 通过，私有化版连接器子进程模式可用；
- 任一 FAIL → 结论落档"单机版限 Linux"，并启用连接器双模式中的编译内置路线（v1.3 P1-3）。

## 已知注意点

- host/main.go 中 `execPlugin()` 与 `runtimeOS()` 为占位函数，需在落地时补齐（拼出 `go run ./plugin` 或编译产物路径）；
- 冒烟通过后，把结论写入 ADR-006 关键外部依赖清单的 go-plugin 行。
