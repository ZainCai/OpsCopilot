// Package pkg 通用库：可被所有模块依赖，自身禁止依赖 internal/ 下任何业务模块。
// 约束由 scripts/check_module_boundaries.py 在 CI 中强制（ADR 相关：v1.3 §5.2 调用纪律）。
// 放置原则：只有真正与业务无关、且被 ≥2 个模块复用的代码才放这里。
package pkg
