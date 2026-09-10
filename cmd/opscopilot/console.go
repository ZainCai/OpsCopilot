// W5-2.3 控制台最小视图：单文件 HTML（go:embed 内嵌，无外部依赖、
// 无构建链），数据全部来自 W5-2.2 的 REST 只读端点。
//
// 功能边界（M1）：
//   - 簇列表：state 过滤 + 点击展开详情 + 30s 可选自动刷新；
//   - 拓扑图：SVG 环形布局（力导向是 W6+ 的事）+ ADR-007 置信度三色
//     渲染（high/medium/low）+ 悬停 title + 点击看邻居与 24h 变更；
//   - P2-1 千节点聚合：节点数超阈值不画 SVG（千圆点不可读），
//     按 node_type 分桶列表渲染（钻取展开留 W6）。
package main

import (
	_ "embed"
	"net/http"
)

//go:embed console.html
var consoleHTML []byte

// consolePath 控制台路由。
const consolePath = "/console"

// registerConsole 把控制台挂到 mux（/ 与 /console 都可达）。
func registerConsole(mux *http.ServeMux) {
	mux.HandleFunc("GET /console", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(consoleHTML)
	})
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, consolePath, http.StatusFound)
	})
}
