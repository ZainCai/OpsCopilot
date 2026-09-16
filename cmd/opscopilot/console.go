// W5-2.3 控制台最小视图：单文件 HTML（go:embed 内嵌，无外部依赖、
// 无构建链），数据全部来自 W5-2.2 的 REST 只读端点。
//
// 功能边界（M1）：
//   - 簇列表：state 过滤 + 点击展开详情 + 30s 可选自动刷新；
//   - 拓扑图：SVG 环形布局（力导向是 W6+ 的事）+ ADR-007 置信度三色
//     渲染（high/medium/low）+ 悬停 title + 点击看邻居与 24h 变更；
//   - P2-1 千节点聚合：节点数超阈值不画 SVG（千圆点不可读），
//     按 node_type 分桶列表渲染（钻取展开留 W6）。
//
// W10：新增「事件」视图（双链路：人工建单 ∥ 外部自动导入）。
//
// 缓存纪律（W10 实事故修复）：/console 必须 no-store。此前响应不带任何
// 缓存头，浏览器按启发式缓存旧副本——已部署的新功能（如事件页）会"看不见"，
// 极易被误判为"功能没上线"。运维控制台页面必须每次回源，并显示构建标识。
package main

import (
	"bytes"
	_ "embed"
	"net/http"
	"runtime/debug"
)

//go:embed console.html
var consoleHTML []byte

// buildStamp 当前构建标识（vcs 短哈希 · 提交时间），注入页面顶栏。
// 用途：一眼确认"我打开的是哪个构建"——排查"功能不见了"时先看这里。
// 取不到 vcs 信息（如 -buildvcs=false）时回退 "dev"。
func buildStamp() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "dev"
	}
	var rev, at string
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			rev = s.Value
		case "vcs.time":
			at = s.Value
		}
	}
	if rev == "" {
		return "dev"
	}
	if len(rev) > 7 {
		rev = rev[:7]
	}
	if at != "" {
		return rev + " · " + at
	}
	return rev
}

// consolePage 注入构建信息后的页面（启动时算一次，避免每请求做字符串替换）。
var consolePage = bytes.Replace(consoleHTML, []byte("{{BUILD}}"), []byte(buildStamp()), 1)

// consolePath 控制台路由。
const consolePath = "/console"

// registerConsole 把控制台挂到 mux（/console 恒注册；根路由仅在换皮 UI 未
// 挂载时注册 302——rootTaken 由 registerWebUI 的返回值给出，避免 ServeMux
// 模式冲突，保证 dist 缺失时行为与历史一致）。
func registerConsole(mux *http.ServeMux, rootTaken bool) {
	serve := func(w http.ResponseWriter, _ *http.Request) {
		// no-store：控制台页面必须每次回源（见包注释「缓存纪律」）。
		w.Header().Set("Cache-Control", "no-store, must-revalidate")
		w.Header().Set("Pragma", "no-cache")
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(consolePage)
	}
	mux.HandleFunc("GET /console", serve)
	if !rootTaken {
		mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
			// 302 同样必须 no-store：此前的根跳转被浏览器启发式缓存过，
			// 换皮 UI 挂载后旧跳转会继续把用户带到 /console（W-UI 实测坑）。
			w.Header().Set("Cache-Control", "no-store, must-revalidate")
			http.Redirect(w, r, consolePath, http.StatusFound)
		})
	}
}
