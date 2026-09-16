package main

// W-UI 换皮控制台静态挂载：把 web/ React 工程构建产物（web/dist）作为首页
// 服务，替代只读 console.html（后者保留为 /console 最小视图与回退）。
//
// 路由设计（与 console.go 协调）：
//   - GET /{$}       由本文件注册（dist 有效时）；返回 index.html（HashRouter 单入口）
//   - GET /assets/   静态产物（vite 内容哈希文件名天然不可变，交给浏览器长缓存）
//   - GET /console   仍由 console.go 注册（go:embed 冻结版，独立可达）
//
// 缓存纪律：index.html 必须 no-store（与 console.html W10 事故修复同款：
// 前端资源随构建更新，首页缓存会把用户钉死在旧版本）。assets 由哈希文件名
// 保证不可变，不设显式缓存头（http.FileServer 默认 Last-Modified/304）。
//
// 降级：distDir 为空、或缺少 index.html 时记录警告并返回 false，装配层保持
// 既有 /console 302 行为——任何构建缺失都不影响 API 与最小控制台。

import (
	"log"
	"net/http"
	"os"
	"path/filepath"
)

// registerWebUI 挂载 web/dist；返回是否成功挂载（false = 调用方保留根路由回退）。
func registerWebUI(mux *http.ServeMux, distDir string) bool {
	if distDir == "" {
		return false
	}
	index := filepath.Join(distDir, "index.html")
	if fi, err := os.Stat(index); err != nil || fi.IsDir() {
		log.Printf("WARNING: OPS_WEB_DIST=%q 无 index.html（%v）——控制台回退 /console", distDir, err)
		return false
	}
	assets := filepath.Join(distDir, "assets")
	mux.Handle("GET /assets/", http.StripPrefix("/assets/", http.FileServer(http.Dir(assets))))
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		http.ServeFile(w, r, index)
	})
	log.Printf("web UI 已挂载：%s（GET / 换皮控制台；/console 最小视图仍可用）", distDir)
	return true
}
