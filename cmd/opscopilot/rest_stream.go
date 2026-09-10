// 实时推送（SSE）与写权限探测 handler（D10 决策 B：自 rest_gateway.go 按域拆出）。
package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

func (g *RESTGateway) handleAuthStatus(w http.ResponseWriter, r *http.Request) {
	mode := "shared_secret"
	ok := false
	if strings.TrimSpace(g.token) == "" {
		mode = "open" // 未配置密钥（仅限回环/内网；main 启动已告警）
		ok = true
	} else {
		ok = authorized(r.Header.Get(AuthHeader), g.token)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"write_authorized": ok,
		"mode":             mode,
	})
}

// handleEventStream GET /api/v1/events/stream —— 事件实时推送（SSE）。
//
// 控制台事件页据此把"30s 轮询"换成"变更即推"。事件名固定 incident，
// data 为 SSEMessage（{type, incident}）JSON——前端按事件名订阅，无需
// 每种 type 各挂一个监听。读路径、无鉴权（与其余 GET 一致；S1 下
// loopback-only / 前置反代）。
//
// 连接管理：
//   - 首帧发注释行 ": connected" 立即刷新，让客户端确定连接已建立；
//   - 每 20s 发 ": ping" 心跳——穿过反代/代理的空闲超时，也便于客户端
//     感知连接存活；
//   - r.Context().Done() 触发（客户端断开/超时/服务停机）即退订返回。
//
// 慢客户端不阻塞写路径：Hub 缓冲满即丢该条（事件页是全量刷新语义，
// 丢一帧下一帧自愈）。
func (g *RESTGateway) handleEventStream(w http.ResponseWriter, r *http.Request) {
	if g.hub == nil {
		writeErr(w, http.StatusServiceUnavailable, "event stream disabled")
		return
	}
	fl, ok := w.(http.Flusher)
	if !ok {
		writeErr(w, http.StatusInternalServerError, "streaming unsupported by server")
		return
	}
	// 先订阅再写响应头：订阅失败（超限/停机）要能干净地回 503 而不是先发 200。
	ch, cancel, ok := g.hub.Subscribe()
	if !ok {
		writeErr(w, http.StatusServiceUnavailable, "too many event stream subscribers, retry later")
		return
	}
	defer cancel()

	h := w.Header()
	h.Set("Content-Type", "text/event-stream; charset=utf-8")
	h.Set("Cache-Control", "no-store, must-revalidate")
	h.Set("Connection", "keep-alive")
	// 反代（nginx）默认缓冲响应，会攒够才下发——显式关闭，否则推送变"批量"。
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, ": connected\n\n")
	fl.Flush()

	ka := time.NewTicker(20 * time.Second)
	defer ka.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case msg, open := <-ch:
			if !open { // Hub 关闭（停机）：结束连接
				return
			}
			b, err := json.Marshal(msg)
			if err != nil {
				continue // 理论不可达；坏帧跳过，不断流
			}
			fmt.Fprintf(w, "event: incident\ndata: %s\n\n", b)
			fl.Flush()
		case <-ka.C:
			fmt.Fprint(w, ": ping\n\n")
			fl.Flush()
		}
	}
}

// handleTransitionIncident POST /api/v1/incidents/{id}/transition —— 事件状态流转
// （ack/mitigate/resolve）。写路径鉴权必过、actor 必填（审计）。
//
// R2 由状态机兜底：转 acked 会自动置 auto_close_policy=manual_only（人工接手后
// 外部恢复不得自动关单）——端点不重复做这个判断，避免两处口径漂移。
