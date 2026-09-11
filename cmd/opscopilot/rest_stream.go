// 实时推送（SSE）与写权限探测 handler（D10 决策 B：自 rest_gateway.go 按域拆出）。
package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// SSE 连接参数。
const (
	// sseHeartbeat 心跳间隔：穿过反代/代理的空闲超时，也便于客户端感知存活。
	sseHeartbeat = 20 * time.Second
	// sseWriteDeadline 单次写的截止时间，每次写前重置（见 handleEventStream
	// 的 renewWrite）。必须显著大于心跳间隔，否则正常心跳会被自己的截止时间打断。
	sseWriteDeadline = 60 * time.Second
)

// handleAuthStatus GET /api/v1/auth/status —— 写权限探测（控制台据此决定是否
// 显示 Token 输入框）。
//
// 为什么不用服务端下发 cookie：本服务只有一枚共享密钥、**无用户体系**。服务端
// 下发"可写 cookie"等于把"知道密钥"降级为"能打开页面"——任何能访问 /console
// 的客户端都能拿到写权限，鉴权边界反而消失。正确做法是边界留在网络/代理层：
// 反代做鉴权后向下游注入 X-OpsCopilot-Token，浏览器侧不持有密钥。本端点让
// 控制台能识别这种情况并自动隐藏输入框。
//
// 安全：只回布尔与模式名，不回任何密钥材料；GET 无副作用。
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
//   - 每 20s 发 ": ping" 心跳（sseHeartbeat）——穿过反代/代理的空闲超时，
//     也便于客户端感知连接存活；
//   - 写截止时间每次写前重置（sseWriteDeadline）：服务端有全局 WriteTimeout
//     时（main.go），长连接需要它才能续命，同时不至于对半开连接永久阻塞；
//   - 写失败（含截止时间到点）即退出循环——否则半开连接会永久占住
//     goroutine 与订阅名额；
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

	// W9-5（第八轮审核 D7）：http.Server 现在有全局 WriteTimeout（30s），
	// 而 SSE 是长连接——必须在每次写前把写截止时间**往后推**。
	//
	// 为什么不是简单地清空截止时间（SetWriteDeadline(time.Time{})）：
	// 清空后，对"半开连接"（客户端网络静默失联、没有 FIN/RST）的写会永久
	// 阻塞，goroutine 与连接都回收不了——把超时问题换成了泄漏问题。
	// 推进式截止时间两头都顾：正常心跳（20s）远早于新截止时间（60s），
	// 长连接可无限续命；一旦某次写真的卡住超过 60s，写会失败，下面的
	// 错误检查随即退出循环。
	rc := http.NewResponseController(w)
	deadlineUnsupported := false
	renewWrite := func() {
		if deadlineUnsupported {
			return
		}
		if err := rc.SetWriteDeadline(time.Now().Add(sseWriteDeadline)); err != nil {
			// 真实 net/http 服务恒支持；不支持只出现在自定义包装/测试替身，
			// 此时连接会在全局 WriteTimeout 到点时被切断——留痕即可，
			// 不因此拒绝提供 SSE（那对测试与嵌入方是更差的回归）。
			deadlineUnsupported = true
			g.logf("WARNING: event stream cannot set write deadline (stream may be cut by the global WriteTimeout): %v", err)
		}
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
	renewWrite()
	w.WriteHeader(http.StatusOK)
	if _, err := fmt.Fprint(w, ": connected\n\n"); err != nil {
		return // 写失败即连接不可用（含写截止时间到点），不必再循环
	}
	fl.Flush()

	ka := time.NewTicker(sseHeartbeat)
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
			renewWrite()
			if _, err := fmt.Fprintf(w, "event: incident\ndata: %s\n\n", b); err != nil {
				return // 连接已不可用：退出，回收 goroutine 与订阅名额
			}
			fl.Flush()
		case <-ka.C:
			renewWrite()
			if _, err := fmt.Fprint(w, ": ping\n\n"); err != nil {
				return
			}
			fl.Flush()
		}
	}
}

// handleTransitionIncident POST /api/v1/incidents/{id}/transition —— 事件状态流转
// （ack/mitigate/resolve）。写路径鉴权必过、actor 必填（审计）。
//
// R2 由状态机兜底：转 acked 会自动置 auto_close_policy=manual_only（人工接手后
// 外部恢复不得自动关单）——端点不重复做这个判断，避免两处口径漂移。
