// W9-5（第八轮审核 D7）：http.Server 超时口径 + SSE 长连接兼容性的回归测试。
package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"opscopilot/internal/config"
)

// TestNewHTTPServerTimeouts 超时口径：三个超时必须都在，且 **ReadTimeout 必须
// 保持 0**——Go 会把读截止时间覆盖到整个请求，长连接期间的后台读超时会被当作
// 读错误并取消 request context，SSE 会被服务端主动掐断（见 newHTTPServer 注释）。
// 超时值唯一来源是 config.MetricsSection（#10 单一定义），断言取 config 默认。
func TestNewHTTPServerTimeouts(t *testing.T) {
	m := config.Defaults().Metrics
	srv := newHTTPServer(m, "127.0.0.1:0", http.NewServeMux())
	if srv.ReadHeaderTimeout != m.HTTPReadHeaderTimeout {
		t.Fatalf("ReadHeaderTimeout = %v, want %v", srv.ReadHeaderTimeout, m.HTTPReadHeaderTimeout)
	}
	if srv.WriteTimeout != m.HTTPWriteTimeout {
		t.Fatalf("WriteTimeout = %v, want %v", srv.WriteTimeout, m.HTTPWriteTimeout)
	}
	if srv.IdleTimeout != m.HTTPIdleTimeout {
		t.Fatalf("IdleTimeout = %v, want %v", srv.IdleTimeout, m.HTTPIdleTimeout)
	}
	if srv.ReadTimeout != 0 {
		t.Fatalf("ReadTimeout must stay 0 (it would cancel long-lived SSE streams), got %v", srv.ReadTimeout)
	}
}

// TestEventStreamSurvivesWriteTimeout 回归：服务端设了全局 WriteTimeout 时，
// SSE 长连接不能被它掐断——handler 在每次写前推进写截止时间。
//
// 把 WriteTimeout 调到 300ms（真实是 30s）让回归能在毫秒级跑完：不推进截止
// 时间的话，越过 300ms 之后的写会立即失败（net.Conn 在超过截止时间时直接返回
// ErrDeadlineExceeded，不落盘），事件永远到不了客户端。
func TestEventStreamSurvivesWriteTimeout(t *testing.T) {
	asm, h := restTest(t)
	defer asm.Close()

	const writeTimeout = 300 * time.Millisecond
	srv := httptest.NewUnstartedServer(h)
	srv.Config.WriteTimeout = writeTimeout
	srv.Start()
	// srv.Close 必须经 t.Cleanup 且注册在 openSSE 之前：cleanup LIFO 保证
	// openSSE 的 body-close 先执行（SSE 连接先断），否则 srv.Close 在
	// defer 阶段等待永不退出的 handler 死锁（openSSE 抽取时踩过的坑）。
	t.Cleanup(srv.Close)

	// WriteTimeout 自响应头写出（建流）起算；openSSE 返回时流已建立，
	// 以此刻为锚点向后推"越过 2×WriteTimeout"的等待条件。
	streamStart := time.Now()
	br := openSSE(t, srv.URL+"/api/v1/events/stream")

	deadline := time.Now().Add(3 * time.Second)
	for asm.Events.Subscribers() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("subscriber never registered")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// 越过服务端 WriteTimeout 再触发写（原实现是裸 sleep(600ms)）：轮询
	// "已越过 2×WriteTimeout 且流仍存活"这一条件——存活以订阅者仍在册为证
	// （连接若被 WriteTimeout 掐断，handler 退出即退订）；超时失败带最后状态。
	waitUntil(t, 5*time.Second, 10*time.Millisecond, "stream alive past server WriteTimeout",
		func() (bool, string) {
			elapsed := time.Since(streamStart)
			subs := asm.Events.Subscribers()
			return elapsed > 2*writeTimeout && subs == 1,
				fmt.Sprintf("elapsed=%v subscribers=%d", elapsed, subs)
		})

	if _, err := asm.Incidents.Create("INC-SSE-WT", "sse write timeout regression", "high", "tester"); err != nil {
		t.Fatal(err)
	}
	data := readSSEData(t, br, "created", 3*time.Second)
	if !strings.Contains(data, "INC-SSE-WT") {
		t.Fatalf("created data=%q, want INC-SSE-WT (stream broke at WriteTimeout?)", data)
	}
}
