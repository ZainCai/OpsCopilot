// W9-5（第八轮审核 D7）：http.Server 超时口径 + SSE 长连接兼容性的回归测试。
package main

import (
	"bufio"
	"context"
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
// 时间的话，睡过 300ms 之后的写会立即失败（net.Conn 在超过截止时间时直接返回
// ErrDeadlineExceeded，不落盘），事件永远到不了客户端。
func TestEventStreamSurvivesWriteTimeout(t *testing.T) {
	asm, h := restTest(t)
	defer asm.Close()

	srv := httptest.NewUnstartedServer(h)
	srv.Config.WriteTimeout = 300 * time.Millisecond
	srv.Start()
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/api/v1/events/stream", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	br := bufio.NewReader(resp.Body)
	first, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("read first line: %v", err)
	}
	if !strings.HasPrefix(first, ": connected") {
		t.Fatalf("first line=%q, want comment line", first)
	}

	deadline := time.Now().Add(3 * time.Second)
	for asm.Events.Subscribers() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("subscriber never registered")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// 睡过服务端 WriteTimeout：截止时间若不被推进，后续写必定失败。
	time.Sleep(600 * time.Millisecond)

	if _, err := asm.Incidents.Create("INC-SSE-WT", "sse write timeout regression", "high", "tester"); err != nil {
		t.Fatal(err)
	}
	data := readSSEData(t, br, "created", 3*time.Second)
	if !strings.Contains(data, "INC-SSE-WT") {
		t.Fatalf("created data=%q, want INC-SSE-WT (stream broke at WriteTimeout?)", data)
	}
}
