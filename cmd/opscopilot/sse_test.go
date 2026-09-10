// W11 实时推送测试：Hub 语义 + SSE 端点端到端。
package main

import (
	"bufio"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"opscopilot/internal/incident"
)

// TestEventHubPublishSubscribe 订阅者能收到广播；退订后不再收到。
func TestEventHubPublishSubscribe(t *testing.T) {
	hub := NewEventHub()
	defer hub.Close()
	ch, cancel := hub.Subscribe()
	if n := hub.Subscribers(); n != 1 {
		t.Fatalf("subscribers=%d, want 1", n)
	}
	hub.Publish(SSEMessage{Type: "created", Incident: incident.Incident{ID: "X"}})
	select {
	case m := <-ch:
		if m.Type != "created" || m.Incident.ID != "X" {
			t.Fatalf("got %+v", m)
		}
	case <-time.After(time.Second):
		t.Fatal("no message within 1s")
	}
	cancel()
	if n := hub.Subscribers(); n != 0 {
		t.Fatalf("after cancel subscribers=%d, want 0", n)
	}
	// 退订后该通道被关闭。
	if _, open := <-ch; open {
		t.Fatal("channel should be closed after cancel")
	}
}

// TestEventHubSlowSubscriberDoesNotBlock 缓冲满时 Publish 不阻塞（丢弃）。
func TestEventHubSlowSubscriberDoesNotBlock(t *testing.T) {
	hub := NewEventHub()
	defer hub.Close()
	_, cancel := hub.Subscribe() // 不消费
	defer cancel()
	done := make(chan struct{})
	go func() {
		// 远超缓冲深度：若 Publish 会阻塞，这里将超时。
		for i := 0; i < sseSubBuffer*4; i++ {
			hub.Publish(SSEMessage{Type: "updated", Incident: incident.Incident{ID: "X"}})
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Publish blocked on slow subscriber")
	}
}

// TestEventHubCloseUnsubscribesAll Close 后所有订阅通道关闭、计数归零。
func TestEventHubCloseUnsubscribesAll(t *testing.T) {
	hub := NewEventHub()
	a, _ := hub.Subscribe()
	b, _ := hub.Subscribe()
	if n := hub.Subscribers(); n != 2 {
		t.Fatalf("subscribers=%d, want 2", n)
	}
	hub.Close()
	if n := hub.Subscribers(); n != 0 {
		t.Fatalf("after close subscribers=%d, want 0", n)
	}
	if _, open := <-a; open {
		t.Fatal("a should be closed")
	}
	if _, open := <-b; open {
		t.Fatal("b should be closed")
	}
	// 关后订阅得到已关闭通道，不悬挂。
	c, cancel := hub.Subscribe()
	defer cancel()
	if _, open := <-c; open {
		t.Fatal("post-close subscribe should be closed")
	}
}

// TestEventStreamEndpoint SSE 端点端到端：连上后建单/流转即收到推送。
func TestEventStreamEndpoint(t *testing.T) {
	asm, h := restTest(t)
	srv := httptest.NewServer(h)
	defer srv.Close()
	defer asm.Close()

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
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("content-type=%q", ct)
	}
	br := bufio.NewReader(resp.Body)
	first, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("read first line: %v", err)
	}
	if !strings.HasPrefix(first, ": connected") {
		t.Fatalf("first line=%q, want comment line", first)
	}

	// 等待服务端完成订阅注册（Flush 早于 Subscribe，避免建单早于订阅而漏帧）。
	deadline := time.Now().Add(3 * time.Second)
	for asm.Events.Subscribers() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("subscriber never registered")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// 建单（链路 B）→ 应广播 created。
	if _, err := asm.Incidents.Create("INC-SSE-1", "sse test", "high", "tester"); err != nil {
		t.Fatal(err)
	}

	data := readSSEData(t, br, "created", 3*time.Second)
	if !strings.Contains(data, "INC-SSE-1") {
		t.Fatalf("created data=%q, want INC-SSE-1", data)
	}

	// 流转 open→acked → 应广播 updated。
	if _, err := asm.Incidents.Transition("INC-SSE-1", incident.StateAcked, "tester"); err != nil {
		t.Fatal(err)
	}
	data = readSSEData(t, br, "updated", 3*time.Second)
	if !strings.Contains(data, `"state":"acked"`) {
		t.Fatalf("updated data=%q, want state acked", data)
	}
}

// readSSEData 从 SSE 流读取下一条 event=incident 且 data.type == wantType 的
// data 行（事件名固定 incident，具体动作在 data 的 type 字段里）。
func readSSEData(t *testing.T, br *bufio.Reader, wantType string, timeout time.Duration) string {
	t.Helper()
	type res struct {
		data string
		err  error
	}
	done := make(chan res, 1)
	go func() {
		seenEvent := false
		for {
			line, err := br.ReadString('\n')
			if err != nil {
				done <- res{"", err}
				return
			}
			trimmed := strings.TrimRight(line, "\r\n")
			if strings.HasPrefix(trimmed, "event: ") {
				seenEvent = strings.TrimSpace(strings.TrimPrefix(trimmed, "event: ")) == "incident"
				continue
			}
			if seenEvent && strings.HasPrefix(trimmed, "data: ") {
				data := strings.TrimSpace(strings.TrimPrefix(trimmed, "data: "))
				if strings.Contains(data, `"type":"`+wantType+`"`) {
					done <- res{data, nil}
					return
				}
				seenEvent = false // 类型不符：继续读下一条
			}
		}
	}()
	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("read SSE (%s): %v", wantType, r.err)
		}
		return r.data
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for SSE event type %q", wantType)
		return ""
	}
}
