// httpclient_test.go 传输层真实发送行为（httptest 回环，零外网）：
// 头透传（Authorization 只进请求）、状态码/响应体回传、超时经
// context.DeadlineExceeded 可判（llmgw 错误分类的前提）、响应体上限、
// 调用方 ctx 提前取消、零值超时兜底。
package transport

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestHTTPClientPost(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		switch r.URL.Path {
		case "/ok":
			if r.Method != http.MethodPost {
				t.Errorf("method = %s, want POST", r.Method)
			}
			if got := r.Header.Get("Authorization"); got != "Bearer sk-1" {
				t.Errorf("authorization not forwarded: %q", got)
			}
			if got := r.Header.Get("X-Marker"); got != "m" {
				t.Errorf("custom header not forwarded: %q", got)
			}
			_, _ = io.WriteString(w, "echo:"+string(body))
		case "/status":
			w.WriteHeader(http.StatusTeapot)
			_, _ = io.WriteString(w, `{"error":"i'm a teapot"}`)
		case "/slow":
			time.Sleep(300 * time.Millisecond)
			_, _ = io.WriteString(w, "late")
		case "/huge":
			_, _ = io.WriteString(w, strings.Repeat("z", DefaultHTTPMaxResponseBytes+128))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	h := NewHTTPClient(2 * time.Second)
	ctx := context.Background()
	headers := map[string]string{"Authorization": "Bearer sk-1", "X-Marker": "m"}

	t.Run("200 + body + headers 透传", func(t *testing.T) {
		status, body, err := h.Post(ctx, srv.URL+"/ok", headers, []byte("ping"))
		if err != nil {
			t.Fatal(err)
		}
		if status != 200 || string(body) != "echo:ping" {
			t.Fatalf("status=%d body=%q", status, body)
		}
	})
	t.Run("非 2xx 不算错误、状态码回传", func(t *testing.T) {
		status, body, err := h.Post(ctx, srv.URL+"/status", headers, nil)
		if err != nil {
			t.Fatalf("non-2xx must not be transport error: %v", err)
		}
		if status != http.StatusTeapot || !strings.Contains(string(body), "teapot") {
			t.Fatalf("status=%d body=%q", status, body)
		}
	})
	t.Run("超时以 DeadlineExceeded 可判（分类契约）", func(t *testing.T) {
		slow := NewHTTPClient(50 * time.Millisecond)
		_, _, err := slow.Post(ctx, srv.URL+"/slow", headers, nil)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("want errors.Is DeadlineExceeded, got %T %v", err, err)
		}
	})
	t.Run("调用方 ctx 更早取消", func(t *testing.T) {
		cctx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
		defer cancel()
		_, _, err := h.Post(cctx, srv.URL+"/slow", headers, nil)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("want DeadlineExceeded, got %v", err)
		}
	})
	t.Run("响应体超上限即拒读", func(t *testing.T) {
		_, _, err := h.Post(ctx, srv.URL+"/huge", headers, nil)
		if err == nil || !strings.Contains(err.Error(), "exceeds") {
			t.Fatalf("want oversize error, got %v", err)
		}
	})
	t.Run("连接拒绝：错误可取但上层不裸传", func(t *testing.T) {
		dead := httptest.NewServer(http.NotFoundHandler())
		url := dead.URL
		dead.Close()
		_, _, err := h.Post(ctx, url, headers, nil)
		if err == nil {
			t.Fatal("want error from dead endpoint")
		}
	})
}

// TestNewHTTPClientZeroTimeoutFallback timeout<=0 兜底 10s：120ms 的慢请求
// 在兜底预算内正常完成（"存在宽松但有限的默认预算"；精确超时行为由上一组
// 用例的 50ms 路径锁定）。
func TestNewHTTPClientZeroTimeoutFallback(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(120 * time.Millisecond)
		_, _ = io.WriteString(w, "done")
	}))
	defer srv.Close()
	h := NewHTTPClient(0)
	status, body, err := h.Post(t.Context(), srv.URL, nil, nil)
	if err != nil || status != 200 || string(body) != "done" {
		t.Fatalf("10s fallback must complete a 120ms request: %d %q %v", status, body, err)
	}
}
