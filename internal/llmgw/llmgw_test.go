// llmgw 表驱动测试：假 Sender（零真实网络、零 http 栈——出口纪律修订后
// llmgw 只见接口）。锁定请求组装（URL 拼接/Bearer 头/体字段）、成功解析、
// 超时/取消/StatusError/坏 JSON/空 choices/超体的错误分类，以及 Stats
// 脱敏摘要与 New 的配置拒绝。传输层真实发送的行为在
// internal/transport/httpclient_test.go（httptest）单独锁。
package llmgw

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// fakeSender 可编程发送桩：记录请求、按脚本返回。
type fakeSender struct {
	post func(ctx context.Context, url string, headers map[string]string, body []byte) (int, []byte, error)

	calls       int
	lastURL     string
	lastHeaders map[string]string
	lastBody    []byte
}

func (f *fakeSender) Post(ctx context.Context, url string, headers map[string]string, body []byte) (int, []byte, error) {
	f.calls++
	f.lastURL, f.lastHeaders, f.lastBody = url, headers, body
	if f.post == nil {
		return 200, []byte(`{"choices":[{"message":{"content":"ok"}}]}`), nil
	}
	return f.post(ctx, url, headers, body)
}

func okResp(content string) func(context.Context, string, map[string]string, []byte) (int, []byte, error) {
	return func(_ context.Context, _ string, _ map[string]string, _ []byte) (int, []byte, error) {
		b, _ := json.Marshal(map[string]any{
			"choices": []any{map[string]any{
				"index":         0,
				"message":       map[string]string{"role": "assistant", "content": content},
				"finish_reason": "stop",
			}},
		})
		return 200, b, nil
	}
}

func errResp(status int, body string) func(context.Context, string, map[string]string, []byte) (int, []byte, error) {
	return func(_ context.Context, _ string, _ map[string]string, _ []byte) (int, []byte, error) {
		return status, []byte(body), nil
	}
}

func newTestClient(t *testing.T, s Sender) *Client {
	t.Helper()
	c, err := New(Config{Endpoint: "http://gw.internal:8000/v1/", APIKey: "sk-secret", Model: "m-1", MaxTokens: 256}, s)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	return c
}

func testMessages() []Message {
	return []Message{
		{Role: "system", Content: "仅依据证据总结"},
		{Role: "user", Content: "evidence-payload"},
	}
}

func TestChatTableDriven(t *testing.T) {
	cases := []struct {
		name    string
		post    func(context.Context, string, map[string]string, []byte) (int, []byte, error)
		want    string
		wantErr string // 错误子串；"" = 期望成功
		wantSE  bool   // 期望 *StatusError
		wantDL  bool   // 期望 errors.Is DeadlineExceeded
		wantCan bool   // 期望 errors.Is Canceled
	}{
		{
			name: "成功：返回首 choice 内容（原样，不 Trim）",
			post: okResp("  结论文本  "),
			want: "  结论文本  ",
		},
		{
			name: "超时：sender 包装 DeadlineExceeded 可判",
			post: func(_ context.Context, _ string, _ map[string]string, _ []byte) (int, []byte, error) {
				return 0, nil, fmt.Errorf("dial timeout: %w", context.DeadlineExceeded)
			},
			wantErr: "timed out", wantDL: true,
		},
		{
			name: "调用方取消：Ctx Canceled 分类",
			post: func(_ context.Context, _ string, _ map[string]string, _ []byte) (int, []byte, error) {
				return 0, nil, fmt.Errorf("aborted: %w", context.Canceled)
			},
			wantErr: "canceled", wantCan: true,
		},
		{
			name:    "401：StatusError 且错误值不含响应体",
			post:    errResp(401, `{"error":{"message":"invalid api key sk-leak-candidate"}}`),
			wantErr: "HTTP 401", wantSE: true,
		},
		{
			name:    "500 网关故障",
			post:    errResp(500, "boom"),
			wantErr: "HTTP 500", wantSE: true,
		},
		{
			name:    "坏 JSON",
			post:    errResp(200, "<html>proxy error</html>"),
			wantErr: "bad response JSON",
		},
		{
			name:    "空 choices",
			post:    errResp(200, `{"choices":[]}`),
			wantErr: "no choices",
		},
		{
			name:    "content 全空白",
			post:    errResp(200, `{"choices":[{"message":{"content":"   "}}]}`),
			wantErr: "empty completion",
		},
		{
			name:    "响应超业务上限（1MiB）即拒，不吞大响应",
			post:    errResp(200, `{"choices":[{"message":{"content":"`+strings.Repeat("x", maxResponseBytes+16)+`"}}]}`),
			wantErr: "exceeds",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fs := &fakeSender{post: tc.post}
			c := newTestClient(t, fs)
			got, st, err := c.Chat(context.Background(), testMessages(), 0.2)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want substring %q", err, tc.wantErr)
				}
				var se *StatusError
				if tc.wantSE && !errors.As(err, &se) {
					t.Errorf("want *StatusError, got %T", err)
				}
				if tc.wantDL && !errors.Is(err, context.DeadlineExceeded) {
					t.Errorf("want DeadlineExceeded via errors.Is, got %v", err)
				}
				if tc.wantCan && !errors.Is(err, context.Canceled) {
					t.Errorf("want Canceled via errors.Is, got %v", err)
				}
				// 脱敏纪律：错误值绝不携带 key / prompt 正文 / 响应体片段。
				for _, banned := range []string{"sk-secret", "evidence-payload", "sk-leak-candidate"} {
					if strings.Contains(err.Error(), banned) {
						t.Fatalf("error leaks sensitive material %q: %v", banned, err)
					}
				}
				return
			}
			if err != nil {
				t.Fatalf("chat: %v", err)
			}
			if got != tc.want {
				t.Fatalf("content = %q, want %q", got, tc.want)
			}
			if st.Model != "m-1" || st.HTTPStatus != 200 || st.PromptChars == 0 || st.PromptHash == "" {
				t.Fatalf("stats not sanitized-summary shaped: %+v", st)
			}
			if len(st.PromptHash) != 12 {
				t.Errorf("prompt hash = %q, want 12 hex chars", st.PromptHash)
			}
		})
	}
}

// TestChatRequestShape 请求组装形态：URL 尾斜杠归一 + /chat/completions、
// Bearer 头注入、OpenAI 体字段（model/messages/temperature/max_tokens）、
// 空 key 不发 Authorization。
func TestChatRequestShape(t *testing.T) {
	t.Run("全量", func(t *testing.T) {
		fs := &fakeSender{}
		c := newTestClient(t, fs)
		if _, _, err := c.Chat(context.Background(), testMessages(), 0.2); err != nil {
			t.Fatal(err)
		}
		if want := "http://gw.internal:8000/v1/chat/completions"; fs.lastURL != want {
			t.Fatalf("url = %q, want %q", fs.lastURL, want)
		}
		if got := fs.lastHeaders["Authorization"]; got != "Bearer sk-secret" {
			t.Errorf("authorization = %q", got)
		}
		if ct := fs.lastHeaders["Content-Type"]; ct != "application/json" {
			t.Errorf("content-type = %q", ct)
		}
		var req chatRequest
		if err := json.Unmarshal(fs.lastBody, &req); err != nil {
			t.Fatalf("body not JSON: %v", err)
		}
		if req.Model != "m-1" || req.MaxTokens != 256 || req.Temperature != 0.2 {
			t.Errorf("req meta = %+v", req)
		}
		if len(req.Messages) != 2 || req.Messages[0].Role != "system" || req.Messages[1].Role != "user" {
			t.Errorf("messages = %+v", req.Messages)
		}
	})
	t.Run("空 key 不发 Authorization", func(t *testing.T) {
		fs := &fakeSender{}
		c, err := New(Config{Endpoint: "http://localhost:11434/v1", Model: "m"}, fs)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := c.Chat(context.Background(), testMessages(), 0); err != nil {
			t.Fatal(err)
		}
		if _, ok := fs.lastHeaders["Authorization"]; ok {
			t.Errorf("empty key must not set Authorization header")
		}
	})
}

// TestChatPassesCallerContext ctx 逐层下传 Sender（超时决策归传输层，
// 协议层只分类不抢跑）。
func TestChatPassesCallerContext(t *testing.T) {
	fs := &fakeSender{post: func(ctx context.Context, _ string, _ map[string]string, _ []byte) (int, []byte, error) {
		<-ctx.Done()
		return 0, nil, ctx.Err()
	}}
	c := newTestClient(t, fs)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, _, err := c.Chat(ctx, testMessages(), 0)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want deadline exceeded passthrough, got %v", err)
	}
}

// TestChatTransportErrorSanitized 非 ctx 类传输错（连接拒绝等）收敛为
// 固定文案——url.Error 原文含请求 URL（内网地址），绝不透传。
func TestChatTransportErrorSanitized(t *testing.T) {
	fs := &fakeSender{post: func(_ context.Context, _ string, _ map[string]string, _ []byte) (int, []byte, error) {
		return 0, nil, fmt.Errorf(`Post "http://sk-secret@gw.internal:8000/v1/chat/completions": connection refused`)
	}}
	c := newTestClient(t, fs)
	_, _, err := c.Chat(context.Background(), testMessages(), 0)
	if err == nil || !strings.Contains(err.Error(), "transport error") {
		t.Fatalf("want generic transport error, got %v", err)
	}
	if strings.Contains(err.Error(), "gw.internal") || strings.Contains(err.Error(), "sk-secret") {
		t.Fatalf("transport error must be sanitized: %v", err)
	}
}

// TestNewRejectsBadConfig 构造期拒绝：空 model / 无 scheme / 空 endpoint /
// nil sender；错误消息不含 endpoint 原文（内网地址亦属敏感信息）。
func TestNewRejectsBadConfig(t *testing.T) {
	fs := &fakeSender{}
	cases := []struct {
		name string
		cfg  Config
		snd  Sender
		want string
	}{
		{"无 scheme", Config{Endpoint: "gw.internal:8000", Model: "m"}, fs, "absolute http(s)"},
		{"空 endpoint", Config{Model: "m"}, fs, "endpoint is required"},
		{"空 model", Config{Endpoint: "http://gw"}, fs, "model is required"},
		{"nil sender", Config{Endpoint: "http://gw", Model: "m"}, nil, "sender is required"},
	}
	for _, tc := range cases {
		_, err := New(tc.cfg, tc.snd)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: got %v, want substring %q", tc.name, err, tc.want)
		}
		if err != nil && strings.Contains(err.Error(), "gw.internal") {
			t.Errorf("%s: error must redact endpoint: %v", tc.name, err)
		}
	}
}

// TestChatDoesNotLogStatsLeaks 结构体面锁定：Stats 字段集里没有装
// prompt/key 正文的位置（新增字段带正文会在这里被抓）。
func TestChatPromptHashStable(t *testing.T) {
	fs := &fakeSender{}
	c := newTestClient(t, fs)
	_, s1, err := c.Chat(context.Background(), testMessages(), 0.2)
	if err != nil {
		t.Fatal(err)
	}
	_, s2, _ := c.Chat(context.Background(), testMessages(), 0.2)
	if s1.PromptHash != s2.PromptHash || s1.PromptChars != s2.PromptChars {
		t.Fatalf("hash unstable for same prompt: %+v vs %+v", s1, s2)
	}
	_, s3, _ := c.Chat(context.Background(), []Message{{Role: "user", Content: "other"}}, 0.2)
	if s3.PromptHash == s1.PromptHash {
		t.Fatal("different prompt must hash differently")
	}
	if strings.Contains(s1.PromptHash, "仅依据") {
		t.Fatal("hash must not contain plaintext")
	}
}
