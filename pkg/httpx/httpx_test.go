package httpx

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestGet_BearerAndAccept 验证 Bearer 注入与 Accept 头（C1 共享层行为契约）。
func TestGet_BearerAndAccept(t *testing.T) {
	var gotAuth, gotAccept string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotAccept = r.Header.Get("Accept")
		w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	_, _, body, err := Get(context.Background(), nil, srv.URL, "tok-123", DefaultMaxResponseBytes)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if gotAuth != "Bearer tok-123" {
		t.Errorf("Authorization = %q, want Bearer tok-123", gotAuth)
	}
	if gotAccept != "application/json" {
		t.Errorf("Accept = %q, want application/json", gotAccept)
	}
	if string(body) != `{"ok":true}` {
		t.Errorf("body = %q", body)
	}
}

// TestGet_NoBearerHeader 空 bearer 不得发送 Authorization 头（匿名端点）。
func TestGet_NoBearerHeader(t *testing.T) {
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.Write([]byte("ok"))
	}))
	defer srv.Close()

	if _, _, _, err := Get(context.Background(), nil, srv.URL, "", DefaultMaxResponseBytes); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if gotAuth != "" {
		t.Errorf("Authorization = %q, want empty", gotAuth)
	}
}

// TestReadLimited_OverLimit 超限响应必须报错而非截断返回（防打爆内存的
// 契约原点——各连接器都依赖它）。
func TestReadLimited_OverLimit(t *testing.T) {
	if _, err := ReadLimited(strings.NewReader(strings.Repeat("x", 11)), 10); err == nil {
		t.Fatal("expected error for over-limit body")
	}
	got, err := ReadLimited(strings.NewReader("12345"), 10)
	if err != nil || string(got) != "12345" {
		t.Errorf("ReadLimited within limit = %q, %v", got, err)
	}
}

// TestReadLimited_ExactLimit 恰好等于上限的响应合法。
func TestReadLimited_ExactLimit(t *testing.T) {
	if _, err := ReadLimited(strings.NewReader("1234567890"), 10); err != nil {
		t.Errorf("exact-limit body should pass: %v", err)
	}
}

// TestExcerpt_Truncation 错误摘要截断（多字节安全由调用方负责，本层只做
// 简单字节截断——长 JSON 排障摘要场景足够）。
func TestExcerpt_Truncation(t *testing.T) {
	if s := Excerpt([]byte("hello world"), 5); s != "hello" {
		t.Errorf("Excerpt = %q, want hello", s)
	}
	if s := Excerpt([]byte("hi"), 5); s != "hi" {
		t.Errorf("Excerpt short = %q, want hi", s)
	}
}
