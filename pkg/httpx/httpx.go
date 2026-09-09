// Package httpx 连接器共享的只读 HTTP 工具（全局审查 C1）。
//
// 抽取动机：prometheus 与 azure 两个连接器的 GET 封装高度重复
// （Bearer 注入、响应体上限、错误摘要），第三、第四个连接器接入前
// 下沉为共享层，各连接器只保留 URL 拼装、分页与归一化。
// 本包只做"发请求、读响应"，不感知任何数据源语义。
package httpx

import (
	"context"
	"fmt"
	"io"
	"net/http"
)

// DefaultMaxResponseBytes 单次响应体读取上限默认值（32MiB）。
const DefaultMaxResponseBytes = 32 << 20

// Get 执行只读 GET 并读取响应体（受 maxBytes 上限约束）。
// 返回状态码、响应头（Retry-After 等控制信息由调用方消费）与响应体。
// bearer 为空时不设置 Authorization 头；client 为空时用 http.DefaultClient。
func Get(ctx context.Context, client *http.Client, url, bearer string, maxBytes int64) (int, http.Header, []byte, error) {
	if client == nil {
		client = http.DefaultClient
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, nil, nil, err
	}
	req.Header.Set("Accept", "application/json")
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, nil, err
	}
	defer resp.Body.Close()
	body, err := ReadLimited(resp.Body, maxBytes)
	if err != nil {
		return resp.StatusCode, resp.Header, nil, err
	}
	return resp.StatusCode, resp.Header, body, nil
}

// ReadLimited 按上限读取响应体，超过 limit 直接报错，防止异常响应打爆内存。
func ReadLimited(r io.Reader, limit int64) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(r, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > limit {
		return nil, fmt.Errorf("response exceeds %d bytes", limit)
	}
	return body, nil
}

// Excerpt 截取响应体前 n 字节作为错误消息摘要（排障用，不含凭证——
// Authorization 只存在于请求头）。
func Excerpt(b []byte, n int) string {
	s := string(b)
	if len(s) > n {
		return s[:n]
	}
	return s
}
