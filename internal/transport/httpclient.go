// httpclient.go 通用出站 HTTP 发送器（二期池波二 #3 / ADR-015）。
//
// 为什么在 transport（"模块间通信抽象"的出口纪律核心件）：ADR-003 的
// 单出口纪律要求业务 internal 模块不自行持有 http.Client——出站 IO 统一
// 由传输层承载（redis client 在装配层构造的先例；波三出口迁移后，
// connector/azure 与 connector/prometheus 也改挂本发送器，旧的
// `&http.Client{Timeout:15s}` 自建兜底已删除）。本文件把"发一个 HTTP
// 请求、读回响应"下沉为共享件，供 llmgw 协议层经注入的 Sender 接口
// 消费、notify 渠道与 connector 数据源直挂（结构满足，transport 不
// import 任何业务模块，依赖方向不反）。
//
// 纪律：本层不认识 LLM/OpenAI 语义——只见 url/headers/body 进出；
// 超时由构造参数决定（每次 Post/Get 再受调用方 ctx 提前量钳制）；错误以
// %w 保住 context.DeadlineExceeded / context.Canceled 链，供上层分类。
package transport

import (
	"bytes"
	"context"
	"net/http"
	"time"

	"opscopilot/pkg/httpx"
)

// DefaultHTTPMaxResponseBytes HTTP 发送器单次响应体读取上限（4MiB）：
// JSON 类 API 响应千字级顶天，异常响应（HTML 错误页/超大 JSON）截断
// 报错。业务侧更小的上限（如 llmgw 的 1MiB）由协议层在解析前自行判定。
const DefaultHTTPMaxResponseBytes = 4 << 20

// HTTPClient 并发安全的出站发送器（http.Client 只读、底层 Transport 共享
// 连接池；字段构造后不再变更）。
type HTTPClient struct {
	client  *http.Client
	timeout time.Duration
	maxResp int64
}

// NewHTTPClient 构造发送器（响应体上限取 DefaultHTTPMaxResponseBytes）。
// timeout <= 0 视为上游配置事故，兜底取保守
// 10s（不 panic——调用方的 config schema 层已对 timeout fail-fast）。
func NewHTTPClient(timeout time.Duration) *HTTPClient {
	return NewHTTPClientLimit(timeout, DefaultHTTPMaxResponseBytes)
}

// NewHTTPClientLimit 构造发送器并显式指定响应体上限（<=0 取默认 4MiB）。
// 面向数据源拉取类出口（connector/prometheus、connector/azure：目标清单
// 响应可达 MiB 级，4MiB 默认档不够用）；timeout 语义同 NewHTTPClient。
func NewHTTPClientLimit(timeout time.Duration, maxResponseBytes int64) *HTTPClient {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	if maxResponseBytes <= 0 {
		maxResponseBytes = DefaultHTTPMaxResponseBytes
	}
	return &HTTPClient{
		// 不设 http.Client.Timeout：超时唯一来源是 Post 内派生的 ctx
		// deadline——超时错误必然是包装 context.DeadlineExceeded 的
		// url.Error（errors.Is 可判），上层才能把"慢网关"与"网关
		// 拒绝"准确分桶（llmgw/指标 outcome=timeout 的前提）。
		client:  &http.Client{},
		timeout: timeout,
		maxResp: maxResponseBytes,
	}
}

// Get 发送只读 GET 并读取响应体（受上限约束），返回状态码、响应头与
// 原始字节。headers 原样设置（同 Post：凭证头只进请求、绝不进错误值）；
// 返回 http.Header 是为数据源拉取类消费者（connector 的 429 Retry-After
// 退避）保留控制头读取——消费方拿到的头只作解析，不得反向持有连接层。
// ctx 与内部 timeout 取更早到期者；网络层错误原样包装返回（%w）。
func (h *HTTPClient) Get(ctx context.Context, url string, headers map[string]string) (int, http.Header, []byte, error) {
	ctx, cancel := context.WithTimeout(ctx, h.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, nil, nil, err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := h.client.Do(req)
	if err != nil {
		return 0, nil, nil, err
	}
	defer resp.Body.Close()
	raw, err := httpx.ReadLimited(resp.Body, h.maxResp)
	if err != nil {
		return resp.StatusCode, resp.Header, nil, err
	}
	return resp.StatusCode, resp.Header, raw, nil
}

// Post 发送 POST 并读取响应体（受上限约束），返回状态码与原始字节。
// headers 原样设置（Authorization 等凭证头只进请求、绝不进错误值）；
// ctx 与内部 timeout 取更早到期者。网络层错误原样包装返回（%w），
// 由协议层决定脱敏文案。
func (h *HTTPClient) Post(ctx context.Context, url string, headers map[string]string, body []byte) (int, []byte, error) {
	ctx, cancel := context.WithTimeout(ctx, h.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := h.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	raw, err := httpx.ReadLimited(resp.Body, h.maxResp)
	if err != nil {
		return resp.StatusCode, nil, err
	}
	return resp.StatusCode, raw, nil
}
