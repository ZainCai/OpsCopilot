// Package llmgw 通用 OpenAI-compatible chat 协议层（二期池波二 #3 / ADR-015）。
//
// 定位：全系统唯一"以 LLM 协议说话"的地方（ADR-003 单出口纪律）——
// 业务模块不得自行拼 LLM 请求，出站调用一律经本包 Chat。本期唯一消费方
// 是 RCA conclude 步的 Summarizer（cmd/opscopilot/llm_summarizer.go）。
// 本包不 import 任何 internal 兄弟模块（scripts/check_module_boundaries.py
// 强制），也不认识 RCA 语义：只见 messages 进、content 出，厂商中立
// （endpoint/model/key 全部外置，OpenAI chat 补全事实标准，vLLM/Ollama/
// 国产网关均兼容）。
//
// 出口纪律（ADR-015 修订）：本包**不持有任何 http.Client、不 import
// net/http**——只做"请求组装 + 响应解析 + 错误分类"的纯协议逻辑；真正的
// HTTP 发送由装配层（cmd）注入的 Sender 完成（实现：
// transport.NewHTTPClient，出站 IO 统一归 transport 层，与
// connector/redisclient 同款"业务包不碰 socket"路径）。单出口不破：
// LLM 协议调用点唯一（本包 Chat），物理发送点唯一（transport HTTPClient），
// 二者串联即完整出口，且被 boundary_test.go 的源码扫描锁死。
//
// 脱敏纪律（ADR-015）：本包不打任何日志（无 logf 注入口，物理上无法
// 泄密）；错误值只含状态码/长度/固定文案，绝不回显请求体、响应体片段或
// key。调用方如需日志，只许用 Stats 里的长度/SHA-256/模型/状态码/耗时。
package llmgw

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// maxResponseBytes 单次响应体业务上限（1MiB）：结论类输出千字级顶天，
// 超限响应（网关 HTML 错误页/超大 JSON）判为坏响应，不进解析。
const maxResponseBytes = 1 << 20

// Message chat 消息（OpenAI 兼容形态；role: system/user/assistant）。
type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// chatRequest 出站请求体。stream 刻意缺省（false）：RCA 结论要整段 JSON
// 解析，流式只增加客户端复杂度无收益。
type chatRequest struct {
	Model       string    `json:"model"`
	Messages    []Message `json:"messages"`
	Temperature float64   `json:"temperature"`
	MaxTokens   int       `json:"max_tokens,omitempty"`
}

type chatResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
		FinishReason string `json:"finish_reason"`
	} `json:"choices"`
}

// Sender 出站发送抽象（出口纪律落点）：POST url + headers + body，返回
// 状态码与响应体。实现方（transport.HTTPClient）负责传输超时；错误须
// 用 %w 保住 context.DeadlineExceeded / context.Canceled 链供分类。
// 测试注入假 sender 即可完成全协议面单测——单测路径零真实网络。
type Sender interface {
	Post(ctx context.Context, url string, headers map[string]string, body []byte) (status int, resp []byte, err error)
}

// Stats 单次请求的脱敏摘要（ADR-015 日志纪律的唯一合法字段集）：
// 只有长度 / prompt 的 SHA-256 / 模型 / 状态码——不含 prompt 正文与 key。
type Stats struct {
	Model       string
	PromptChars int    // 全部 messages 内容字符数合计
	PromptHash  string // prompt 的 sha256 hex（截 12 位，够对账不还原）
	HTTPStatus  int    // 0 = 未收到响应（超时/传输错）
	RespBytes   int    // 响应体字节数（不含正文）
}

// StatusError 非 2xx 响应（错误值刻意不含响应体——排障看 Stats.RespBytes
// 与状态码，正文可能回显 prompt 片段，不进错误链避免被上层日志带出）。
type StatusError struct {
	Status int
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("llmgw: gateway returned HTTP %d", e.Status)
}

// Client OpenAI-compatible chat 协议客户端。构造后并发安全（字段只读）。
type Client struct {
	url       string
	apiKey    string
	model     string
	maxTokens int
	sender    Sender
}

// Config 协议参数（cmd 装配层从 config.LLMSection 字段显式注入纯值——
// 本包不 import internal/config：跨模块 import 违边界纪律，schema→纯值
// 的搬运是装配层职责，与 internal/rca 收 DTO 同一先例）。
// 传输超时不在这里：它是 Sender（transport）的构造参数，不是协议字段。
type Config struct {
	Endpoint  string // chat/completions 的 base URL（可带 /v1 等前缀路径）
	APIKey    string // Bearer key；空 = 不发 Authorization 头（本地无鉴权网关）
	Model     string // 模型名，必填（无厂商默认——预设模型名就是厂商绑定）
	MaxTokens int    // 补全 token 上限；<=0 时不带 max_tokens 字段
}

// New 构造并校验协议配置。sender 必非 nil（装配层注 transport.NewHTTPClient
// 或测试假件）；endpoint 形态与 internal/config.ValidateLLMEndpoint 同规则
// （http/https 绝对 URL），但本包不 import config——内联最小等价实现。
func New(cfg Config, sender Sender) (*Client, error) {
	if cfg.Model == "" {
		return nil, errors.New("llmgw: model is required")
	}
	if sender == nil {
		return nil, errors.New("llmgw: sender is required (outbound IO must be injected, see ADR-015 egress discipline)")
	}
	if err := validateEndpoint(cfg.Endpoint); err != nil {
		return nil, err
	}
	return &Client{
		url:       strings.TrimRight(cfg.Endpoint, "/") + "/chat/completions",
		apiKey:    cfg.APIKey,
		model:     cfg.Model,
		maxTokens: cfg.MaxTokens,
		sender:    sender,
	}, nil
}

func validateEndpoint(raw string) error {
	if raw == "" {
		return errors.New("llmgw: endpoint is required (empty endpoint must disable the egress upstream, not reach here)")
	}
	if !strings.HasPrefix(raw, "http://") && !strings.HasPrefix(raw, "https://") {
		return fmt.Errorf("llmgw: endpoint must be an absolute http(s) URL, got %s", redact(raw))
	}
	return nil
}

// Chat 发送一次 chat 补全请求并返回首 choice 的文本内容。
//
// 错误分类（供调用方计入 outcome 指标与 fail-open 决策）：
//   - 包装 context.DeadlineExceeded（errors.Is 可判）：传输超时；
//   - 包装 context.Canceled：调用方取消；
//   - *StatusError：非 2xx；
//   - 其余 error：传输错 / 响应超限 / JSON 解析失败 / 空 choices。
//
// 任何路径都不泄露请求体、响应体或 key 到错误值（脱敏纪律见文件头）。
func (c *Client) Chat(ctx context.Context, messages []Message, temperature float64) (string, Stats, error) {
	payload, err := json.Marshal(chatRequest{
		Model: c.model, Messages: messages, Temperature: temperature, MaxTokens: c.maxTokens,
	})
	if err != nil { // 纯字符串 DTO 不可序列化 = 编程错误
		return "", Stats{}, fmt.Errorf("llmgw: marshal request: %w", err)
	}
	var promptChars int
	h := sha256.New()
	for _, m := range messages {
		promptChars += len(m.Content)
		h.Write([]byte(m.Role))
		h.Write([]byte{0})
		h.Write([]byte(m.Content))
		h.Write([]byte{0})
	}
	st := Stats{Model: c.model, PromptChars: promptChars, PromptHash: hex.EncodeToString(h.Sum(nil))[:12]}

	headers := map[string]string{"Content-Type": "application/json", "Accept": "application/json"}
	if c.apiKey != "" {
		headers["Authorization"] = "Bearer " + c.apiKey
	}
	status, body, serr := c.sender.Post(ctx, c.url, headers, payload)
	if serr != nil {
		// 分类只依据 errors.Is 链（transport 层以 %w 保住 ctx 错误）；
		// 传输错误原文可能内嵌请求 URL——一律收敛成固定文案，绝不透传。
		if errors.Is(serr, context.DeadlineExceeded) {
			return "", st, fmt.Errorf("llmgw: request timed out: %w", context.DeadlineExceeded)
		}
		if errors.Is(serr, context.Canceled) {
			return "", st, fmt.Errorf("llmgw: request canceled: %w", context.Canceled)
		}
		return "", st, errors.New("llmgw: transport error (request not completed)")
	}
	st.HTTPStatus = status
	st.RespBytes = len(body)
	if status < 200 || status >= 300 {
		return "", st, &StatusError{Status: status}
	}
	if int64(len(body)) > maxResponseBytes {
		return "", st, fmt.Errorf("llmgw: response exceeds %d bytes", maxResponseBytes)
	}
	var parsed chatResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return "", st, fmt.Errorf("llmgw: bad response JSON (%d bytes)", len(body))
	}
	if len(parsed.Choices) == 0 {
		return "", st, errors.New("llmgw: response has no choices")
	}
	content := parsed.Choices[0].Message.Content
	if strings.TrimSpace(content) == "" {
		return "", st, errors.New("llmgw: empty completion content")
	}
	return content, st, nil
}

// redact 错误消息里的用户输入摘要：只保留长度，绝不原样带出（endpoint
// 理论上不含密钥，但内网 URL 本身就是敏感信息——长度足够定位配置事故）。
func redact(s string) string {
	return fmt.Sprintf("<%d chars>", len(s))
}
