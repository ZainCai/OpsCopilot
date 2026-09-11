// channel_webhook.go W9-2 通知渠道实现（F-11 转正后的"真通知"出口）。
//
// 三种形态共用一套 HTTP 投递逻辑，差别只在**载荷模板**：
//   - generic —— 本系统自定义 JSON（供自建网关/自动化消费）
//   - feishu  —— 飞书自定义机器人（{"msg_type":"text","content":{"text":...}}）
//   - wecom   —— 企业微信群机器人（{"msgtype":"text","text":{"content":...}}）
//
// 纪律（继承 Notifier 契约，R6 审核固化）：
//  1. 并发安全（http.Client 本身并发安全，字段只读）；
//  2. **自带超时**（默认 5s）——Gate.Admit 同步调用 Send，渠道卡住会拖垮
//     告警处理路径；
//  3. 失败返回 error（不 panic）；非 2xx 一律算失败——IM 机器人常以
//     HTTP 200 + body errcode 报错，故再解析一次 errcode/code 字段。
package notify

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// 渠道类型常量（与 DB notify_channel.kind 取值一致）。
const (
	KindGeneric = "generic"
	KindFeishu  = "feishu"
	KindWecom   = "wecom"
)

// ValidKind 渠道类型白名单校验。
func ValidKind(kind string) bool {
	switch kind {
	case KindGeneric, KindFeishu, KindWecom:
		return true
	}
	return false
}

// defaultSendTimeout 单次投递超时（Notifier 契约建议 ≤5s）。
const defaultSendTimeout = 5 * time.Second

// bodySnippetLimit 失败时回读 body 的上限（IM 平台报错信息在前几百字节内，
// 全量读入会把渠道错误变成内存风险）。
const bodySnippetLimit = 512

// WebhookChannel 通用 webhook 渠道（按 kind 选载荷模板）。
type WebhookChannel struct {
	name    string
	kind    string
	url     string
	client  *http.Client
	timeout time.Duration
}

// WebhookOptions 构造参数（timeout 为 0 取默认 5s）。
type WebhookOptions struct {
	Name    string
	Kind    string
	URL     string
	Timeout time.Duration
}

// NewWebhookChannel 构造。校验 kind 与 URL 形态——非法配置应该在建渠道
// 时（API 校验）就被拒，这里是第二道防线（配置从 DB 恢复时也走这条）。
func NewWebhookChannel(o WebhookOptions) (*WebhookChannel, error) {
	if strings.TrimSpace(o.Name) == "" {
		return nil, fmt.Errorf("notify: webhook channel needs a name")
	}
	if !ValidKind(o.Kind) {
		return nil, fmt.Errorf("notify: unknown channel kind %q (want generic|feishu|wecom)", o.Kind)
	}
	u := strings.TrimSpace(o.URL)
	if !strings.HasPrefix(u, "http://") && !strings.HasPrefix(u, "https://") {
		return nil, fmt.Errorf("notify: channel %q url must be http(s)://", o.Name)
	}
	to := o.Timeout
	if to <= 0 {
		to = defaultSendTimeout
	}
	return &WebhookChannel{
		name:    o.Name,
		kind:    o.Kind,
		url:     u,
		client:  &http.Client{Timeout: to},
		timeout: to,
	}, nil
}

// Name 渠道名（Registry 键）。
func (c *WebhookChannel) Name() string { return c.name }

// Kind 渠道类型（前端与诊断用）。
func (c *WebhookChannel) Kind() string { return c.kind }

// Send 按 kind 模板投递。非 2xx 或平台 errcode 非 0 都算失败。
func (c *WebhookChannel) Send(m Message) error {
	payload, err := c.payload(m)
	if err != nil {
		return fmt.Errorf("notify[%s]: build payload: %w", c.name, err)
	}
	req, err := http.NewRequest(http.MethodPost, c.url, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("notify[%s]: build request: %w", c.name, err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("notify[%s]: send: %w", c.name, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, bodySnippetLimit))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("notify[%s]: http %d: %s", c.name, resp.StatusCode, truncate(string(body), 200))
	}
	// IM 平台常见"HTTP 200 + body errcode!=0"：再解析一次，避免假成功。
	if code, msg, ok := platformError(body); ok && code != 0 {
		return fmt.Errorf("notify[%s]: platform error %d: %s", c.name, code, msg)
	}
	return nil
}

// payload 按渠道类型生成载荷。
func (c *WebhookChannel) payload(m Message) ([]byte, error) {
	text := formatText(m)
	switch c.kind {
	case KindFeishu:
		return json.Marshal(map[string]any{
			"msg_type": "text",
			"content":  map[string]string{"text": text},
		})
	case KindWecom:
		return json.Marshal(map[string]any{
			"msgtype": "text",
			"text":    map[string]string{"content": text},
		})
	default: // generic
		return json.Marshal(map[string]any{
			"tenant_id":   m.TenantID,
			"cluster_key": m.ClusterKey,
			"severity":    m.Severity,
			"title":       m.Title,
			"body":        m.Body,
			"source":      "opscopilot",
		})
	}
}

// formatText 纯文本渲染（IM 渠道内容一致，便于值班阅读）。
func formatText(m Message) string {
	var b strings.Builder
	b.WriteString("[OpsCopilot] ")
	if sev := strings.TrimSpace(m.Severity); sev != "" {
		b.WriteString(strings.ToUpper(sev))
		b.WriteString(" ")
	}
	b.WriteString(strings.TrimSpace(m.Title))
	if m.ClusterKey != "" {
		b.WriteString("\n簇: ")
		b.WriteString(m.ClusterKey)
	}
	if m.Body != "" {
		b.WriteString("\n")
		b.WriteString(m.Body)
	}
	return b.String()
}

// platformError 解析 IM 平台错误码（飞书 code / 企微 errcode）。
// 返回 ok=false 表示 body 不是平台响应（generic 渠道或非 JSON）。
func platformError(body []byte) (int, string, bool) {
	var probe struct {
		Code    *int   `json:"code"`
		Msg     string `json:"msg"`
		ErrCode *int   `json:"errcode"`
		ErrMsg  string `json:"errmsg"`
	}
	if err := json.Unmarshal(body, &probe); err != nil {
		return 0, "", false
	}
	if probe.ErrCode != nil {
		return *probe.ErrCode, probe.ErrMsg, true
	}
	if probe.Code != nil {
		return *probe.Code, probe.Msg, true
	}
	return 0, "", false
}

// truncate 截断（渠道错误信息回写日志用）。
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
