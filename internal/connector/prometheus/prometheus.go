// Package prometheus 实现 connector.Connector 的第一个可用数据源：
// 对接 Prometheus 的告警、指标与抓取目标（只读）。
//
// 只读纪律（ADR-002）：
//   - 默认全部走 GET（/-/healthy、/api/v1/alerts、/api/v1/targets、
//     /api/v1/query），绝不产生任何写操作；
//   - 唯一例外：当 PromQL 过长导致 URL 放不下时，/api/v1/query 自动改用 POST
//     form。这是 Prometheus 官方推荐的查询方式，仅传递查询参数、
//     不改变服务端任何状态，仍属只读查询（不产生写操作才是只读的判定标准，
//     而非 HTTP 方法）。阈值见 maxQueryURLLen；
//   - 鉴权 Token 必须来自只读凭证（Config.Credential），并在构造期由
//     pkg/readonly.Validate 强制校验——把"只读"从注释固化为代码。
package prometheus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"opscopilot/internal/connector"
	"opscopilot/pkg/httpx"
	"opscopilot/pkg/readonly"
)

// defaultMaxResponseBytes 单次响应体读取上限，防止异常/恶意响应打爆内存。
const defaultMaxResponseBytes = 32 << 20 // 32 MiB

// maxQueryURLLen 单条 PromQL 编码后允许放进 URL 的最大长度。
// 超过则自动改用 POST form，规避代理/服务端对 URL 长度的常见限制（约 2~8KB）。
const maxQueryURLLen = 1800

// Query 一条指标查询配置。
type Query struct {
	// Name 归一化后的指标名。为空时取返回结果中的 __name__ 标签。
	Name string
	// Expr PromQL 表达式，如 `up{job="node"}`。
	Expr string
}

// Config Prometheus 连接器配置。
type Config struct {
	// ID 连接器唯一标识（Host 注册键）。必填。
	ID string
	// BaseURL Prometheus 基址，如 http://localhost:9090。必填。
	BaseURL string
	// Credential 只读凭证（推荐的生产路径）。
	// 设置后其 Secret 自动用作 Bearer Token，并在 New() 中强制校验：
	// 非只读 / 空 ID / 已过期一律拒绝构造。
	Credential *readonly.Credential
	// Token 裸 Bearer Token。**仅用于无鉴权或本地测试场景**；
	// 生产路径请使用 Credential，否则只读纪律无法被代码强制。
	// 同时设置时 Credential 优先；单独使用裸 Token 必须显式置
	// AllowInsecureToken=true（全局审查 C8：把绕过只读强制的口子
	// 从"默认可用"改为"显式承认"）。
	Token string
	// AllowInsecureToken 允许裸 Token 绕过只读凭证强制。仅限本地调试
	// 与无鉴权数据源（如公开 demo）；生产配置一律走 Credential。
	AllowInsecureToken bool
	// TenantID 多租户隔离标记，透传到采集结果。
	TenantID string
	// Queries 指标查询列表（PromQL）。
	// 为空则不采集指标（默认，向后兼容 W2 早期只采告警的行为）；
	// 非空时逐条执行 /api/v1/query 并归一化进 CollectResult.Metrics。
	Queries []Query
	// HTTPClient 可选自定义客户端；为空则用带超时的默认客户端。
	HTTPClient *http.Client
	// MaxResponseBytes 响应体上限，<=0 时用默认值 32MiB。
	MaxResponseBytes int64
	// 以下端点路径可覆盖（一般不必）。
	AlertsPath  string
	TargetsPath string
	QueryPath   string
}

// PrometheusConnector 对接 Prometheus 的只读连接器。
type PrometheusConnector struct {
	cfg    Config
	client *http.Client
}

// New 构造连接器并做基本校验（含只读凭证强制）。
func New(cfg Config) (*PrometheusConnector, error) {
	if cfg.ID == "" {
		return nil, fmt.Errorf("prometheus: ID is required")
	}
	if cfg.BaseURL == "" {
		return nil, fmt.Errorf("prometheus: BaseURL is required")
	}

	// 只读闸门：凭证必须在构造期通过校验，写权限凭证无法进入采集链路。
	if cfg.Credential != nil {
		if err := readonly.Validate(*cfg.Credential); err != nil {
			return nil, fmt.Errorf("prometheus: %w", err)
		}
		cfg.Token = cfg.Credential.Secret
	}
	// 裸 Token 绕过闸门必须显式承认（C8）：默认拒绝，防止配置者
	// 在不知情的情况下跳过只读强制。
	if cfg.Token != "" && cfg.Credential == nil && !cfg.AllowInsecureToken {
		return nil, fmt.Errorf("prometheus: bare Token bypasses read-only credential enforcement — set AllowInsecureToken=true (local/debug only) or use Credential")
	}

	// 规范化基址，去掉尾部斜杠
	if n := len(cfg.BaseURL); n > 0 && cfg.BaseURL[n-1] == '/' {
		cfg.BaseURL = cfg.BaseURL[:n-1]
	}
	if cfg.AlertsPath == "" {
		cfg.AlertsPath = "/api/v1/alerts"
	}
	if cfg.TargetsPath == "" {
		cfg.TargetsPath = "/api/v1/targets"
	}
	if cfg.QueryPath == "" {
		cfg.QueryPath = "/api/v1/query"
	}
	if cfg.MaxResponseBytes <= 0 {
		cfg.MaxResponseBytes = defaultMaxResponseBytes
	}
	// 查询配置在构造期校验：空 PromQL 属配置错误，早失败好过运行时静默跳过。
	for i, q := range cfg.Queries {
		if q.Expr == "" {
			return nil, fmt.Errorf("prometheus: Queries[%d].Expr is empty", i)
		}
	}
	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	return &PrometheusConnector{cfg: cfg, client: client}, nil
}

// ID 实现 connector.Connector。
func (p *PrometheusConnector) ID() string { return p.cfg.ID }

// Type 实现 connector.Connector。
func (p *PrometheusConnector) Type() string { return "prometheus" }

// HealthCheck 探测 Prometheus 健康端点 /-/healthy。
func (p *PrometheusConnector) HealthCheck(ctx context.Context) (connector.Health, error) {
	status, body, err := p.get(ctx, "/-/healthy")
	if err != nil {
		return connector.Health{Status: connector.HealthDown, Detail: err.Error(), CheckedAt: time.Now()}, err
	}
	if status != http.StatusOK {
		d := fmt.Sprintf("unexpected status %d: %s", status, httpx.Excerpt(body, 200))
		return connector.Health{Status: connector.HealthDegraded, Detail: d, CheckedAt: time.Now()}, nil
	}
	return connector.Health{Status: connector.HealthHealthy, Detail: "ok", CheckedAt: time.Now()}, nil
}

// Collect 拉取当前告警（/api/v1/alerts）与配置的指标（/api/v1/query），
// 归一化为 connector.Alert / connector.MetricSample 列表。
//
// 增量与过滤：本连接器当前不支持 Since（增量水位）与 Scope（作用域过滤），
// 一旦调用方传入即返回 connector.ErrUnsupported——**不静默忽略**，
// 以免调用方误以为过滤生效而按全量数据做下游决策。
func (p *PrometheusConnector) Collect(ctx context.Context, req connector.CollectRequest) (*connector.CollectResult, error) {
	if !req.Since.IsZero() {
		return nil, fmt.Errorf("prometheus: incremental collect (Since) %w", connector.ErrUnsupported)
	}
	if req.Scope != "" {
		return nil, fmt.Errorf("prometheus: scoped collect (Scope) %w", connector.ErrUnsupported)
	}

	// ---- 告警 ----
	status, body, err := p.get(ctx, p.cfg.AlertsPath)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("prometheus: alerts endpoint status %d: %s", status, httpx.Excerpt(body, 200))
	}
	var resp alertsResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("prometheus: decode alerts: %w", err)
	}
	alerts := make([]connector.Alert, 0, len(resp.Data.Alerts))
	for _, a := range resp.Data.Alerts {
		alerts = append(alerts, normalizeAlert(a, p.cfg.ID))
	}

	// ---- 指标（仅在配置了 PromQL 时采集）----
	// 多条查询相互独立：单条失败不吞掉其余已成功的指标，
	// 所有失败聚合后随结果一并返回——显式报错，不是静默忽略。
	metrics := make([]connector.MetricSample, 0)
	var qErrs []error
	for _, q := range p.cfg.Queries {
		if q.Expr == "" {
			continue
		}
		samples, err := p.queryMetrics(ctx, q)
		if err != nil {
			qErrs = append(qErrs, fmt.Errorf("query %q: %w", q.Name, err))
			continue
		}
		metrics = append(metrics, samples...)
	}

	result := &connector.CollectResult{
		Metrics:     metrics,
		Alerts:      alerts,
		TenantID:    p.cfg.TenantID,
		CollectedAt: time.Now(),
	}
	if len(qErrs) > 0 {
		// 带上已成功采集的部分指标，由调用方决定是降级使用还是整体重试。
		return result, errors.Join(qErrs...)
	}
	return result, nil
}

// queryMetrics 执行一次 instant 查询（/api/v1/query）并归一化为指标样本。
//
// 默认 GET 以严守"只读"可审计性；仅当 PromQL 编码后超过 maxQueryURLLen 时
// 自动降级为 POST form（仍为只读查询，见包注释），兼顾纪律与可用性。
func (p *PrometheusConnector) queryMetrics(ctx context.Context, q Query) ([]connector.MetricSample, error) {
	var (
		status int
		body   []byte
		err    error
	)
	if len(url.QueryEscape(q.Expr)) > maxQueryURLLen {
		status, body, err = p.postQuery(ctx, q.Expr)
	} else {
		status, body, err = p.get(ctx, p.cfg.QueryPath+"?query="+url.QueryEscape(q.Expr))
	}
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("prometheus: query endpoint status %d: %s", status, httpx.Excerpt(body, 200))
	}
	var resp queryResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("prometheus: decode query result: %w", err)
	}
	if resp.Data.ResultType != "" && resp.Data.ResultType != "vector" {
		return nil, fmt.Errorf("prometheus: unsupported resultType %q (only instant vector supported)", resp.Data.ResultType)
	}
	out := make([]connector.MetricSample, 0, len(resp.Data.Result))
	for _, s := range resp.Data.Result {
		m, err := normalizeSample(s, q.Name, p.cfg.ID)
		if err != nil {
			// 样本格式异常**不静默跳过**：宁可让本轮失败并进入退避把问题暴露出来，
			// 也不能悄悄产出缺失/错误的指标——降噪与拓扑都建立在指标完整性上。
			return nil, fmt.Errorf("prometheus: normalize sample: %w", err)
		}
		out = append(out, m)
	}
	return out, nil
}

// Discover 拉取活跃抓取目标（/api/v1/targets?state=active），归一化为 ResourceNode 列表。
func (p *PrometheusConnector) Discover(ctx context.Context) (*connector.DiscoverResult, error) {
	status, body, err := p.get(ctx, p.cfg.TargetsPath+"?state=active")
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("prometheus: targets endpoint status %d: %s", status, httpx.Excerpt(body, 200))
	}
	var resp targetsResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("prometheus: decode targets: %w", err)
	}
	// C2：轮内统一观测时刻——逐节点 time.Now() 会让同一轮发现的
	// 节点时间戳各不相同，与 DiscoveredAt 语义也不一致。
	now := time.Now()
	nodes := make([]connector.ResourceNode, 0, len(resp.Data.ActiveTargets))
	for _, t := range resp.Data.ActiveTargets {
		nodes = append(nodes, normalizeTarget(t, p.cfg.ID, now))
	}
	return &connector.DiscoverResult{
		Nodes:        nodes,
		TenantID:     p.cfg.TenantID,
		DiscoveredAt: now,
	}, nil
}

// get 发起只读 GET，返回状态码、响应体、错误。
// 响应体读取受 MaxResponseBytes 限制，避免异常响应耗尽内存。
func (p *PrometheusConnector) get(ctx context.Context, path string) (int, []byte, error) {
	// 共享 HTTP 层（全局审查 C1）：Bearer 注入与响应体上限统一在 pkg/httpx。
	status, _, body, err := httpx.Get(ctx, p.client, p.cfg.BaseURL+path, p.cfg.Token, p.cfg.MaxResponseBytes)
	return status, body, err
}

// postQuery 以 POST form 方式执行查询，仅在 PromQL 超长（> maxQueryURLLen）时启用。
//
// 说明：Prometheus 官方对 /api/v1/query 同时支持 GET 与 POST（form-encoded），
// 本方法只用于规避 URL 长度限制——它仅传递查询参数，不改变服务端任何状态，
// 因此仍属只读查询，不违反 ADR-002。
func (p *PrometheusConnector) postQuery(ctx context.Context, expr string) (int, []byte, error) {
	form := url.Values{}
	form.Set("query", expr)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		p.cfg.BaseURL+p.cfg.QueryPath, strings.NewReader(form.Encode()))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	if p.cfg.Token != "" {
		req.Header.Set("Authorization", "Bearer "+p.cfg.Token)
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()

	body, err := httpx.ReadLimited(resp.Body, p.cfg.MaxResponseBytes)
	if err != nil {
		return resp.StatusCode, nil, err
	}
	return resp.StatusCode, body, nil
}

// ---- 归一化 ----

func normalizeAlert(a rawAlert, source string) connector.Alert {
	sev := a.Labels["severity"]
	status := ""
	if a.Status.State != "" {
		status = a.Status.State
	} else if a.StatusText != "" {
		status = a.StatusText
	}
	return connector.Alert{
		Fingerprint:  a.Fingerprint,
		GeneratorURL: a.GeneratorURL,
		Labels:       a.Labels,
		Annotations:  a.Annotations,
		StartsAt:     parseTime(a.StartsAt),
		EndsAt:       parseTime(a.EndsAt),
		Status:       status,
		Severity:     sev,
		Source:       source,
	}
}

func normalizeTarget(t rawTarget, source string, now time.Time) connector.ResourceNode {
	key := "prometheus://" + t.ScrapePool + "/"
	if inst, ok := t.Labels["instance"]; ok {
		key += inst
	} else {
		key += t.ScrapeURL
	}
	typ := "target"
	if job, ok := t.Labels["job"]; ok && job != "" {
		typ = job
	}
	// 复制标签，避免改写解码产生的原始 map；
	// 把 target 健康状态（up/down）保留进标签，防止归一化丢信息。
	labels := make(map[string]string, len(t.Labels)+1)
	for k, v := range t.Labels {
		labels[k] = v
	}
	if t.Health != "" {
		labels["health"] = t.Health
	}
	return connector.ResourceNode{
		Key:        key,
		Type:       typ,
		Labels:     labels,
		Source:     source,
		ObservedAt: now,
	}
}

// normalizeSample 把 instant vector 的一条样本归一化为 MetricSample。
func normalizeSample(s rawSample, queryName, source string) (connector.MetricSample, error) {
	if len(s.Value) < 2 {
		return connector.MetricSample{}, fmt.Errorf("malformed sample: value length %d", len(s.Value))
	}
	var tsSec float64
	if err := json.Unmarshal(s.Value[0], &tsSec); err != nil {
		return connector.MetricSample{}, fmt.Errorf("decode sample timestamp: %w", err)
	}
	var rawVal string
	if err := json.Unmarshal(s.Value[1], &rawVal); err != nil {
		return connector.MetricSample{}, fmt.Errorf("decode sample value: %w", err)
	}
	val, err := strconv.ParseFloat(rawVal, 64)
	if err != nil {
		// 注意：Prometheus 的 NaN / +Inf 是合法值，ParseFloat 可正常解析为
		// 对应浮点值，不会走到这里；此处仅捕获真正的格式异常。
		return connector.MetricSample{}, fmt.Errorf("parse sample value %q: %w", rawVal, err)
	}
	name := queryName
	if name == "" {
		name = s.Metric["__name__"]
	}
	labels := make(map[string]string, len(s.Metric))
	for k, v := range s.Metric {
		labels[k] = v
	}
	// tsSec 是浮点秒、含亚秒小数。若直接 int64 截断（time.Unix(sec, 0)）
	// 会丢弃小数部分，导致同一时刻的多条样本精度失真；
	// 故换算为纳秒时间戳，保留数据源返回的完整精度。
	return connector.MetricSample{
		Name:      name,
		Labels:    labels,
		Value:     val,
		Timestamp: time.Unix(0, int64(tsSec*float64(time.Second))),
		Source:    source,
	}, nil
}

// ---- Prometheus API 响应结构 ----

type alertsResponse struct {
	Status string `json:"status"`
	Data   struct {
		Alerts []rawAlert `json:"alerts"`
	} `json:"data"`
}

type rawAlert struct {
	Fingerprint  string            `json:"fingerprint"`
	GeneratorURL string            `json:"generatorURL"`
	Labels       map[string]string `json:"labels"`
	Annotations  map[string]string `json:"annotations"`
	StartsAt     string            `json:"startsAt"`
	EndsAt       string            `json:"endsAt"`
	// StatusRaw 先以 RawMessage 兜住两种形态（对象或字符串），避免类型冲突。
	StatusRaw json.RawMessage `json:"status"`
	// 归一化后填充：
	Status struct {
		State string `json:"state"`
	}
	StatusText string
}

// UnmarshalJSON 兼容 Prometheus 两种告警 status 表达：
//   - 新版：{"status":{"state":"firing"}}
//   - 旧版：{"status":"firing"}
//
// 标准解码遇到同 key 不同类型会失败，故先以 RawMessage 兜住再分支解析。
func (a *rawAlert) UnmarshalJSON(data []byte) error {
	type alias struct {
		Fingerprint  string            `json:"fingerprint"`
		GeneratorURL string            `json:"generatorURL"`
		Labels       map[string]string `json:"labels"`
		Annotations  map[string]string `json:"annotations"`
		StartsAt     string            `json:"startsAt"`
		EndsAt       string            `json:"endsAt"`
		Status       json.RawMessage   `json:"status"`
	}
	var aux alias
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	a.Fingerprint = aux.Fingerprint
	a.GeneratorURL = aux.GeneratorURL
	a.Labels = aux.Labels
	a.Annotations = aux.Annotations
	a.StartsAt = aux.StartsAt
	a.EndsAt = aux.EndsAt
	if len(aux.Status) > 0 && aux.Status[0] == '{' {
		_ = json.Unmarshal(aux.Status, &a.Status)
	} else {
		var s string
		if err := json.Unmarshal(aux.Status, &s); err == nil {
			a.StatusText = s
		}
	}
	return nil
}

type targetsResponse struct {
	Status string `json:"status"`
	Data   struct {
		ActiveTargets []rawTarget `json:"activeTargets"`
	} `json:"data"`
}

type rawTarget struct {
	DiscoveredLabels map[string]string `json:"discoveredLabels"`
	Labels           map[string]string `json:"labels"`
	ScrapePool       string            `json:"scrapePool"`
	ScrapeURL        string            `json:"scrapeUrl"`
	Health           string            `json:"health"`
}

// queryResponse /api/v1/query 的 instant query 响应。
type queryResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string      `json:"resultType"`
		Result     []rawSample `json:"result"`
	} `json:"data"`
}

type rawSample struct {
	Metric map[string]string `json:"metric"`
	// Value 为 [时间戳(秒, float), 值(string)]，形态混合故用 RawMessage 延后解析。
	Value []json.RawMessage `json:"value"`
}

// parseTime 解析 RFC3339 时间戳；失败返回零值。
func parseTime(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}
	}
	return t
}
