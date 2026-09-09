// Package azure 对接 Azure ARM（Azure Resource Manager）的只读发现连接器（M1 W3）。
//
// 职责：调 ARM 的虚拟机列表 API，把云上资源归一化为 connector.ResourceNode，
// 供 W3 拓扑 Builder 消费——与 Prometheus 连接器并列的第二个数据源，
// 是"跨源推断置信度降级"（ADR-007）的另一半证据来源。
//
// 设计纪律：
//   - 只读（ADR-002）：仅 GET 类操作；凭证必须通过 readonly.Validate
//     （写权限凭证在构造期即被拒绝）。
//   - 零第三方 SDK：直接走 ARM REST（net/http），M1 只需要"列表 + 分页"，
//     引入 azure-sdk-for-go 的收益配不上其依赖体积；后续需求复杂了再评估。
//   - 归一化：ARM 资源形状（id/name/location/properties）不外泄，
//     出口只有 ResourceNode；Key 采用 "azure://vm/<vmId>"（vmId 是订阅内
//     稳定 GUID，不随改名/迁移变化）。
//
// 凭证约定：Credential.Secret 直接承载 ARM 访问令牌（Bearer）。
// 令牌获取（client credentials / managed identity 流程）属于编排层职责，
// 本连接器不承担——保持"认证与发现"分离，测试时也好替换。
package azure

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

const (
	// defaultBaseURL ARM 公有云端点；Azure China 等其他云实例由 Config.BaseURL 覆盖。
	defaultBaseURL = "https://management.azure.com"
	// defaultAPIVersion 虚拟机列表 API 版本（稳定 GA 版本，不追新）。
	defaultAPIVersion = "2022-11-01"
	// defaultMaxResponseBytes 单页响应体上限，防止异常响应打爆内存。
	defaultMaxResponseBytes = 32 << 20 // 32 MiB
	// maxPages 分页硬上限：防御恶意/异常服务端用 nextLink 造成死循环。
	maxPages = 100
	// maxRetriesOn429 ARM 限流（HTTP 429）的最大重试次数。
	maxRetriesOn429 = 3
	// maxRetryAfterSecs / maxRetryAfter Retry-After 退避封顶（防御恶意大值）。
	maxRetryAfterSecs = 30
	maxRetryAfter     = 30 * time.Second
)

// Config Azure ARM 发现器配置。
type Config struct {
	// ID 连接器唯一标识（Host 注册键），如 "azure-prod"。必填。
	ID string
	// SubscriptionID 目标订阅 ID（ARM 路径参数）。必填。
	SubscriptionID string
	// Credential 只读凭证；Secret 承载 ARM 访问令牌。必填，且必须通过只读校验。
	Credential *readonly.Credential
	// BaseURL ARM 端点，默认公有云 https://management.azure.com。
	// Azure China（management.chinacloudapi.cn）/ 测试假服务端均可覆盖。
	BaseURL string
	// APIVersion ARM api-version，默认 2022-11-01。
	APIVersion string
	// MaxResponseBytes 单页响应体上限，<=0 用默认 32MiB。
	MaxResponseBytes int64
	// HTTPClient 可注入自定义客户端（测试用）；默认 15s 超时。
	HTTPClient *http.Client
}

// Discoverer Azure ARM 只读发现连接器。
// 实现 connector.Connector：Discover/HealthCheck 为真实实现，
// Collect 明确返回 ErrUnsupported（指标采集要走 Azure Monitor，
// 属后续迭代，不静默假装支持）。
type Discoverer struct {
	cfg    Config
	client *http.Client
	// armHost BaseURL 的主机名（构造期解析）。nextLink 跟随前必须校验
	// 目标 host 与之一致——被入侵/配置错乱的源若能引导我们带着 Bearer
	// 访问任意主机，等于把令牌递出去（W3 审查 P2-2）。
	armHost string
}

// New 构造并做构造期校验（配置错误早失败）。
func New(cfg Config) (*Discoverer, error) {
	if cfg.ID == "" {
		return nil, fmt.Errorf("azure: ID is required")
	}
	if cfg.SubscriptionID == "" {
		return nil, fmt.Errorf("azure: SubscriptionID is required")
	}
	if cfg.Credential == nil {
		return nil, fmt.Errorf("azure: Credential is required")
	}
	// 只读闸门：与 Prometheus 连接器同一套强制（pkg/readonly）。
	if err := readonly.Validate(*cfg.Credential); err != nil {
		return nil, fmt.Errorf("azure: %w", err)
	}
	if cfg.BaseURL == "" {
		cfg.BaseURL = defaultBaseURL
	}
	cfg.BaseURL = strings.TrimRight(cfg.BaseURL, "/")
	if cfg.APIVersion == "" {
		cfg.APIVersion = defaultAPIVersion
	}
	if cfg.MaxResponseBytes <= 0 {
		cfg.MaxResponseBytes = defaultMaxResponseBytes
	}
	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	base, err := url.Parse(cfg.BaseURL)
	if err != nil || base.Host == "" {
		return nil, fmt.Errorf("azure: invalid BaseURL %q", cfg.BaseURL)
	}
	return &Discoverer{cfg: cfg, client: client, armHost: base.Host}, nil
}

// ID 实现 connector.Connector。
func (d *Discoverer) ID() string { return d.cfg.ID }

// Type 实现 connector.Connector。
func (d *Discoverer) Type() string { return "azure" }

// Collect 实现 connector.Connector：指标采集需走 Azure Monitor（独立 API +
// 独立凭证范围），M1 不支持。显式 ErrUnsupported 而非空结果，
// 让调用方知道"没有采到"是能力边界而非"云上没有指标"。
func (d *Discoverer) Collect(_ context.Context, _ connector.CollectRequest) (*connector.CollectResult, error) {
	return nil, fmt.Errorf("azure: metric collection requires Azure Monitor, planned for a later iteration: %w", connector.ErrUnsupported)
}

// HealthCheck 实现 connector.Connector：GET 订阅资源（最便宜的只读探针），
// 200 即视为凭证有效、ARM 可达。
//
// 错误语义与 prometheus 连接器统一（全局审查 G4）：网络错误上抛 err
// （Host 记日志并退避），HTTP 非 200 归入 degraded 但不上抛——
// "云端拒绝/异常"是可自愈状态，"连不上"才是故障。
func (d *Discoverer) HealthCheck(ctx context.Context) (connector.Health, error) {
	status, _, _, err := d.get(ctx,
		"/subscriptions/"+url.PathEscape(d.cfg.SubscriptionID),
		"2022-12-01")
	now := time.Now()
	if err != nil {
		return connector.Health{Status: connector.HealthDown, Detail: err.Error(), CheckedAt: now}, err
	}
	switch {
	case status == http.StatusOK:
		return connector.Health{Status: connector.HealthHealthy, Detail: "ARM reachable, credential valid", CheckedAt: now}, nil
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		return connector.Health{Status: connector.HealthDegraded, Detail: fmt.Sprintf("credential rejected: HTTP %d", status), CheckedAt: now}, nil
	default:
		return connector.Health{Status: connector.HealthDegraded, Detail: fmt.Sprintf("unexpected HTTP %d", status), CheckedAt: now}, nil
	}
}

// Discover 实现 connector.Connector：列出订阅内全部虚拟机并归一化。
//
// 归一化规则：
//   - Key   = "azure://vm/<vmId>"（properties.vmId，订阅内稳定 GUID）
//   - Type  = "vm"
//   - Labels：name / resource_group / location / vm_size / provisioning_state
//     （ARM 资源标签 tags 同样并入，键加 "tag:" 前缀避免与系统标签冲突）
//   - Source = 连接器 ID；ObservedAt 取本地处理时刻——ARM 列表响应
//     不含服务端观测时间戳，这是数据源形状决定的（注释如实记录）。
func (d *Discoverer) Discover(ctx context.Context) (*connector.DiscoverResult, error) {
	// 首页 URL 由配置拼装；后续页直接跟随服务端 nextLink（绝对 URL）。
	pageURL := fmt.Sprintf("%s/subscriptions/%s/providers/Microsoft.Compute/virtualMachines?api-version=%s",
		d.cfg.BaseURL, url.PathEscape(d.cfg.SubscriptionID), url.QueryEscape(d.cfg.APIVersion))

	nodes := make([]connector.ResourceNode, 0, 16)
	for page := 0; ; page++ {
		status, _, body, err := d.getWithRetry(ctx, pageURL)
		if err != nil {
			return nil, fmt.Errorf("azure: list virtualMachines: %w", err)
		}
		if status != http.StatusOK {
			// 带响应体摘要便于排障（ARM 错误 body 含错误码与消息；
			// 不含凭证——Authorization 只在请求头）。
			return nil, fmt.Errorf("azure: list virtualMachines: HTTP %d: %.256s", status, body)
		}
		var listing vmList
		if err := json.Unmarshal(body, &listing); err != nil {
			return nil, fmt.Errorf("azure: decode virtualMachines: %w", err)
		}
		now := time.Now().UTC()
		for i, vm := range listing.Value {
			n, err := d.normalize(vm, now)
			if err != nil {
				// 异常资源显式失败而非静默跳过：拓扑缺一截比发现失败更危险，
				// 静默丢资源会让下游误以为覆盖完整。
				return nil, fmt.Errorf("azure: vm[%d]: %w", i, err)
			}
			nodes = append(nodes, n)
		}
		if listing.NextLink == "" {
			break
		}
		// nextLink host 必须与 ARM 端点一致（P2-2）：不一致的游标
		// 极可能是注入/劫持——拒绝比带着 Bearer 跟过去安全得多。
		if u, err := url.Parse(listing.NextLink); err != nil || u.Host == "" ||
			!strings.EqualFold(u.Host, d.armHost) {
			return nil, fmt.Errorf("azure: nextLink host mismatch (want %q): %q",
				d.armHost, listing.NextLink)
		}
		pageURL = listing.NextLink
		if page+1 >= maxPages {
			// 分页超限仍未取完：报错而非静默截断——
			// 拓扑缺一截资源比发现失败更危险（下游会误以为覆盖完整）。
			return nil, fmt.Errorf("azure: pagination exceeded %d pages (possible nextLink loop)", maxPages)
		}
	}
	// 循环正常退出仅发生在 NextLink 为空时——分页超限已在循环内显式报错。
	return &connector.DiscoverResult{
		Nodes:        nodes,
		DiscoveredAt: time.Now(),
	}, nil
}

// normalize 单台虚拟机 → ResourceNode。缺 vmId 且缺资源 ID 的数据
// 视为源异常，返回错误（由 Discover 携带下标拒绝整批）。
func (d *Discoverer) normalize(vm vmResource, now time.Time) (connector.ResourceNode, error) {
	if vm.Properties.VMID == "" && vm.ID == "" {
		return connector.ResourceNode{}, errors.New("resource has neither vmId nor id")
	}
	labels := map[string]string{
		"name":            vm.Name,
		"location":        vm.Location,
		"resource_group":  resourceGroupOf(vm.ID),
		"subscription_id": d.cfg.SubscriptionID,
	}
	if vm.Properties.VMID != "" {
		labels["vm_id"] = vm.Properties.VMID
	}
	if vm.Properties.HardwareProfile.VMSize != "" {
		labels["vm_size"] = vm.Properties.HardwareProfile.VMSize
	}
	if vm.Properties.ProvisioningState != "" {
		labels["provisioning_state"] = vm.Properties.ProvisioningState
	}
	for k, v := range vm.Tags {
		if v != "" {
			labels["tag:"+k] = v
		}
	}
	key := "azure://vm/" + vm.Properties.VMID
	if key == "azure://vm/" { // 无 vmId 的异常数据：退回资源 ID 保证唯一
		key = "azure://vm/id:" + vm.ID
	}
	return connector.ResourceNode{
		Key:        key,
		Type:       "vm",
		Labels:     labels,
		Source:     d.cfg.ID,
		ObservedAt: now,
	}, nil
}

// resourceGroupOf 从 ARM 资源 ID 提取资源组名。
// 形如 /subscriptions/<sub>/resourceGroups/<rg>/providers/...
// 解析失败返回空串（不猜——标签缺一个好过标错一个）。
func resourceGroupOf(id string) string {
	parts := strings.Split(id, "/")
	for i, p := range parts {
		if strings.EqualFold(p, "resourceGroups") && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ""
}

// get 统一 GET：注入 Bearer 凭证，读取响应体受上限约束。
// 返回响应头（Retry-After 等控制信息由调用方消费）。
// pathOrURL 为相对路径时用 baseURL+path 并按需追加 api-version；
// 为绝对 URL 时原样请求（分页 nextLink 场景，host 校验在调用方）。
func (d *Discoverer) get(ctx context.Context, pathOrURL, apiVersion string) (int, http.Header, []byte, error) {
	reqURL := pathOrURL
	if !strings.HasPrefix(reqURL, "http://") && !strings.HasPrefix(reqURL, "https://") {
		reqURL = d.cfg.BaseURL + pathOrURL
		if apiVersion != "" {
			sep := "?"
			if strings.Contains(reqURL, "?") {
				sep = "&"
			}
			reqURL += sep + "api-version=" + url.QueryEscape(apiVersion)
		}
	}
	// 共享 HTTP 层（全局审查 C1）：Bearer 注入与响应体上限统一在 pkg/httpx。
	return httpx.Get(ctx, d.client, reqURL, d.cfg.Credential.Secret, d.cfg.MaxResponseBytes)
}

// getWithRetry 对 429（ARM 限流）按 Retry-After 退避重试，最多
// maxRetriesOn429 次——生产 ARM 在批量列举时几乎必然限流，
// 不处理就是"测试通过、上线即挂"（W3 审查 P2-3）。
func (d *Discoverer) getWithRetry(ctx context.Context, reqURL string) (int, http.Header, []byte, error) {
	for attempt := 0; ; attempt++ {
		status, hdr, body, err := d.get(ctx, reqURL, "")
		if err != nil || status != http.StatusTooManyRequests || attempt >= maxRetriesOn429 {
			return status, hdr, body, err
		}
		wait := retryAfterDelay(hdr)
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return status, hdr, body, ctx.Err()
		case <-timer.C:
		}
	}
}

// retryAfterDelay 从响应头解析退避时长：支持秒数与 HTTP-date 两种
// 形式；解析失败退回 1s；封顶 maxRetryAfterSecs 防御恶意大值。
func retryAfterDelay(h http.Header) time.Duration {
	v := h.Get("Retry-After")
	if secs, err := strconv.Atoi(v); err == nil {
		if secs > maxRetryAfterSecs {
			secs = maxRetryAfterSecs
		}
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		d := time.Until(t)
		if d < 0 {
			d = 0
		}
		if d > maxRetryAfter {
			d = maxRetryAfter
		}
		return d
	}
	return time.Second
}

// ---- ARM 响应形状（只声明用到的字段）----

// vmList ARM 列表响应：value + nextLink（分页游标）。
type vmList struct {
	Value    []vmResource `json:"value"`
	NextLink string       `json:"nextLink"`
}

// vmResource ARM VirtualMachine 资源（只取归一化所需字段）。
type vmResource struct {
	ID         string            `json:"id"`
	Name       string            `json:"name"`
	Location   string            `json:"location"`
	Tags       map[string]string `json:"tags"`
	Properties struct {
		VMID              string `json:"vmId"`
		ProvisioningState string `json:"provisioningState"`
		HardwareProfile   struct {
			VMSize string `json:"vmSize"`
		} `json:"hardwareProfile"`
	} `json:"properties"`
}
