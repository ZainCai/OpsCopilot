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
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"opscopilot/internal/connector"
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
	return &Discoverer{cfg: cfg, client: client}, nil
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
func (d *Discoverer) HealthCheck(ctx context.Context) (connector.Health, error) {
	status, _, err := d.get(ctx,
		"/subscriptions/"+url.PathEscape(d.cfg.SubscriptionID),
		"2022-12-01")
	now := time.Now()
	if err != nil {
		return connector.Health{Status: connector.HealthDown, Detail: err.Error(), CheckedAt: now}, nil
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
		status, body, err := d.get(ctx, pageURL, "")
		if err != nil {
			return nil, fmt.Errorf("azure: list virtualMachines: %w", err)
		}
		if status != http.StatusOK {
			return nil, fmt.Errorf("azure: list virtualMachines: HTTP %d", status)
		}
		var listing vmList
		if err := json.Unmarshal(body, &listing); err != nil {
			return nil, fmt.Errorf("azure: decode virtualMachines: %w", err)
		}
		for _, vm := range listing.Value {
			nodes = append(nodes, d.normalize(vm))
		}
		if listing.NextLink == "" {
			break
		}
		pageURL = listing.NextLink
		// nextLink 是绝对 URL，直接作为下一页地址（get 会原样请求）。
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

// normalize 单台虚拟机 → ResourceNode。
func (d *Discoverer) normalize(vm vmResource) connector.ResourceNode {
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
		ObservedAt: time.Now().UTC(),
	}
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
// fullURL 为空时用 baseURL+path，否则直接请求 fullURL（分页 nextLink 场景）。
func (d *Discoverer) get(ctx context.Context, pathOrURL, apiVersion string) (int, []byte, error) {
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
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "Bearer "+d.cfg.Credential.Secret)

	resp, err := d.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	body, err := readLimited(resp.Body, d.cfg.MaxResponseBytes)
	if err != nil {
		return resp.StatusCode, nil, err
	}
	return resp.StatusCode, body, nil
}

// readLimited 按上限读取响应体，超过 limit 直接报错。
func readLimited(r io.Reader, limit int64) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(r, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > limit {
		return nil, fmt.Errorf("azure: response exceeds %d bytes", limit)
	}
	return body, nil
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
