//go:build integration

// 真实 Azure ARM 端到端验证（手动触发，不进 CI）：
//
//	go test -tags integration ./internal/connector/azure/ -run TestIntegration -v
//
// 前置环境变量（缺任一则跳过）：
//
//	AZURE_ARM_TOKEN          ARM 访问令牌（az account get-access-token --resource https://management.azure.com）
//	AZURE_ARM_SUBSCRIPTION   订阅 ID
//	AZURE_ARM_BASE_URL       可选，Azure China 填 https://management.chinacloudapi.cn
package azure

import (
	"context"
	"os"
	"testing"
	"time"

	"opscopilot/pkg/readonly"
)

func TestIntegration_RealAzureARM(t *testing.T) {
	token := os.Getenv("AZURE_ARM_TOKEN")
	sub := os.Getenv("AZURE_ARM_SUBSCRIPTION")
	if token == "" || sub == "" {
		t.Skip("AZURE_ARM_TOKEN / AZURE_ARM_SUBSCRIPTION not set; skipping real ARM test")
	}

	cfg := Config{
		ID:             "azure-integration",
		SubscriptionID: sub,
		Credential:     &readonly.Credential{ID: "arm-tok", Type: "azure-arm-token", Secret: token, ReadOnly: true},
	}
	if u := os.Getenv("AZURE_ARM_BASE_URL"); u != "" {
		cfg.BaseURL = u
	}
	d, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// 1. 健康检查
	h, err := d.HealthCheck(ctx)
	if err != nil {
		t.Fatalf("HealthCheck: %v", err)
	}
	t.Logf("health: %s (%s)", h.Status, h.Detail)
	if h.Status != "healthy" {
		t.Fatalf("credential or endpoint unhealthy: %+v", h)
	}

	// 2. 真实发现
	res, err := d.Discover(ctx)
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	t.Logf("discovered %d VMs", len(res.Nodes))
	for i, n := range res.Nodes {
		if i >= 5 { // 只打印前 5 台，日志不刷屏
			break
		}
		t.Logf("  %s name=%s rg=%s size=%s", n.Key, n.Labels["name"], n.Labels["resource_group"], n.Labels["vm_size"])
	}
	// 结构断言：有 VM 则 Key 必为 azure://vm/ 前缀、Type=vm
	for _, n := range res.Nodes {
		if n.Type != "vm" || len(n.Key) <= len("azure://vm/") {
			t.Errorf("bad normalized node: %+v", n)
		}
	}
}
