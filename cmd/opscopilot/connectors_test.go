// W4-1.1 连接器装配测试：锁定 env → 连接器注册的行为契约。
package main

import (
	"log"
	"testing"

	"opscopilot/internal/credential"
)

// newQuietLogger 静音日志（测试不刷屏）。
func newQuietLogger() *log.Logger { return log.New(&discardWriter{}, "", 0) }

type discardWriter struct{}

func (d *discardWriter) Write(p []byte) (int, error) { return len(p), nil }

func TestNewConnectorHost_NoEnv(t *testing.T) {
	creds := credential.NewStore()
	host, registered, err := newConnectorHost(newQuietLogger(), creds)
	if err != nil {
		t.Fatalf("no env should not error: %v", err)
	}
	if host == nil {
		t.Fatal("host must not be nil (empty-run host keeps topology+webhook serviceable)")
	}
	if len(registered) != 0 {
		t.Fatalf("registered = %v, want empty", registered)
	}
}

func TestNewConnectorHost_Prometheus(t *testing.T) {
	t.Setenv("OPS_PROM_URL", "http://localhost:9090")
	t.Setenv("OPS_PROM_TOKEN", "test-token")

	creds := credential.NewStore()
	host, registered, err := newConnectorHost(newQuietLogger(), creds)
	if err != nil {
		t.Fatalf("prometheus assembly: %v", err)
	}
	if len(registered) != 1 || registered[0] != "prometheus" {
		t.Fatalf("registered = %v, want [prometheus]", registered)
	}
	if _, ok := host.Get("prometheus"); !ok {
		t.Fatal("prometheus connector not registered into host")
	}
	// 凭证必须经只读闸门入库（W4-1.1 的 credential 接线点）。
	if _, err := creds.Get("prometheus"); err != nil {
		t.Fatalf("credential not in store: %v", err)
	}
}

func TestNewConnectorHost_AzureRequiresBoth(t *testing.T) {
	// 只配订阅 ID 不配令牌：视为未配置该数据源，不注册、不报错。
	t.Setenv("OPS_AZURE_SUBSCRIPTION_ID", "sub-000")
	creds := credential.NewStore()
	host, registered, err := newConnectorHost(newQuietLogger(), creds)
	if err != nil {
		t.Fatalf("partial azure env should not error: %v", err)
	}
	if len(registered) != 0 {
		t.Fatalf("registered = %v, want empty", registered)
	}

	// 齐备才注册。
	t.Setenv("OPS_AZURE_TOKEN", "arm-token")
	host, registered, err = newConnectorHost(newQuietLogger(), creds)
	if err != nil {
		t.Fatalf("azure assembly: %v", err)
	}
	if len(registered) != 1 || registered[0] != "azure" {
		t.Fatalf("registered = %v, want [azure]", registered)
	}
	if _, ok := host.Get("azure"); !ok {
		t.Fatal("azure connector not registered into host")
	}
	if _, err := creds.Get("azure"); err != nil {
		t.Fatalf("azure credential not in store: %v", err)
	}
}

func TestNewConnectorHost_BothSources(t *testing.T) {
	t.Setenv("OPS_PROM_URL", "http://prom:9090")
	t.Setenv("OPS_PROM_TOKEN", "prom-token")
	t.Setenv("OPS_AZURE_SUBSCRIPTION_ID", "sub-000")
	t.Setenv("OPS_AZURE_TOKEN", "arm-token")

	creds := credential.NewStore()
	_, registered, err := newConnectorHost(newQuietLogger(), creds)
	if err != nil {
		t.Fatalf("both-source assembly: %v", err)
	}
	if len(registered) != 2 {
		t.Fatalf("registered = %v, want 2 entries", registered)
	}
	if creds.Len() != 2 {
		t.Fatalf("credential store Len = %d, want 2", creds.Len())
	}
}
