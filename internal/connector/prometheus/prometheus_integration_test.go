//go:build integration

package prometheus

import (
	"context"
	"testing"
	"time"

	"opscopilot/internal/connector"
)

// demoPrometheus Prometheus 官方维护的公开 demo 实例（只读）。
//
// 端点可用性验证（2026-09-09）：
//   - /-/healthy                    -> "Prometheus Server is Healthy."
//   - /api/v1/query?query=up        -> 真实 vector 数据（含 blackbox/caddy/node/prometheus 等 job）
//   - /api/v1/targets?state=active  -> 真实抓取目标
//   - 不存在路径 -> 404（说明是真实服务而非代理拦截）
//
// 注意：曾尝试的 prometheus.demo.do.prometheus.io（域名失效）与
// demo.robustperception.io:9090（502）均已不可用，勿再使用。
const demoPrometheus = "https://prometheus.demo.prometheus.io"

// TestIntegration_RealPrometheus 真实数据源端到端验证（依赖外网）。
//
// 与单元测试隔离的原因：外网可用性不受控，不应影响 CI。
// 运行方式：
//
//	go test -tags integration ./internal/connector/prometheus/ -v -run Integration
func TestIntegration_RealPrometheus(t *testing.T) {
	c, err := New(Config{
		ID:      "demo",
		BaseURL: demoPrometheus,
		Queries: []Query{{Name: "up", Expr: "up"}},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// 1) 健康检查
	h, err := c.HealthCheck(ctx)
	if err != nil {
		t.Fatalf("HealthCheck: %v", err)
	}
	if h.Status != connector.HealthHealthy {
		t.Fatalf("health = %q, want healthy", h.Status)
	}
	t.Logf("[1] 健康检查: %s (%s)", h.Status, h.Detail)

	// 2) 指标采集（真实 PromQL）
	res, err := c.Collect(ctx, connector.CollectRequest{})
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if len(res.Metrics) == 0 {
		t.Fatal("真实 Prometheus 应返回 up 指标样本")
	}
	t.Logf("[2] 指标采集: %d 条样本", len(res.Metrics))
	for i, m := range res.Metrics {
		if i >= 4 {
			break
		}
		t.Logf("    %s{job=%s,instance=%s} = %v @ %s",
			m.Name, m.Labels["job"], m.Labels["instance"], m.Value,
			m.Timestamp.Format("15:04:05.000"))
	}

	// 3) 亚秒精度：demo 返回的时间戳带小数（如 1788930723.964），
	//    若实现把浮点秒截断为整秒，这里会全部为 0。
	var subSecond int
	for _, m := range res.Metrics {
		if m.Timestamp.Nanosecond() != 0 {
			subSecond++
		}
	}
	if subSecond == 0 {
		t.Error("所有样本时间戳的亚秒部分均为 0 —— 疑似精度被截断")
	} else {
		t.Logf("[3] 时间戳亚秒精度: %d/%d 条保留亚秒", subSecond, len(res.Metrics))
	}

	// 4) 资源发现
	disc, err := c.Discover(ctx)
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(disc.Nodes) == 0 {
		t.Fatal("真实 Prometheus 应有活跃抓取目标")
	}
	t.Logf("[4] 资源发现: %d 个节点", len(disc.Nodes))
	for i, n := range disc.Nodes {
		if i >= 4 {
			break
		}
		t.Logf("    %s (type=%s, health=%s)", n.Key, n.Type, n.Labels["health"])
	}

	// 5) 告警（demo 可能无 firing 告警，只要拉取不报错即可）
	t.Logf("[5] 告警: %d 条（demo 可能为空）", len(res.Alerts))

	// 6) 增量参数必须被明确拒绝，而不是静默返回全量
	if _, err := c.Collect(ctx, connector.CollectRequest{Since: time.Now()}); err == nil {
		t.Error("Since 应返回 ErrUnsupported")
	} else {
		t.Logf("[6] Since 已明确拒绝: %v", err)
	}
}
