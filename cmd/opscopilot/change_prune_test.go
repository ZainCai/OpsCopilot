// W9-5：变更事件库保留窗清理的测试（第八轮审核 C7：PruneBefore 生产未接线）。
package main

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"opscopilot/internal/config"
	"opscopilot/internal/topology"
)

// TestParseRetentionDefaultChangeWindow 变更库保留窗的取值口径：
// 与工单归档共用解析器（现居 internal/config），但**空值默认 7d**（而不是 90d）。
func TestParseRetentionDefaultChangeWindow(t *testing.T) {
	cases := []struct {
		raw  string
		want time.Duration
		on   bool
		bad  bool
	}{
		{"", 7 * 24 * time.Hour, true, false}, // 空值 = 默认 7d（本模块口径）
		{"7d", 7 * 24 * time.Hour, true, false},
		{"168h", 168 * time.Hour, true, false},
		{"off", 0, false, false},
		{"0", 0, false, false},
		{"bogus", 0, false, true},
	}
	for _, c := range cases {
		d, on, err := config.ParseRetention(c.raw, config.DefaultChangeRetention)
		if c.bad {
			if err == nil {
				t.Fatalf("%q: want error", c.raw)
			}
			continue
		}
		if err != nil {
			t.Fatalf("%q: %v", c.raw, err)
		}
		if on != c.on || (on && d != c.want) {
			t.Fatalf("%q: got (%v,%v), want (%v,%v)", c.raw, d, on, c.want, c.on)
		}
	}
}

// TestChangePrunerPrunesOldEvents 过期事件被清、新事件保留，且**每轮都打容量日志**。
func TestChangePrunerPrunesOldEvents(t *testing.T) {
	store := topology.NewChangeStore(nil) // nodeCheck=nil：不校验节点存在性
	now := time.Now()
	seed := func(id string, at time.Time) {
		t.Helper()
		if _, err := store.Record(topology.ChangeEvent{
			ID: id, NodeKey: "n1", Type: topology.ChangeDeploy,
			Source: "manual", OccurredAt: at,
		}); err != nil {
			t.Fatalf("record %s: %v", id, err)
		}
	}
	seed("old-1", now.Add(-8*24*time.Hour))             // 超期
	seed("old-2", now.Add(-7*24*time.Hour-time.Second)) // 超期（严格早于 cutoff）
	seed("fresh-1", now.Add(-time.Hour))                // 保留
	seed("fresh-2", now)                                // 保留
	if got := store.Len(); got != 4 {
		t.Fatalf("seeded size = %d, want 4", got)
	}

	var logs []string
	p := NewChangePruner(store, 7*24*time.Hour, time.Hour, func(f string, a ...any) {
		logs = append(logs, fmt.Sprintf(f, a...))
	})

	removed, size := p.pruneOnce(now)
	if removed != 2 {
		t.Fatalf("removed = %d, want 2", removed)
	}
	if size != 2 {
		t.Fatalf("size after prune = %d, want 2", size)
	}
	if store.Len() != 2 {
		t.Fatalf("store.Len() = %d, want 2", store.Len())
	}
	// 容量日志是本次接线的另一半价值：数字必须出现在日志里。
	if len(logs) != 1 || !strings.Contains(logs[0], "size=2") || !strings.Contains(logs[0], "pruned=2") {
		t.Fatalf("capacity log missing/wrong: %v", logs)
	}

	// 幂等：再跑一轮无变化，但仍打日志（容量是持续观测量）。
	removed, size = p.pruneOnce(now)
	if removed != 0 || size != 2 {
		t.Fatalf("second round = (%d,%d), want (0,2)", removed, size)
	}
	if len(logs) != 2 {
		t.Fatalf("want a log line per round, got %d", len(logs))
	}
}

// TestChangePrunerWarnsOnLargeStore 容量超阈值时打 WARNING（内存膨胀必须可见）。
func TestChangePrunerWarnsOnLargeStore(t *testing.T) {
	store := topology.NewChangeStore(nil)
	now := time.Now()
	for i := 0; i < changeStoreWarnSize; i++ {
		if _, err := store.Record(topology.ChangeEvent{
			ID: fmt.Sprintf("ev-%d", i), NodeKey: "n1", Type: topology.ChangeConfig,
			Source: "manual", OccurredAt: now,
		}); err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
	}
	var logs []string
	p := NewChangePruner(store, 7*24*time.Hour, time.Hour, func(f string, a ...any) {
		logs = append(logs, fmt.Sprintf(f, a...))
	})
	removed, size := p.pruneOnce(now)
	if removed != 0 || size != changeStoreWarnSize {
		t.Fatalf("got (%d,%d), want (0,%d)", removed, size, changeStoreWarnSize)
	}
	if len(logs) != 2 || !strings.HasPrefix(logs[0], "WARNING:") {
		t.Fatalf("want WARNING + capacity log, got %v", logs)
	}
}

// TestChangePrunerFromConfigOff 关闭时 Run 立即返回且不打点（store 保持不碰）。
func TestChangePrunerFromConfigOff(t *testing.T) {
	topo := config.Defaults().Topology
	topo.ChangeEnabled = false
	p := NewChangePrunerFromConfig(topology.NewChangeStore(nil), topo, nil)
	if p.store != nil || p.retention != 0 {
		t.Fatalf("off 应当不持有 store/retention，得到 store=%v retention=%v", p.store, p.retention)
	}
	var logs []string
	p2 := NewChangePruner(nil, 0, 0, func(f string, a ...any) { logs = append(logs, fmt.Sprintf(f, a...)) })
	p2.Run(context.Background()) // 未启用：立即返回，不阻塞
	if len(logs) != 1 || !strings.Contains(logs[0], "OFF") {
		t.Fatalf("want OFF log, got %v", logs)
	}
}

// TestChangePrunerInvalidEnvFailsAtLoad 非法配置 fail-fast 的新归属：
// OPS_CHANGE_RETENTION / OPS_CHANGE_PRUNE_INTERVAL 的非法值在 config.Load
// 聚合报错（原 NewChangePrunerFromEnv 的构造期报错随之上移，静默回退取消）。
func TestChangePrunerInvalidEnvFailsAtLoad(t *testing.T) {
	env := func(m map[string]string) config.LookupFunc {
		return func(k string) (string, bool) { v, ok := m[k]; return v, ok }
	}
	base := func() map[string]string {
		return map[string]string{
			config.EnvRedisAlertAddr: "127.0.0.1:6380",
			config.EnvRedisCacheAddr: "127.0.0.1:6381",
		}
	}
	for _, c := range []struct {
		key, val string
	}{
		{config.EnvChangeRetention, "bogus"},
		{config.EnvChangePruneInterval, "not-a-duration"},
		{config.EnvChangePruneInterval, "-1s"},
	} {
		m := base()
		m[c.key] = c.val
		if _, err := config.LoadFrom(env(m)); err == nil {
			t.Fatalf("%s=%q must fail fast, got nil error", c.key, c.val)
		}
	}
	// 合法值正常装载，off 透传为 ChangeEnabled=false。
	m := base()
	m[config.EnvChangeRetention] = "off"
	cfg, err := config.LoadFrom(env(m))
	if err != nil || cfg.Topology.ChangeEnabled {
		t.Fatalf("retention=off must load enabled=false: %v %+v", err, cfg.Topology)
	}
}
