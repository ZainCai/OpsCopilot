// W4-1.4 影子引擎测试：判决语义 + "只标注不拦截"纪律。
package noise

import (
	"testing"
	"time"
)

func TestShadowFirstAlertIsNewIncident(t *testing.T) {
	s := NewShadow(10*time.Minute, nil)
	v := s.Process(Event{Fingerprint: "fp1", NodeKey: "n1", OccurredAt: base})
	if v.WouldConverge || v.WouldSuppress || v.ClusterCreated != true {
		t.Fatalf("first alert verdict wrong: %+v", v)
	}
	if v.Reason != ReasonNewIncident {
		t.Fatalf("reason = %s, want new-incident", v.Reason)
	}
	if v.ClusterKey == "" {
		t.Fatal("clustered alert must carry cluster key")
	}
}

func TestShadowDuplicateWithinWindow(t *testing.T) {
	s := NewShadow(10*time.Minute, nil)
	s.Process(Event{Fingerprint: "fp1", NodeKey: "n1", OccurredAt: base})
	v := s.Process(Event{Fingerprint: "fp1", NodeKey: "n1", OccurredAt: base.Add(time.Minute)})
	if !v.WouldSuppress || !v.WouldConverge {
		t.Fatalf("duplicate must be suppressed+converge: %+v", v)
	}
	if v.Reason != ReasonDedupWindow {
		t.Fatalf("reason = %s, want dedup-window", v.Reason)
	}
	if v.ClusterCreated {
		t.Fatal("duplicate must not create a second cluster")
	}
}

func TestShadowDomainMergeNotSuppressButConverge(t *testing.T) {
	d := fakeDomain{"n1": {"n2"}}
	s := NewShadow(10*time.Minute, d.same)
	s.Process(Event{Fingerprint: "fpA", NodeKey: "n1", OccurredAt: base})
	v := s.Process(Event{Fingerprint: "fpB", NodeKey: "n2", OccurredAt: base.Add(time.Minute)})
	// 不同指纹：去重器不拦；但故障域连通 → 并入既有簇 → 应收敛。
	if v.WouldSuppress {
		t.Fatal("different fingerprint must not hit dedup")
	}
	if !v.WouldConverge || v.Reason != ReasonClusterMerge {
		t.Fatalf("domain-merged alert verdict wrong: %+v", v)
	}
}

func TestShadowEmptyFingerprintPassthrough(t *testing.T) {
	s := NewShadow(10*time.Minute, nil)
	v := s.Process(Event{Fingerprint: "", OccurredAt: base})
	if v.WouldConverge || v.ClusterKey != "" || v.Reason != ReasonNewIncident {
		t.Fatalf("empty fingerprint verdict wrong: %+v", v)
	}
	if s.Dedup().Len() != 0 || s.Clusterer().ActiveCount() != 0 {
		t.Fatal("empty fingerprint must not be recorded anywhere")
	}
}

func TestShadowWindowExpiryResets(t *testing.T) {
	s := NewShadow(10*time.Minute, nil)
	s.Process(Event{Fingerprint: "fp1", NodeKey: "n1", OccurredAt: base})
	v := s.Process(Event{Fingerprint: "fp1", NodeKey: "n1", OccurredAt: base.Add(20 * time.Minute)})
	if v.WouldSuppress || !v.ClusterCreated {
		t.Fatalf("post-window alert must be a fresh incident: %+v", v)
	}
	if v.Reason != ReasonNewIncident {
		t.Fatalf("reason = %s, want new-incident", v.Reason)
	}
}

func TestShadowZeroWindowAllNewIncidents(t *testing.T) {
	s := NewShadow(0, nil)
	v1 := s.Process(Event{Fingerprint: "fp1", OccurredAt: base})
	v2 := s.Process(Event{Fingerprint: "fp1", OccurredAt: base.Add(time.Second)})
	if v1.WouldConverge || v2.WouldConverge {
		t.Fatal("zero window =降噪关闭, everything is new-incident")
	}
}

// TestShadowNeverBlocks 影子纪律：Process 无论返回什么判决，
// 引擎内部状态只做记录——没有可"消费掉"告警的副作用。
// 本测试用同一条告警反复 Process，确认判定稳定可重放。
func TestShadowNeverBlocks(t *testing.T) {
	s := NewShadow(10*time.Minute, nil)
	e := Event{Fingerprint: "fp1", NodeKey: "n1", OccurredAt: base}
	first := s.Process(e)
	// 同一事件重复判定：确定性（判决定义在事件序上，重放得同果）。
	again := s.Process(Event{Fingerprint: e.Fingerprint, NodeKey: e.NodeKey, OccurredAt: e.OccurredAt})
	if first.ClusterKey != again.ClusterKey {
		t.Fatalf("same replay diverged: %s vs %s", first.ClusterKey, again.ClusterKey)
	}
	if !again.WouldConverge {
		t.Fatal("replay of the same event must be flagged as converge (it IS the duplicate)")
	}
}

func TestShadowSetDomainSwapsTopology(t *testing.T) {
	s := NewShadow(10*time.Minute, nil)
	s.Process(Event{Fingerprint: "fpA", NodeKey: "n1", OccurredAt: base})
	// 无域函数：n2 新建簇。
	if v := s.Process(Event{Fingerprint: "fpB", NodeKey: "n2", OccurredAt: base.Add(time.Second)}); !v.ClusterCreated {
		t.Fatal("without domain func, distinct nodes must not merge")
	}
	// 注入域函数后：同窗口内新指纹 n3（与 n1 连通）并入 fpA 所在簇。
	s.SetDomain(fakeDomain{"n1": {"n3"}}.same)
	v := s.Process(Event{Fingerprint: "fpC", NodeKey: "n3", OccurredAt: base.Add(2 * time.Second)})
	if v.ClusterCreated || v.Reason != ReasonClusterMerge {
		t.Fatalf("domain swap not effective: %+v", v)
	}
}
