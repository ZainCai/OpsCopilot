// M2 主干骨架测试：状态机合法性 / 簇关联 / 反查。
package incident

import (
	"errors"
	"testing"
	"time"
)

func TestStateMachineTransitions(t *testing.T) {
	legal := [][2]State{
		{StateOpen, StateAcked}, {StateOpen, StateMitigated}, {StateOpen, StateResolved},
		{StateAcked, StateMitigated}, {StateAcked, StateResolved},
		{StateMitigated, StateResolved},
	}
	for _, p := range legal {
		if !CanTransition(p[0], p[1]) {
			t.Fatalf("legal transition rejected: %s -> %s", p[0], p[1])
		}
	}
	illegal := [][2]State{
		{StateAcked, StateOpen}, {StateMitigated, StateAcked},
		{StateResolved, StateOpen}, {StateResolved, StateAcked},
	}
	for _, p := range illegal {
		if CanTransition(p[0], p[1]) {
			t.Fatalf("illegal transition accepted: %s -> %s", p[0], p[1])
		}
	}
}

func TestStoreCreateAndTransition(t *testing.T) {
	s := NewMemStore()
	var now time.Time
	s.SetClock(func() time.Time { now = now.Add(time.Minute); return now })

	// 缺字段拒绝。
	if _, err := s.Create("", "t", "critical", "ops"); err == nil {
		t.Fatal("empty id accepted")
	}
	if _, err := s.Create("INC-1", "", "critical", "ops"); err == nil {
		t.Fatal("empty title accepted")
	}
	// 重复 ID 拒绝。
	if _, err := s.Create("INC-1", "t1", "critical", "ops"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := s.Create("INC-1", "t1", "critical", "ops"); err == nil {
		t.Fatal("duplicate id accepted")
	}

	// 先 acked，再试非法回退 open → 拒绝。
	if _, err := s.Transition("INC-1", StateAcked, "ops"); err != nil {
		t.Fatalf("-> acked: %v", err)
	}
	if _, err := s.Transition("INC-1", StateOpen, "ops"); err == nil {
		t.Fatal("acked -> open accepted (want rejected per table)")
	}
	// 合法链路 acked→resolved，ResolvedAt 落戳。
	got, err := s.Transition("INC-1", StateResolved, "ops")
	if err != nil {
		t.Fatalf("-> resolved: %v", err)
	}
	if got.State != StateResolved || got.ResolvedAt.IsZero() || got.AckBy != "ops" {
		t.Fatalf("resolved incident wrong: %+v", got)
	}
	// 不存在的事件。
	if _, err := s.Transition("INC-X", StateAcked, "ops"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing incident: err = %v, want ErrNotFound", err)
	}
}

func TestAttachClusterAndLookup(t *testing.T) {
	s := NewMemStore()
	if _, err := s.Create("INC-1", "disk full", "critical", "ops"); err != nil {
		t.Fatalf("create: %v", err)
	}
	// 关联 + 幂等。
	if err := s.AttachCluster("INC-1", "c:fp-a@1"); err != nil {
		t.Fatalf("attach: %v", err)
	}
	if err := s.AttachCluster("INC-1", "c:fp-a@1"); err != nil {
		t.Fatalf("attach idempotent: %v", err)
	}
	// 一簇只能挂一事件。
	if _, err := s.Create("INC-2", "other", "warning", "ops"); err != nil {
		t.Fatalf("create2: %v", err)
	}
	if err := s.AttachCluster("INC-2", "c:fp-a@1"); err == nil {
		t.Fatal("cluster double-attach accepted")
	}
	// 反查。
	inc, ok := s.IncidentForCluster("c:fp-a@1")
	if !ok || inc.ID != "INC-1" {
		t.Fatalf("lookup = %+v ok=%v, want INC-1", inc, ok)
	}
	// 事件不存在。
	if err := s.AttachCluster("INC-X", "c:x@1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("attach to missing: err = %v", err)
	}
	// List 过滤。
	if got := len(s.List(StateOpen)); got != 2 {
		t.Fatalf("open list = %d, want 2", got)
	}
}
