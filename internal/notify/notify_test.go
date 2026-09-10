// M2 主干骨架测试：渠道注册/分发/抑制闸门计数。
package notify

import (
	"errors"
	"strings"
	"testing"
)

type chanStub struct {
	name string
	err  error
	sent *int
}

func (c *chanStub) Name() string { return c.name }
func (c *chanStub) Send(m Message) error {
	if c.sent != nil {
		*c.sent++
	}
	return c.err
}

func TestRegistryRegisterAndNames(t *testing.T) {
	r := NewRegistry()
	r.Register(&chanStub{name: "console"})
	r.Register(&chanStub{name: "webhook"})
	names := r.Names()
	if len(names) != 2 || names[0] != "console" || names[1] != "webhook" {
		t.Fatalf("names = %v, want [console webhook]", names)
	}
}

func TestDispatchAllFailAggregates(t *testing.T) {
	r := NewRegistry()
	r.Register(&chanStub{name: "a", err: errors.New("x")})
	r.Register(&chanStub{name: "b", err: errors.New("y")})
	err := r.Dispatch(Message{Title: "t"})
	if err == nil || !strings.Contains(err.Error(), "x") || !strings.Contains(err.Error(), "y") {
		t.Fatalf("dispatch err = %v, want aggregated x+y", err)
	}
	// 无渠道 → 明确报错。
	if err := NewRegistry().Dispatch(Message{Title: "t"}); !errors.Is(err, ErrNoChannel) {
		t.Fatalf("empty registry: err = %v", err)
	}
}

func TestGateSuppressionCounting(t *testing.T) {
	var sent int
	r := NewRegistry()
	r.Register(&chanStub{name: "console", sent: &sent})
	g := NewGate(r)

	// 噪声 → 拦截，不发。
	ok, err := g.Admit(Decision{ClusterKey: "c:fp@1", Title: "noise", WouldSuppress: true})
	if err != nil || ok {
		t.Fatalf("suppress: ok=%v err=%v, want false/nil", ok, err)
	}
	// 真事件 → 放行，渠道收到。
	ok, err = g.Admit(Decision{ClusterKey: "c:fp2@1", Title: "real", Severity: "critical"})
	if err != nil || !ok {
		t.Fatalf("admit: ok=%v err=%v, want true/nil", ok, err)
	}
	if sent != 1 {
		t.Fatalf("channel sent = %d, want 1", sent)
	}
	st := g.Stats()
	if st.Suppressed != 1 || st.Dispatched != 1 {
		t.Fatalf("stats = %+v, want 1/1", st)
	}
	// 空判定拒绝。
	if _, err := g.Admit(Decision{}); err == nil {
		t.Fatal("empty decision accepted")
	}
}
