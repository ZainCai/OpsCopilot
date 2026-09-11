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

// sevChanStub 带最低严重级的渠道（SeverityFilter 实现），记录收到的严重级。
type sevChanStub struct {
	name string
	min  string
	got  *[]string
}

func (c *sevChanStub) Name() string        { return c.name }
func (c *sevChanStub) MinSeverity() string { return c.min }
func (c *sevChanStub) Send(m Message) error {
	*c.got = append(*c.got, m.Severity)
	return nil
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

// TestSeverityRank 严重级排序：critical>warning>info；空/未知按最严（critical）。
func TestSeverityRank(t *testing.T) {
	for in, want := range map[string]int{
		"critical": 3, "CRITICAL": 3, " warning ": 2, "info": 1,
		"": 3, "bogus": 3, // 未知值不得降级为"最低"——失败模式是多通知而非静默丢弃
	} {
		if got := SeverityRank(in); got != want {
			t.Fatalf("SeverityRank(%q) = %d, want %d", in, got, want)
		}
	}
}

// TestDispatchSeverityRouting 按严重级路由：
// console(info) 全收；warn-only 收 warning+critical；crit-only 只收 critical。
func TestDispatchSeverityRouting(t *testing.T) {
	var warnOnly, critOnly []string
	r := NewRegistry()
	r.Register(&ConsoleChannel{Logf: func(string, ...any) {}}) // info 全收
	r.Register(&sevChanStub{name: "warn", min: "warning", got: &warnOnly})
	r.Register(&sevChanStub{name: "crit", min: "critical", got: &critOnly})

	// info：只有 console 收（warn/crit 过滤掉）
	if err := r.Dispatch(Message{Title: "t", Severity: "info"}); err != nil {
		t.Fatalf("dispatch info: %v", err)
	}
	// warning：console + warn
	if err := r.Dispatch(Message{Title: "t", Severity: "warning"}); err != nil {
		t.Fatalf("dispatch warning: %v", err)
	}
	// critical：console + warn + crit
	if err := r.Dispatch(Message{Title: "t", Severity: "critical"}); err != nil {
		t.Fatalf("dispatch critical: %v", err)
	}
	if len(warnOnly) != 2 || warnOnly[0] != "warning" || warnOnly[1] != "critical" {
		t.Fatalf("warn-only got %v, want [warning critical]", warnOnly)
	}
	if len(critOnly) != 1 || critOnly[0] != "critical" {
		t.Fatalf("crit-only got %v, want [critical]", critOnly)
	}
}

// TestDispatchAllFilteredNoChannel 全部渠道被严重级过滤掉 → ErrNoChannel
// （确定性：而不是静默返回 nil 让调用方以为发出去了）。
func TestDispatchAllFilteredNoChannel(t *testing.T) {
	var got []string
	r := NewRegistry()
	r.Register(&sevChanStub{name: "crit", min: "critical", got: &got})
	if err := r.Dispatch(Message{Title: "t", Severity: "info"}); !errors.Is(err, ErrNoChannel) {
		t.Fatalf("all-filtered dispatch err = %v, want ErrNoChannel", err)
	}
	if len(got) != 0 {
		t.Fatalf("crit channel received %v, want none", got)
	}
}
