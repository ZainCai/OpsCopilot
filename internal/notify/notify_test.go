// M2 主干骨架测试：渠道注册/分发/抑制闸门计数。
package notify

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
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

// ---- 优化方案 #3：Gate.Admit 锁边界（渠道网络 IO 不持锁）----

// blockingSendChan 假慢渠道：Send 阻塞直到 release 关闭（模拟单渠道网络
// 慢响应）。min 严重级路由：info 消息不会被它收走。
type blockingSendChan struct {
	name    string
	min     string
	entered chan struct{} // Send 进入时关闭一次
	release chan struct{} // 关闭后 Send 返回
}

func (c *blockingSendChan) Name() string        { return c.name }
func (c *blockingSendChan) MinSeverity() string { return c.min }
func (c *blockingSendChan) Send(m Message) error {
	select {
	case <-c.entered: // 只报第一次进入
	default:
		close(c.entered)
	}
	<-c.release
	return nil
}

// countedChan 并发安全计数渠道（不带 SeverityFilter = 全收）。
type countedChan struct {
	mu   sync.Mutex
	name string
	n    int
}

func (c *countedChan) Name() string { return c.name }
func (c *countedChan) Send(m Message) error {
	c.mu.Lock()
	c.n++
	c.mu.Unlock()
	return nil
}
func (c *countedChan) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}

// TestGateAdmitSendsOutsideLock 慢渠道 Send 期间，闸门锁必须已释放：
// ① 拦截判决不被串行阻塞；② Stats 不被阻塞；③ 只路由到快渠道的
// 放行不被慢渠道拖住。修复前（Dispatch 在 g.mu 临界区内）这三条都会
// 卡到 release 关闭才返回 → 各自 2s 超时判负。
func TestGateAdmitSendsOutsideLock(t *testing.T) {
	slow := &blockingSendChan{
		name: "slow", min: "critical",
		entered: make(chan struct{}), release: make(chan struct{}),
	}
	fast := &countedChan{name: "fast"}
	r := NewRegistry()
	r.Register(slow)
	r.Register(fast)
	g := NewGate(r)

	admit1 := make(chan error, 1)
	go func() {
		_, err := g.Admit(Decision{ClusterKey: "c:slow", Title: "慢渠道阻塞中", Severity: "critical"})
		admit1 <- err
	}()
	select {
	case <-slow.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("慢渠道 Send 未启动（Admit #1 没走到分发？）")
	}

	// 慢渠道仍阻塞时，锁必须已交出：三件事都得在毫秒级完成。
	t.Run("拦截判决不被慢渠道串行", func(t *testing.T) {
		done := make(chan struct{})
		go func() {
			ok, err := g.Admit(Decision{ClusterKey: "c:dup", Title: "dup",
				WouldSuppress: true, Reason: "dedup-window"})
			if err != nil || ok {
				t.Errorf("suppress admit: ok=%v err=%v, want false/nil", ok, err)
			}
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("拦截判决被慢渠道 Send 阻塞 —— Admit 持锁跨网络 IO")
		}
	})
	t.Run("Stats不被慢渠道阻塞", func(t *testing.T) {
		done := make(chan Stats)
		go func() { done <- g.Stats() }()
		select {
		case st := <-done:
			if st.Suppressed != 1 {
				t.Fatalf("stats = %+v, want suppressed=1", st)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("Stats 被慢渠道 Send 阻塞 —— Admit 持锁跨网络 IO")
		}
	})
	t.Run("快渠道不被慢渠道串行", func(t *testing.T) {
		done := make(chan error, 1)
		go func() {
			// info 被 slow(min=critical) 路由掉 → 只发 fast，不该等慢渠道。
			_, err := g.Admit(Decision{ClusterKey: "c:fast", Title: "快", Severity: "info"})
			done <- err
		}()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("fast admit: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("快渠道放行被慢渠道 Send 阻塞 —— Admit 持锁跨网络 IO")
		}
		// 计数不在这里断言：Admit #1（critical）的快照里 fast 可能排在
		// slow 之前已先行送达，快照顺序不稳定；总送达数在 release 后校验。
	})

	close(slow.release)
	if err := <-admit1; err != nil {
		t.Fatalf("slow admit: %v", err)
	}
	// critical 那单也送达 fast（slow+fast 都收）；计数与放行语义不变。
	if got := fast.count(); got != 2 {
		t.Fatalf("fast count = %d, want 2", got)
	}
	st := g.Stats()
	if st.Dispatched != 2 || st.Suppressed != 1 {
		t.Fatalf("gate stats = %+v, want dispatched=2 suppressed=1", st)
	}
}
