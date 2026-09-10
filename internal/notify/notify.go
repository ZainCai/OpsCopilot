// Package notify 通知与抑制（M2 主干骨架，功能点 F-11）。
//
// 定位：影子降噪转正的执行件——影子期 verdict 只标注，转正后
// WouldSuppress=true 的告警被**真正拦截**，不进通知渠道。
//
// 主干范围：Notifier 接口、渠道注册表、Console 渠道（落日志，联调用）、
// Gate 抑制闸门（核心判定落地：WouldSuppress → 拦截并计数）。
// 渠道具体实现（IM/webhook）与升级规则留 W10（TODO 标注）。
package notify

import (
	"errors"
	"fmt"
	"strings"
	"sync"
)

// ErrNoChannel 未注册任何渠道（Gate 直接拒发——静默丢通知比报错危险）。
var ErrNoChannel = errors.New("notify: no channel registered")

// Message 通知消息（对齐告警/簇语义，不耦合内部类型）。
type Message struct {
	TenantID   string
	ClusterKey string
	Title      string
	Body       string
	Severity   string // critical / warning / info
}

// Notifier 通知渠道接口。
// 实现契约（R6 审核固化，W10 实渠道必须遵守）：
//  1. 并发安全；
//  2. **自带超时**（建议 ≤5s）——Gate.Admit 同步调用 Send，实现若长期
//     阻塞会拖垮调用方（告警处理路径）；
//  3. 失败返回 error 由 Gate/调用方计数，不得 panic。
type Notifier interface {
	Name() string
	Send(m Message) error
}

// ConsoleChannel 控制台渠道：落日志（主干默认渠道，联调用；W10 换 IM/webhook）。
type ConsoleChannel struct {
	Logf func(format string, args ...any) // 注入日志函数（main 的 logger）
}

// Name 渠道名。
func (c *ConsoleChannel) Name() string { return "console" }

// Send 落日志即成功。
func (c *ConsoleChannel) Send(m Message) error {
	if c.Logf == nil {
		return errors.New("notify: console channel has no logger")
	}
	c.Logf("[notify/%s] [%s] %s — %s", c.Name(), m.Severity, m.Title, m.Body)
	return nil
}

// Registry 渠道注册表（并发安全）。
type Registry struct {
	mu       sync.RWMutex
	channels map[string]Notifier
}

// NewRegistry 构造。
func NewRegistry() *Registry { return &Registry{channels: map[string]Notifier{}} }

// Register 注册渠道（同名覆盖——升级渠道即重注册）。
func (r *Registry) Register(n Notifier) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.channels[n.Name()] = n
}

// Names 已注册渠道名（稳定序）。
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.channels))
	for k := range r.channels {
		out = append(out, k)
	}
	// 简单插入序不稳定 → 这里排序保证测试确定性。
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j] < out[j-1]; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}

// Dispatch 全渠道分发（单渠道失败不拖累其余；全失败才报错）。
func (r *Registry) Dispatch(m Message) error {
	r.mu.RLock()
	chs := make([]Notifier, 0, len(r.channels))
	for _, c := range r.channels {
		chs = append(chs, c)
	}
	r.mu.RUnlock()
	if len(chs) == 0 {
		return ErrNoChannel
	}
	var errs []string
	for _, c := range chs {
		if err := c.Send(m); err != nil {
			errs = append(errs, c.Name()+": "+err.Error())
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("notify: dispatch failures: %s", strings.Join(errs, "; "))
	}
	return nil
}

// Gate 抑制闸门（F-11 核心判定，落地）：影子判决 → 决定放行或拦截。
type Gate struct {
	registry   *Registry
	mu         sync.Mutex
	suppressed int // 拦截计数（运维观察降噪效果的第一指标）
	dispatched int
}

// NewGate 构造。
func NewGate(r *Registry) *Gate { return &Gate{registry: r} }

// Decision 影子判决输入（只取判定所需字段，字符串接口解耦）。
type Decision struct {
	ClusterKey    string
	Severity      string
	Title         string
	WouldSuppress bool // noise 影子判决：true = 判定为噪声应拦截
}

// Stats 闸门计数。
type Stats struct{ Suppressed, Dispatched int }

// Admit 判定入口：WouldSuppress=true → 拦截（计数，不发）；false → 放行
// 全渠道。返回是否放行 + 错误（放行失败才算错误；拦截是正常结果）。
func (g *Gate) Admit(d Decision) (admitted bool, err error) {
	if strings.TrimSpace(d.ClusterKey) == "" && strings.TrimSpace(d.Title) == "" {
		return false, errors.New("notify: empty decision (no cluster/title)")
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if d.WouldSuppress {
		g.suppressed++
		return false, nil
	}
	if err := g.registry.Dispatch(Message{
		ClusterKey: d.ClusterKey, Title: d.Title,
		Body:     "cluster " + d.ClusterKey + " 未被降噪判定收敛，需要人工关注",
		Severity: d.Severity,
	}); err != nil {
		return false, err
	}
	g.dispatched++
	return true, nil
}

// Stats 读计数（锁内拷贝）。
func (g *Gate) Stats() Stats {
	g.mu.Lock()
	defer g.mu.Unlock()
	return Stats{Suppressed: g.suppressed, Dispatched: g.dispatched}
}
