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
//  2. **自带超时**（建议 ≤5s）——Gate.Admit 同步调用 Send（锁外），
//     实现长期阻塞会拖垮本次放行（告警处理路径），但不再串行化
//     其余判决；
//  3. 失败返回 error 由 Gate/调用方计数，不得 panic。
type Notifier interface {
	Name() string
	Send(m Message) error
}

// SeverityFilter 渠道可选能力（W9-3 值班路由）：声明本渠道接收的**最低
// 严重级**。不实现 = 接收全部（等价 info）。Gate 按告警严重级逐个渠道
// 判定——"critical 进 IM、info 只留日志"由此表达。
type SeverityFilter interface {
	MinSeverity() string
}

// 严重级排序：critical > warning > info。**未知/空值按 critical 处理**
// ——路由的失败模式必须是"多通知"而不是"静默丢弃"。
func SeverityRank(sev string) int {
	switch strings.ToLower(strings.TrimSpace(sev)) {
	case "critical":
		return 3
	case "warning":
		return 2
	case "info":
		return 1
	case "":
		return 3
	default:
		return 3
	}
}

// channelAccepts 渠道是否接收该严重级（未实现 SeverityFilter = 全接）。
func channelAccepts(c Notifier, msgSeverity string) bool {
	f, ok := c.(SeverityFilter)
	if !ok {
		return true
	}
	return SeverityRank(msgSeverity) >= SeverityRank(f.MinSeverity())
}

// ConsoleChannel 控制台渠道：落日志（兜底渠道，联调与降级用）。
// MinSeverity 恒为 info（接收全部）：enforce 下零渠道 = 通知静默丢失，
// 兜底渠道必须"什么都能收"，它是最后一道留痕。
type ConsoleChannel struct {
	Logf func(format string, args ...any) // 注入日志函数（main 的 logger）
}

// Name 渠道名。
func (c *ConsoleChannel) Name() string { return "console" }

// MinSeverity 兜底渠道最低严重级（info = 全部）。
func (c *ConsoleChannel) MinSeverity() string { return "info" }

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

// Remove 注销渠道（配置删除/禁用后重载时调用）。
func (r *Registry) Remove(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.channels, name)
}

// Clear 清空全部渠道（配置整体重载用：先清后注册，禁用/删除不留残影）。
func (r *Registry) Clear() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.channels = map[string]Notifier{}
}

// Len 已注册渠道数。
func (r *Registry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.channels)
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

// channelsFor 按严重级快照"应收"渠道列表（复制 slice）：Registry 读锁
// 只保护查表，Send 一律在调用方锁外执行——渠道网络 IO 不得持有任何锁。
func (r *Registry) channelsFor(severity string) []Notifier {
	r.mu.RLock()
	defer r.mu.RUnlock()
	chs := make([]Notifier, 0, len(r.channels))
	for _, c := range r.channels {
		if channelAccepts(c, severity) {
			chs = append(chs, c)
		}
	}
	return chs
}

// dispatchTo 向快照渠道逐个 Send（单渠道失败不拖累其余；有失败才聚合
// 报错）。零渠道 = ErrNoChannel（静默丢通知比报错危险）。
func dispatchTo(chs []Notifier, m Message) error {
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

// Dispatch 全渠道分发（W9-3：按严重级路由——低于渠道 MinSeverity 的
// 渠道跳过；单渠道失败不拖累其余；全失败才报错）。
//
// 路由跳过不计入错误：通知已由其它渠道送达，且 console 兜底渠道
// 恒接收全部，不存在"全被路由掉导致静默丢失"的组合。
func (r *Registry) Dispatch(m Message) error {
	return dispatchTo(r.channelsFor(m.Severity), m)
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
	TenantID      string
	ClusterKey    string
	Severity      string
	Title         string
	WouldSuppress bool // noise 影子判决：true = 判定为窗口内重复，应拦截
	// Reason 收敛原因（noise.Reason* 取值）：
	//   "new-incident"  → 放行通知（新事件，人要第一次知道）
	//   "cluster-merge" → 拦截（同一故障域并入既有簇，通知过一次了）
	//   "dedup-window"  → 拦截（窗口内重复）
	// 空串按 new-incident 处理（无指纹/未参与判定的告警：宁可多通知）。
	Reason string
}

// Stats 闸门计数。
// Suppressed 是"未发通知"的总数（窗口去重 + 故障域并入两类之和）——
// 细分原因看 alert_event 的 payload.reason（每告警一条判决，可对账）。
type Stats struct{ Suppressed, Dispatched int }

// ShouldNotify 通知判定（W9-2 语义，验收口径"dedup/merge 不重复通知"）：
// 只有新事件放行；窗口重复与故障域并入都不发通知——一个故障域一个通知，
// 这正是降噪要交付的价值。
func ShouldNotify(d Decision) bool {
	if d.WouldSuppress {
		return false
	}
	switch d.Reason {
	case "dedup-window", "cluster-merge":
		return false
	}
	return true
}

// Admit 判定入口：ShouldNotify=false → 拦截（计数，不发）；true → 放行
// 全渠道。返回是否放行 + 错误（放行失败才算错误；拦截是正常结果）。
//
// 锁边界（优化方案 #3）：g.mu 只保护判决 + 计数 + 待发渠道快照，
// 渠道 Send（网络 IO）全部在锁外执行——单渠道慢响应不再串行化
// 整个通知出口，也不阻塞 Stats/SetStats。返回值语义不变：Admit
// 返回时全渠道 Send 已完成（同步投递，调用方据此打端到端延迟点）。
func (g *Gate) Admit(d Decision) (admitted bool, err error) {
	if strings.TrimSpace(d.ClusterKey) == "" && strings.TrimSpace(d.Title) == "" {
		return false, errors.New("notify: empty decision (no cluster/title)")
	}
	g.mu.Lock()
	if !ShouldNotify(d) {
		g.suppressed++
		g.mu.Unlock()
		return false, nil
	}
	chs := g.registry.channelsFor(d.Severity) // 快照复制，锁内不 Send
	g.mu.Unlock()

	err = dispatchTo(chs, Message{
		TenantID:   d.TenantID,
		ClusterKey: d.ClusterKey,
		Title:      d.Title,
		Body:       "新事件簇 " + d.ClusterKey + "（降噪后首个告警）需要人工关注",
		Severity:   d.Severity,
	})
	if err != nil {
		return false, err
	}
	g.mu.Lock()
	g.dispatched++
	g.mu.Unlock()
	return true, nil
}

// Stats 读计数（锁内拷贝）。
func (g *Gate) Stats() Stats {
	g.mu.Lock()
	defer g.mu.Unlock()
	return Stats{Suppressed: g.suppressed, Dispatched: g.dispatched}
}

// SetStats 恢复累计计数（W9-1 转正：进程重启后从真相源拉平，R6-6——
// 拦截数是核心运维指标，重启清零会让"降噪省了多少"永远从零看起）。
// 只允许启动期调用一次；运行中调用会破坏单调累计语义（调用方责任）。
func (g *Gate) SetStats(s Stats) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.suppressed = s.Suppressed
	g.dispatched = s.Dispatched
}
