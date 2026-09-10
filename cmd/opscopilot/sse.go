// W11 事件页实时推送：SSE 广播器 + 事件 Store 装饰器。
//
// 动机：控制台事件页原本 30s 轮询（setInterval）——人工建单/外部导入后
// 最长要等 30s 才可见，演示与联调体验差。改为服务端推送（SSE）：
// 事件一变即广播给所有已连接控制台。
//
// 设计取舍：
//   - **装饰器而非改 Store 接口**：publishStore 包一层 incident.Store，
//     写成功后广播；读路径原样透传。internal/incident 保持纯净（不认识
//     广播/Hub 概念），跨模块接线留在 cmd/（v1.3 §5.2 汇合点纪律）。
//   - **广播不阻塞业务**：订阅者缓冲满即丢该条（慢客户端不拖垮写路径），
//     客户端据下一帧状态自愈——事件列表本就是"全量拉取"，丢一帧无损。
//   - **每订阅者独立缓冲**：慢客户端互不干扰。
package main

import (
	"sync"

	"opscopilot/internal/incident"
)

// SSEMessage 推送给控制台的实时消息。Type 让前端区分动作（created /
// updated / resolved / merged / attached），Incident 为变更后的完整快照
// ——前端可全量刷新，也可增量更新，不必再发一次 GET。
type SSEMessage struct {
	Type     string            `json:"type"`
	Incident incident.Incident `json:"incident"`
}

// sseSubBuffer 单个订阅者的缓冲深度。事件页刷新是幂等全量，浅缓冲足够；
// 缓冲满时丢弃（见 Publish），不阻塞写路径。
const sseSubBuffer = 32

// maxSSESubscribers 单实例并发 SSE 订阅上限。每个订阅者占一个 goroutine 与
// 一个缓冲，且 Publish 要遍历全部订阅者——无上限时，网络可达即可用大量长连接
// 把服务拖垮。事件页是幂等全量刷新语义，超限直接拒绝（503）比拖垮服务好。
const maxSSESubscribers = 200

// EventHub 事件广播器：把事件变更推给所有已连接控制台（SSE）。
// 并发安全（写路径多 goroutine 调 Publish，HTTP 连接 goroutine 调 Subscribe）。
type EventHub struct {
	mu     sync.Mutex
	subs   map[int]chan SSEMessage
	nextID int
	closed bool
}

// NewEventHub 构造。
func NewEventHub() *EventHub {
	return &EventHub{subs: map[int]chan SSEMessage{}}
}

// Subscribe 注册一个订阅者，返回只读通道、取消函数与是否成功。
// Hub 已关闭、或订阅数已达 maxSSESubscribers 时返回 false（通道已关闭，
// 调用方应立即结束请求，不要悬挂等待）。
func (h *EventHub) Subscribe() (<-chan SSEMessage, func(), bool) {
	ch := make(chan SSEMessage, sseSubBuffer)
	h.mu.Lock()
	if h.closed || len(h.subs) >= maxSSESubscribers {
		h.mu.Unlock()
		close(ch)
		return ch, func() {}, false
	}
	id := h.nextID
	h.nextID++
	h.subs[id] = ch
	h.mu.Unlock()

	var once sync.Once
	cancel := func() {
		once.Do(func() {
			h.mu.Lock()
			if c, ok := h.subs[id]; ok {
				delete(h.subs, id)
				close(c)
			}
			h.mu.Unlock()
		})
	}
	return ch, cancel, true
}

// Publish 广播一条消息给所有订阅者。订阅者缓冲满则丢弃该条（不阻塞、
// 不阻塞写路径）——事件页是全量刷新语义，丢一帧下一帧自愈。
func (h *EventHub) Publish(m SSEMessage) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, ch := range h.subs {
		select {
		case ch <- m:
		default: // 慢订阅者：丢弃本条
		}
	}
}

// Subscribers 当前订阅者数量（运维观察/测试断言）。
func (h *EventHub) Subscribers() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.subs)
}

// Close 关闭全部订阅（停机；再次调用幂等）。
func (h *EventHub) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return
	}
	h.closed = true
	for id, ch := range h.subs {
		close(ch)
		delete(h.subs, id)
	}
}

// publishStore 事件 Store 装饰器：写成功后向 Hub 广播，读路径原样透传。
type publishStore struct {
	incident.Store
	hub *EventHub
}

// NewPublishStore 用广播装饰器包裹事件 Store。hub 为 nil 时退回原 Store
// （广播关闭，写路径行为不变）。
func NewPublishStore(s incident.Store, hub *EventHub) incident.Store {
	if hub == nil {
		return s
	}
	return &publishStore{Store: s, hub: hub}
}

// publish 广播（hub 必非 nil，构造期已保证）。
func (p *publishStore) publish(t string, inc incident.Incident) {
	p.hub.Publish(SSEMessage{Type: t, Incident: inc})
}

// Create 人工建单（链路 B）成功后广播 created。
func (p *publishStore) Create(id, title, severity, createdBy string) (*incident.Incident, error) {
	inc, err := p.Store.Create(id, title, severity, createdBy)
	if err == nil && inc != nil {
		p.publish("created", *inc)
	}
	return inc, err
}

// UpsertExternal 外部幂等写入（链路 A）成功后广播：新建 created，更新
// updated（前端两者都按"该单变了"处理）。
func (p *publishStore) UpsertExternal(origin incident.Origin, sourceRef, title, severity, createdBy, meta string) (*incident.Incident, bool, error) {
	inc, isNew, err := p.Store.UpsertExternal(origin, sourceRef, title, severity, createdBy, meta)
	if err == nil && inc != nil {
		t := "updated"
		if isNew {
			t = "created"
		}
		p.publish(t, *inc)
	}
	return inc, isNew, err
}

// Transition 状态流转成功后广播（转 resolved 记 resolved，其余 updated）。
func (p *publishStore) Transition(id string, to incident.State, actor string) (incident.Incident, error) {
	inc, err := p.Store.Transition(id, to, actor)
	if err == nil {
		t := "updated"
		if to == incident.StateResolved {
			t = "resolved"
		}
		p.publish(t, inc)
	}
	return inc, err
}

// AttachCluster 簇关联成功后广播 attached（cluster_keys 变了，表里可见）。
func (p *publishStore) AttachCluster(id, clusterKey string) error {
	err := p.Store.AttachCluster(id, clusterKey)
	if err == nil {
		if inc, gerr := p.Store.Get(id); gerr == nil {
			p.publish("attached", inc)
		}
	}
	return err
}

// MergeInto 人工合并成功后广播：被合并单 merged（已 resolved）、主单
// updated（簇关联可能转入）。
func (p *publishStore) MergeInto(id, targetID string) error {
	err := p.Store.MergeInto(id, targetID)
	if err == nil {
		if src, gerr := p.Store.Get(id); gerr == nil {
			p.publish("merged", src)
		}
		if tgt, gerr := p.Store.Get(targetID); gerr == nil {
			p.publish("updated", tgt)
		}
	}
	return err
}
