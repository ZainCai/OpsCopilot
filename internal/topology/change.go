package topology

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// 变更记录库（M1 W3）：把"什么时候谁改了什么"作为一等公民存下来，
// 供告警与拓扑做变更关联（"这个告警前 30 分钟内有没有变更？"）。
//
// 设计要点：
//   - 变更事件属于拓扑语义域（关联对象是拓扑节点的 Key），故放在
//     internal/topology；HTTP/webhook 适配在 cmd/（编排层唯一合法汇合点）。
//   - 置信度约定（ADR-007）：变更事件由外部系统（Git/Jenkins/人工）声明，
//     不是连接器直接观测到的事实，因此零值默认 **medium**；确属直接
//     观测（如变更平台同机直采）由调用方显式给 high。
//   - 存储为进程内内存实现（M1 规模：百台主机、分钟级窗口查询足够）；
//     接口按"可换后端"设计，持久化属后续迭代。
//   - 所有返回值为值拷贝，调用方修改不影响库内状态（并发安全）。

// ChangeType 变更事件类型（强类型）。
type ChangeType string

// 已知变更类型。未知类型在 Record 时被拒绝——变更类型是下游关联
// 规则（如"回滚优先于部署"）的分支依据，必须封闭集合。
const (
	ChangeDeploy   ChangeType = "deploy"        // 部署/发版
	ChangeConfig   ChangeType = "config_change" // 配置修改
	ChangeRollback ChangeType = "rollback"      // 回滚
)

// ParseChangeType 解析变更类型字符串（webhook 外部输入用）。
// 错误以 ErrUnknownChangeType 为根因（%w 包装），调用方可 errors.Is 匹配。
func ParseChangeType(s string) (ChangeType, error) {
	switch ChangeType(s) {
	case ChangeDeploy, ChangeConfig, ChangeRollback:
		return ChangeType(s), nil
	default:
		return "", fmt.Errorf("%w %q (want deploy|config_change|rollback)", ErrUnknownChangeType, s)
	}
}

// 哨兵错误：调用方按 errors.Is 分支处理（webhook 侧映射到不同 HTTP 状态码）。
var (
	// ErrEmptyChangeID 变更事件缺少唯一 ID。
	ErrEmptyChangeID = errors.New("topology: change event requires non-empty id")
	// ErrEmptyNodeKey 变更事件缺少关联节点 Key。
	ErrEmptyNodeKey = errors.New("topology: change event requires non-empty node key")
	// ErrDuplicateChange 同 ID 变更事件重复提交。
	ErrDuplicateChange = errors.New("topology: duplicate change event id")
	// ErrNodeNotFound 严格模式下，事件关联的节点不在拓扑图中。
	ErrNodeNotFound = errors.New("topology: change event node not in topology")
	// ErrUnknownChangeType 变更类型不在封闭集合内（ParseChangeType 返回的错误以此为根因）。
	ErrUnknownChangeType = errors.New("topology: unknown change type")
)

// ChangeEvent 一次变更的事实记录。
type ChangeEvent struct {
	// ID 唯一标识（如 Git commit sha、Jenkins build URL、人工生成的 UUID）。
	// 重复 ID 拒绝入库——变更事件的幂等键。
	ID string `json:"id"`
	// NodeKey 关联的拓扑节点 Key（必须与拓扑图中的节点 Key 一致）。
	NodeKey string `json:"node_key"`
	// Type 变更类型（deploy / config_change / rollback，封闭集合）。
	Type ChangeType `json:"type"`
	// Source 变更来源标识，如 "git" / "jenkins" / "manual"。
	// W3 先做手动 webhook，Source 填 "manual"；对接 Git/Jenkins 时替换即可。
	Source string `json:"source"`
	// Author 操作者（Git author / Jenkins 触发人 / 人工填写的姓名）。
	Author string `json:"author,omitempty"`
	// Ref 来源引用（分支名 / 流水线名），可选。
	Ref string `json:"ref,omitempty"`
	// Revision 精确版本（commit sha / build number），可选。
	Revision string `json:"revision,omitempty"`
	// Summary 一句话说明改了什么。
	Summary string `json:"summary,omitempty"`
	// OccurredAt 变更发生时间；零值由 Record 填充为当前时间。
	OccurredAt time.Time `json:"occurred_at"`
	// TenantID 租户隔离（多租户场景预留，M1 单租户可空）。
	TenantID string `json:"tenant_id,omitempty"`
	// Confidence 事件置信度；零值默认 medium（外部声明非直接观测，见文件头）。
	Confidence Confidence `json:"confidence,omitempty"`
}

// ChangeStore 变更事件存储（进程内实现，并发安全）。
//
// NodeCheck：可选的严格校验钩子——非 nil 时，Record 会先确认事件关联的
// 节点确实存在于拓扑图中（ErrNodeNotFound），防止"变更关联到一个不存在的
// 节点"这种静默失败。由编排层（cmd/）注入图的节点查找函数，本模块不
// 反向依赖图的持有者。
//
// **钩子约束（P2-5）**：nodeCheck 在 ChangeStore.mu 持锁期间被调用，
// 因此钩子内**严禁**回调本 Store 的任何方法（会死锁），也不得做慢 IO
// （会阻塞所有 Record/查询）。当前编排层注入的是 Sink.HasNode（另一把
// 独立的锁，锁序恒定 Store→Sink，无死锁风险）。
type ChangeStore struct {
	mu        sync.RWMutex
	events    map[string]*ChangeEvent // ID → 事件（存指针，取值时拷贝）
	byNode    map[string][]string     // nodeKey → 事件 ID（插入序，查询时按时间排序）
	nodeCheck func(string) bool
}

// NewChangeStore 构造空存储。nodeCheck 为 nil 时不做节点存在性校验。
func NewChangeStore(nodeCheck func(string) bool) *ChangeStore {
	return &ChangeStore{
		events:    make(map[string]*ChangeEvent),
		byNode:    make(map[string][]string),
		nodeCheck: nodeCheck,
	}
}

// Record 记录一条变更事件，返回**补全默认值后**的存储副本（OccurredAt、
// Confidence 等字段可能被填充）。
//
// 校验规则：
//   - ID、NodeKey 非空；Type 必须是已知类型；Confidence 若显式给出必须合法；
//   - ID 重复 → ErrDuplicateChange（幂等保护，不做覆盖）；
//   - NodeCheck 非 nil 且节点不存在 → ErrNodeNotFound。
func (s *ChangeStore) Record(ev ChangeEvent) (ChangeEvent, error) {
	if s == nil {
		return ChangeEvent{}, errors.New("topology: nil change store")
	}
	if ev.ID == "" {
		return ChangeEvent{}, ErrEmptyChangeID
	}
	if ev.NodeKey == "" {
		return ChangeEvent{}, ErrEmptyNodeKey
	}
	typ, err := ParseChangeType(string(ev.Type))
	if err != nil {
		return ChangeEvent{}, err
	}
	conf := ev.Confidence
	if conf == "" {
		conf = ConfidenceMedium // 外部声明的事件：同源推断档（ADR-007）
	}
	if _, err := ParseConfidence(string(conf)); err != nil {
		return ChangeEvent{}, fmt.Errorf("topology: change %s: %w", ev.ID, err)
	}
	if ev.OccurredAt.IsZero() {
		ev.OccurredAt = time.Now()
	}
	ev.Type = typ
	ev.Confidence = conf

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, dup := s.events[ev.ID]; dup {
		return ChangeEvent{}, fmt.Errorf("topology: change %q: %w", ev.ID, ErrDuplicateChange)
	}
	if s.nodeCheck != nil && !s.nodeCheck(ev.NodeKey) {
		return ChangeEvent{}, fmt.Errorf("topology: change %q node %q: %w", ev.ID, ev.NodeKey, ErrNodeNotFound)
	}
	stored := ev
	s.events[ev.ID] = &stored
	s.byNode[ev.NodeKey] = append(s.byNode[ev.NodeKey], ev.ID)
	return ev, nil
}

// ByNode 返回某节点的全部变更事件，按 OccurredAt 升序。
// 返回值为拷贝，调用方修改不影响库内状态。
func (s *ChangeStore) ByNode(nodeKey string) []ChangeEvent {
	return s.byNodeWithin(nodeKey, time.Time{}, time.Time{})
}

// ByNodeWithin 返回某节点在 [from, to] 闭区间内的变更事件（按时间升序）。
// from/to 为零值时表示该侧不设限。这是"告警发生前 N 分钟内有无变更"
// 关联查询的直接原语。
func (s *ChangeStore) ByNodeWithin(nodeKey string, from, to time.Time) []ChangeEvent {
	return s.byNodeWithin(nodeKey, from, to)
}

func (s *ChangeStore) byNodeWithin(nodeKey string, from, to time.Time) []ChangeEvent {
	if s == nil || nodeKey == "" {
		return nil
	}
	s.mu.RLock()
	ids := s.byNode[nodeKey]
	out := make([]ChangeEvent, 0, len(ids))
	for _, id := range ids {
		out = append(out, *s.events[id])
	}
	s.mu.RUnlock()

	out = filterWindow(out, from, to)
	sort.Slice(out, func(i, j int) bool {
		return out[i].OccurredAt.Before(out[j].OccurredAt)
	})
	return out
}

// Within 返回全部节点在 [from, to] 内的变更事件（按时间升序）。
// 用于"这波告警风暴前全局有过哪些变更"的粗查。
func (s *ChangeStore) Within(from, to time.Time) []ChangeEvent {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	out := make([]ChangeEvent, 0, len(s.events))
	for _, ev := range s.events {
		out = append(out, *ev)
	}
	s.mu.RUnlock()

	out = filterWindow(out, from, to)
	sort.Slice(out, func(i, j int) bool {
		return out[i].OccurredAt.Before(out[j].OccurredAt)
	})
	return out
}

// Get 按 ID 取单条事件；不存在返回第二个返回值 false。
func (s *ChangeStore) Get(id string) (ChangeEvent, bool) {
	if s == nil || id == "" {
		return ChangeEvent{}, false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	ev, ok := s.events[id]
	if !ok {
		return ChangeEvent{}, false
	}
	return *ev, true
}

// Len 返回已存事件总数。
func (s *ChangeStore) Len() int {
	if s == nil {
		return 0
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.events)
}

// PruneBefore 清除 OccurredAt 早于 cutoff 的事件，返回清除条数。
//
// 这是保留窗口的清理原语（W3 审查 P2-4：进程内 map 永不清理会无限
// 增长）。调用约定：由编排层定期调用（如每小时清一次、保留 7 天），
// 本模块不内置后台 goroutine——定时策略属于编排层，模块保持纯粹。
// 清理用到的最小保留窗由调用方对齐变更关联查询的最大时间窗（如
// "告警前 30 分钟"远小于 7 天，7 天窗留足了回溯余地）。
func (s *ChangeStore) PruneBefore(cutoff time.Time) int {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	removed := 0
	for id, ev := range s.events {
		if ev.OccurredAt.Before(cutoff) {
			delete(s.events, id)
			removed++
		}
	}
	if removed > 0 {
		// byNode 索引整体重建：O(n) 且实现简单，胜过逐链表摘除的边界处理。
		s.byNode = make(map[string][]string, len(s.byNode))
		for id, ev := range s.events {
			s.byNode[ev.NodeKey] = append(s.byNode[ev.NodeKey], id)
		}
	}
	return removed
}

// filterWindow 原地过滤出 [from, to] 闭区间内的事件（零值表示不设限）。
func filterWindow(evs []ChangeEvent, from, to time.Time) []ChangeEvent {
	filtered := evs[:0]
	for _, ev := range evs {
		if !from.IsZero() && ev.OccurredAt.Before(from) {
			continue
		}
		if !to.IsZero() && ev.OccurredAt.After(to) {
			continue
		}
		filtered = append(filtered, ev)
	}
	return filtered
}
