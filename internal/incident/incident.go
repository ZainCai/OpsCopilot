// Package incident 事件域（M2 主干骨架，功能点 F-01/F-02）。
//
// 定位：簇（topology/noise 产物）是机器视角的降噪结果，事件是人的工单。
// 两层解耦——本包只持有 cluster_key 字符串引用，不 import topology/noise
// （边界纪律：internal 禁互 import）。
//
// 主干范围：实体 + 状态机（核心逻辑，落地）+ Store 接口（内存 MemStore /
// TimescaleDB PGStore 双实现，W9 落地 pg_store.go）+ 簇关联。事件自动
// 创建策略（告警簇自动升级事件 vs 人工建单）留 W9/W10 决策。
package incident

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// State 事件状态机（原型 incident 页口径：open→acked→mitigated→resolved）。
type State string

const (
	StateOpen      State = "open"
	StateAcked     State = "acked"
	StateMitigated State = "mitigated"
	StateResolved  State = "resolved"
)

// transitions 合法状态转换表（单向前进；回退走人工重开——M2 不做）。
var transitions = map[State][]State{
	StateOpen:      {StateAcked, StateMitigated, StateResolved},
	StateAcked:     {StateMitigated, StateResolved},
	StateMitigated: {StateResolved},
	StateResolved:  {},
}

// CanTransition 判定 from→to 是否合法。
func CanTransition(from, to State) bool {
	for _, s := range transitions[from] {
		if s == to {
			return true
		}
	}
	return false
}

// ErrNotFound 事件不存在。
var ErrNotFound = errors.New("incident not found")

// ErrInvalidTransition 非法状态转换（宁可报错，不静默改状态）。
type ErrInvalidTransition struct{ From, To State }

func (e ErrInvalidTransition) Error() string {
	return fmt.Sprintf("invalid transition %s -> %s", e.From, e.To)
}

// Incident 事件实体。
type Incident struct {
	ID          string // 幂等键（调用方生成，如 INC-2401 风格）
	Title       string
	Severity    string // critical / warning / info（沿用告警严重级口径）
	State       State
	ClusterKeys []string // 关联的降噪簇（F-02：簇→事件关联，字符串引用）
	AckBy       string   // 确认人（M2 骨架：转 resolved 时记录）
	CreatedAt   time.Time
	UpdatedAt   time.Time
	ResolvedAt  time.Time // 零值 = 未解决（KPI 统计口径）
}

// Store 事件存储接口（W9：内存与 TimescaleDB 双实现）。
// 全部方法并发安全；返回值为拷贝，调用方修改不影响库内状态。
type Store interface {
	Create(id, title, severity string) (*Incident, error)
	Get(id string) (Incident, error)
	Transition(id string, to State, actor string) (Incident, error)
	AttachCluster(id, clusterKey string) error
	List(state State) []Incident
	IncidentForCluster(clusterKey string) (Incident, bool)
	// Persistence 声明落库形态（"memory" | "timescaledb"）——REST 响应
	// 据此提示调用方（R6-4：内存态重启即丢，必须让消费者知道）。
	Persistence() string
}

// MemStore 内存事件存储（默认；重启即丢，见 Persistence）。
type MemStore struct {
	mu        sync.Mutex
	byID      map[string]*Incident
	byCluster map[string]string // cluster_key → incident_id（一簇最多挂一事件）
	order     []string          // 创建序（List 稳定输出）
	now       func() time.Time  // 可注入时钟（测试用）
}

// NewMemStore 构造。
func NewMemStore() *MemStore {
	return &MemStore{
		byID:      map[string]*Incident{},
		byCluster: map[string]string{},
		now:       time.Now,
	}
}

// Persistence 内存态标识（R6-4）。
func (s *MemStore) Persistence() string { return "memory" }

// SetClock 注入时钟（测试；生产勿调）。
func (s *MemStore) SetClock(f func() time.Time) { s.now = f }

// Create 新建事件。ID 必填且唯一；初始状态恒为 open（不信任外部状态）。
func (s *MemStore) Create(id, title, severity string) (*Incident, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return nil, errors.New("incident: id is required")
	}
	if strings.TrimSpace(title) == "" {
		return nil, errors.New("incident: title is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, dup := s.byID[id]; dup {
		return nil, fmt.Errorf("incident: duplicate id %q", id)
	}
	now := s.now()
	inc := &Incident{ID: id, Title: title, Severity: severity,
		State: StateOpen, CreatedAt: now, UpdatedAt: now}
	s.byID[id] = inc
	s.order = append(s.order, id)
	return clone(inc), nil
}

// Get 读取事件（值拷贝）。
func (s *MemStore) Get(id string) (Incident, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	inc, ok := s.byID[id]
	if !ok {
		return Incident{}, ErrNotFound
	}
	return *clone(inc), nil
}

// Transition 状态推进（状态机校验；ResolvedAt 在转入 resolved 时落戳）。
func (s *MemStore) Transition(id string, to State, actor string) (Incident, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	inc, ok := s.byID[id]
	if !ok {
		return Incident{}, ErrNotFound
	}
	if !CanTransition(inc.State, to) {
		return Incident{}, ErrInvalidTransition{From: inc.State, To: to}
	}
	inc.State = to
	inc.UpdatedAt = s.now()
	if to == StateResolved {
		inc.ResolvedAt = inc.UpdatedAt
		if strings.TrimSpace(actor) != "" {
			inc.AckBy = actor
		}
	}
	return *clone(inc), nil
}

// AttachCluster 簇→事件关联（F-02 主干：一簇最多挂一事件；重复挂同一
// 事件幂等）。事件不存在返回 ErrNotFound。
func (s *MemStore) AttachCluster(id, clusterKey string) error {
	if strings.TrimSpace(clusterKey) == "" {
		return errors.New("incident: empty cluster key")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	inc, ok := s.byID[id]
	if !ok {
		return ErrNotFound
	}
	if prev, taken := s.byCluster[clusterKey]; taken && prev != id {
		return fmt.Errorf("incident: cluster %q already attached to %q", clusterKey, prev)
	}
	for _, k := range inc.ClusterKeys {
		if k == clusterKey {
			return nil // 幂等
		}
	}
	inc.ClusterKeys = append(inc.ClusterKeys, clusterKey)
	inc.UpdatedAt = s.now()
	s.byCluster[clusterKey] = id
	return nil
}

// List 按创建序返回全部事件（值拷贝；state 过滤，空串=全部）。
func (s *MemStore) List(state State) []Incident {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Incident, 0, len(s.order))
	for _, id := range s.order {
		inc := s.byID[id]
		if state != "" && inc.State != state {
			continue
		}
		out = append(out, *clone(inc))
	}
	return out
}

// IncidentForCluster 反查簇所属事件（F-02 动线的读侧）。
func (s *MemStore) IncidentForCluster(clusterKey string) (Incident, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id, ok := s.byCluster[clusterKey]
	if !ok {
		return Incident{}, false
	}
	return *clone(s.byID[id]), true
}

// clone 深拷贝切片字段（锁外返回值不共享底层数组）。
func clone(in *Incident) *Incident {
	out := *in
	out.ClusterKeys = append([]string(nil), in.ClusterKeys...)
	sort.Strings(out.ClusterKeys) // 稳定输出（关联顺序不定）
	return &out
}
