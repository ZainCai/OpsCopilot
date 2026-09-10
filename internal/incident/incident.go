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

// Origin 事件来源（双链路：外部自动导入 ∥ 人工建单）。
type Origin string

const (
	OriginManual       Origin = "manual"       // 人工建单（链路 B）
	OriginWebhook      Origin = "webhook"      // 通用 webhook（链路 A）
	OriginAlertmanager Origin = "alertmanager" // Alertmanager（链路 A，一期）
	OriginPrometheus   Origin = "prometheus"   // 预留
	OriginAzure        Origin = "azure"        // 预留
	OriginPull         Origin = "pull"         // 预留（定时拉取）
	OriginAPI          Origin = "api"          // 预留（内部 API）
)

// ValidOrigin 校验来源枚举。
func ValidOrigin(o Origin) bool {
	switch o {
	case OriginManual, OriginWebhook, OriginAlertmanager,
		OriginPrometheus, OriginAzure, OriginPull, OriginAPI:
		return true
	}
	return false
}

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
	ID          string    `json:"id"`
	Title       string    `json:"title"`
	Severity    string    `json:"severity"`
	State       State     `json:"state"`
	ClusterKeys []string  `json:"cluster_keys"`
	AckBy       string    `json:"ack_by"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
	ResolvedAt  time.Time `json:"resolved_at"`

	// ---- 双链路来源字段（迁移 000005）----
	Origin     Origin `json:"origin"`
	SourceRef  string `json:"source_ref"`
	SourceMeta string `json:"source_meta"`
	DedupKey   string `json:"dedup_key"`
	CreatedBy  string `json:"created_by"`
	MergedInto string `json:"merged_into"`
	// AutoClosePolicy 外部恢复能否自动关单：auto / manual_only（人工
	// 干预过的单置 manual_only——R2：防止外部恢复吞掉人工处置）。
	AutoClosePolicy string `json:"auto_close_policy"`
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
	// UpsertExternal 外部链路幂等写入（链路 A）：同 (origin, sourceRef)
	// 已存在则更新（刷新标题/严重级/载荷/时间），不新建；返回是否新建。
	UpsertExternal(origin Origin, sourceRef, title, severity, createdBy, meta string) (*Incident, bool, error)
	// MergeInto 人工合并（L2 只提示不自动，合并动作由人触发）：被合并单
	// 置 resolved 并记录 merged_into，簇关联转移给主单。
	MergeInto(id, targetID string) error
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
		State: StateOpen, CreatedAt: now, UpdatedAt: now,
		Origin: OriginManual, AutoClosePolicy: "manual_only"}
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
	if to == StateAcked {
		// R2：人工确认过的单，外部恢复不得自动关闭（防吞掉人工处置）。
		inc.AutoClosePolicy = "manual_only"
	}
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

// UpsertExternal 外部链路幂等写入：同 (origin, sourceRef) 已存在 → 更新
// 标题/严重级/载荷/时间，不新建（L0 幂等，抗重放）。
func (s *MemStore) UpsertExternal(origin Origin, sourceRef, title, severity, createdBy, meta string) (*Incident, bool, error) {
	if !ValidOrigin(origin) {
		return nil, false, errors.New("incident: invalid origin " + string(origin))
	}
	if strings.TrimSpace(sourceRef) == "" {
		return nil, false, errors.New("incident: source_ref is required for external upsert")
	}
	if strings.TrimSpace(title) == "" {
		return nil, false, errors.New("incident: title is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	id := string(origin) + ":" + sourceRef
	if inc, ok := s.byID[id]; ok {
		inc.Title = title
		inc.Severity = severity
		inc.SourceMeta = meta
		inc.UpdatedAt = s.now()
		return clone(inc), false, nil
	}
	now := s.now()
	inc := &Incident{ID: id, Title: title, Severity: severity, State: StateOpen,
		Origin: origin, SourceRef: sourceRef, SourceMeta: meta, CreatedBy: createdBy,
		AutoClosePolicy: "auto", CreatedAt: now, UpdatedAt: now}
	s.byID[id] = inc
	s.order = append(s.order, id)
	return clone(inc), true, nil
}

// MergeInto 人工合并：被合并单置 resolved + 记录 merged_into，簇关联转移
// 给主单（主单不存在报错；重复合并幂等）。
func (s *MemStore) MergeInto(id, targetID string) error {
	if id == targetID {
		return errors.New("incident: cannot merge into itself")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	src, ok := s.byID[id]
	if !ok {
		return ErrNotFound
	}
	tgt, ok := s.byID[targetID]
	if !ok {
		return ErrNotFound
	}
	if src.MergedInto == targetID && src.State == StateResolved {
		return nil // 幂等
	}
	for _, k := range src.ClusterKeys {
		if _, taken := s.byCluster[k]; !taken {
			s.byCluster[k] = targetID
			tgt.ClusterKeys = append(tgt.ClusterKeys, k)
		}
	}
	src.MergedInto = targetID
	src.State = StateResolved
	src.ResolvedAt = s.now()
	src.UpdatedAt = src.ResolvedAt
	tgt.UpdatedAt = src.ResolvedAt
	return nil
}

// clone 深拷贝切片字段（锁外返回值不共享底层数组）。
func clone(in *Incident) *Incident {
	out := *in
	out.ClusterKeys = append([]string(nil), in.ClusterKeys...)
	sort.Strings(out.ClusterKeys) // 稳定输出（关联顺序不定）
	return &out
}
