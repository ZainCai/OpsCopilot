// mem_store.go 内存事件 Store（MemStore）：Store 接口的进程内实现 + 容量护栏淘汰。
package incident

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"opscopilot/pkg/memguard"
)

// MemStore 内存事件存储（默认；重启即丢，见 Persistence）。
//
// 内存有界化（优化方案 #6，仅 DB 缺席降级路径需要）：byID 条数超过护栏
// 上限时淘汰**最久未活跃**（UpdatedAt 最早）的一条——同为最旧时刻按 ID
// 字典序；**已 resolved 者优先**（活跃单承载未闭环的处置现场，最后才动）。
// 默认上限（5 万）下正常/演示规模永不触发；触发即计数 + WARN。
// guard = nil（NewMemStore）不设限，行为与历史一致。
type MemStore struct {
	mu         sync.Mutex
	byID       map[string]*Incident
	byCluster  map[string]string // cluster_key → incident_id（一簇最多挂一事件）
	byExternal map[string]string // (origin, source_ref) → 当前代 incident_id
	order      []string          // 创建序（List 稳定输出）
	now        func() time.Time  // 可注入时钟（测试用）
	guard      *memguard.Guard
}

// NewMemStore 构造（无容量上限）。
func NewMemStore() *MemStore { return NewMemStoreWithLimits(nil) }

// NewMemStoreWithLimits 构造并装配容量护栏（guard nil = 不设限）。
func NewMemStoreWithLimits(guard *memguard.Guard) *MemStore {
	s := &MemStore{
		byID:       map[string]*Incident{},
		byCluster:  map[string]string{},
		byExternal: map[string]string{},
		now:        time.Now,
		guard:      guard,
	}
	guard.SetSize(s.Size)
	return s
}

// MemGuards 返回装配的护栏（供装配层注册进指标注册表）。
func (s *MemStore) MemGuards() []*memguard.Guard {
	if s.guard == nil {
		return nil
	}
	return []*memguard.Guard{s.guard}
}

// Size 当前事件条数（锁内安全读；gauge 回调即此）。
func (s *MemStore) Size() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.byID)
}

// Persistence 内存态标识（R6-4）。
func (s *MemStore) Persistence() string { return "memory" }

// SetClock 注入时钟（测试用）。**只能在构造后、并发读写开始前调用**：
// s.now 在锁内被读，这里加锁是为了不与锁内读构成竞争——但运行中热替换时钟
// 仍会改变业务时间语义，生产禁止。
func (s *MemStore) SetClock(f func() time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.now = f
}

// Create 新建事件。ID 必填且唯一；初始状态恒为 open（不信任外部状态）。
func (s *MemStore) Create(id, title, severity, createdBy string) (*Incident, error) {
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
	inc := &Incident{ID: id, Title: title, Severity: normalizeSeverity(severity),
		State: StateOpen, CreatedAt: now, UpdatedAt: now,
		Origin: OriginManual, CreatedBy: createdBy, AutoClosePolicy: "manual_only"}
	s.byID[id] = inc
	s.order = append(s.order, id)
	s.enforceLimitLocked()
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

// Transition 状态推进（判定与副作用走状态机单一实现；见 planTransition）。
func (s *MemStore) Transition(id string, to State, actor string) (Incident, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	inc, ok := s.byID[id]
	if !ok {
		return Incident{}, ErrNotFound
	}
	plan, err := planTransition(inc.State, to, actor)
	if err != nil {
		return Incident{}, err
	}
	applyTransitionPlan(inc, plan, s.now())
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
		return &ClusterTakenError{ClusterKey: clusterKey, Owner: prev}
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
func (s *MemStore) List(state State) ([]Incident, error) {
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
	return out, nil
}

// ListPage 分页读取（最新优先）。MemStore 直接在内存里排序/切片。
func (s *MemStore) ListPage(q PageQuery) (Page, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := normalizePageQuery(&q); err != nil {
		return Page{}, err
	}
	limit := pageLimit(q.Limit)

	var curT time.Time
	var curID string
	if q.Cursor != "" {
		t, id, err := decodeCursor(q.Cursor)
		if err != nil {
			return Page{}, err
		}
		curT, curID = t, id
	}

	cands := make([]*Incident, 0, len(s.byID))
	for _, inc := range s.byID {
		if pageMatch(q, *inc) {
			cands = append(cands, inc)
		}
	}
	// 与 PG 同一排序契约：created_at DESC, incident_id DESC（稳定且可游标）。
	sort.Slice(cands, func(i, j int) bool {
		if !cands[i].CreatedAt.Equal(cands[j].CreatedAt) {
			return cands[i].CreatedAt.After(cands[j].CreatedAt)
		}
		return cands[i].ID > cands[j].ID
	})

	items := make([]Incident, 0, limit+1)
	for _, inc := range cands {
		if q.Cursor != "" && !afterCursor(*inc, curT, curID) {
			continue // 上一页已包含
		}
		items = append(items, *clone(inc))
		if len(items) > limit {
			break
		}
	}
	next := ""
	if len(items) > limit {
		last := items[limit-1]
		next = encodeCursor(last.CreatedAt, last.ID)
		items = items[:limit]
	}
	return Page{Items: items, NextCursor: next, Stats: s.statsLocked()}, nil
}

// statsLocked 全量聚合（不受过滤影响）。
func (s *MemStore) statsLocked() PageStats {
	var st PageStats
	for _, inc := range s.byID {
		if inc.State == StateResolved {
			st.Resolved++
		} else {
			st.Active++
		}
		if inc.Origin == OriginManual {
			st.Manual++
		} else {
			st.External++
		}
	}
	return st
}

// pageMatch 服务端过滤：state（含 "active"=未解决）与 origin。
func pageMatch(q PageQuery, inc Incident) bool {
	switch q.State {
	case "":
	case StateActive:
		if inc.State == StateResolved {
			return false
		}
	default:
		if inc.State != q.State {
			return false
		}
	}
	if q.Origin != "" && string(inc.Origin) != q.Origin {
		return false
	}
	return true
}

// afterCursor 判断 inc 是否严格排在游标之后（倒序序列里"更旧"）。
func afterCursor(inc Incident, curT time.Time, curID string) bool {
	if inc.CreatedAt.Equal(curT) {
		return inc.ID < curID
	}
	return inc.CreatedAt.Before(curT)
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
	if strings.Contains(sourceRef, "#") {
		// '#' 是 M9 复发代际后缀的分隔符（externalIncidentID）：含 '#' 的
		// sourceRef 会让"ref=a 的 gen2"与"ref=a#2 的 gen1"得到同一 incident_id，
		// 造成跨告警串单（第七轮 H2）。存量数据已查证为 0 行，可直接拒绝。
		return nil, false, errors.New("incident: source_ref must not contain '#'")
	}
	if strings.TrimSpace(title) == "" {
		return nil, false, errors.New("incident: title is required")
	}
	severity = normalizeSeverity(severity) // D2/D4 入口收口（与 PGStore 同一函数）
	s.mu.Lock()
	defer s.mu.Unlock()
	base := string(origin) + ":" + sourceRef
	key := externalKey(origin, sourceRef)

	// 沿代链走到当前代：逐代**精确**查找（不用前缀通配——sourceRef 本身可能
	// 含 '#'，通配会把别的告警的代串进来）。在第一个空缺代新建；遇到未解决
	// 的当前代就地刷新。
	gen, id := 1, base
	for {
		inc, ok := s.byID[id]
		if !ok {
			break // 此代空缺 → 在这一代新建
		}
		if inc.Origin != origin || inc.SourceRef != sourceRef {
			// ID 空间被非本系列单占用（如人工单恰好起名 origin:ref）：拒绝，
			// 绝不静默改写（D8 护栏——与 PG 撞唯一索引报错的行为对齐）。
			return nil, false, fmt.Errorf("incident: id %q occupied by %s incident %q, external upsert refused",
				id, inc.Origin, inc.ID)
		}
		if inc.State != StateResolved {
			inc.Title = title
			inc.Severity = severity
			inc.SourceMeta = meta
			inc.UpdatedAt = s.now()
			return clone(inc), false, nil
		}
		gen++ // 已解决 → 复发，看下一代
		id = externalIncidentID(base, gen)
	}
	now := s.now()
	inc := &Incident{ID: id, Title: title, Severity: severity, State: StateOpen,
		Origin: origin, SourceRef: sourceRef, SourceMeta: meta, CreatedBy: createdBy,
		AutoClosePolicy: "auto", CreatedAt: now, UpdatedAt: now}
	s.byID[id] = inc
	s.order = append(s.order, id)
	s.byExternal[key] = id
	s.enforceLimitLocked()
	return clone(inc), true, nil
}

// ExternalActive 报告该 (origin, source_ref) 是否存在未解决的事件（O(1)，
// 走进程内索引）。
func (s *MemStore) ExternalActive(origin Origin, sourceRef string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id, ok := s.byExternal[externalKey(origin, sourceRef)]
	if !ok {
		return false, nil
	}
	inc, ok := s.byID[id]
	if !ok { // 索引与主表不一致（不应发生）：按"不存在"处理
		return false, nil
	}
	return inc.State != StateResolved, nil
}

// enforceLimitLocked 插入收尾的超限淘汰（必须持 s.mu 调用）。
// guard nil（无界）时零成本直返。每轮淘汰一条最久未活跃记录并计数，
// 直到回到上限内；无可淘汰对象（理论上不可能——byID 非空必有受害者）
// 时终止并保命优先。
func (s *MemStore) enforceLimitLocked() {
	if s.guard == nil {
		return
	}
	total := 0
	for excess := s.guard.Over(len(s.byID)); excess > 0; excess = s.guard.Over(len(s.byID)) {
		removed := 0
		for i := 0; i < excess; i++ {
			id, ok := s.pickEvictVictimLocked()
			if !ok {
				break
			}
			s.removeLocked(id)
			removed++
		}
		if removed == 0 {
			break
		}
		total += removed
	}
	if total > 0 {
		s.guard.Evicted(total)
	}
}

// pickEvictVictimLocked 挑最久未活跃的一条：**已 resolved 者优先**
// （工单闭环后才可丢；活跃单承载处置现场，最后才动），组内按 UpdatedAt
// 最早、同刻按 ID 字典序（决定性）。
func (s *MemStore) pickEvictVictimLocked() (string, bool) {
	var bestID string
	var best *Incident
	for id, inc := range s.byID {
		better := func(a, b *Incident) bool {
			// 先比"是否 resolved"，再比 UpdatedAt，最后比 ID。
			ar, br := a.State == StateResolved, b.State == StateResolved
			if ar != br {
				return ar
			}
			if !a.UpdatedAt.Equal(b.UpdatedAt) {
				return a.UpdatedAt.Before(b.UpdatedAt)
			}
			return a.ID < b.ID
		}
		if best == nil || better(inc, best) {
			bestID, best = id, inc
		}
	}
	return bestID, best != nil
}

// removeLocked 全索引一致性移除（byID/order/byCluster/byExternal）。
func (s *MemStore) removeLocked(id string) {
	delete(s.byID, id)
	kept := s.order[:0]
	for _, o := range s.order {
		if o != id {
			kept = append(kept, o)
		}
	}
	s.order = kept
	for k, v := range s.byCluster {
		if v == id {
			delete(s.byCluster, k)
		}
	}
	for k, v := range s.byExternal {
		if v == id {
			delete(s.byExternal, k)
		}
	}
}

// externalKey (origin, source_ref) 的进程内索引键。用 \x00 分隔——两者都可能
// 含任意字符（含 ':' 与 '#'），不能用 ':' 拼接。
func externalKey(origin Origin, sourceRef string) string {
	return string(origin) + "\x00" + sourceRef
}

// MergeInto 人工合并：被合并单置 resolved + 记录 merged_into，簇关联**真实
// 转移**给主单（D7，以 PG 语义为准——主单不存在报错；重复合并幂等；主单
// 已占用某簇则该簇跳过，一簇一事件）。
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
	// 簇转移：反查改指主单、src 释放（旧实现的 `!taken` 判定对已挂簇恒为
	// taken——整段是 no-op，违背本函数注释与 PG 行为，即漂移 D7）。
	for _, k := range src.ClusterKeys {
		held := false
		for _, tk := range tgt.ClusterKeys {
			if tk == k {
				held = true
				break
			}
		}
		if !held {
			tgt.ClusterKeys = append(tgt.ClusterKeys, k)
		}
		s.byCluster[k] = targetID
	}
	src.ClusterKeys = nil
	now := s.now()
	closeAsMerged(src, targetID, now)
	tgt.UpdatedAt = now
	return nil
}

// clone 深拷贝切片字段（锁外返回值不共享底层数组）。
func clone(in *Incident) *Incident {
	out := *in
	out.ClusterKeys = append([]string(nil), in.ClusterKeys...)
	sort.Strings(out.ClusterKeys) // 稳定输出（关联顺序不定）
	return &out
}
