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
	"encoding/base64"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"opscopilot/pkg/memguard"
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
	// StateActive 过滤别名（不是真实状态）：表示"未解决"（state<>resolved）。
	// 仅供查询过滤使用，任何事件的实际状态都不会是它。
	StateActive State = "active"
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

// ---- 状态机单一实现（#5：双 Store 共用的转移判定 + 副作用计划）----
//
// 转移的"合法性判定 + 副作用"只有下面这一份代码。PGStore 的 UPDATE 语句
// 只按 transitionPlan 产出的参数打 CASE（不在 SQL 里藏第二套判定）；
// MemStore 经 applyTransitionPlan 直接落字段。契约测试（store_contract_
// test.go 契约 2/11）锁死两侧可观测结果必须一致。

// transitionPlan 一次合法转移的副作用计划。
type transitionPlan struct {
	To State
	// FlipManualOnly → 转入 acked：人工确认过的单，外部恢复不得自动关闭（R2）。
	FlipManualOnly bool
	// StampResolved → 转入 resolved：resolved_at == updated_at
	//（Mem 同 clock 值；PG 同语句双 now()，同一事务时间戳）。
	StampResolved bool
	// AckBy → resolved 且 actor 去空白后非空：记录处置人；空串 = 不改写。
	AckBy string
}

// planTransition 判定 from→to 合法性并产出副作用计划；非法返回 ErrInvalidTransition。
func planTransition(from, to State, actor string) (transitionPlan, error) {
	if !CanTransition(from, to) {
		return transitionPlan{}, ErrInvalidTransition{From: from, To: to}
	}
	p := transitionPlan{To: to}
	if to == StateAcked {
		p.FlipManualOnly = true
	}
	if to == StateResolved {
		p.StampResolved = true
		if strings.TrimSpace(actor) != "" {
			p.AckBy = actor
		}
	}
	return p, nil
}

// applyTransitionPlan 把计划落到实体（MemStore 侧；now 由调用方时钟注入）。
func applyTransitionPlan(inc *Incident, p transitionPlan, now time.Time) {
	inc.State = p.To
	inc.UpdatedAt = now
	if p.FlipManualOnly {
		inc.AutoClosePolicy = "manual_only"
	}
	if p.StampResolved {
		inc.ResolvedAt = now
	}
	if p.AckBy != "" {
		inc.AckBy = p.AckBy
	}
}

// closeAsMerged 关闭被合并单：resolved + merged_into，resolved_at == updated_at
// （落戳语义与 planTransition 的 StampResolved 同源；PG 侧 MergeInto 用同语句
// now() 双写兑现同一契约）。合并**不走转移表**——已 resolved 的单仍可被合并。
func closeAsMerged(inc *Incident, targetID string, at time.Time) {
	inc.State = StateResolved
	inc.MergedInto = targetID
	inc.ResolvedAt = at
	inc.UpdatedAt = at
}

// normalizeSeverity 空严重级入口收口（D2/D4）：空 → "info"——与 REST 层
// 兜底、DB 列默认值（000004 severity NOT NULL DEFAULT 'info'）同向。两
// Store 的 Create / UpsertExternal（新建与刷新分支）共用，杜绝"PG 撞
// NOT NULL / Mem 静默存空"与"PG 刷新分支反而能写空"的分叉。
func normalizeSeverity(s string) string {
	if strings.TrimSpace(s) == "" {
		return "info"
	}
	return s
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

// PageQuery 分页查询参数（D4 决策 A：游标分页，按创建时间倒序=最新优先）。
type PageQuery struct {
	State  State  // "" = 全部；"active" = 未解决；其余为具体状态
	Origin string // "" = 全部；否则按 origin 精确过滤（人工/外部）
	Limit  int    // <=0 用 PageLimitDefault；超过 PageLimitMax 截断
	Cursor string // 上一页返回的 next_cursor；空 = 第一页
}

// 分页默认与上限（D4）。
const (
	PageLimitDefault = 200
	PageLimitMax     = 1000
)

// PageStats 事件面**全量聚合**（不受分页与过滤影响）——控制台 KPI 数据源。
type PageStats struct {
	Active   int `json:"active"`
	Resolved int `json:"resolved"`
	Manual   int `json:"manual"`
	External int `json:"external"`
}

// Page 分页结果。NextCursor 为空表示已到末尾。
type Page struct {
	Items      []Incident `json:"incidents"`
	NextCursor string     `json:"next_cursor"`
	Stats      PageStats  `json:"stats"`
}

// Store 事件存储接口（W9：内存与 TimescaleDB 双实现）。
// 全部方法并发安全；返回值为拷贝，调用方修改不影响库内状态。
type Store interface {
	Create(id, title, severity, createdBy string) (*Incident, error)
	Get(id string) (Incident, error)
	Transition(id string, to State, actor string) (Incident, error)
	AttachCluster(id, clusterKey string) error
	List(state State) ([]Incident, error)
	// ListPage 分页读取事件（D4 决策 A：游标分页，按创建时间倒序=最新优先）。
	// 服务端做 state/origin 过滤，客户端不再拉全量；返回 next_cursor，
	// 空串 = 已到末尾。Stats 为**全量聚合**（不受分页与过滤影响），供看板 KPI。
	ListPage(q PageQuery) (Page, error)
	IncidentForCluster(clusterKey string) (Incident, bool)
	// UpsertExternal 外部链路写入（链路 A）。幂等语义（M9：复发即新建）：
	//   - 命中未解决（open/acked/mitigated）的当前代 → 刷新（重推不新建）；
	//   - 命中已解决的当前代 → 视为告警复发，新开一代（incident_id 追加 #N）。
	//     否则新故障只会去刷新一条已关闭的旧单，运维看不见。
	// 返回是否"新建了一单"（复发新建也算 true）。
	UpsertExternal(origin Origin, sourceRef, title, severity, createdBy, meta string) (*Incident, bool, error)
	// ExternalActive 报告该 (origin, source_ref) 当前是否存在**未解决**的事件。
	// M9 之后"存在已解决的单"不代表"没有"——复发会新开一代。消费者（建单
	// 限流）据此区分"刷新既有单"与"会新建单"。
	ExternalActive(origin Origin, sourceRef string) (bool, error)
	// MergeInto 人工合并（L2 只提示不自动，合并动作由人触发）：被合并单
	// 置 resolved 并记录 merged_into，簇关联转移给主单。
	MergeInto(id, targetID string) error
	// Persistence 声明落库形态（"memory" | "timescaledb"）——REST 响应
	// 据此提示调用方（R6-4：内存态重启即丢，必须让消费者知道）。
	Persistence() string
}

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

// normalizePageQuery 校验/归一化分页参数（两个实现共用，语义必须一致）。
func normalizePageQuery(q *PageQuery) error {
	switch q.State {
	case "", StateActive, StateOpen, StateAcked, StateMitigated, StateResolved:
	default:
		return errors.New("incident: invalid state filter " + string(q.State))
	}
	return nil
}

// pageLimit 归一化 limit。
func pageLimit(n int) int {
	if n <= 0 {
		return PageLimitDefault
	}
	if n > PageLimitMax {
		return PageLimitMax
	}
	return n
}

// afterCursor 判断 inc 是否严格排在游标之后（倒序序列里"更旧"）。
func afterCursor(inc Incident, curT time.Time, curID string) bool {
	if inc.CreatedAt.Equal(curT) {
		return inc.ID < curID
	}
	return inc.CreatedAt.Before(curT)
}

// ErrBadCursor 游标不合法（base64 解不开 / 无分隔符 / 时间错）。
// 调用方（REST 层）用 errors.Is 映射为 400——不要按文案子串匹配。
var ErrBadCursor = errors.New("incident: bad cursor")

// encodeCursor/decodeCursor 游标编解码：base64url("RFC3339Nano\x1fincidentID")。
// 不透明字符串，客户端只透传。
func encodeCursor(at time.Time, id string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(at.Format(time.RFC3339Nano) + "\x1f" + id))
}

func decodeCursor(s string) (time.Time, string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return time.Time{}, "", fmt.Errorf("%w: %v", ErrBadCursor, err)
	}
	i := strings.IndexByte(string(raw), 0x1f)
	if i < 0 {
		return time.Time{}, "", ErrBadCursor
	}
	at, err := time.Parse(time.RFC3339Nano, string(raw[:i]))
	if err != nil {
		return time.Time{}, "", fmt.Errorf("%w: %v", ErrBadCursor, err)
	}
	return at, string(raw[i+1:]), nil
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

// externalIncidentID 外部事件第 gen 代的 incident_id。
// 第 1 代用裸 base（兼容历史数据与反查习惯），复发代追加 #N。
func externalIncidentID(base string, gen int) string {
	if gen <= 1 {
		return base
	}
	return base + "#" + strconv.Itoa(gen)
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

// ---- L2 疑似重复（方案决策：只提示不自动合并）----

// Candidate 疑似重复候选（供人工确认，绝不自动合并）。
type Candidate struct {
	IncidentID string   `json:"incident_id"`
	Title      string   `json:"title"`
	Reasons    []string `json:"reasons"`
	Score      int      `json:"score"` // 0-100，越高越像重复
}

// SimilarCandidates 在候选集里找出与 target 相似的事件（L2）。
// 纯函数：不依赖存储实现，调用方传入候选集（通常是同状态/近期事件）。
//
// 判定维度（保守：任一**内容**维命中即入候选，命中越多分越高）：
//   - dedup_key 相同（节点+指纹+时间窗归一化，最强信号）
//   - 簇关联重叠（同一 cluster_key）
//   - 标题规范化后相同（去空白/大小写）
//   - 时间邻近（同一窗口内创建）——**仅加权，不作独立入选项**
//
// 时间邻近单独不足以称"疑似重复"（批量数据同一分钟创建会让候选列表被噪声
// 淹没）；至少命中一个内容维才入候选，时间只在此之上加分。
//
// **不自动合并**：运维领域误并两个不同故障是灾难，一律交给人确认。
func SimilarCandidates(target Incident, others []Incident, window time.Duration) []Candidate {
	var out []Candidate
	for _, o := range others {
		if o.ID == target.ID || o.State == StateResolved {
			continue
		}
		var reasons []string
		score := 0
		if target.DedupKey != "" && target.DedupKey == o.DedupKey {
			reasons = append(reasons, "去重键相同")
			score += 50
		}
		if overlap(target.ClusterKeys, o.ClusterKeys) {
			reasons = append(reasons, "关联同一故障簇")
			score += 40
		}
		if normTitle(target.Title) == normTitle(o.Title) && normTitle(target.Title) != "" {
			reasons = append(reasons, "标题相同")
			score += 25
		}
		if len(reasons) == 0 {
			// 无内容信号：时间邻近单独不作候选（避免噪声淹没真重复）。
			continue
		}
		if window > 0 && absDur(target.CreatedAt.Sub(o.CreatedAt)) <= window {
			reasons = append(reasons, "创建时间相近")
			score += 10
		}
		if score > 100 {
			score = 100
		}
		out = append(out, Candidate{IncidentID: o.ID, Title: o.Title, Reasons: reasons, Score: score})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Score > out[j].Score })
	return out
}

// DedupKeyFor 生成归一化去重键（L1/L2 共用）：节点集合 + 指纹 + 时间窗。
// 调用方（装配层）负责在事件创建时填充 Incident.DedupKey。
//
// window < 1s 时 `int64(window.Seconds())` 为 0 → 除零 panic；子秒窗口在
// 语义上等同"不按窗前分桶"，故退化为秒级桶（见下）。window >= 1s 时
// 结果与历史一致。
func DedupKeyFor(nodeKeys []string, fingerprint string, at time.Time, window time.Duration) string {
	keys := append([]string(nil), nodeKeys...)
	sort.Strings(keys)
	bucket := at.Unix()
	if secs := int64(window / time.Second); secs > 0 {
		bucket = at.Unix() / secs
	}
	return strings.Join(keys, ",") + "|" + fingerprint + "|" + strconv.FormatInt(bucket, 10)
}

func overlap(a, b []string) bool {
	for _, x := range a {
		for _, y := range b {
			if x == y {
				return true
			}
		}
	}
	return false
}

func normTitle(s string) string {
	return strings.ToLower(strings.Join(strings.Fields(s), ""))
}

func absDur(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}
