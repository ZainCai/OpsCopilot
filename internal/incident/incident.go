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
type MemStore struct {
	mu         sync.Mutex
	byID       map[string]*Incident
	byCluster  map[string]string // cluster_key → incident_id（一簇最多挂一事件）
	byExternal map[string]string // (origin, source_ref) → 当前代 incident_id
	order      []string          // 创建序（List 稳定输出）
	now        func() time.Time  // 可注入时钟（测试用）
}

// NewMemStore 构造。
func NewMemStore() *MemStore {
	return &MemStore{
		byID:       map[string]*Incident{},
		byCluster:  map[string]string{},
		byExternal: map[string]string{},
		now:        time.Now,
	}
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
	inc := &Incident{ID: id, Title: title, Severity: severity,
		State: StateOpen, CreatedAt: now, UpdatedAt: now,
		Origin: OriginManual, CreatedBy: createdBy, AutoClosePolicy: "manual_only"}
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

// encodeCursor/decodeCursor 游标编解码：base64url("RFC3339Nano\x1fincidentID")。
// 不透明字符串，客户端只透传。
func encodeCursor(at time.Time, id string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(at.Format(time.RFC3339Nano) + "\x1f" + id))
}

func decodeCursor(s string) (time.Time, string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return time.Time{}, "", fmt.Errorf("incident: bad cursor: %w", err)
	}
	i := strings.IndexByte(string(raw), 0x1f)
	if i < 0 {
		return time.Time{}, "", errors.New("incident: bad cursor")
	}
	at, err := time.Parse(time.RFC3339Nano, string(raw[:i]))
	if err != nil {
		return time.Time{}, "", fmt.Errorf("incident: bad cursor time: %w", err)
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
