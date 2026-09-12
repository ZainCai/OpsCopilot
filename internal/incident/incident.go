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
	"strconv"
	"strings"
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

// ErrClusterTaken 簇已被另一事件挂走的哨兵根因（一簇一事件唯一约束，
// idx_incident_cluster_unique）。W10-6 生产自动挂簇（OPS_AUTOATTACH）用
// errors.Is(err, ErrClusterTaken) 区分"共簇冲突→跳过并计数"与真实存储
// 故障——多指纹共簇仅首单持故障域，冲突不是错误，绝不级联上抛。
var ErrClusterTaken = errors.New("incident: cluster already attached")

// ClusterTakenError 冲突详情（簇键 + 现主事件）。Error() 文本与历史裸
// fmt.Errorf 逐字节一致（有调用方按整串消息断言），errors.Is 经 Unwrap
// 命中 ErrClusterTaken。
type ClusterTakenError struct{ ClusterKey, Owner string }

func (e *ClusterTakenError) Error() string {
	return fmt.Sprintf("incident: cluster %q already attached to %q", e.ClusterKey, e.Owner)
}

// Unwrap 挂上哨兵，供 errors.Is(err, ErrClusterTaken) 判定。
func (e *ClusterTakenError) Unwrap() error { return ErrClusterTaken }

// ErrInvalidTransition 非法状态转换（宁可报错，不静默改状态）。
type ErrInvalidTransition struct{ From, To State }

func (e ErrInvalidTransition) Error() string {
	return fmt.Sprintf("invalid transition %s -> %s", e.From, e.To)
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
	// AckedAt 首次进入 acked 的时间戳（W10-2 补齐，MTTA 分子来源）。零值 =
	// 从未被 acked（含 open→mitigated/resolved 直达链）。落戳单点在状态机
	// （statemachine.go），**只落一次**——后续转移绝不改写（新增确定性行为，
	// 契约测试硬断言双 Store 一致）。
	AckedAt time.Time `json:"acked_at"`
	// SLAMinutes 事件级 SLA 目标时长**覆盖**（分钟，迁移 000019）。0 = 无
	// 覆盖，有效目标按 severity 取装配层注入的 config 默认（OPS_SLA_*）。
	// deadline/剩余/超时是 GET 时的只读派生视图，不落库（理由见迁移头注释）。
	SLAMinutes int `json:"sla_minutes"`

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
	// AttachCluster 簇→事件关联。语义：重复挂同簇同单幂等（不产生副本、
	// 不覆盖首挂）；簇已被他单占用返回可被 errors.Is(err, ErrClusterTaken)
	// 判定的冲突（W10-6 自动挂簇据此"跳过并计数"，不当故障上抛）。
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
	// SetSLA 设置事件级 SLA 目标时长覆盖（分钟，>0；0 合法 = 清除覆盖回到
	// 按级默认）。只写 sla_minutes，不动状态机字段。事件不存在返回 ErrNotFound。
	SetSLA(id string, minutes int) error
	// KPI 聚合窗口队列（口径唯一定义在 kpi.go，PG 是 SQL 翻译；双 store
	// 一致性由契约测试 TestContractKPI 锁死）。
	KPI(since time.Time, severity string) (KPIStats, error)
	// Persistence 声明落库形态（"memory" | "timescaledb"）——REST 响应
	// 据此提示调用方（R6-4：内存态重启即丢，必须让消费者知道）。
	Persistence() string
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

// externalIncidentID 外部事件第 gen 代的 incident_id。
// 第 1 代用裸 base（兼容历史数据与反查习惯），复发代追加 #N。
func externalIncidentID(base string, gen int) string {
	if gen <= 1 {
		return base
	}
	return base + "#" + strconv.Itoa(gen)
}
