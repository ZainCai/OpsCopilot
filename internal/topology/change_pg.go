// PGChangeStore：变更事件库的 TimescaleDB 持久化实现（优化方案 #4）。
//
// 定位（项目哲学：PG 是真相源，内存是可丢的加速层）：
//   - 写：先走内存在内存中判重/校验（Record 语义与纯内存实现逐字一致），
//     再尽力落 PG——PG 写失败**只记日志、不向上报错**：证据不能因 DB
//     抖动丢出采集链路（webhook 是 RCA 取证入口，500 会诱导 CI 侧重发风暴）；
//     丢失的落库行由 DB 恢复后自然被后续窗口查询遗漏——如实降级、响亮日志；
//   - 读：全部走内存缓存（*ChangeStore 嵌入方法直接复用，零改动）；
//   - 启动回放：LoadSince 把保留窗内的历史证据从 PG 灌回内存，
//     重启不再丢变更取证（本实现存在的唯一理由）；
//   - 清理：PruneBefore 同时清内存与 PG，两侧保留窗对齐（000015 的
//     (tenant_id, occurred_at) 索引即为此查询而生）。
//
// 表：复用 change_record（000001 建表 + 000002 对齐列映射与幂等键），
// 不新建表。列映射（000002 文件头契约的兑现）：
//
//	ID→event_id（幂等键，UNIQUE(tenant_id,event_id) + ON CONFLICT DO NOTHING）
//	NodeKey→node_key  Type→change_type（DB CHECK 与封闭集合一致）
//	OccurredAt→occurred_at  Author→actor  Source→source（空→DB 默认 'manual'）
//	Ref/Revision/Summary/Confidence→000002 第 4 步独立列；TenantID→tenant_id
//	（空则装配注入的默认租户）；detail 保持默认 '{}'。
//
// 治理（对齐 incident/pg_store.go 的纪律）：
//   - 全部语句带超时 ctx——DB 慢不拖垮调用方（R6-5 同款）；
//   - tenant 行幂等确保 + 进程内记忆（FK 要求，写路径每租户只查一次）；
//   - 回放对脏数据防御：未知 change_type 跳过并告警（内存侧是封闭集合，
//     混进 legacy 行会污染下游分支）；非法 confidence 归 medium。
//
// DB 缺席/故障时的装配策略在 cmd/assembly.go：构造或回放失败 → 降级纯
// 内存 ChangeStore，不阻塞启动。
package topology

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// 编译期保证：持久化实现满足接口。
var _ ChangeBackend = (*PGChangeStore)(nil)

// changeSQLTimeout 单语句超时（incident pg 同款纪律：pgxpool 无默认
// statement timeout，DB 挂死时写路径必须能自行收口）。回放用
// changeLoadTimeout：启动一次性操作，可以比逐条写宽。
const (
	changeSQLTimeout  = 5 * time.Second
	changeLoadTimeout = 15 * time.Second
)

// PGChangeStore 变更事件库的 PG 持久化实现：内存在此退化为"读缓存"。
// 并发安全（内存侧靠嵌入的 *ChangeStore 自身锁；tenant 记忆独立小锁）。
type PGChangeStore struct {
	*ChangeStore // 读缓存：ByNode/ByNodeWithin/Within/Get/Len 直接继承
	pool         *pgxpool.Pool
	tenantID     string // 事件未带 tenant_id 时的默认租户
	logf         func(string, ...any)

	// 租户存在性的进程内记忆（FK 要求）：incident.PGStore 同范式，
	// 按租户记——多租户事件不互相覆盖记忆。
	tenantMu   sync.Mutex
	tenantDone map[string]bool
}

// NewPGChangeStore 包装一个既有内存实现为持久化后端。mem 必须非 nil
// （nodeCheck 等钩子在构造时已注入，回放走 load 绕过）。构造本身不做
// IO——能否真用 PG 由调用方用 LoadSince 的返回值决定（失败即弃用本实现、
// 继续用纯内存，见 assembly）。
func NewPGChangeStore(mem *ChangeStore, pool *pgxpool.Pool, tenantID string, logf func(string, ...any)) *PGChangeStore {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	if mem == nil {
		mem = NewChangeStore(nil)
	}
	return &PGChangeStore{
		ChangeStore: mem,
		pool:        pool,
		tenantID:    tenantID,
		logf:        logf,
		tenantDone:  make(map[string]bool),
	}
}

// Persistence 落库形态（ChangeBackend 契约）。
func (s *PGChangeStore) Persistence() string { return "timescaledb" }

// Record 内存落库（校验/判重/nodeCheck 全在此完成）后尽力写 PG。
//
// PG 写失败（含超时、FK 断裂、连接耗尽）→ WARNING 日志 + 保留内存副本，
// 返回成功：采集链路不能被 DB 故障阻断（优化方案 #4 的降级承诺）。
// (tenant_id, event_id) 撞唯一键 = PG 已有同一幂等键 → DO NOTHING 视为
// 成功——"重复提交 = 已经成功"（G5 语义），且原记录不被覆盖。
//
// 注意：内存判重先于 PG——重启后内存只回放保留窗，更早的历史重复会以
// "新事件"进内存并撞 PG 唯一键，最终仍收敛为幂等成功。
func (s *PGChangeStore) Record(ev ChangeEvent) (ChangeEvent, error) {
	stored, err := s.ChangeStore.Record(ev)
	if err != nil {
		return ChangeEvent{}, err
	}
	if perr := s.persist(stored); perr != nil {
		s.logf("WARNING: change %s persist failed (memory only): %v", stored.ID, perr)
	}
	return stored, nil
}

// persist 单条落库（同步：webhook 量级 = 分钟几条，异步队列是过度设计；
// 5s 超时兜底，DB 挂了最坏拖慢单条提交而非全链路）。
func (s *PGChangeStore) persist(ev ChangeEvent) error {
	tenant := ev.TenantID
	if tenant == "" {
		tenant = s.tenantID
	}
	ctx, cancel := context.WithTimeout(context.Background(), changeSQLTimeout)
	defer cancel()
	if err := s.ensureTenant(ctx, tenant); err != nil {
		return err
	}
	_, err := s.pool.Exec(ctx, `
INSERT INTO change_record
  (tenant_id, event_id, node_key, change_type, occurred_at, actor, source,
   ref, revision, summary, confidence)
VALUES ($1, $2, $3, $4, $5, NULLIF($6, ''), COALESCE(NULLIF($7, ''), 'manual'),
   NULLIF($8, ''), NULLIF($9, ''), NULLIF($10, ''), $11)
ON CONFLICT (tenant_id, event_id) DO NOTHING`,
		tenant, ev.ID, ev.NodeKey, string(ev.Type), ev.OccurredAt,
		ev.Author, ev.Source, ev.Ref, ev.Revision, ev.Summary, string(ev.Confidence))
	if err != nil {
		return fmt.Errorf("change pg: insert %s: %w", ev.ID, err)
	}
	return nil
}

// PruneBefore 保留窗清理：先清内存（返回条数以内存为准，与纯内存实现
// 口径一致），再清 PG 同窗口。PG 删除失败只告警——残留行在下次启动
// 回放时按窗口过滤，不会再进读缓存，下一轮清理会再试。
func (s *PGChangeStore) PruneBefore(cutoff time.Time) int {
	removed := s.ChangeStore.PruneBefore(cutoff)
	ctx, cancel := context.WithTimeout(context.Background(), changeSQLTimeout)
	defer cancel()
	if _, err := s.pool.Exec(ctx,
		`DELETE FROM change_record WHERE occurred_at < $1`, cutoff); err != nil {
		s.logf("WARNING: change pg prune failed (rows linger outside replay window): %v", err)
	}
	return removed
}

// LoadSince 启动回放：把 occurred_at ≥ since 的全部事件从 PG 灌进内存
// 缓存，返回灌入条数。since 零值 = 不设下界（保留窗关闭时的"全量回放"）。
//
// 任何 DB 错误原样上抛——调用方（assembly）据此降级纯内存，不阻塞启动。
// 幂等：走 load（ID 已存在则跳过），可安全重复调用。
// 跨租户：回放不限租户（内存判重以全局 ID 为准，读缓存本就不分租户；
// M1 单租户下等价，多租户隔离落地时在 WHERE 里补 tenant 过滤）。
func (s *PGChangeStore) LoadSince(since time.Time) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), changeLoadTimeout)
	defer cancel()
	query := `
SELECT tenant_id, event_id, node_key, change_type, occurred_at,
       COALESCE(actor, ''), source, COALESCE(ref, ''), COALESCE(revision, ''),
       COALESCE(summary, ''), confidence
FROM change_record`
	args := []any{}
	if !since.IsZero() {
		query += ` WHERE occurred_at >= $1`
		args = append(args, since)
	}
	query += ` ORDER BY occurred_at`
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("change pg: replay query: %w", err)
	}
	defer rows.Close()

	loaded := 0
	for rows.Next() {
		var ev ChangeEvent
		if err := rows.Scan(&ev.TenantID, &ev.ID, &ev.NodeKey, &ev.Type,
			&ev.OccurredAt, &ev.Author, &ev.Source, &ev.Ref, &ev.Revision,
			&ev.Summary, &ev.Confidence); err != nil {
			return loaded, fmt.Errorf("change pg: replay scan: %w", err)
		}
		// 脏数据防御（见文件头）：类型必须在封闭集合，否则下游分支会踩空。
		typ, err := ParseChangeType(string(ev.Type))
		if err != nil {
			s.logf("WARNING: change pg replay skipped %s: %v", ev.ID, err)
			continue
		}
		ev.Type = typ
		if _, err := ParseConfidence(string(ev.Confidence)); err != nil {
			s.logf("WARNING: change pg replay %s: %v (fallback medium)", ev.ID, err)
			ev.Confidence = ConfidenceMedium
		}
		if s.ChangeStore.load(ev) {
			loaded++
		}
	}
	if err := rows.Err(); err != nil {
		return loaded, fmt.Errorf("change pg: replay rows: %w", err)
	}
	return loaded, nil
}

// ensureTenant 幂等确保租户行（change_record.tenant_id 的 FK 要求）。
// incident.PGStore.ensureTenant 同范式：进程内记忆 + 失败不记忆（下次重试）。
func (s *PGChangeStore) ensureTenant(ctx context.Context, tenant string) error {
	if tenant == "" {
		return fmt.Errorf("change pg: empty tenant id")
	}
	s.tenantMu.Lock()
	done := s.tenantDone[tenant]
	s.tenantMu.Unlock()
	if done {
		return nil
	}
	if _, err := s.pool.Exec(ctx,
		`INSERT INTO tenant (id, name) VALUES ($1, $1) ON CONFLICT (id) DO NOTHING`,
		tenant); err != nil {
		return fmt.Errorf("change pg: ensure tenant %s: %w", tenant, err)
	}
	s.tenantMu.Lock()
	s.tenantDone[tenant] = true
	s.tenantMu.Unlock()
	return nil
}
