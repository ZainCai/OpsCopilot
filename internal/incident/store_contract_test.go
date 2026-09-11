// store_contract_test.go 双 Store（MemStore / PGStore）契约测试（优化方案 #5 第一步）。
//
// 目的：把 MemStore 与 internal/incident/incident.go、pg_store.go 两套实现的
// **现状行为**锁成一张安全网——同一测试体分别跑两个实现。后续若把状态机等
// 双实现逻辑收敛为一份（重构），这张网必须原样通过。
//
// 纪律：本文件**只观测、不修正**。凡 Mem 与 PG 行为不一致之处：
//   - 能同时硬断言的（双方现状一致）→ 放 runContract 测试体，强断言；
//   - 不一致的 → 放 TestContractDriftSnapshot 的探针（probe），用 t.Log 把
//     双方现状显式记录成"漂移快照"（测试仍通过），疑点写进 suspect 标签与
//     下方"漂移清单"。重构前请先读那张表。
//
// PG 门控沿用 pgStoreForTest 的既有模式：OPS_TEST_PG_DSN 未设置时 pgstore
// 子测试整体 skip，本机 / CI 恒绿。
//
// ---------------------------------------------------------------------------
// 漂移清单（代码走查 + 探针实测；★=疑似 bug）
//
//	D1 ack_by 读不回      ★PG：Transition 会写 ack_by 列（resolved+actor 时），
//	                       但 Get/List/ListPage/Upsert 的查询与 RETURNING 均不
//	                       SELECT ack_by → 驱动侧永远读成 ""。Mem 会把 AckBy
//	                       带回并读出。probe: ack_by_never_read_back
//	D2 空串字段 NULLIF 误用 ★PG：Create 对 severity/created_by 用 NULLIF(x,'')，
//	                       而列是 NOT NULL DEFAULT …——显式 NULL 不走 DEFAULT，
//	                       空串直接 23502 违例报错；Mem 接受并原样存 ""。
//	                       （生产未爆仅因 REST 层先把 severity 兜底成 info、
//	                       created_by 必填 400。）probe: create_empty_*
//	D3 复发代际语义键      结构差：Mem 沿 incident_id 链（base,#2,#3…）逐代走，
//	                       PG 按 generation 列 ORDER BY DESC 取当前代。合法流程
//	                       下可观测结果一致（本文件已锁），但"id 恰为
//	                       origin:ref 的人工单"会暴露分歧 → probe:
//	                       external_id_hijack（Mem 静默刷新人工单，PG 8 次
//	                       重试后报 cannot allocate generation——两边都可疑）。
//	D4 source_meta 类型面   PG 列是 JSONB：""→强制 "{}"，非法 JSON 报错，回读
//	                       是规范化文本；Mem 是自由文本原样进出。probe:
//	                       source_meta_surface
//	D5 upsert 刷新返回值    Mem 刷新返回带 ClusterKeys（clone），PG upsert 返回
//	                       不做 fillClusters → 恒空。probe: refresh_return_clusters
//	D6 合并簇转移          ★Mem：MergeInto 里 `if _, taken := s.byCluster[k];
//	                       !taken` 对已挂载的簇恒为 taken==true → 转移整段是
//	                       no-op：目标单拿不到簇、byCluster 仍指旧单、
//	                       IncidentForCluster 反查返回已合并的旧单。PG 真实
//	                       转移（UPDATE incident_cluster）。Mem 违背了自己的
//	                       文档注释"簇关联转移给主单"。probe: merge_cluster_transfer
//	D7 List 排序基底        Mem=插入序（s.order），PG=ORDER BY created_at ASC
//	                       且**无第二排序键**（同刻顺序不稳）。单调时钟下两者
//	                       重合（本文件 List 契约即按"创建序"断言），时钟回拨
//	                       /同微秒插入即漂移。ListPage 双方均已锁
//	                       (created_at DESC, id DESC)——List 未对齐 ListPage。
//	D8 ExternalActive 判据  Mem 只看 byExternal 指向的**当前代**，PG 看**任意
//	                       代**存在 state<>'resolved'。合法流程下旧代必已
//	                       resolved，结果等价（已锁）；语义面不同，记录之。
//	D9 归档/审计不在接口    Store 无归档与审计方法：MemStore 数据永驻、resolved
//	                       单永远可见可转；PG 侧 retention（cmd/opscopilot/
//	                       retention.go，迁移 000009）会把到期 resolved 单移出
//	                       热表 → 归档后 Get=ErrNotFound、List/stats 不再计入，
//	                       且归档数据无 API 找回路径（ADR-010 已知代价）。审计
//	                       （迁移 000006）由 REST 层外挂 cmd/opscopilot/audit.go，
//	                       两个 Store 现状都无审计钩子——此项一致但缺位。
//	D10 时间戳来源          Mem 全部可注入时钟（SetClock），PG 恒 DB now()。
//	                       本文件只断言跨实现可比的**关系**（单调、同时刻相
//	                       等），不断言绝对值。
//
// ---------------------------------------------------------------------------
package incident

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// harness：同一测试体分别跑两实现
// ---------------------------------------------------------------------------

// contractPrefix 生成一次子测试独占的 ID 前缀（隔离共享 PG 库里的其他行；
// 仅含 [A-Za-z0-9-]，LIKE 通配安全）。
func contractPrefix(t *testing.T) string {
	t.Helper()
	var b strings.Builder
	for _, r := range t.Name() {
		switch {
		case r >= 'A' && r <= 'Z', r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}
	s := b.String()
	if len(s) > 28 {
		s = s[len(s)-28:]
	}
	return "CT" + s + strconv.FormatInt(time.Now().UnixNano()%1e12, 36)
}

// newMemContract 每子测试一个全新 MemStore + 步进时钟（秒级单调 → 创建序
// 与 created_at 序重合，两实现在"合法时序"下语义对齐；闭包只在 MemStore
// 锁内被调用，无竞争）。
func newMemContract(t *testing.T, _ string) Store {
	t.Helper()
	s := NewMemStore()
	base := time.Now()
	var ticks int
	s.SetClock(func() time.Time {
		ticks++
		return base.Add(time.Duration(ticks) * time.Second)
	})
	return s
}

// newPGContract 复用 pgStoreForTest 的 skip 门控；按本次前缀前后清场，
// incident_id / source_ref 含前缀的行都算"我的"。
func newPGContract(t *testing.T, px string) Store {
	t.Helper()
	s := pgStoreForTest(t)
	del := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, _ = s.pool.Exec(ctx, `DELETE FROM incident_cluster WHERE incident_row_id IN
			(SELECT id FROM incident WHERE tenant_id=$1
			 AND (incident_id LIKE '%'||$2||'%' OR source_ref LIKE $2||'%'))`,
			s.tenantID, px)
		_, _ = s.pool.Exec(ctx, `DELETE FROM incident WHERE tenant_id=$1
			AND (incident_id LIKE '%'||$2||'%' OR source_ref LIKE $2||'%')`,
			s.tenantID, px)
	}
	del()
	t.Cleanup(del)
	return s
}

var contractStores = []struct {
	name string
	make func(t *testing.T, px string) Store
}{
	{name: "memstore", make: newMemContract},
	{name: "pgstore", make: newPGContract},
}

// runContract 同一测试体分别跑两个实现（现状一致的行为在此硬断言）。
func runContract(t *testing.T, body func(t *testing.T, s Store, px string)) {
	t.Helper()
	for _, f := range contractStores {
		t.Run(f.name, func(t *testing.T) {
			px := contractPrefix(t)
			body(t, f.make(t, px), px)
		})
	}
}

// runProbe 漂移探针：body 在两实现上各跑一遍并返回"行为快照串"（不得
// Fatal——快照永远通过）；双方串不同 → t.Log 一条 DRIFT 记录。
func runProbe(t *testing.T, name, suspect string, body func(t *testing.T, s Store, px string) string) {
	t.Helper()
	seen := map[string]string{}
	for _, f := range contractStores {
		t.Run(f.name, func(t *testing.T) {
			px := contractPrefix(t)
			seen[f.name] = body(t, f.make(t, px), px)
		})
	}
	m, _ := seen["memstore"]
	p, po := seen["pgstore"]
	switch {
	case !po:
		t.Logf("[drift-snapshot] %s：pgstore 跳过（无 OPS_TEST_PG_DSN）。memstore 现状=%q；疑点：%s", name, m, suspect)
	case m == p:
		t.Logf("[aligned] %s：双方现状一致=%q", name, m)
	default:
		t.Logf("[DRIFT] %s | memstore=%q | pgstore=%q | suspect: %s", name, m, p, suspect)
	}
}

// ---------------------------------------------------------------------------
// 小工具
// ---------------------------------------------------------------------------

func contractCreate(t *testing.T, s Store, px, tag, severity, createdBy string) *Incident {
	t.Helper()
	inc, err := s.Create(px+"-"+tag, "title-"+tag, severity, createdBy)
	if err != nil {
		t.Fatalf("create %s: %v", tag, err)
	}
	return inc
}

func contractGet(t *testing.T, s Store, id string) Incident {
	t.Helper()
	inc, err := s.Get(id)
	if err != nil {
		t.Fatalf("get %s: %v", id, err)
	}
	return inc
}

func owns(inc Incident, px string) bool {
	return strings.Contains(inc.ID, px) || strings.Contains(inc.SourceRef, px)
}

func ownedIDs(list []Incident, px string) []string {
	var out []string
	for _, inc := range list {
		if owns(inc, px) {
			out = append(out, inc.ID)
		}
	}
	return out
}

func statsOf(t *testing.T, s Store) PageStats {
	t.Helper()
	p, err := s.ListPage(PageQuery{Limit: 1})
	if err != nil {
		t.Fatalf("stats probe: %v", err)
	}
	return p.Stats
}

func diffStats(a, b PageStats) PageStats {
	return PageStats{Active: b.Active - a.Active, Resolved: b.Resolved - a.Resolved,
		Manual: b.Manual - a.Manual, External: b.External - a.External}
}

// walkAll 翻完所有页：每页不超 limit、页内不重；返回本前缀 ID 的全局序列。
// 只断言两实现都成立的不变量（PG 共享库里混着别人的行，页大小/游标是否
// 为空不可比；不重、不漏、全局序可比）。
func walkAll(t *testing.T, s Store, base PageQuery, px string) []string {
	t.Helper()
	limit := pageLimit(base.Limit)
	var out []string
	seen := map[string]bool{}
	cur := base.Cursor
	for pages := 0; pages < 100; pages++ {
		q := base
		q.Cursor = cur
		p, err := s.ListPage(q)
		if err != nil {
			t.Fatalf("walk page: %v", err)
		}
		if len(p.Items) > limit {
			t.Fatalf("page over limit: %d > %d", len(p.Items), limit)
		}
		for _, inc := range p.Items {
			if seen[inc.ID] {
				t.Fatalf("duplicate id within walk: %s", inc.ID)
			}
			seen[inc.ID] = true
			if owns(inc, px) {
				out = append(out, inc.ID)
			}
		}
		if p.NextCursor == "" {
			return out
		}
		cur = p.NextCursor
	}
	t.Fatal("walk did not terminate within 100 pages")
	return nil
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func validState(st State) bool {
	switch st {
	case StateOpen, StateAcked, StateMitigated, StateResolved:
		return true
	}
	return false
}

// expectExternal 走一步外部 upsert。
func expectExternal(t *testing.T, s Store, origin Origin, ref, title, severity string) (*Incident, bool) {
	t.Helper()
	inc, isNew, err := s.UpsertExternal(origin, ref, title, severity, "system:contract", "{}")
	if err != nil {
		t.Fatalf("upsert external %s/%s: %v", origin, ref, title)
	}
	return inc, isNew
}

// ---------------------------------------------------------------------------
// 契约 1：Create / Get / 值拷贝语义 / Persistence
// ---------------------------------------------------------------------------

func TestContractCreateAndGet(t *testing.T) {
	runContract(t, func(t *testing.T, s Store, px string) {
		// 必填校验（两实现同一入口校验路径）。
		for _, bad := range []struct{ id, title string }{{"", "t"}, {"   ", "t"}, {px, ""}, {px, "  "}} {
			if _, err := s.Create(bad.id, bad.title, "critical", "ops"); err == nil {
				t.Fatalf("accepted invalid create id=%q title=%q", bad.id, bad.title)
			}
		}
		inc := contractCreate(t, s, px, "A", "critical", "ops")

		// 初始状态恒 open、origin=manual、auto_close=manual_only（不信任外部）。
		if inc.State != StateOpen || inc.Origin != OriginManual ||
			inc.AutoClosePolicy != "manual_only" || inc.CreatedBy != "ops" ||
			inc.Title != "title-A" || inc.Severity != "critical" ||
			inc.SourceRef != "" || inc.MergedInto != "" {
			t.Fatalf("create entity mismatch: %+v", inc)
		}
		if inc.CreatedAt.IsZero() || !inc.CreatedAt.Equal(inc.UpdatedAt) || !inc.ResolvedAt.IsZero() {
			t.Fatalf("create timestamps mismatch: %+v", inc)
		}

		// 重复 ID 拒绝。
		if _, err := s.Create(px+"-A", "dup", "critical", "ops"); err == nil {
			t.Fatal("duplicate id accepted")
		}
		// Get 回读与返回体一致；缺失 → ErrNotFound。
		got := contractGet(t, s, px+"-A")
		if got.ID != inc.ID || got.State != inc.State || got.Title != inc.Title ||
			got.Severity != inc.Severity || !got.CreatedAt.Equal(inc.CreatedAt) {
			t.Fatalf("get roundtrip mismatch: %+v vs %+v", got, inc)
		}
		if _, err := s.Get(px + "-NOPE"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("missing get: err=%v, want ErrNotFound", err)
		}

		// ---- 值拷贝契约（Store 文档承诺"返回值为拷贝"）----
		b := contractCreate(t, s, px, "B", "warning", "ops")
		if err := s.AttachCluster(b.ID, px+"-ck"); err != nil {
			t.Fatalf("attach: %v", err)
		}
		g := contractGet(t, s, b.ID)
		if len(g.ClusterKeys) != 1 || g.ClusterKeys[0] != px+"-ck" {
			t.Fatalf("get clusters: %v", g.ClusterKeys)
		}
		g.Title = "HACKED"
		g.ClusterKeys[0] = "HACKED"
		g2 := contractGet(t, s, b.ID)
		if g2.Title != "title-B" || g2.ClusterKeys[0] != px+"-ck" {
			t.Fatalf("returned value aliases store state: %+v", g2)
		}
		b.Title = "HACKED" // Create 的返回指针同样不得与库内共享
		if contractGet(t, s, b.ID).Title != "title-B" {
			t.Fatal("create return aliases store state")
		}

		// Persistence 标识（R6-4）。
		switch p := s.Persistence(); p {
		case "memory", "timescaledb":
		default:
			t.Fatalf("persistence = %q", p)
		}
	})
}

// ---------------------------------------------------------------------------
// 契约 2：状态机转移矩阵（CanTransition 表 × 两实现）
// ---------------------------------------------------------------------------

func TestContractTransitionMatrix(t *testing.T) {
	states := []State{StateOpen, StateAcked, StateMitigated, StateResolved}

	runContract(t, func(t *testing.T, s Store, px string) {
		n := 0
		for _, from := range states {
			for _, to := range append(append([]State{}, states...), StateActive, State("bogus")) {
				n++
				id := fmt.Sprintf("%s-M%02d", px, n)
				if _, err := s.Create(id, "matrix", "info", "ops"); err != nil {
					t.Fatalf("create %s: %v", id, err)
				}
				// 走到 from（open 免走；acked 一步；mitigated 两步；resolved 三步——
				// 全部沿合法链前进，不借道 Transition 之外的后门）。
				chain := map[State][]State{
					StateOpen:      {},
					StateAcked:     {StateAcked},
					StateMitigated: {StateAcked, StateMitigated},
					StateResolved:  {StateAcked, StateMitigated, StateResolved},
				}[from]
				for _, step := range chain {
					if _, err := s.Transition(id, step, "ops"); err != nil {
						t.Fatalf("reach %s via %s: %v", from, step, err)
					}
				}
				before := contractGet(t, s, id)

				got, err := s.Transition(id, to, "alice")
				legal := CanTransition(from, to)

				if !legal {
					if err == nil {
						t.Fatalf("illegal transition accepted by store: %s -> %s", from, to)
					}
					var it ErrInvalidTransition
					if !errors.As(err, &it) {
						t.Fatalf("%s -> %s: err=%v, want ErrInvalidTransition", from, to, err)
					}
					if it.From != from || it.To != to {
						t.Fatalf("ErrInvalidTransition payload = {%v,%v}, want {%v,%v}", it.From, it.To, from, to)
					}
					if after := contractGet(t, s, id); after.State != from {
						t.Fatalf("rejected transition mutated state: %s -> %s", from, after.State)
					}
					continue
				}

				if err != nil {
					t.Fatalf("legal transition rejected: %s -> %s: %v", from, to, err)
				}
				if got.State != to {
					t.Fatalf("returned state %s, want %s", got.State, to)
				}
				after := contractGet(t, s, id)
				if after.State != to {
					t.Fatalf("persisted state %s, want %s", after.State, to)
				}
				if !after.CreatedAt.Equal(before.CreatedAt) {
					t.Fatal("created_at mutated by transition")
				}
				if after.UpdatedAt.Before(before.UpdatedAt) {
					t.Fatal("updated_at went backwards")
				}
				switch {
				case to == StateResolved:
					// resolved 落戳：resolved_at == updated_at（同语句/同时钟）。
					if after.ResolvedAt.IsZero() || !after.ResolvedAt.Equal(after.UpdatedAt) {
						t.Fatalf("resolved stamp mismatch: %+v", after)
					}
				case after.ResolvedAt != before.ResolvedAt:
					t.Fatalf("resolved_at touched on %s -> %s", from, to)
				}
				if to == StateAcked && after.AutoClosePolicy != "manual_only" {
					t.Fatalf("acked must flip auto_close_policy to manual_only (R2): %s", after.AutoClosePolicy)
				}
			}
		}
		if n != 24 {
			t.Fatalf("matrix cases = %d, want 24", n)
		}
		// 不存在的事件。
		if _, err := s.Transition(px+"-MISS", StateAcked, "ops"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("transition missing: err=%v, want ErrNotFound", err)
		}
	})
}

// ---------------------------------------------------------------------------
// 契约 3：簇关联（一簇一事件、幂等、所有权、反查）
// ---------------------------------------------------------------------------

func TestContractAttachCluster(t *testing.T) {
	runContract(t, func(t *testing.T, s Store, px string) {
		a := contractCreate(t, s, px, "A", "critical", "ops")
		b := contractCreate(t, s, px, "B", "warning", "ops")

		if err := s.AttachCluster(a.ID, "  "); err == nil {
			t.Fatal("blank cluster key accepted")
		}
		if err := s.AttachCluster(px+"-MISS", px+"-ck0"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("attach missing incident: err=%v", err)
		}
		if err := s.AttachCluster(a.ID, px+"-ck1"); err != nil {
			t.Fatalf("attach: %v", err)
		}
		if err := s.AttachCluster(a.ID, px+"-ck1"); err != nil {
			t.Fatalf("re-attach same incident must be idempotent: %v", err)
		}
		// 一簇最多一事件：他单占用 → 拒绝。
		if err := s.AttachCluster(b.ID, px+"-ck1"); err == nil {
			t.Fatal("cluster double-ownership accepted")
		}
		// 多簇 + 稳定排序输出。
		if err := s.AttachCluster(a.ID, px+"-ck2"); err != nil {
			t.Fatalf("attach ck2: %v", err)
		}
		ga := contractGet(t, s, a.ID)
		if len(ga.ClusterKeys) != 2 || ga.ClusterKeys[0] > ga.ClusterKeys[1] {
			t.Fatalf("clusters = %v, want 2 sorted", ga.ClusterKeys)
		}
		// 反查。
		if inc, ok := s.IncidentForCluster(px + "-ck1"); !ok || inc.ID != a.ID {
			t.Fatalf("for-cluster = %+v ok=%v, want %s", inc, ok, a.ID)
		}
		if _, ok := s.IncidentForCluster(px + "-ck-none"); ok {
			t.Fatal("for-cluster on unknown key returned true")
		}
		// resolved 事件仍可挂簇（两实现现状都不拦）。
		if _, err := s.Transition(a.ID, StateResolved, "ops"); err != nil {
			t.Fatalf("resolve: %v", err)
		}
		if err := s.AttachCluster(a.ID, px+"-ck3"); err != nil {
			t.Fatalf("attach after resolved: %v", err)
		}
	})
}

// ---------------------------------------------------------------------------
// 契约 4：UpsertExternal 幂等 + 代际（M9/000008）+ ExternalActive
// ---------------------------------------------------------------------------

func TestContractUpsertExternalGenerations(t *testing.T) {
	runContract(t, func(t *testing.T, s Store, px string) {
		// 入口校验（两实现同一串前置检查）。
		if _, _, err := s.UpsertExternal(Origin("bogus"), px+"-x", "t", "info", "sys", "{}"); err == nil {
			t.Fatal("invalid origin accepted")
		}
		if _, _, err := s.UpsertExternal(OriginWebhook, "  ", "t", "info", "sys", "{}"); err == nil {
			t.Fatal("blank source_ref accepted")
		}
		if _, _, err := s.UpsertExternal(OriginWebhook, px+"-a#b", "t", "info", "sys", "{}"); err == nil {
			t.Fatal("source_ref containing '#' accepted (H2)")
		}
		if _, _, err := s.UpsertExternal(OriginWebhook, px+"-x", " ", "info", "sys", "{}"); err == nil {
			t.Fatal("blank title accepted")
		}

		ref := px + "-ext"
		base := "alertmanager:" + ref

		inc1, isNew := expectExternal(t, s, OriginAlertmanager, ref, "v1", "critical")
		if !isNew || inc1.ID != base {
			t.Fatalf("gen1: id=%s isNew=%v, want %s/true", inc1.ID, isNew, base)
		}
		if inc1.State != StateOpen || inc1.Origin != OriginAlertmanager ||
			inc1.SourceRef != ref || inc1.AutoClosePolicy != "auto" ||
			inc1.CreatedBy != "system:contract" || !inc1.ResolvedAt.IsZero() {
			t.Fatalf("gen1 entity mismatch: %+v", inc1)
		}

		// 未解决期间重推 → 只刷新不新建（同 key 幂等）。
		before := contractGet(t, s, base)
		inc1r, isNew := expectExternal(t, s, OriginAlertmanager, ref, "v1-refresh", "warning")
		if isNew || inc1r.ID != base {
			t.Fatalf("refresh created a new row: id=%s isNew=%v", inc1r.ID, isNew)
		}
		g1r := contractGet(t, s, base)
		if g1r.Title != "v1-refresh" || g1r.Severity != "warning" {
			t.Fatalf("refresh not persisted: %+v", g1r)
		}
		if !g1r.CreatedAt.Equal(before.CreatedAt) || g1r.UpdatedAt.Before(before.UpdatedAt) {
			t.Fatal("refresh must keep created_at and move updated_at forward")
		}
		if got := walkAll(t, s, PageQuery{Limit: 2, Origin: string(OriginAlertmanager)}, px); len(got) != 1 {
			t.Fatalf("rows for one active ref = %v, want exactly 1", got)
		}

		// resolved 后重推 → 复发新开一代（#N），旧单不可被动。
		if _, err := s.Transition(base, StateResolved, "ops"); err != nil {
			t.Fatalf("resolve gen1: %v", err)
		}
		inc2, isNew := expectExternal(t, s, OriginAlertmanager, ref, "v2", "critical")
		if !isNew || inc2.ID != base+"#2" || inc2.State != StateOpen {
			t.Fatalf("gen2: id=%s isNew=%v state=%s", inc2.ID, isNew, inc2.State)
		}
		if old := contractGet(t, s, base); old.State != StateResolved || old.Title != "v1-refresh" {
			t.Fatalf("recurrence touched history: %+v", old)
		}
		// 刷新落在当前代。
		if inc2r, isNew := expectExternal(t, s, OriginAlertmanager, ref, "v2-refresh", "critical"); isNew || inc2r.ID != inc2.ID {
			t.Fatalf("refresh must land on current gen: %+v isNew=%v", inc2r, isNew)
		}
		// 三代链：#2 resolved → #3。
		if _, err := s.Transition(inc2.ID, StateResolved, "ops"); err != nil {
			t.Fatalf("resolve gen2: %v", err)
		}
		inc3, isNew := expectExternal(t, s, OriginAlertmanager, ref, "v3", "info")
		if !isNew || inc3.ID != base+"#3" {
			t.Fatalf("gen3: id=%s isNew=%v", inc3.ID, isNew)
		}

		// ExternalActive 生命周期：只有"存在未解决的单"才算 active。
		if active, err := s.ExternalActive(OriginAlertmanager, ref); err != nil || !active {
			t.Fatalf("gen3 open → active=%v err=%v", active, err)
		}
		if _, err := s.Transition(inc3.ID, StateResolved, "ops"); err != nil {
			t.Fatalf("resolve gen3: %v", err)
		}
		if active, err := s.ExternalActive(OriginAlertmanager, ref); err != nil || active {
			t.Fatalf("all resolved → active=%v err=%v", active, err)
		}
		if active, err := s.ExternalActive(OriginAlertmanager, px+"-never"); err != nil || active {
			t.Fatalf("unknown ref → active=%v err=%v", active, err)
		}

		// 同 ref 不同 origin 各立一单（幂等键含 origin）。
		if w, isNew := expectExternal(t, s, OriginWebhook, ref, "w", "info"); !isNew || w.ID != "webhook:"+ref {
			t.Fatalf("cross-origin: id=%s isNew=%v", w.ID, isNew)
		}
		if a1, _ := s.ExternalActive(OriginWebhook, ref); !a1 {
			t.Fatal("webhook row must be active")
		}
	})
}

// ---------------------------------------------------------------------------
// 契约 5：MergeInto（合并本身的可观测面；簇转移现状不一致 → 见漂移探针）
// ---------------------------------------------------------------------------

func TestContractMerge(t *testing.T) {
	runContract(t, func(t *testing.T, s Store, px string) {
		a := contractCreate(t, s, px, "A", "critical", "ops")
		b := contractCreate(t, s, px, "B", "warning", "ops")

		if err := s.MergeInto(px+"-MISS", b.ID); !errors.Is(err, ErrNotFound) {
			t.Fatalf("merge missing src: err=%v", err)
		}
		if err := s.MergeInto(a.ID, px+"-MISS"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("merge missing tgt: err=%v", err)
		}
		if err := s.MergeInto(a.ID, a.ID); err == nil {
			t.Fatal("self merge accepted")
		}

		bBefore := contractGet(t, s, b.ID)
		if err := s.MergeInto(a.ID, b.ID); err != nil {
			t.Fatalf("merge: %v", err)
		}
		ga := contractGet(t, s, a.ID)
		if ga.State != StateResolved || ga.MergedInto != b.ID {
			t.Fatalf("merged src mismatch: %+v", ga)
		}
		if ga.ResolvedAt.IsZero() || !ga.ResolvedAt.Equal(ga.UpdatedAt) {
			t.Fatalf("merge must stamp resolved_at == updated_at: %+v", ga)
		}
		if gb := contractGet(t, s, b.ID); gb.State != StateOpen || gb.MergedInto != "" ||
			gb.UpdatedAt.Before(bBefore.UpdatedAt) {
			t.Fatalf("merge target mismatch: %+v", gb)
		}
		// 幂等：重复合并静默成功。
		if err := s.MergeInto(a.ID, b.ID); err != nil {
			t.Fatalf("re-merge must be idempotent: %v", err)
		}
		// 被合并单已 resolved → 一切状态推进被拒。
		if _, err := s.Transition(a.ID, StateAcked, "ops"); err == nil {
			t.Fatal("transition on merged incident accepted")
		}
	})
}

// ---------------------------------------------------------------------------
// 契约 6：List（过滤 + 创建序 + active 别名）与 Stats 增量
// ---------------------------------------------------------------------------

func TestContractListAndStats(t *testing.T) {
	runContract(t, func(t *testing.T, s Store, px string) {
		before := statsOf(t, s)

		var want []string
		for i := 1; i <= 5; i++ {
			inc := contractCreate(t, s, px, fmt.Sprintf("%02d", i), "info", "ops")
			want = append(want, inc.ID)
		}
		if _, err := s.Transition(want[1], StateResolved, "ops"); err != nil {
			t.Fatalf("resolve %s: %v", want[1], err)
		}
		if err := s.AttachCluster(want[2], px+"-k1"); err != nil {
			t.Fatalf("attach: %v", err)
		}

		// 两实现共有的过滤+排序面（PG 共享库有外人行 → 只看本前缀子序列）。
		cases := []struct {
			filter State
			want   []string
		}{
			{"", want},
			{StateOpen, []string{want[0], want[2], want[3], want[4]}},
			{StateResolved, []string{want[1]}},
			// StateActive 是查询别名、不是真实状态：List 按字面量匹配 → 双方都空。
			// （与 ListPage 的 active≠"" 处理不同——ListPage 契约里另行锁定。）
			{StateActive, nil},
			{State("bogus"), nil},
		}
		for _, c := range cases {
			list, err := s.List(c.filter)
			if err != nil {
				t.Fatalf("list %q: %v", c.filter, err)
			}
			if got := ownedIDs(list, px); !equalStrings(got, c.want) {
				t.Fatalf("list(%q) owned = %v, want %v", c.filter, got, c.want)
			}
		}
		// List 结果同样带簇关联。
		for _, inc := range mustListAll(t, s) {
			if inc.ID == want[2] && (len(inc.ClusterKeys) != 1 || inc.ClusterKeys[0] != px+"-k1") {
				t.Fatalf("list row clusters = %v", inc.ClusterKeys)
			}
		}

		// Stats 全量聚合（增量可比：PG 侧不受他人存量影响）。
		after := statsOf(t, s)
		if d := diffStats(before, after); d != (PageStats{Active: 4, Resolved: 1, Manual: 5}) {
			t.Fatalf("stats delta = %+v, want {Active:4 Resolved:1 Manual:5 External:0}", d)
		}
	})
}

func mustListAll(t *testing.T, s Store) []Incident {
	t.Helper()
	l, err := s.List("")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	return l
}

// ---------------------------------------------------------------------------
// 契约 7：ListPage——游标翻页、过滤、参数归一化、错误语义
// ---------------------------------------------------------------------------

func TestContractListPage(t *testing.T) {
	runContract(t, func(t *testing.T, s Store, px string) {
		var own []string // 创建序升序 → (created_at DESC, id DESC) 期望为逆序
		for i := 1; i <= 5; i++ {
			inc := contractCreate(t, s, px, fmt.Sprintf("%02d", i), "info", "ops")
			own = append(own, inc.ID)
		}
		wantDesc := []string{own[4], own[3], own[2], own[1], own[0]}

		// limit 归一化：0（默认 200）/ 超上限（截 1000）都不报错、页大小有界。
		for _, lim := range []int{0, 1, 2, 100, PageLimitMax * 5} {
			got := walkAll(t, s, PageQuery{Limit: lim}, px)
			if lim > 0 && lim < PageLimitMax {
				if !equalStrings(got, wantDesc) {
					t.Fatalf("walk(limit=%d) owned = %v, want %v", lim, got, wantDesc)
				}
			} else if len(got) != 5 {
				t.Fatalf("walk(limit=%d) got %d owned rows, want 5", lim, len(got))
			}
		}

		// state 过滤（含 active 别名——注意与 List 的差别：ListPage 认识别名）。
		if _, err := s.Transition(own[2], StateResolved, "ops"); err != nil {
			t.Fatalf("resolve: %v", err)
		}
		if got := walkAll(t, s, PageQuery{Limit: 2, State: StateActive}, px); !equalStrings(got,
			[]string{own[4], own[3], own[1], own[0]}) {
			t.Fatalf("active page owned = %v", got)
		}
		if got := walkAll(t, s, PageQuery{Limit: 2, State: StateResolved}, px); !equalStrings(got,
			[]string{own[2]}) {
			t.Fatalf("resolved page owned = %v", got)
		}
		// 非法 state 过滤：两实现共用 normalizePageQuery。
		if _, err := s.ListPage(PageQuery{State: State("nope")}); err == nil {
			t.Fatal("invalid state filter accepted")
		}
		// 坏游标 → ErrBadCursor（REST 层靠 errors.Is 映射 400）。
		if _, err := s.ListPage(PageQuery{Cursor: "!!!not-a-cursor"}); !errors.Is(err, ErrBadCursor) {
			t.Fatalf("bad cursor: err=%v, want ErrBadCursor", err)
		}

		// origin 过滤：人工单不混入外部页，反之亦然。
		ext, isNew := expectExternal(t, s, OriginAlertmanager, px+"-pg", "ext", "critical")
		if !isNew {
			t.Fatal("first external upsert must create")
		}
		if got := walkAll(t, s, PageQuery{Limit: 2, Origin: string(OriginAlertmanager)}, px); len(got) != 1 || got[0] != ext.ID {
			t.Fatalf("origin=alertmanager owned = %v, want [%s]", got, ext.ID)
		}
		if got := walkAll(t, s, PageQuery{Limit: 2, Origin: string(OriginManual)}, px); equalStrings(got, []string{ext.ID}) || len(got) != 5 {
			t.Fatalf("origin=manual owned = %v, want the 5 manual rows", got)
		}
		// Stats 增量含 external。
		if _, _, err := s.UpsertExternal(OriginWebhook, px+"-pg2", "ext2", "critical", "sys", "{}"); err != nil {
			t.Fatalf("upsert webhook: %v", err)
		}
		st := statsOf(t, s)
		if st.External < 2 {
			t.Fatalf("stats after 2 external upserts: %+v", st)
		}
	})
}

// ---------------------------------------------------------------------------
// 契约 8：时间戳单调（跨实现可比的"关系"而非绝对值）
// ---------------------------------------------------------------------------

func TestContractTimestamps(t *testing.T) {
	runContract(t, func(t *testing.T, s Store, px string) {
		inc := contractCreate(t, s, px, "TS", "info", "ops")
		g0 := contractGet(t, s, inc.ID)
		if !g0.CreatedAt.Equal(g0.UpdatedAt) || !g0.ResolvedAt.IsZero() {
			t.Fatalf("initial stamps: %+v", g0)
		}
		if _, err := s.Transition(inc.ID, StateAcked, "ops"); err != nil {
			t.Fatalf("ack: %v", err)
		}
		g1 := contractGet(t, s, inc.ID)
		if !g1.CreatedAt.Equal(g0.CreatedAt) || g1.UpdatedAt.Before(g0.UpdatedAt) || !g1.ResolvedAt.IsZero() {
			t.Fatalf("after ack: %+v (prev %+v)", g1, g0)
		}
		got, err := s.Transition(inc.ID, StateMitigated, "ops")
		if err != nil {
			t.Fatalf("mitigate: %v", err)
		}
		if got.UpdatedAt.Before(g1.UpdatedAt) {
			t.Fatal("mitigated updated_at went backwards")
		}
		ret, err := s.Transition(inc.ID, StateResolved, "ops")
		if err != nil {
			t.Fatalf("resolve: %v", err)
		}
		if ret.ResolvedAt.IsZero() || !ret.ResolvedAt.Equal(ret.UpdatedAt) {
			t.Fatalf("resolve stamp: %+v", ret)
		}
		g2 := contractGet(t, s, inc.ID)
		if g2.UpdatedAt.Before(g1.UpdatedAt) || !g2.ResolvedAt.Equal(g2.UpdatedAt) {
			t.Fatalf("persisted resolve mismatch: %+v", g2)
		}

		// 外部单：创建同时刻、刷新不动 created_at。
		u, isNew := expectExternal(t, s, OriginWebhook, px+"-ts", "v1", "info")
		if !isNew || !u.CreatedAt.Equal(u.UpdatedAt) || !u.ResolvedAt.IsZero() {
			t.Fatalf("external initial stamps: %+v isNew=%v", u, isNew)
		}
		r, _ := expectExternal(t, s, OriginWebhook, px+"-ts", "v2", "info")
		if !r.CreatedAt.Equal(u.CreatedAt) || r.UpdatedAt.Before(u.CreatedAt) {
			t.Fatalf("external refresh stamps: %+v", r)
		}
	})
}

// ---------------------------------------------------------------------------
// 契约 9：并发——写写互斥收敛、并发读不撕裂
// ---------------------------------------------------------------------------

func TestContractConcurrency(t *testing.T) {
	runContract(t, func(t *testing.T, s Store, px string) {
		// (a) 并发 ack：恰好一人成功，其余 ErrInvalidTransition。
		inc := contractCreate(t, s, px, "ACK", "critical", "ops")
		const ackers = 8
		var ackOK atomic.Int64
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := 0; i < ackers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				_, err := s.Transition(inc.ID, StateAcked, "ops")
				if err == nil {
					ackOK.Add(1)
					return
				}
				var it ErrInvalidTransition
				if !errors.As(err, &it) && !errors.Is(err, ErrNotFound) {
					t.Errorf("concurrent ack: unexpected err %v", err)
				}
			}()
		}
		close(start)
		wg.Wait()
		if got := ackOK.Load(); got != 1 {
			t.Fatalf("concurrent ack successes = %d, want exactly 1", got)
		}
		if st := contractGet(t, s, inc.ID).State; st != StateAcked {
			t.Fatalf("final state = %s, want acked", st)
		}

		// (b) 并发同 ref 外部 upsert：恰好新建一次、不串号、不裂代。
		ref := px + "-race"
		const pushers = 6
		var newCount atomic.Int64
		var pushErrs atomic.Int64
		ids := make([]string, pushers)
		for i := 0; i < pushers; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				inc, isNew, err := s.UpsertExternal(OriginAlertmanager, ref, "race", "critical", "sys", "{}")
				if err != nil {
					pushErrs.Add(1)
					return
				}
				ids[i] = inc.ID
				if isNew {
					newCount.Add(1)
				}
			}(i)
		}
		wg.Wait()
		if n := pushErrs.Load(); n != 0 {
			t.Fatalf("concurrent upsert errors = %d, want 0", n)
		}
		if n := newCount.Load(); n != 1 {
			t.Fatalf("concurrent upsert created %d rows (isNew=true), want exactly 1", n)
		}
		for _, id := range ids {
			if id != "alertmanager:"+ref {
				t.Fatalf("concurrent upsert diverged: %s != %s", id, "alertmanager:"+ref)
			}
		}
		if got := walkAll(t, s, PageQuery{Limit: 2, Origin: string(OriginAlertmanager)}, px); len(got) != 1 {
			t.Fatalf("ref rows after race = %v, want exactly 1", got)
		}

		// (c) 边推进状态边并发读：不得读到撕裂/非法状态/幻影错误。
		rd := contractCreate(t, s, px, "READ", "critical", "ops")
		stop := make(chan struct{})
		var readErrs atomic.Int64
		var readIters atomic.Int64
		for i := 0; i < 4; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-stop:
						return
					default:
					}
					g, err := s.Get(rd.ID)
					if err != nil || !validState(g.State) {
						readErrs.Add(1)
					}
					if l, err := s.List(""); err != nil {
						readErrs.Add(1)
					} else {
						for _, inc := range l {
							if !validState(inc.State) {
								readErrs.Add(1)
							}
						}
					}
					if _, err := s.ListPage(PageQuery{Limit: 2}); err != nil {
						readErrs.Add(1)
					}
					readIters.Add(1)
				}
			}()
		}
		for _, to := range []State{StateAcked, StateMitigated, StateResolved} {
			if _, err := s.Transition(rd.ID, to, "ops"); err != nil {
				t.Fatalf("write-hammer -> %s: %v", to, err)
			}
			time.Sleep(10 * time.Millisecond) // 让读者插队
		}
		close(stop)
		wg.Wait()
		if n := readErrs.Load(); n != 0 {
			t.Fatalf("concurrent readers observed %d torn/invalid results", n)
		}
		if n := readIters.Load(); n < 4 {
			t.Fatalf("readers barely ran (%d iters) — test didn't probe concurrency", n)
		}
		if st := contractGet(t, s, rd.ID).State; st != StateResolved {
			t.Fatalf("final state after concurrent reads = %s, want resolved", st)
		}
	})
}

// ---------------------------------------------------------------------------
// 契约 10：resolved 单的可见性现状（归档/审计不在 Store 接口——D9）
// ---------------------------------------------------------------------------

func TestContractResolvedVisibility(t *testing.T) {
	runContract(t, func(t *testing.T, s Store, px string) {
		inc := contractCreate(t, s, px, "VIS", "critical", "ops")
		if _, err := s.Transition(inc.ID, StateResolved, "ops"); err != nil {
			t.Fatalf("resolve: %v", err)
		}
		// Store 接口现状：resolved 单在两个实现里都**可见**（Get/List/ListPage），
		// 且仍参与 stats。PG 侧的到期归档发生在接口之外
		// （cmd/opscopilot/retention.go 删热表行 → 归档后 Get=ErrNotFound、
		// stats 不计、控制台不可见；MemStore 永无此路径——漂移清单 D9）。
		if got := contractGet(t, s, inc.ID); got.State != StateResolved {
			t.Fatalf("resolved incident invisible: %+v", got)
		}
		if got := ownedIDs(mustListAll(t, s), px); !containsStr(got, inc.ID) {
			t.Fatalf("List must still contain resolved row, got %v", got)
		}
		if got := walkAll(t, s, PageQuery{Limit: 2, State: StateResolved}, px); !containsStr(got, inc.ID) {
			t.Fatalf("ListPage(resolved) must still contain row, got %v", got)
		}
		// 迁移面探针（仅 PG）：归档/审计表必须已建（000006/000009）。
		if pgs, ok := s.(*PGStore); ok {
			for _, tbl := range []string{"incident_archive", "incident_audit", "incident_cluster"} {
				var n int
				if err := pgs.pool.QueryRow(context.Background(),
					`SELECT count(*) FROM information_schema.tables WHERE table_name=$1`, tbl).Scan(&n); err != nil {
					t.Fatalf("table probe %s: %v", tbl, err)
				}
				if n == 0 {
					t.Fatalf("migration surface missing: table %s", tbl)
				}
			}
			t.Log("[surface] pgstore：incident_archive/audit 表存在，但 Store 接口无归档/审计方法——归档仅由 retention 任务落地")
		} else {
			t.Log("[surface] memstore：无归档/审计概念——resolved 单永驻，除非进程重启整库蒸发（Persistence=memory）")
		}
	})
}

func containsStr(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// 漂移快照：Mem 与 PG 现状不一致处——只记录，不修正（测试永远通过）
// ---------------------------------------------------------------------------

func TestContractDriftSnapshot(t *testing.T) {

	runProbe(t, "D1_ack_by_never_read_back",
		"PG：ack_by 列有写无读（所有查询/RETURNING 都不含 ack_by），驱动侧永远读空——疑似 PG bug",
		func(t *testing.T, s Store, px string) string {
			inc := contractCreate(t, s, px, "ACKBY", "critical", "ops")
			ret, err := s.Transition(inc.ID, StateResolved, "alice")
			if err != nil {
				return "transition-err"
			}
			got := contractGet(t, s, inc.ID)
			return fmt.Sprintf("transition-return.AckBy=%q get.AckBy=%q", ret.AckBy, got.AckBy)
		})

	runProbe(t, "D2_create_empty_severity",
		"PG：NULLIF('','')→显式 NULL 撞 severity NOT NULL（DEFAULT 不生效）直接报错；Mem 原样接受存空串——PG 侧 SQL 误用",
		func(t *testing.T, s Store, px string) string {
			_, err := s.Create(px+"-E1", "blank severity", "", "ops")
			if err != nil {
				return "err"
			}
			return "ok(severity=" + strconv.Quote(contractGet(t, s, px+"-E1").Severity) + ")"
		})

	runProbe(t, "D3_create_empty_created_by",
		"同 D2 一族：created_by NULLIF 误用；生产未爆仅因 REST 层强制 created_by 非空",
		func(t *testing.T, s Store, px string) string {
			_, err := s.Create(px+"-E2", "blank by", "critical", "")
			if err != nil {
				return "err"
			}
			return "ok"
		})

	runProbe(t, "D4_upsert_external_empty_severity",
		"D2 同款在外部链路：PG 新建代时报 NOT NULL；Mem 接受。注意 PG 的刷新分支（不新建）反而能写空串——PG 内部自相矛盾",
		func(t *testing.T, s Store, px string) string {
			_, _, err := s.UpsertExternal(OriginWebhook, px+"-sev", "sev-blank", "", "sys", "{}")
			if err != nil {
				return "create-rejects"
			}
			return "create-accepts"
		})

	runProbe(t, "D5_source_meta_surface",
		"PG 列是 JSONB（空→'{}'、非法 JSON 报错、回读是规范化文本）；Mem 是自由文本。类型差异非 bug，但依赖 meta 原文回读的行为会漂",
		func(t *testing.T, s Store, px string) string {
			i1, _, err1 := s.UpsertExternal(OriginWebhook, px+"-m1", "meta-empty", "info", "sys", "")
			meta1 := "n/a"
			if err1 == nil {
				meta1 = strconv.Quote(contractGet(t, s, i1.ID).SourceMeta)
			}
			_, _, err2 := s.UpsertExternal(OriginWebhook, px+"-m2", "meta-nonjson", "info", "sys", "not-json")
			return fmt.Sprintf("emptyMeta=%s nonJSON=%v", meta1, err2 == nil)
		})

	runProbe(t, "D6_refresh_return_omits_clusters",
		"PG：UpsertExternal 刷新分支不 fillClusters → 返回体 ClusterKeys 恒空；Mem 返回带簇。Get/List 双方都带，仅返回值漂移",
		func(t *testing.T, s Store, px string) string {
			ref := px + "-d6"
			inc, _ := expectExternal(t, s, OriginAlertmanager, ref, "v1", "critical")
			if err := s.AttachCluster(inc.ID, px+"-ck"); err != nil {
				return "attach-err"
			}
			r, _ := expectExternal(t, s, OriginAlertmanager, ref, "v2", "critical")
			return fmt.Sprintf("refresh-return.ClusterKeys=%v", r.ClusterKeys)
		})

	runProbe(t, "D7_merge_cluster_transfer",
		"★Mem bug 嫌疑最大：MergeInto 的簇转移被 `!taken` 判空挡（已挂簇必 taken）→ 目标单没拿到簇、IncidentForCluster 反查仍命中已合并旧单，与函数文档注释相悖；PG 真正转移",
		func(t *testing.T, s Store, px string) string {
			a := contractCreate(t, s, px, "SRC", "critical", "ops")
			b := contractCreate(t, s, px, "TGT", "critical", "ops")
			ck := px + "-ck"
			if err := s.AttachCluster(a.ID, ck); err != nil {
				return "attach-err"
			}
			if err := s.MergeInto(a.ID, b.ID); err != nil {
				return "merge-err"
			}
			owner := "none"
			if inc, ok := s.IncidentForCluster(ck); ok {
				if inc.ID == a.ID {
					owner = "SRC"
				} else {
					owner = "TGT"
				}
			}
			return fmt.Sprintf("forCluster=%s tgt.Clusters=%d src.Clusters=%d",
				owner, len(contractGet(t, s, b.ID).ClusterKeys), len(contractGet(t, s, a.ID).ClusterKeys))
		})

	runProbe(t, "D8_external_id_hijack_vs_retry_error",
		"incident_id 空间分叉：Mem 按 id 链找代（人工单若恰好占用 origin:ref 会被外部 upsert 静默刷新改写），PG 按 (origin,source_ref) 找代再插 id（撞 UNIQUE 重试 8 次后报错）。两种行为都可疑，重构时须定一个",
		func(t *testing.T, s Store, px string) string {
			ref := px + "-h"
			id := "alertmanager:" + ref // Mem 的 gen1 id 与人工占用者同名
			if _, err := s.Create(id, "manual-original", "info", "ops"); err != nil {
				return "precreate-err"
			}
			inc, isNew, err := s.UpsertExternal(OriginAlertmanager, ref, "HIJACKED", "critical", "sys", "{}")
			switch {
			case err != nil:
				return fmt.Sprintf("upsert-error(isNew=%v) stored-title=%q", isNew, contractGet(t, s, id).Title)
			default:
				return fmt.Sprintf("upsert-ok(isNew=%v,origin=%s) stored-title=%q", isNew, inc.Origin, contractGet(t, s, id).Title)
			}
		})

	runProbe(t, "D9_resolved_visibility_archival_gap",
		"接口外归档面（D9）：Mem 单永驻；PG resolved 满期由 retention 删热表→Get/List/stats 全部消失且无 API 找回（ADR-010 已知代价）。当前接口层面双方一致（均可见）",
		func(t *testing.T, s Store, px string) string {
			if pgs, ok := s.(*PGStore); ok {
				var n int
				_ = pgs.pool.QueryRow(context.Background(),
					`SELECT count(*) FROM information_schema.tables WHERE table_name='incident_archive'`).Scan(&n)
				return fmt.Sprintf("visible=true;archive-table-exists=%v;NOTE:retention 会把到期 resolved 单物理移出热表", n > 0)
			}
			return "visible=true;archive-table-exists=false;NOTE:MemStore 无归档概念，数据只随进程生死"
		})

	t.Log("[drift-note] D7-list-ordering（List 基底不同：Mem=插入序、PG=ORDER BY created_at 无第二排序键，同刻不稳）与 D8-ExternalActive 判据（Mem 只看当前代索引、PG 看任意代）在合法时序/单调时钟下不可区分，探针无法复现，详见文件头清单")
}
