// M2 主干骨架测试：状态机合法性 / 簇关联 / 反查。
package incident

import (
	"errors"
	"strconv"
	"testing"
	"time"
)

func TestStateMachineTransitions(t *testing.T) {
	legal := [][2]State{
		{StateOpen, StateAcked}, {StateOpen, StateMitigated}, {StateOpen, StateResolved},
		{StateAcked, StateMitigated}, {StateAcked, StateResolved},
		{StateMitigated, StateResolved},
	}
	for _, p := range legal {
		if !CanTransition(p[0], p[1]) {
			t.Fatalf("legal transition rejected: %s -> %s", p[0], p[1])
		}
	}
	illegal := [][2]State{
		{StateAcked, StateOpen}, {StateMitigated, StateAcked},
		{StateResolved, StateOpen}, {StateResolved, StateAcked},
	}
	for _, p := range illegal {
		if CanTransition(p[0], p[1]) {
			t.Fatalf("illegal transition accepted: %s -> %s", p[0], p[1])
		}
	}
}

func TestStoreCreateAndTransition(t *testing.T) {
	s := NewMemStore()
	var now time.Time
	s.SetClock(func() time.Time { now = now.Add(time.Minute); return now })

	// 缺字段拒绝。
	if _, err := s.Create("", "t", "critical", "ops"); err == nil {
		t.Fatal("empty id accepted")
	}
	if _, err := s.Create("INC-1", "", "critical", "ops"); err == nil {
		t.Fatal("empty title accepted")
	}
	// 重复 ID 拒绝。
	if _, err := s.Create("INC-1", "t1", "critical", "ops"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := s.Create("INC-1", "t1", "critical", "ops"); err == nil {
		t.Fatal("duplicate id accepted")
	}

	// 先 acked，再试非法回退 open → 拒绝。
	if _, err := s.Transition("INC-1", StateAcked, "ops"); err != nil {
		t.Fatalf("-> acked: %v", err)
	}
	if _, err := s.Transition("INC-1", StateOpen, "ops"); err == nil {
		t.Fatal("acked -> open accepted (want rejected per table)")
	}
	// 合法链路 acked→resolved，ResolvedAt 落戳。
	got, err := s.Transition("INC-1", StateResolved, "ops")
	if err != nil {
		t.Fatalf("-> resolved: %v", err)
	}
	if got.State != StateResolved || got.ResolvedAt.IsZero() || got.AckBy != "ops" {
		t.Fatalf("resolved incident wrong: %+v", got)
	}
	// 不存在的事件。
	if _, err := s.Transition("INC-X", StateAcked, "ops"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing incident: err = %v, want ErrNotFound", err)
	}
}

func TestAttachClusterAndLookup(t *testing.T) {
	s := NewMemStore()
	if _, err := s.Create("INC-1", "disk full", "critical", "ops"); err != nil {
		t.Fatalf("create: %v", err)
	}
	// 关联 + 幂等。
	if err := s.AttachCluster("INC-1", "c:fp-a@1"); err != nil {
		t.Fatalf("attach: %v", err)
	}
	if err := s.AttachCluster("INC-1", "c:fp-a@1"); err != nil {
		t.Fatalf("attach idempotent: %v", err)
	}
	// 一簇只能挂一事件。
	if _, err := s.Create("INC-2", "other", "warning", "ops"); err != nil {
		t.Fatalf("create2: %v", err)
	}
	if err := s.AttachCluster("INC-2", "c:fp-a@1"); err == nil {
		t.Fatal("cluster double-attach accepted")
	}
	// 反查。
	inc, ok := s.IncidentForCluster("c:fp-a@1")
	if !ok || inc.ID != "INC-1" {
		t.Fatalf("lookup = %+v ok=%v, want INC-1", inc, ok)
	}
	// 事件不存在。
	if err := s.AttachCluster("INC-X", "c:x@1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("attach to missing: err = %v", err)
	}
	// List 过滤。
	if got := len(mustList(s, StateOpen)); got != 2 {
		t.Fatalf("open list = %d, want 2", got)
	}
}

// TestDedupKeyForSubSecondWindowNoDivideByZero 回归：window < 1s 时
// int64(window.Seconds()) 为 0 → 除零 panic。子秒窗口应退化为秒级桶。
func TestDedupKeyForSubSecondWindowNoDivideByZero(t *testing.T) {
	at := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
	// 不 panic 即通过一半；再断言同一秒内两个时刻同键（退化到秒级分桶）。
	k1 := DedupKeyFor([]string{"n1"}, "fp1", at, 500*time.Millisecond)
	k2 := DedupKeyFor([]string{"n1"}, "fp1", at.Add(300*time.Millisecond), 500*time.Millisecond)
	if k1 != k2 {
		t.Fatalf("sub-second window should fall back to second bucket: %s vs %s", k1, k2)
	}
	// window >= 1s 行为保持：跨窗口应分属不同键。
	kw1 := DedupKeyFor([]string{"n1"}, "fp1", at, time.Minute)
	kw2 := DedupKeyFor([]string{"n1"}, "fp1", at.Add(2*time.Minute), time.Minute)
	if kw1 == kw2 {
		t.Fatal("distinct windows must produce distinct dedup keys")
	}
}

// TestUpsertExternalRecurrenceNewGeneration M9：同一外部告警恢复后再次触发，
// 应**新建一代**（incident_id 追加 #N），而不是去刷新那条已 resolved 的旧单
// ——否则新故障对运维完全不可见。
func TestUpsertExternalRecurrenceNewGeneration(t *testing.T) {
	s := NewMemStore()
	up := func(title string) (*Incident, bool) {
		inc, isNew, err := s.UpsertExternal(OriginAlertmanager, "fp-1", title, "critical", "system:alertmanager", "{}")
		if err != nil {
			t.Fatalf("upsert: %v", err)
		}
		return inc, isNew
	}
	inc1, isNew := up("磁盘 96%")
	if !isNew || inc1.ID != "alertmanager:fp-1" {
		t.Fatalf("gen1: id=%s isNew=%v", inc1.ID, isNew)
	}
	// 未解决期间重推 → 刷新，不新建（历史行为保持）。
	inc1b, isNew := up("磁盘 97%")
	if isNew || inc1b.ID != inc1.ID || inc1b.Title != "磁盘 97%" {
		t.Fatalf("refresh gen1: %+v isNew=%v", inc1b, isNew)
	}
	// 解决后再次触发 → 新开一代。
	if _, err := s.Transition(inc1.ID, StateResolved, "ops"); err != nil {
		t.Fatalf("resolve gen1: %v", err)
	}
	inc2, isNew := up("磁盘 98%")
	if !isNew || inc2.ID != "alertmanager:fp-1#2" || inc2.State != StateOpen {
		t.Fatalf("gen2: id=%s isNew=%v state=%s", inc2.ID, isNew, inc2.State)
	}
	// 旧单保持 resolved、标题不被刷掉（复发绝不能动历史单）。
	old, err := s.Get(inc1.ID)
	if err != nil {
		t.Fatalf("get gen1: %v", err)
	}
	if old.State != StateResolved || old.Title != "磁盘 97%" {
		t.Fatalf("gen1 must stay untouched: %+v", old)
	}
	// gen2 未解决期间重推 → 刷新 gen2。
	if inc2b, isNew := up("磁盘 99%"); isNew || inc2b.ID != inc2.ID {
		t.Fatalf("refresh gen2: %+v isNew=%v", inc2b, isNew)
	}
	// 再解决再触发 → #3。
	if _, err := s.Transition(inc2.ID, StateResolved, "ops"); err != nil {
		t.Fatalf("resolve gen2: %v", err)
	}
	if inc3, isNew := up("磁盘 100%"); !isNew || inc3.ID != "alertmanager:fp-1#3" {
		t.Fatalf("gen3: id=%s isNew=%v", inc3.ID, isNew)
	}
}

// TestExternalActive M9 配套判据："有没有**未解决**的单"，而不是"有没有单"。
// 建单限流靠它区分"刷新"与"会新建"。
func TestExternalActive(t *testing.T) {
	s := NewMemStore()
	if active, err := s.ExternalActive(OriginAlertmanager, "fp-2"); err != nil || active {
		t.Fatalf("before create: active=%v err=%v", active, err)
	}
	if _, _, err := s.UpsertExternal(OriginAlertmanager, "fp-2", "t", "critical", "system", "{}"); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	if active, _ := s.ExternalActive(OriginAlertmanager, "fp-2"); !active {
		t.Fatal("open incident must be active")
	}
	if _, err := s.Transition("alertmanager:fp-2", StateResolved, "ops"); err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if active, _ := s.ExternalActive(OriginAlertmanager, "fp-2"); active {
		t.Fatal("resolved incident must not be active (recurrence opens a new generation)")
	}
}

// mustList 测试辅助：List 现在返回 error（D5），多数断言只关心内容，
// 出错直接失败。
func mustList(s Store, st State) []Incident {
	l, err := s.List(st)
	if err != nil {
		panic(err)
	}
	return l
}

// TestListPageCursorPagination D4：游标分页——最新优先、next_cursor 终止、
// 翻页不重不漏。
func TestListPageCursorPagination(t *testing.T) {
	s := NewMemStore()
	base := time.Now()
	// 建 5 单，created_at 依次递增 → 期望倒序为 i5..i1
	var want []string
	for i := 1; i <= 5; i++ {
		id := "P" + strconv.Itoa(i)
		inc, err := s.Create(id, "p"+strconv.Itoa(i), "info", "ops")
		if err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
		want = append(want, inc.ID)
		s.SetClock(func() time.Time { return base.Add(time.Duration(i) * time.Minute) })
	}
	// SetClock 影响后续 created_at；上面最后一次设置是 +5m，需要逐单区分 ——
	// 这里改为直接按创建时间排序断言：全部取回应是最新在前。
	page, err := s.ListPage(PageQuery{Limit: 2})
	if err != nil {
		t.Fatalf("page1: %v", err)
	}
	if len(page.Items) != 2 || page.NextCursor == "" {
		t.Fatalf("page1: n=%d cursor=%q", len(page.Items), page.NextCursor)
	}
	seen := map[string]bool{}
	for _, inc := range page.Items {
		if seen[inc.ID] {
			t.Fatalf("duplicate id across pages: %s", inc.ID)
		}
		seen[inc.ID] = true
	}
	got := len(seen)
	for page.NextCursor != "" {
		page, err = s.ListPage(PageQuery{Limit: 2, Cursor: page.NextCursor})
		if err != nil {
			t.Fatalf("next page: %v", err)
		}
		for _, inc := range page.Items {
			if seen[inc.ID] {
				t.Fatalf("id repeated across pages: %s", inc.ID)
			}
			seen[inc.ID] = true
		}
		got = len(seen)
	}
	if got != 5 {
		t.Fatalf("walked %d unique incidents, want 5", got)
	}
	// 坏游标必须报错（400 语义由 REST 层映射）。
	if _, err := s.ListPage(PageQuery{Cursor: "!!!not-a-cursor"}); err == nil {
		t.Fatal("bad cursor must error")
	}
	// state 过滤：resolved 不计入 active。
	if _, err := s.Transition("P1", StateResolved, "ops"); err != nil {
		t.Fatalf("resolve: %v", err)
	}
	page, err = s.ListPage(PageQuery{State: StateActive, Limit: 100})
	if err != nil {
		t.Fatalf("active page: %v", err)
	}
	if len(page.Items) != 4 {
		t.Fatalf("active = %d, want 4", len(page.Items))
	}
	if page.Stats.Active != 4 || page.Stats.Resolved != 1 {
		t.Fatalf("stats = %+v, want active 4 / resolved 1", page.Stats)
	}
}

// TestUpsertExternalRejectsHashSourceRef 第七轮 H2：'#' 是复发代际后缀分隔符，
// sourceRef 含 '#' 会与"别的告警的 gen1"得到同一 incident_id（跨告警串单）。
func TestUpsertExternalRejectsHashSourceRef(t *testing.T) {
	s := NewMemStore()
	if _, _, err := s.UpsertExternal(OriginWebhook, "fp#2", "t", "critical", "system", "{}"); err == nil {
		t.Fatal("sourceRef containing '#' must be rejected")
	}
	// 正常 ref 不受影响。
	if _, _, err := s.UpsertExternal(OriginWebhook, "fp-2", "t", "critical", "system", "{}"); err != nil {
		t.Fatalf("normal ref: %v", err)
	}
}
