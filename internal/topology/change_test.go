package topology

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func mustRecord(t *testing.T, s *ChangeStore, ev ChangeEvent) ChangeEvent {
	t.Helper()
	stored, err := s.Record(ev)
	if err != nil {
		t.Fatalf("Record(%s): unexpected error: %v", ev.ID, err)
	}
	return stored
}

func TestRecord_FillsDefaults(t *testing.T) {
	s := NewChangeStore(nil)
	before := time.Now()
	stored := mustRecord(t, s, ChangeEvent{
		ID:      "c1",
		NodeKey: "host:a",
		Type:    ChangeDeploy,
	})

	if !stored.OccurredAt.After(before.Add(-time.Minute)) {
		t.Errorf("zero OccurredAt should be filled with now, got %v", stored.OccurredAt)
	}
	if stored.Confidence != ConfidenceMedium {
		t.Errorf("default confidence = %q, want medium (external declaration, ADR-007)", stored.Confidence)
	}
	if stored.Type != ChangeDeploy {
		t.Errorf("type = %q, want deploy", stored.Type)
	}
	if s.Len() != 1 {
		t.Errorf("Len = %d, want 1", s.Len())
	}
}

func TestRecord_ExplicitHighConfidence(t *testing.T) {
	s := NewChangeStore(nil)
	stored := mustRecord(t, s, ChangeEvent{
		ID:         "c2",
		NodeKey:    "host:a",
		Type:       ChangeConfig,
		Confidence: ConfidenceHigh, // 变更平台同机直采等直接观测场景
	})
	if stored.Confidence != ConfidenceHigh {
		t.Errorf("confidence = %q, want high", stored.Confidence)
	}
}

func TestRecord_Validation(t *testing.T) {
	s := NewChangeStore(nil)
	cases := []struct {
		name string
		ev   ChangeEvent
		want error
	}{
		{"empty id", ChangeEvent{NodeKey: "h", Type: ChangeDeploy}, ErrEmptyChangeID},
		{"empty node key", ChangeEvent{ID: "x", Type: ChangeDeploy}, ErrEmptyNodeKey},
		{"unknown type", ChangeEvent{ID: "x", NodeKey: "h", Type: ChangeType("restart")}, nil}, // 非 errors.Is 匹配，单独断言
		{"bad confidence", ChangeEvent{ID: "x", NodeKey: "h", Type: ChangeDeploy, Confidence: Confidence("HIGH")}, nil},
	}
	for _, tc := range cases {
		_, err := s.Record(tc.ev)
		if err == nil {
			t.Errorf("%s: expected error, got nil", tc.name)
			continue
		}
		if tc.want != nil && !errors.Is(err, tc.want) {
			t.Errorf("%s: error = %v, want %v", tc.name, err, tc.want)
		}
	}
	// 非法类型/置信度必须被拒绝（上面表驱动里已确认 err != nil，这里不再重复）
	if _, err := s.Record(ChangeEvent{ID: "t1", NodeKey: "h", Type: ChangeType("restart")}); err == nil {
		t.Error("unknown change type must be rejected")
	}
	if _, err := s.Record(ChangeEvent{ID: "t2", NodeKey: "h", Type: ChangeDeploy, Confidence: Confidence("HIGH")}); err == nil {
		t.Error("invalid confidence must be rejected")
	}
}

func TestRecord_DuplicateID(t *testing.T) {
	s := NewChangeStore(nil)
	mustRecord(t, s, ChangeEvent{ID: "dup", NodeKey: "h", Type: ChangeDeploy})
	_, err := s.Record(ChangeEvent{ID: "dup", NodeKey: "h", Type: ChangeRollback})
	if !errors.Is(err, ErrDuplicateChange) {
		t.Fatalf("error = %v, want ErrDuplicateChange", err)
	}
	// 重复提交不得覆盖原记录
	got, ok := s.Get("dup")
	if !ok || got.Type != ChangeDeploy {
		t.Errorf("duplicate overwrote original: got %+v", got)
	}
}

func TestRecord_StrictNodeCheck(t *testing.T) {
	keys := map[string]bool{"host:exists": true}
	s := NewChangeStore(func(k string) bool { return keys[k] })

	if _, err := s.Record(ChangeEvent{ID: "ok", NodeKey: "host:exists", Type: ChangeDeploy}); err != nil {
		t.Fatalf("existing node rejected: %v", err)
	}
	_, err := s.Record(ChangeEvent{ID: "bad", NodeKey: "host:ghost", Type: ChangeDeploy})
	if !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("error = %v, want ErrNodeNotFound", err)
	}
	if s.Len() != 1 {
		t.Errorf("rejected event should not be stored, Len = %d", s.Len())
	}
}

func TestByNode_SortedByTime(t *testing.T) {
	s := NewChangeStore(nil)
	base := time.Now().Add(-time.Hour)
	// 故意乱序插入
	mustRecord(t, s, ChangeEvent{ID: "m2", NodeKey: "h", Type: ChangeConfig, OccurredAt: base.Add(20 * time.Minute)})
	mustRecord(t, s, ChangeEvent{ID: "m1", NodeKey: "h", Type: ChangeDeploy, OccurredAt: base.Add(5 * time.Minute)})
	mustRecord(t, s, ChangeEvent{ID: "m3", NodeKey: "h", Type: ChangeRollback, OccurredAt: base.Add(40 * time.Minute)})
	mustRecord(t, s, ChangeEvent{ID: "other", NodeKey: "h2", Type: ChangeDeploy, OccurredAt: base})

	got := s.ByNode("h")
	if len(got) != 3 {
		t.Fatalf("ByNode len = %d, want 3", len(got))
	}
	for i, want := range []string{"m1", "m2", "m3"} {
		if got[i].ID != want {
			t.Errorf("got[%d].ID = %s, want %s (not sorted by time)", i, got[i].ID, want)
		}
	}
	// ByNode 对未知节点返回空切片而非 nil 崩溃
	if n := s.ByNode("ghost"); len(n) != 0 {
		t.Errorf("unknown node returned %d events, want 0", len(n))
	}
}

func TestByNodeWithin_ClosedWindow(t *testing.T) {
	s := NewChangeStore(nil)
	base := time.Now().Add(-time.Hour)
	times := []time.Duration{0, 10 * time.Minute, 20 * time.Minute, 30 * time.Minute}
	for i, d := range times {
		mustRecord(t, s, ChangeEvent{
			ID: string(rune('a' + i)), NodeKey: "h", Type: ChangeDeploy,
			OccurredAt: base.Add(d),
		})
	}
	// 闭区间：边界时刻必须命中
	got := s.ByNodeWithin("h", base.Add(10*time.Minute), base.Add(20*time.Minute))
	if len(got) != 2 {
		t.Fatalf("window [10m,20m] got %d events, want 2 (closed interval)", len(got))
	}
	// 单侧不设限
	if got := s.ByNodeWithin("h", base.Add(15*time.Minute), time.Time{}); len(got) != 2 {
		t.Errorf("window [15m,∞) got %d events, want 2", len(got))
	}
	// 全局窗口（跨节点）：(∞,5m] 只含 a@0m
	if got := s.Within(time.Time{}, base.Add(5*time.Minute)); len(got) != 1 {
		t.Errorf("global window (∞,5m] got %d events, want 1 (only a@0m)", len(got))
	}
}

func TestGet_ReturnsCopy(t *testing.T) {
	s := NewChangeStore(nil)
	mustRecord(t, s, ChangeEvent{ID: "c", NodeKey: "h", Type: ChangeDeploy, Summary: "orig"})
	got, _ := s.Get("c")
	got.Summary = "mutated"
	again, _ := s.Get("c")
	if again.Summary != "orig" {
		t.Errorf("Get returned internal pointer: mutation leaked (summary=%q)", again.Summary)
	}
}

func TestByNode_ReturnsCopy(t *testing.T) {
	s := NewChangeStore(nil)
	mustRecord(t, s, ChangeEvent{ID: "c", NodeKey: "h", Type: ChangeDeploy})
	got := s.ByNode("h")
	got[0].Author = "attacker"
	again := s.ByNode("h")
	if again[0].Author != "" {
		t.Errorf("ByNode returned internal slice: mutation leaked (author=%q)", again[0].Author)
	}
}

func TestChangeStore_Concurrent(t *testing.T) {
	s := NewChangeStore(nil)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_, _ = s.Record(ChangeEvent{
					ID:         time.Duration(n*1000+j).String() + "-id",
					NodeKey:    "h",
					Type:       ChangeDeploy,
					OccurredAt: time.Now(),
				})
				_ = s.ByNode("h")
				_, _ = s.Get("probe")
			}
		}(i)
	}
	wg.Wait()
	if s.Len() != 400 {
		t.Errorf("Len = %d, want 400", s.Len())
	}
}
