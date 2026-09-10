// L2 疑似重复候选的判定测试（方案决策：只提示不自动合并）。
package incident

import (
	"testing"
	"time"
)

// TestSimilarCandidatesContentSignalRequired L2：只有内容信号（去重键/簇/
// 标题）才入候选；**仅时间相近不算疑似重复**——否则批量同分钟创建会把候选
// 列表淹没在噪声里（实机暴露的问题，回归锁定）。
func TestSimilarCandidatesContentSignalRequired(t *testing.T) {
	now := time.Date(2026, 9, 10, 10, 0, 0, 0, time.UTC)
	target := Incident{ID: "T", Title: "disk full on n1", CreatedAt: now, State: StateOpen}
	others := []Incident{
		// 仅时间相近、标题不同、无簇无去重键 → 不应入候选。
		{ID: "N1", Title: "cpu high on n9", CreatedAt: now.Add(time.Minute), State: StateOpen},
		// 标题相同（大小写/空白归一化后） → 入候选。
		{ID: "D1", Title: "disk  FULL on n1", CreatedAt: now.Add(2 * time.Minute), State: StateOpen},
	}
	got := SimilarCandidates(target, others, 30*time.Minute)
	if len(got) != 1 || got[0].IncidentID != "D1" {
		t.Fatalf("candidates = %+v, want only D1 (title match; time-only must be excluded)", got)
	}
	if got[0].Score < 25 {
		t.Fatalf("score = %d, want >= 25 (title match)", got[0].Score)
	}
}

// TestSimilarCandidatesDedupKeyAndCluster 去重键 / 簇命中入候选；已解决单跳过；
// 排序按分值降序（去重键 50 > 簇 40）。
func TestSimilarCandidatesDedupKeyAndCluster(t *testing.T) {
	now := time.Date(2026, 9, 10, 10, 0, 0, 0, time.UTC)
	target := Incident{ID: "T", Title: "x", ClusterKeys: []string{"c:fp1@1"},
		DedupKey: "n1|fp1|100", CreatedAt: now, State: StateOpen}
	others := []Incident{
		{ID: "K1", Title: "y", DedupKey: "n1|fp1|100", CreatedAt: now, State: StateOpen},
		{ID: "C1", Title: "z", ClusterKeys: []string{"c:fp1@1"}, CreatedAt: now, State: StateOpen},
		{ID: "R1", Title: "x", CreatedAt: now, State: StateResolved}, // 已解决：跳过
	}
	got := SimilarCandidates(target, others, time.Hour)
	if len(got) != 2 {
		t.Fatalf("candidates = %+v, want 2 (resolved skipped)", got)
	}
	if got[0].IncidentID != "K1" || got[0].Score < got[1].Score {
		t.Fatalf("order = %+v, want K1 (dedup_key 50) first", got)
	}
}

// TestDedupKeyForStable 归一化去重键：节点集合顺序无关，时间落入同桶则相同。
func TestDedupKeyForStable(t *testing.T) {
	at := time.Date(2026, 9, 10, 10, 7, 0, 0, time.UTC)
	a := DedupKeyFor([]string{"n2", "n1"}, "fp1", at, 30*time.Minute)
	b := DedupKeyFor([]string{"n1", "n2"}, "fp1", at.Add(time.Minute), 30*time.Minute)
	if a != b {
		t.Fatalf("dedup keys differ: %q vs %q (want stable within window)", a, b)
	}
	c := DedupKeyFor([]string{"n1"}, "fp1", at, 30*time.Minute)
	if a == c {
		t.Fatalf("dedup keys equal despite different node sets: %q", a)
	}
}
