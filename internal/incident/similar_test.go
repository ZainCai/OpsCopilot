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

// TestSimilarCandidatesClusterAndTitle 簇 / 标题命中入候选；已解决单跳过；
// 排序按分值降序（簇 40 > 标题 25）。dedup_key 维度已删（第十一轮 P1-2：
// 装配层从未填充的死快路，L1 去重需求如复活另立项）。
func TestSimilarCandidatesClusterAndTitle(t *testing.T) {
	now := time.Date(2026, 9, 10, 10, 0, 0, 0, time.UTC)
	target := Incident{ID: "T", Title: "x", ClusterKeys: []string{"c:fp1@1"},
		CreatedAt: now, State: StateOpen}
	others := []Incident{
		{ID: "K1", Title: " X ", CreatedAt: now, State: StateOpen},                                 // 标题规范化相同 +25
		{ID: "C1", Title: "z", ClusterKeys: []string{"c:fp1@1"}, CreatedAt: now, State: StateOpen}, // 同簇 +40
		{ID: "R1", Title: "x", CreatedAt: now, State: StateResolved},                               // 已解决：跳过
	}
	got := SimilarCandidates(target, others, time.Hour)
	if len(got) != 2 {
		t.Fatalf("candidates = %+v, want 2 (resolved skipped)", got)
	}
	if got[0].IncidentID != "C1" || got[0].Score < got[1].Score {
		t.Fatalf("order = %+v, want C1 (cluster 40) first", got)
	}
}
