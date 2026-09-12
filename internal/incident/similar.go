// similar.go L2 疑似重复相似度匹配（纯函数，不触存储）。
package incident

import (
	"sort"
	"strconv"
	"strings"
	"time"
)

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
