// pg_query.go PGStore 游标分页（ListPage）与簇→事件反查（IncidentForCluster）。
package incident

import (
	"fmt"
	"strconv"
)

// ListPage 分页读取（D4 决策 A：游标分页，最新优先；服务端过滤 state/origin）。
// 游标 = 上一页最后一行的 (created_at, incident_id)，行值比较一次定位，
// 无 OFFSET 的"翻深页越翻越慢"问题。Stats 为全量聚合（不受过滤影响）。
func (s *PGStore) ListPage(q PageQuery) (Page, error) {
	ctx, cancel := s.ctx()
	defer cancel()
	if err := normalizePageQuery(&q); err != nil {
		return Page{}, err
	}
	limit := pageLimit(q.Limit)

	// 全量聚合（KPI 数据源）：一条带 FILTER 的聚合，代价可忽略。
	var st PageStats
	if err := s.pool.QueryRow(ctx, `
SELECT count(*) FILTER (WHERE state <> 'resolved'),
       count(*) FILTER (WHERE state = 'resolved'),
       count(*) FILTER (WHERE origin = 'manual'),
       count(*) FILTER (WHERE origin <> 'manual')
FROM incident WHERE tenant_id=$1`, s.tenantID).Scan(
		&st.Active, &st.Resolved, &st.Manual, &st.External); err != nil {
		return Page{}, fmt.Errorf("incident pg: page stats: %w", err)
	}

	args := []any{s.tenantID, string(q.State), q.Origin}
	cond := ""
	if q.Cursor != "" {
		at, id, err := decodeCursor(q.Cursor)
		if err != nil {
			return Page{}, err
		}
		args = append(args, at, id)
		cond = fmt.Sprintf(" AND (created_at, incident_id) < ($%d, $%d)", len(args)-1, len(args))
	}
	args = append(args, limit+1) // 多取一行判断是否还有下一页
	rows, err := s.pool.Query(ctx, `
SELECT `+pgIncidentCols+`
FROM incident
WHERE tenant_id=$1
  AND ($2 = '' OR ($2 = 'active' AND state <> 'resolved') OR state = $2)
  AND ($3 = '' OR origin = $3)`+cond+`
ORDER BY created_at DESC, incident_id DESC
LIMIT $`+strconv.Itoa(len(args)), args...)
	if err != nil {
		return Page{}, fmt.Errorf("incident pg: page query: %w", err)
	}
	defer rows.Close()

	items := []Incident{}
	for rows.Next() {
		var inc Incident
		if err := scanIncident(rows, &inc); err != nil {
			return Page{}, fmt.Errorf("incident pg: page scan: %w", err)
		}
		items = append(items, inc)
	}
	if err := rows.Err(); err != nil {
		return Page{}, fmt.Errorf("incident pg: page rows: %w", err)
	}
	s.fillClusters(ctx, items)

	next := ""
	if len(items) > limit {
		last := items[limit-1]
		next = encodeCursor(last.CreatedAt, last.ID)
		items = items[:limit]
	}
	return Page{Items: items, NextCursor: next, Stats: st}, nil
}

// IncidentForCluster 反查簇所属事件。
//
// ⚠️ 返回值语义：`(zero, false)` 同时覆盖"该簇没有事件"与"DB 故障"两种情况
// ——接口签名无 error，二者不可区分。当前生产代码**没有**调用本方法
// （仅测试），故无实际影响；若未来接入业务，请优先用 `Get()`（带 error）
// 而不是把 false 当成"无事件"去新建，否则 DB 不可用时会建出重复单。
func (s *PGStore) IncidentForCluster(clusterKey string) (Incident, bool) {
	ctx, cancel := s.ctx()
	defer cancel()
	inc, err := s.queryOne(ctx, `
incident_id = (SELECT i.incident_id FROM incident_cluster ic
  JOIN incident i ON i.id = ic.incident_row_id
  WHERE i.tenant_id=$2 AND ic.cluster_key=$1)`, clusterKey, s.tenantID)
	if err != nil {
		return Incident{}, false
	}
	return inc, true
}
