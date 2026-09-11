// D8 决策 C：事件保留策略 —— resolved 满 N 天的工单**归档**（不是删除）。
//
// 归档 = 事务内把「事件本体 + 关联簇 + 审计轨迹」打包成一行 JSONB 写入
// incident_archive（见 migrations/000009），再删原行（incident_cluster 由
// FK 级联清除）。归档后审计不再占热表，但完整上下文仍可从归档表还原。
//
// 为什么放应用层而不是 TimescaleDB 的 add_job：保留逻辑要跨 incident /
// incident_cluster / incident_audit 三张表并生成聚合 JSON，用应用层实现
// 可单测、可配置、不依赖扩展版本行为；频率低（默认 6h 一轮），开销可忽略。
//
// 审计的口径（与决策一致）：**长留**。审计行只随其事件一起入档；
// 仍活跃的事件其审计行不清理。若日后审计表体积成为问题，再单独决策。
package main

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// isUndefinedTable 判定表/列/函数不存在（SQLSTATE 42P01）。
// 不用错误文案子串——"xxx does not exist" 会连列缺失/角色缺失一起命中。
func isUndefinedTable(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "42P01"
}

// retentionBatchSize 单轮归档上限（防一次拖走过多行、事务过大）。
const retentionBatchSize = 500

// retentionInterval 归档任务的执行周期。
const retentionInterval = 6 * time.Hour

// RetentionSweeper 事件归档清理器。
type RetentionSweeper struct {
	pool      *pgxpool.Pool
	tenant    string
	retention time.Duration // resolved 满多久归档；0 = 关闭
	logf      func(string, ...any)
}

// NewRetentionSweeper 构造。retention<=0 表示关闭（Run 立即返回）。
func NewRetentionSweeper(pool *pgxpool.Pool, tenant string, retention time.Duration, logf func(string, ...any)) *RetentionSweeper {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	return &RetentionSweeper{pool: pool, tenant: tenant, retention: retention, logf: logf}
}

// retentionDefault 保留期默认值（D8 决策 C：工单 resolved 后 90 天归档）。
const retentionDefault = 90 * 24 * time.Hour

// ParseRetention 解析保留期配置：接受 Go duration（"2160h"）或 "<N>d"（天数，
// 如 "90d"）；"off"/"0" 表示关闭。**空值 = 默认 90d（决策 C 的默认行为）**。
// 非法值返回错误（调用方 fail-fast）。
func ParseRetention(raw string) (time.Duration, bool, error) {
	return ParseRetentionDefault(raw, retentionDefault)
}

// ParseRetentionDefault 同 ParseRetention，但显式给定"空值时的默认保留期"。
//
// 抽出动机（W9-5）：变更事件库的保留窗（见 change_prune.go）口径与工单
// 归档完全相同——接受 duration / "<N>d" / off——只是默认值不同（7d vs 90d）。
// 两处各写一份解析必然漂移（"7d" 只在一处被支持这类差异），故共用实现。
func ParseRetentionDefault(raw string, def time.Duration) (time.Duration, bool, error) {
	raw = strings.TrimSpace(strings.ToLower(raw))
	switch raw {
	case "", "-":
		return def, true, nil
	case "off", "0", "never":
		return 0, false, nil
	}
	if d, err := time.ParseDuration(raw); err == nil {
		return d, true, nil
	}
	if strings.HasSuffix(raw, "d") {
		if n, err := strconv.Atoi(strings.TrimSuffix(raw, "d")); err == nil && n > 0 {
			return time.Duration(n) * 24 * time.Hour, true, nil
		}
	}
	return 0, false, fmt.Errorf("invalid retention %q (want a duration like 168h/2160h, days like 7d/90d, or off)", raw)
}

// Run 周期归档；首轮延迟一分钟（避开启动风暴），ctx 取消即退出。
func (r *RetentionSweeper) Run(ctx context.Context) {
	if r.retention <= 0 || r.pool == nil {
		r.logf("retention: OFF")
		return
	}
	r.logf("retention: ON (archive resolved incidents older than %s, every %v)", r.retention, retentionInterval)
	select {
	case <-ctx.Done():
		return
	case <-time.After(time.Minute):
	}
	if n, err := r.sweepOnce(ctx); err != nil {
		r.logf("WARNING: retention sweep: %v", err)
	} else if n > 0 {
		r.logf("retention: archived %d incident(s)", n)
	}
	ticker := time.NewTicker(retentionInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			r.logf("retention: stopped")
			return
		case <-ticker.C:
			n, err := r.sweepOnce(ctx)
			if err != nil {
				// 归档表不存在（没跑迁移 000009）是配置错误：大声报错并停机
				// 自身，避免每 6h 刷一条同样的失败。
				if isUndefinedTable(err) {
					r.logf("ERROR: retention disabled — table incident_archive missing, " +
						"run migrations (000009): " + err.Error())
					return
				}
				r.logf("WARNING: retention sweep: %v", err)
				continue
			}
			if n > 0 {
				r.logf("retention: archived %d incident(s)", n)
			}
			if d, err := r.sweepDeadLetters(ctx); err != nil {
				r.logf("WARNING: retention dead letters: %v", err)
			} else if d > 0 {
				r.logf("retention: purged %d dead-letter message(s)", d)
			}
		}
	}
}

// deadLetterRetention 死信行保留期：达 maxIngestAttempts 的行不再被领取、
// 也不是事件（事件归档覆盖不到它），失败详情已在审计里——留 7 天供排查后清除。
const deadLetterRetention = 7 * 24 * time.Hour

// sweepDeadLetters 清理过期死信（第七轮 L11：否则永久占表）。
func (r *RetentionSweeper) sweepDeadLetters(ctx context.Context) (int64, error) {
	tag, err := r.pool.Exec(ctx, `
DELETE FROM ingest_queue
WHERE tenant_id=$1 AND processed_at IS NULL AND attempts >= $2
  AND received_at < $3`, r.tenant, maxIngestAttempts, time.Now().Add(-deadLetterRetention))
	if err != nil {
		return 0, fmt.Errorf("retention: dead letters: %w", err)
	}
	return tag.RowsAffected(), nil
}

// sweepOnce 归档一批。返回归档条数。
func (r *RetentionSweeper) sweepOnce(ctx context.Context) (int, error) {
	cutoff := time.Now().Add(-r.retention)
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("retention: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// 1) 选出候选（先不锁：聚合与 FOR UPDATE 不能同句）。
	var ids []int64
	err = tx.QueryRow(ctx, `
SELECT COALESCE(array_agg(id), '{}'::bigint[]) FROM (
  SELECT id FROM incident
  WHERE tenant_id=$1 AND state='resolved'
    AND resolved_at IS NOT NULL AND resolved_at < $2
  ORDER BY resolved_at LIMIT $3
) t`, r.tenant, cutoff, retentionBatchSize).Scan(&ids)
	if err != nil {
		return 0, fmt.Errorf("retention: select candidates: %w", err)
	}
	if len(ids) == 0 {
		return 0, tx.Commit(ctx)
	}
	// 2) 锁定并**复核**：候选可能已被其他实例归档/人工处理，只处理此刻
	// 仍存在、仍 resolved、且仍超期的行（FOR UPDATE 防并发重复归档）。
	// 注意：FOR UPDATE 不能与聚合同句，故这里用普通 SELECT 在 Go 侧收集。
	rows, err := tx.Query(ctx, `
SELECT id FROM incident
WHERE id = ANY($1) AND tenant_id=$2 AND state='resolved'
  AND resolved_at IS NOT NULL AND resolved_at < $3
FOR UPDATE`, ids, r.tenant, cutoff)
	if err != nil {
		return 0, fmt.Errorf("retention: lock victims: %w", err)
	}
	locked := []int64{}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, fmt.Errorf("retention: lock scan: %w", err)
		}
		locked = append(locked, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("retention: lock rows: %w", err)
	}
	ids = locked
	if len(ids) == 0 {
		return 0, tx.Commit(ctx)
	}

	// 2) 写归档（事件本体 + 簇 + 审计 打包成一行 JSONB）。
	if _, err := tx.Exec(ctx, `
INSERT INTO incident_archive (id, tenant_id, incident_id, payload)
SELECT i.id, i.tenant_id, i.incident_id,
  jsonb_build_object(
    'incident', to_jsonb(i),
    'clusters', COALESCE((SELECT jsonb_agg(ic.cluster_key ORDER BY ic.cluster_key)
                            FROM incident_cluster ic WHERE ic.incident_row_id = i.id), '[]'::jsonb),
    'audit',    COALESCE((SELECT jsonb_agg(to_jsonb(a) ORDER BY a.occurred_at)
                            FROM incident_audit a
                           WHERE a.tenant_id = i.tenant_id AND a.incident_id = i.incident_id), '[]'::jsonb),
    'reason', 'resolved_retention'
  )
FROM incident i WHERE i.id = ANY($1)`, ids); err != nil {
		return 0, fmt.Errorf("retention: archive: %w", err)
	}

	// 3) 删审计（已随单入档）→ 删事件（incident_cluster 由 FK 级联清除）。
	// 审计表按 (tenant_id, incident_id) 索引，与归档行的主键 id 不同域，
	// 故经 incident 联接定位。
	if _, err := tx.Exec(ctx, `
DELETE FROM incident_audit a
USING incident i
WHERE a.tenant_id = i.tenant_id AND a.incident_id = i.incident_id AND i.id = ANY($1)`, ids); err != nil {
		return 0, fmt.Errorf("retention: delete audit: %w", err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM incident WHERE id = ANY($1)`, ids); err != nil {
		return 0, fmt.Errorf("retention: delete incidents: %w", err)
	}
	return len(ids), tx.Commit(ctx)
}
