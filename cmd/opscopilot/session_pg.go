// session_pg.go 会话真相层 PG 实现（原 rca_session.go 拆分，2026-09-14）。
// 行锁发号 + 参与者去重 + 轮次插入同事务（migration 000018）。
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ---------- 真相层 PG 实现（migration 000018；cmd 直写 SQL 先例：
// PGAuditLog / pGEscalationLedger / ChannelStore） ----------

// pgSessionTruth TimescaleDB 真相源。seq 发号 = 会话行 UPDATE ... RETURNING
// next_seq-1 的行锁串行化（双实例同写恰一序、无空洞无重复）；会话行的
// upsert + 发号 + 插轮次同事务——两条并发 POST 最多互相等锁，不会交错。
type pgSessionTruth struct {
	pool   *pgxpool.Pool
	tenant string
}

func newPGSessionTruth(pool *pgxpool.Pool, tenant string) *pgSessionTruth {
	return &pgSessionTruth{pool: pool, tenant: tenant}
}

func (p *pgSessionTruth) persistence() string { return "timescaledb" }

func (p *pgSessionTruth) appendTurn(ctx context.Context, incidentID, actor, role, content string,
	llmMeta map[string]any) (SessionTurn, error) {
	metaJSON := "{}"
	if len(llmMeta) > 0 {
		b, err := json.Marshal(llmMeta)
		if err != nil {
			return SessionTurn{}, fmt.Errorf("session: marshal llm_meta: %w", err)
		}
		metaJSON = string(b)
	}
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return SessionTurn{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// ① 会话行幂等 upsert：首发者落 created_by（ON CONFLICT 不碰它）。
	if _, err := tx.Exec(ctx, `
INSERT INTO rca_session (tenant_id, incident_id, created_by, participants, next_seq)
VALUES ($1, $2, $3, CASE WHEN $3 = '' THEN '[]'::jsonb ELSE jsonb_build_array($3)::jsonb END, 1)
ON CONFLICT (tenant_id, incident_id) DO UPDATE SET updated_at = now()`,
		p.tenant, incidentID, actor); err != nil {
		return SessionTurn{}, fmt.Errorf("session: upsert: %w", err)
	}

	// ② 行锁发号 + 参与者去重追加 + 轮次插入（一个语句链，无 MAX(seq) 竞态）。
	// participants 只收人类参与者（role='user' 的作者）；assistant 轮作者是
	// 出口机器身份（system:llm），进轮次 created_by 即可，不混入参与者列表。
	var t SessionTurn
	var metaRaw []byte
	if err := tx.QueryRow(ctx, `
WITH nx AS (
  UPDATE rca_session SET
    participants = (
      SELECT COALESCE(jsonb_agg(DISTINCT p), '[]'::jsonb)
      FROM jsonb_array_elements_text(
        rca_session.participants || CASE WHEN $4 = 'user' THEN jsonb_build_array($3::text) ELSE '[]'::jsonb END
      ) AS p
      WHERE p <> ''
    ),
    next_seq = next_seq + 1,
    updated_at = now()
  WHERE tenant_id = $1 AND incident_id = $2
  RETURNING next_seq - 1 AS seq
), ins AS (
  INSERT INTO rca_session_turn (tenant_id, incident_id, seq, role, content, llm_meta, created_by)
  SELECT $1, $2, nx.seq, $4, $5, $6::jsonb, $3 FROM nx
  RETURNING seq, created_at, llm_meta
)
SELECT seq, created_at, llm_meta FROM ins`,
		p.tenant, incidentID, actor, role, content, metaJSON).Scan(&t.Seq, &t.CreatedAt, &metaRaw); err != nil {
		return SessionTurn{}, fmt.Errorf("session: append turn: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return SessionTurn{}, fmt.Errorf("session: commit: %w", err)
	}
	t.Role, t.Content, t.CreatedBy = role, content, actor
	if metaJSON != "{}" {
		t.LLMMeta = llmMeta // 原样回填（PG 已存同一份）
	}
	return t, nil
}

func (p *pgSessionTruth) snapshot(ctx context.Context, incidentID string) ([]SessionTurn, SessionMeta, error) {
	meta, err := p.meta(ctx, incidentID)
	if err != nil {
		return nil, SessionMeta{}, err
	}
	rows, err := p.pool.Query(ctx, `
SELECT seq, role, content, created_by, created_at, llm_meta
FROM rca_session_turn WHERE tenant_id=$1 AND incident_id=$2 ORDER BY seq`,
		p.tenant, incidentID)
	if err != nil {
		return nil, SessionMeta{}, err
	}
	defer rows.Close()
	var out []SessionTurn
	for rows.Next() {
		var (
			t   SessionTurn
			raw []byte
		)
		if err := rows.Scan(&t.Seq, &t.Role, &t.Content, &t.CreatedBy, &t.CreatedAt, &raw); err != nil {
			return nil, SessionMeta{}, err
		}
		if len(raw) > 0 && string(raw) != "{}" {
			var m map[string]any
			if err := json.Unmarshal(raw, &m); err == nil {
				t.LLMMeta = m
			} // 坏 jsonb 不拖垮读（脱敏摘要非契约数据）
		}
		out = append(out, t)
	}
	return out, meta, rows.Err()
}

func (p *pgSessionTruth) meta(ctx context.Context, incidentID string) (SessionMeta, error) {
	var (
		createdBy string
		raw       []byte
	)
	err := p.pool.QueryRow(ctx, `
SELECT created_by, participants FROM rca_session WHERE tenant_id=$1 AND incident_id=$2`,
		p.tenant, incidentID).Scan(&createdBy, &raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return SessionMeta{}, nil // 会话未开：空元信息不是错误
	}
	if err != nil {
		return SessionMeta{}, err
	}
	m := SessionMeta{CreatedBy: createdBy}
	if len(raw) > 0 {
		var ps []string
		if err := json.Unmarshal(raw, &ps); err == nil {
			m.Participants = ps
		}
	}
	return m, nil
}
