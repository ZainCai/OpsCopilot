// notify_gate.go W9-1 转正（ADR-011）：通知闸门的装配与计数真相源。
//
// 接线在装配层而非 main：NewAssembly 是全部组件的汇合点，闸门依赖
// logger 与共享 pg 池都在这里现成——且装配级测试可直接驱动（main 里的
// 接线没有测试覆盖，违反"组件写了+单测过了 ≠ 运行态生效"纪律）。
package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"opscopilot/internal/notify"
)

// pgPoolGateStats Gate 计数真相源（R6-6）：复用事件域共享池
// （R9 纪律：不为计数单开一条池）。表见 migrations/000011。
type pgPoolGateStats struct {
	pool *pgxpool.Pool
}

// SaveGateStats 累计值覆盖写（幂等：同租户重复 Save 结果不变）。
func (p *pgPoolGateStats) SaveGateStats(tenant string, st notify.Stats) error {
	ctx, cancel := context.WithTimeout(context.Background(), pgSinkTimeout)
	defer cancel()
	_, err := p.pool.Exec(ctx, `
INSERT INTO notify_gate_stats (tenant_id, suppressed, dispatched, updated_at)
VALUES ($1, $2, $3, now())
ON CONFLICT (tenant_id) DO UPDATE SET
  suppressed = EXCLUDED.suppressed,
  dispatched = EXCLUDED.dispatched,
  updated_at = now()`,
		tenant, st.Suppressed, st.Dispatched)
	if err != nil {
		return fmt.Errorf("gate stats: upsert: %w", err)
	}
	return nil
}

// LoadGateStats 读历史累计。缺行返回零值不报错（首次运行是常态）。
func (p *pgPoolGateStats) LoadGateStats(tenant string) (notify.Stats, error) {
	ctx, cancel := context.WithTimeout(context.Background(), pgSinkTimeout)
	defer cancel()
	var st notify.Stats
	err := p.pool.QueryRow(ctx, `
SELECT suppressed, dispatched FROM notify_gate_stats WHERE tenant_id = $1`, tenant).
		Scan(&st.Suppressed, &st.Dispatched)
	if errors.Is(err, pgx.ErrNoRows) {
		return notify.Stats{}, nil
	}
	if err != nil {
		return notify.Stats{}, fmt.Errorf("gate stats: load: %w", err)
	}
	return st, nil
}

// attachNoiseGate 按模式挂载通知闸门（enforce 才挂；影子模式不碰闸门，
// 装配代码直接可读出配置意图与运行时行为的对应）。pool 为事件域共享池
// （nil = 无 DB，计数只累计内存、重启清零——降级语义与簇落库一致）。
//
// reg 由装配层统一持有（渠道 CRUD 后要重载进同一个 Registry，见
// Assembly.reloadNotifyChannels），故此处只做"闸门挂载"这一件事。
func attachNoiseGate(ne *NoiseEngine, reg *notify.Registry, pool *pgxpool.Pool, logf func(string, ...any)) {
	if ne == nil || ne.Mode() != ModeEnforce {
		if ne != nil {
			logf("noise mode: shadow (verdicts recorded, nothing intercepted)")
		}
		return
	}
	gate := notify.NewGate(reg)
	var gs GateStatsSink
	if pool != nil {
		gs = &pgPoolGateStats{pool: pool}
	}
	ne.SetGate(gate, gs)
	logf("noise mode: enforce (gate active; set OPS_NOISE_MODE=shadow to roll back)")
}
