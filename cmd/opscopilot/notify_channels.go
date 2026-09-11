// notify_channels.go W9-2 通知渠道配置（DB 真相源）+ 装配期装载。
//
// 职责边界：
//   - 本文件只管"渠道配置的存取与合法性"（DB + 校验 + 装载进 Registry）；
//   - 投递逻辑在 internal/notify（WebhookChannel）；
//   - 路由规则（severity→渠道、值班升级）属 W9-3。
//
// 租户纪律与其它表一致：全部访问带 tenant_id（M1 单租户，但键空间不省）。
package main

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"opscopilot/internal/notify"
)

// ChannelRecord 渠道配置行（API 响应与 DB 行同构，snake_case json）。
type ChannelRecord struct {
	TenantID  string    `json:"tenant_id"`
	Name      string    `json:"name"`
	Kind      string    `json:"kind"`
	URL       string    `json:"url"`
	Enabled   bool      `json:"enabled"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// channelNameRe 渠道名约束：键空间与前端路径参数共用，收窄字符集。
var channelNameRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

// ChannelConsoleName 默认兜底渠道名（注册表恒有，落日志）。
// 保留名：不允许被用户配置覆盖——enforce 下零渠道 = 通知静默丢失，
// 有 console 兜底至少留痕（运维仍能看见"本该发出去的告警"）。
const ChannelConsoleName = "console"

// ValidationError 渠道配置校验错误（REST 400 与 500 的分界）。
// 用类型而非文案匹配：第八轮审核已把"用文案子串做控制流"列为反模式
// （保留名那条就漏了 500——字符串匹配必然漏）。
type ValidationError struct{ Msg string }

func (e *ValidationError) Error() string { return e.Msg }

func invalidChannelf(format string, args ...any) error {
	return &ValidationError{Msg: fmt.Sprintf(format, args...)}
}

// ValidateChannel 渠道配置校验（API 与装配装载共用一套规则）。
func ValidateChannel(name, kind, url string) error {
	if name == ChannelConsoleName {
		return invalidChannelf("name %q is reserved (built-in console sink)", ChannelConsoleName)
	}
	if !channelNameRe.MatchString(name) {
		return invalidChannelf("name must match %s", channelNameRe.String())
	}
	if !notify.ValidKind(kind) {
		return invalidChannelf("kind must be one of generic|feishu|wecom")
	}
	u := strings.TrimSpace(url)
	if !strings.HasPrefix(u, "http://") && !strings.HasPrefix(u, "https://") {
		return invalidChannelf("url must start with http:// or https://")
	}
	if len(u) > 2048 {
		return invalidChannelf("url too long (max 2048)")
	}
	return nil
}

// ChannelStore 渠道配置存取。
type ChannelStore struct {
	pool   *pgxpool.Pool
	tenant string
}

// NewChannelStore 构造（pool 为事件域共享池，R9）。
func NewChannelStore(pool *pgxpool.Pool, tenant string) *ChannelStore {
	return &ChannelStore{pool: pool, tenant: tenant}
}

// List 全部渠道（含禁用；按 name 稳定序）。DB 故障返回 error——
// 渠道列表读失败不能让调用方以为"没有渠道"（同 D5 口径）。
func (s *ChannelStore) List(ctx context.Context) ([]ChannelRecord, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	rows, err := s.pool.Query(ctx, `
SELECT tenant_id, name, kind, url, enabled, created_at, updated_at
FROM notify_channel WHERE tenant_id = $1 ORDER BY name`, s.tenant)
	if err != nil {
		return nil, fmt.Errorf("channel store: list: %w", err)
	}
	defer rows.Close()
	var out []ChannelRecord
	for rows.Next() {
		var r ChannelRecord
		if err := rows.Scan(&r.TenantID, &r.Name, &r.Kind, &r.URL, &r.Enabled, &r.CreatedAt, &r.UpdatedAt); err != nil {
			return nil, fmt.Errorf("channel store: scan: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// ListEnabled 仅启用渠道（装配期装载用）。
func (s *ChannelStore) ListEnabled(ctx context.Context) ([]ChannelRecord, error) {
	all, err := s.List(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]ChannelRecord, 0, len(all))
	for _, r := range all {
		if r.Enabled {
			out = append(out, r)
		}
	}
	return out, nil
}

// Upsert 新建/更新（同 name 覆盖 kind/url/enabled）。
func (s *ChannelStore) Upsert(ctx context.Context, name, kind, url string, enabled bool) error {
	if err := ValidateChannel(name, kind, url); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := s.pool.Exec(ctx, `
INSERT INTO notify_channel (tenant_id, name, kind, url, enabled, updated_at)
VALUES ($1, $2, $3, $4, $5, now())
ON CONFLICT (tenant_id, name) DO UPDATE SET
  kind = EXCLUDED.kind, url = EXCLUDED.url, enabled = EXCLUDED.enabled,
  updated_at = now()`,
		s.tenant, name, kind, strings.TrimSpace(url), enabled)
	if err != nil {
		return fmt.Errorf("channel store: upsert: %w", err)
	}
	return nil
}

// SetEnabled 软开关（渠道灰度：禁用不清配置）。
func (s *ChannelStore) SetEnabled(ctx context.Context, name string, enabled bool) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	tag, err := s.pool.Exec(ctx, `
UPDATE notify_channel SET enabled = $3, updated_at = now()
WHERE tenant_id = $1 AND name = $2`, s.tenant, name, enabled)
	if err != nil {
		return fmt.Errorf("channel store: set enabled: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrChannelNotFound
	}
	return nil
}

// Delete 删除渠道。
func (s *ChannelStore) Delete(ctx context.Context, name string) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	tag, err := s.pool.Exec(ctx,
		`DELETE FROM notify_channel WHERE tenant_id = $1 AND name = $2`, s.tenant, name)
	if err != nil {
		return fmt.Errorf("channel store: delete: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrChannelNotFound
	}
	return nil
}

// ErrChannelNotFound 渠道不存在（REST 404 用；哨兵而非文案匹配）。
var ErrChannelNotFound = errors.New("notify: channel not found")

// loadChannelsIntoRegistry 装配期把启用渠道注册进 Registry。
//
// 传染性纪律：DB 读失败由调用方决定降级方式（装配期 = 记日志继续启动；
// 渠道 CRUD 后重载 = 返回错误给调用方）——渠道配置问题不该阻断服务启动
// （告警链路本身仍要跑），但 enforce 模式下零渠道意味着"所有放行项都发
// 不出去"（Gate 会记 dispatch 失败计数，运维可见）。
// 单条配置非法只跳过该条并记日志（一条坏配置不该让其余渠道失效）。
func loadChannelsIntoRegistry(ctx context.Context, store *ChannelStore, reg *notify.Registry, logf func(string, ...any)) (int, error) {
	rows, err := store.ListEnabled(ctx)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, r := range rows {
		ch, err := notify.NewWebhookChannel(notify.WebhookOptions{Name: r.Name, Kind: r.Kind, URL: r.URL})
		if err != nil {
			logf("WARNING: notify channel %q skipped (invalid config): %v", r.Name, err)
			continue
		}
		reg.Register(ch)
		n++
	}
	return n, nil
}

// channelByName 查单渠道（REST 辅助；不存在返回 ErrChannelNotFound）。
func (s *ChannelStore) channelByName(ctx context.Context, name string) (ChannelRecord, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var r ChannelRecord
	err := s.pool.QueryRow(ctx, `
SELECT tenant_id, name, kind, url, enabled, created_at, updated_at
FROM notify_channel WHERE tenant_id = $1 AND name = $2`, s.tenant, name).
		Scan(&r.TenantID, &r.Name, &r.Kind, &r.URL, &r.Enabled, &r.CreatedAt, &r.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return ChannelRecord{}, ErrChannelNotFound
	}
	if err != nil {
		return ChannelRecord{}, fmt.Errorf("channel store: get %s: %w", name, err)
	}
	return r, nil
}
