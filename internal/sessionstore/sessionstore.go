// Package sessionstore 会话状态存储（P1-1）。
//
// 架构守护线：会话状态必须落持久化告警实例（RedisAlert）。
// 缓存实例（LRU 逐出）承载会话会导致上下文随机蒸发——
// v1.2 C12 评审结论，本包在构造时强制校验角色，违反即 panic（启动期失败优于运行期丢数据）。
package sessionstore

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

// DefaultTTL 会话默认存活时间（v1.2 C12：2h）。
const DefaultTTL = 2 * time.Hour

const keyPrefix = "opscopilot:session:"

// ErrNotFound 会话不存在或已过期。
var ErrNotFound = errors.New("session not found")

// Store 会话状态存储。
type Store struct {
	rdb *redis.Client
	ttl time.Duration
}

// New 构造会话存储。role 必须为 "alert"（持久化实例）。
// 传入其他角色（尤其 "cache"）是架构违例，直接 panic。
func New(rdb *redis.Client, role string) *Store {
	if role != "alert" {
		panic("sessionstore: must bind to persistent 'alert' redis instance, got: " + role)
	}
	return &Store{rdb: rdb, ttl: DefaultTTL}
}

// Save 保存会话状态（覆盖写，刷新 TTL）。
func (s *Store) Save(ctx context.Context, sessionID string, state []byte) error {
	return s.rdb.Set(ctx, keyPrefix+sessionID, state, s.ttl).Err()
}

// Load 读取会话状态。
func (s *Store) Load(ctx context.Context, sessionID string) ([]byte, error) {
	val, err := s.rdb.Get(ctx, keyPrefix+sessionID).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, ErrNotFound
	}
	return val, err
}

// Delete 显式销毁会话（登出/重置）。
func (s *Store) Delete(ctx context.Context, sessionID string) error {
	return s.rdb.Del(ctx, keyPrefix+sessionID).Err()
}
