package sessionstore

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// newTestClient 构造一个不连接任何服务器的 client 占位（go-redis 懒连接，
// NewClient 只建结构体，Save/Load 才会真正 dial；构造期测试用不到网络）。
func newTestClient() *redis.Client {
	return redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
}

// TestNewRejectsCacheInstance 会话存储绑定缓存实例必须 panic（P1-1 关键守护）。
// 缓存实例 LRU 逐出会蒸发会话上下文，宁可启动失败也不能运行期丢数据。
func TestNewRejectsCacheInstance(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic when binding sessionstore to cache instance")
		}
	}()
	_ = New(newTestClient(), "cache")
}

// TestNewAcceptsAlertInstance 绑定持久化告警实例不得 panic。
func TestNewAcceptsAlertInstance(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("unexpected panic for alert instance: %v", r)
		}
	}()
	s := New(newTestClient(), "alert")
	if s.ttl != DefaultTTL {
		t.Fatalf("expected default ttl %v, got %v", DefaultTTL, s.ttl)
	}
}

// TestNewRejectsNilClient C10 回归：nil client 必须在构造期 panic，
// 而不是等到 Save 才崩溃——配置错误尽早暴露（与 role panic 同点）。
func TestNewRejectsNilClient(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic when constructing with nil redis client")
		}
	}()
	_ = New(nil, "alert")
}

// ---- W5-2.4 集成测试（O10 收尾）：miniredis 真实 Redis 协议 ----
// 锁定"会话不蒸发"语义：TTL 刷新、过期、显式销毁、键前缀隔离。

// newTestStore 起 miniredis 并返回 (store, 实例)。
func newTestStore(t *testing.T) (*Store, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return New(rdb, "alert"), mr
}

func TestSaveLoadRoundTrip(t *testing.T) {
	s, _ := newTestStore(t)
	ctx := context.Background()
	payload := []byte(`{"user":"ops","view":"clusters"}`)

	if err := s.Save(ctx, "sess-1", payload); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, err := s.Load(ctx, "sess-1")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("round-trip mismatch: %q vs %q", got, payload)
	}
}

// TestSaveRefreshesTTL 会话活跃即续命（核心"不蒸发"语义）：
// 存活 1.5h 后再次 Save（刷新 TTL），再过 1.5h 仍可读——
// 若 Save 不刷新 TTL，活跃会话会在首存 2h 后被误杀。
func TestSaveRefreshesTTL(t *testing.T) {
	s, mr := newTestStore(t)
	ctx := context.Background()

	if err := s.Save(ctx, "sess-live", []byte("v1")); err != nil {
		t.Fatalf("Save#1: %v", err)
	}
	mr.FastForward(90 * time.Minute) // 1.5h：还活着
	if err := s.Save(ctx, "sess-live", []byte("v2")); err != nil {
		t.Fatalf("Save#2 (refresh): %v", err)
	}
	mr.FastForward(90 * time.Minute) // 距首存 3h、距末存 1.5h
	got, err := s.Load(ctx, "sess-live")
	if err != nil {
		t.Fatalf("active session must survive past first-save TTL: %v", err)
	}
	if string(got) != "v2" {
		t.Fatalf("value = %q, want v2 (覆盖写)", got)
	}
}

func TestLoadExpiredReturnsNotFound(t *testing.T) {
	s, mr := newTestStore(t)
	ctx := context.Background()

	if err := s.Save(ctx, "sess-dead", []byte("x")); err != nil {
		t.Fatalf("Save: %v", err)
	}
	mr.FastForward(DefaultTTL + time.Minute)
	if _, err := s.Load(ctx, "sess-dead"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expired session: err = %v, want ErrNotFound", err)
	}
}

func TestDeleteExplicit(t *testing.T) {
	s, _ := newTestStore(t)
	ctx := context.Background()

	if err := s.Save(ctx, "sess-bye", []byte("x")); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := s.Delete(ctx, "sess-bye"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := s.Load(ctx, "sess-bye"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted session: err = %v, want ErrNotFound", err)
	}
	// Delete 不存在的键也是幂等成功（Redis DEL 语义）。
	if err := s.Delete(ctx, "sess-bye"); err != nil {
		t.Fatalf("Delete idempotent: %v", err)
	}
}

// TestKeyPrefixIsolation 会话键必须带前缀——alert 实例上还跑着告警流
// 与簇镜像（opscopilot:{tenant}:alert_clusters），裸键会撞车。
func TestKeyPrefixIsolation(t *testing.T) {
	s, mr := newTestStore(t)
	ctx := context.Background()

	if err := s.Save(ctx, "sess-1", []byte("x")); err != nil {
		t.Fatalf("Save: %v", err)
	}
	for _, k := range mr.Keys() {
		if !strings.HasPrefix(k, keyPrefix) {
			t.Fatalf("session store wrote non-prefixed key %q", k)
		}
	}
}
