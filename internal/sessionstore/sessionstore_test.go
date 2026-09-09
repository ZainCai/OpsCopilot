package sessionstore

import (
	"testing"

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
