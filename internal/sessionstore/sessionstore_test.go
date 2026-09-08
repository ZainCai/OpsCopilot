package sessionstore

import "testing"

// TestNewRejectsCacheInstance 会话存储绑定缓存实例必须 panic（P1-1 关键守护）。
// 缓存实例 LRU 逐出会蒸发会话上下文，宁可启动失败也不能运行期丢数据。
func TestNewRejectsCacheInstance(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic when binding sessionstore to cache instance")
		}
	}()
	_ = New(nil, "cache")
}

// TestNewAcceptsAlertInstance 绑定持久化告警实例不得 panic。
// 此处传 nil client 即可：角色校验在构造期完成，不需要真实 Redis。
func TestNewAcceptsAlertInstance(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("unexpected panic for alert instance: %v", r)
		}
	}()
	s := New(nil, "alert")
	if s.ttl != DefaultTTL {
		t.Fatalf("expected default ttl %v, got %v", DefaultTTL, s.ttl)
	}
}
