package config

import "testing"

// TestValidateHappyPath 合法双实例配置应通过。
func TestValidateHappyPath(t *testing.T) {
	c := &Config{
		RedisAlert: RedisConfig{Addr: "127.0.0.1:6380", Role: RedisAlert},
		RedisCache: RedisConfig{Addr: "127.0.0.1:6381", Role: RedisCache},
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("expected valid config, got %v", err)
	}
}

// TestValidateSameAddr 同地址合并为单实例必须被拒绝（v1.2 C2 物理隔离约束）。
func TestValidateSameAddr(t *testing.T) {
	c := &Config{
		RedisAlert: RedisConfig{Addr: "127.0.0.1:6380", Role: RedisAlert},
		RedisCache: RedisConfig{Addr: "127.0.0.1:6380", Role: RedisCache},
	}
	if err := c.Validate(); err == nil {
		t.Fatal("expected error when alert and cache share the same address")
	}
}

// TestValidateRoleSwap 角色互换必须被拒绝。
func TestValidateRoleSwap(t *testing.T) {
	c := &Config{
		RedisAlert: RedisConfig{Addr: "127.0.0.1:6380", Role: RedisCache},
		RedisCache: RedisConfig{Addr: "127.0.0.1:6381", Role: RedisAlert},
	}
	if err := c.Validate(); err == nil {
		t.Fatal("expected error when instance roles are swapped")
	}
}

// TestValidateCacheAddrRequired G1 回归：cache 实例地址为空必须拒绝
// （与"两个都必须配置，缺一拒绝启动"的约定一致）。
func TestValidateCacheAddrRequired(t *testing.T) {
	c := &Config{
		RedisAlert: RedisConfig{Addr: "127.0.0.1:6380", Role: RedisAlert},
		RedisCache: RedisConfig{Addr: "", Role: RedisCache},
	}
	if err := c.Validate(); err == nil {
		t.Fatal("expected error when cache redis addr is empty")
	}
}
