package config

import "testing"

// redisPair 构造一对指定地址的双实例配置（角色固定，同 Config.Load 产出的形态）。
func redisPair(alertAddr, cacheAddr string) *Config {
	c := Defaults()
	c.Redis.Alert = RedisConfig{Addr: alertAddr, Role: RedisAlert}
	c.Redis.Cache = RedisConfig{Addr: cacheAddr, Role: RedisCache}
	return c
}

// TestValidateHappyPath 合法双实例配置应通过。
func TestValidateHappyPath(t *testing.T) {
	if err := redisPair("127.0.0.1:6380", "127.0.0.1:6381").Validate(); err != nil {
		t.Fatalf("expected valid config, got %v", err)
	}
}

// TestValidateSameAddr 同地址合并为单实例必须被拒绝（v1.2 C2 物理隔离约束）。
func TestValidateSameAddr(t *testing.T) {
	if err := redisPair("127.0.0.1:6380", "127.0.0.1:6380").Validate(); err == nil {
		t.Fatal("expected error when alert and cache share the same address")
	}
}

// TestValidateRoleSwap 角色互换必须被拒绝。
func TestValidateRoleSwap(t *testing.T) {
	c := Defaults()
	c.Redis.Alert = RedisConfig{Addr: "127.0.0.1:6380", Role: RedisCache}
	c.Redis.Cache = RedisConfig{Addr: "127.0.0.1:6381", Role: RedisAlert}
	if err := c.Validate(); err == nil {
		t.Fatal("expected error when instance roles are swapped")
	}
}

// TestValidateCacheAddrRequired G1 回归：cache 实例地址为空必须拒绝
// （与"两个都必须配置，缺一拒绝启动"的约定一致）。
func TestValidateCacheAddrRequired(t *testing.T) {
	if err := redisPair("127.0.0.1:6380", "").Validate(); err == nil {
		t.Fatal("expected error when cache redis addr is empty")
	}
}

// TestValidateAddrAliasSameInstance 同一实例的不同写法（localhost vs
// 127.0.0.1、大小写、空白）必须被判为"同一个"，否则物理隔离约束可被绕过。
func TestValidateAddrAliasSameInstance(t *testing.T) {
	cases := [][2]string{
		{"localhost:6380", "127.0.0.1:6380"},
		{"127.0.0.1:6380", " 127.0.0.1:6380 "},
		{"LOCALHOST:6380", "localhost:6380"},
	}
	for _, p := range cases {
		if err := redisPair(p[0], p[1]).Validate(); err == nil {
			t.Fatalf("aliases %q / %q must be treated as the same instance", p[0], p[1])
		}
	}
}

// TestValidateAddrFormat 地址必须是 host:port；缺端口的配置启动期就该被拒。
func TestValidateAddrFormat(t *testing.T) {
	for _, bad := range []string{"127.0.0.1", "6380", "127.0.0.1:", ":6380"} {
		if err := redisPair(bad, "127.0.0.1:6381").Validate(); err == nil {
			t.Fatalf("addr %q must be rejected (want host:port)", bad)
		}
	}
}

// TestDefaultsValid Defaults() 基线自身必须通过校验（测试/嵌入式构造的锚点）。
func TestDefaultsValid(t *testing.T) {
	if err := Defaults().Validate(); err != nil {
		t.Fatalf("Defaults() must be valid: %v", err)
	}
}
