// S1 装配骨架测试（二期池 #7 / 设计文档《sessionstore消费方与接线》S1 验收）：
//   - OPS_SESSION=on：cmd 汇合点经 sessionstore.New(alertRDB, RedisAlert) 接线，
//     miniredis 真实协议 round-trip 过（envelope 键约定 = tenant:incident）；
//   - OPS_SESSION=off（默认）：不构造会话 Redis 客户端、SessionHot 为 nil——
//     零行为变更；
//   - role 误绑启动期 panic 的锁定测试在 internal/sessionstore（P1-1 原守卫，
//     构造器逻辑未动，不在这里重复）。
package main

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"

	"opscopilot/internal/config"
	"opscopilot/internal/sessionstore"
)

// TestAssemblySessionOffZeroBehavior 默认（off）：会话热态袋不构造、不建
// 告警 Redis 客户端——off 路径与接线前逐字节一致。
func TestAssemblySessionOffZeroBehavior(t *testing.T) {
	cfg := testAssemblyConfig("tok") // Defaults 基线：Session.Enabled=false
	asm, err := NewAssembly(newQuietLogger(), cfg)
	if err != nil {
		t.Fatalf("assembly: %v", err)
	}
	defer asm.Close()
	if asm.SessionHot != nil {
		t.Fatal("SessionHot must be nil when OPS_SESSION=off")
	}
	if asm.sessionRDB != nil {
		t.Fatal("no session redis client must be constructed when OPS_SESSION=off")
	}
}

// TestAssemblySessionHotRoundTrip on + miniredis：装配产物可直接完成
// envelope 读写往返（S1 验收"装配测试 round-trip 过"），并透到键约定与 TTL。
func TestAssemblySessionHotRoundTrip(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := testAssemblyConfig("tok")
	cfg.Session.Enabled = true
	cfg.Redis.Alert.Addr = mr.Addr() // 告警实例指向 miniredis（测试不碰真机 6380）

	asm, err := NewAssembly(newQuietLogger(), cfg)
	if err != nil {
		t.Fatalf("assembly: %v", err)
	}
	defer asm.Close()
	if asm.SessionHot == nil {
		t.Fatal("SessionHot must be wired when OPS_SESSION=on")
	}

	ctx := context.Background()
	key := sessionstore.SessionKey(cfg.Tenant, "INC-s1")
	env := sessionstore.Envelope{Turns: []sessionstore.Turn{
		{Seq: 1, Role: sessionstore.RoleUser, Content: "根因是哪次发布?", CreatedBy: "alice"},
	}}
	if err := asm.SessionHot.SaveEnvelope(ctx, key, env); err != nil {
		t.Fatalf("SaveEnvelope: %v", err)
	}
	got, err := asm.SessionHot.LoadEnvelope(ctx, key)
	if err != nil {
		t.Fatalf("LoadEnvelope: %v", err)
	}
	if len(got.Turns) != 1 || got.Turns[0].Content != env.Turns[0].Content {
		t.Fatalf("round-trip drift: %+v", got.Turns)
	}
	// 键确实落在 miniredis（告警实例），前缀是包内约定。
	if !mr.Exists("opscopilot:session:" + key) {
		t.Fatalf("key not on alert redis as expected: %q", key)
	}
}

// TestConfigValidateSessionRequiresRCA 聚合校验（拍板④）：OPS_SESSION=on 而
// OPS_RCA=off 是配置矛盾（会话 prompt 必携带 RCA findings），Validate 拒绝。
func TestConfigValidateSessionRequiresRCA(t *testing.T) {
	c := config.Defaults()
	c.Session.Enabled = true
	c.RCA.Enabled = false
	if err := c.Validate(); err == nil {
		t.Fatal("OPS_SESSION=on must require OPS_RCA=on")
	}
	c.RCA.Enabled = true
	if err := c.Validate(); err != nil {
		t.Fatalf("session=on & rca=on must validate clean: %v", err)
	}
}
