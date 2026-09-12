// session_envelope_test.go S1 装配骨架的 envelope/键约定测试（miniredis 真协议）：
// round-trip、schema_ver 拒读（懒恢复触发面）、键约定形态、TTL 语义继承。
package sessionstore

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestSessionKeyConvention(t *testing.T) {
	// 拍板③：incident 级共享——键只到 (tenant, incident)，不带 user 段。
	got := SessionKey("default", "INC-20260912-120000.000")
	want := "default:INC-20260912-120000.000"
	if got != want {
		t.Fatalf("SessionKey = %q, want %q", got, want)
	}
}

func TestEnvelopeRoundTrip(t *testing.T) {
	s, mr := newTestStore(t)
	ctx := context.Background()
	key := SessionKey("t1", "INC-1")
	env := Envelope{Turns: []Turn{
		{Seq: 1, Role: RoleUser, Content: "为什么是这次发布?", CreatedBy: "alice"},
		{Seq: 2, Role: RoleAssistant, Content: "证据链指向 dep-42……"},
	}}
	if err := s.SaveEnvelope(ctx, key, env); err != nil {
		t.Fatalf("SaveEnvelope: %v", err)
	}
	got, err := s.LoadEnvelope(ctx, key)
	if err != nil {
		t.Fatalf("LoadEnvelope: %v", err)
	}
	if got.SchemaVer != SchemaVer || got.SessionID != key {
		t.Fatalf("envelope self-describe broken: %+v", got)
	}
	if len(got.Turns) != 2 || got.Turns[0].Seq != 1 || got.Turns[1].Role != RoleAssistant {
		t.Fatalf("turns drifted: %+v", got.Turns)
	}
	// 覆盖写即刷新 TTL（继承 Save 语义——活跃会话续命，拍板①不改 2h 默认）。
	if ttl := mr.TTL(keyPrefix + key); ttl <= 0 || ttl > DefaultTTL {
		t.Fatalf("ttl = %v, want (0, %v]", ttl, DefaultTTL)
	}
}

// TestEnvelopeSchemaVerTriggersRebuild 不认识的 schema_ver 必须按"热态缺失"
// 处理（ErrNotFound 可判 + ErrSchemaVer 可细分）——宁触发一次 PG 懒恢复重建，
// 绝不拿异代字节当合法会话。
func TestEnvelopeSchemaVerTriggersRebuild(t *testing.T) {
	s, _ := newTestStore(t)
	ctx := context.Background()
	key := "t2:INC-2"
	bad, err := json.Marshal(Envelope{SchemaVer: SchemaVer + 99, SessionID: key})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Save(ctx, key, bad); err != nil { // 底层直存异代信封
		t.Fatalf("Save raw: %v", err)
	}
	if _, err := s.LoadEnvelope(ctx, key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown schema_ver must be ErrNotFound-judgable, got %v", err)
	}
	if _, err := s.LoadEnvelope(ctx, key); !errors.Is(err, ErrSchemaVer) {
		t.Fatalf("unknown schema_ver must carry ErrSchemaVer for triage, got %v", err)
	}
}

// TestEnvelopeNonEnvelopeDecode 键位上躺着非 JSON 字节：解码失败要可诊断、
// 错误文本不回显载荷（脱敏习惯对齐包纪律）。异代 schema_ver 的拒绝路径另测。
func TestEnvelopeNonEnvelopeDecode(t *testing.T) {
	s, _ := newTestStore(t)
	ctx := context.Background()
	key := "t3:INC-3"
	if err := s.Save(ctx, key, []byte("P1-1 era raw view-state, not JSON")); err != nil {
		t.Fatalf("Save legacy blob: %v", err)
	}
	_, err := s.LoadEnvelope(ctx, key)
	if err == nil {
		t.Fatal("non-JSON payload must not decode as envelope")
	}
	if !strings.Contains(err.Error(), "decode envelope") {
		t.Fatalf("error should name the decode failure, got %v", err)
	}
}
