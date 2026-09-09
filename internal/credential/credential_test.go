package credential

import (
	"errors"
	"testing"
	"time"
)

func TestStore_PutReadOnlyEnforced(t *testing.T) {
	s := NewStore()

	// 写权限凭证必须被拒绝
	write := Credential{ID: "w1", ReadOnly: false, Secret: "x"}
	if err := s.Put(write); !errors.Is(err, ErrNotReadOnly) {
		t.Fatalf("write credential should be rejected with ErrNotReadOnly, got %v", err)
	}

	// 只读凭证可入库
	ro := NewReadOnlyBearer("p1", "tok-123")
	if err := s.Put(ro); err != nil {
		t.Fatalf("readonly put: %v", err)
	}
	if len(s.List()) != 1 {
		t.Fatalf("List len = %d, want 1", len(s.List()))
	}

	got, err := s.Get("p1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !got.ReadOnly || got.Secret != "tok-123" {
		t.Fatalf("Get mismatch: %+v", got)
	}

	// 空 ID 拒绝
	if err := s.Put(Credential{ID: "", ReadOnly: true}); err == nil {
		t.Fatal("empty ID should be rejected")
	}

	// Delete
	s.Delete("p1")
	if _, err := s.Get("p1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("after delete, Get should be ErrNotFound, got %v", err)
	}
}

// TestStore_Expiry 过期语义：构造期即拦截 + 入库后过期仍拒绝（防御性分支）。
func TestStore_Expiry(t *testing.T) {
	s := NewStore()

	// 1) 已过期凭证不得入库（更早拦截，避免库中堆积无效凭证）
	expired := NewReadOnlyBearer("old", "t")
	expired.ExpiresAt = time.Now().Add(-time.Hour)
	if err := s.Put(expired); !errors.Is(err, ErrExpired) {
		t.Fatalf("expired credential should be rejected on Put, got %v", err)
	}

	// 2) 入库后过期：Get 应拒绝（覆盖 ExpiresAt 在库中到期的场景）
	soon := NewReadOnlyBearer("soon", "t")
	soon.ExpiresAt = time.Now().Add(2 * time.Millisecond)
	if err := s.Put(soon); err != nil {
		t.Fatalf("put soon-expiring: %v", err)
	}
	time.Sleep(10 * time.Millisecond)
	if _, err := s.Get("soon"); !errors.Is(err, ErrExpired) {
		t.Fatalf("Get after expiry should be ErrExpired, got %v", err)
	}

	// 3) 未过期仍可取回
	valid := NewReadOnlyBearer("new", "t")
	valid.ExpiresAt = time.Now().Add(time.Hour)
	if err := s.Put(valid); err != nil {
		t.Fatalf("put valid: %v", err)
	}
	if _, err := s.Get("new"); err != nil {
		t.Fatalf("valid credential Get: %v", err)
	}
}

// TestCredential_SharesReadOnlyContract 凭证类型必须与连接器侧共享契约是同一类型，
// 否则跨模块传递时需要二次转换，也意味着闸门可被绕过。
func TestCredential_SharesReadOnlyContract(t *testing.T) {
	c := NewReadOnlyBearer("p1", "tok")
	if !c.ReadOnly {
		t.Fatal("NewReadOnlyBearer must produce read-only credential")
	}
	if c.Type != "prometheus-bearer" {
		t.Fatalf("Type = %q", c.Type)
	}
}
