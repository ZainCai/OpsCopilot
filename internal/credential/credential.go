// Package credential 凭证管理基础版（M1 W2）。
//
// 只读强制校验：本产品遵循“算子前置、只读优先”原则（ADR-002），
// 所有数据源连接器只允许持有只读凭证。Store.Put 在写入前强制校验
// Credential.ReadOnly == true，任何写权限凭证一律拒绝入库。
//
// 类型来源：Credential 直接复用共享契约 pkg/readonly（类型别名），
// 使凭证库与连接器两侧是同一个类型——连接器可直接校验而不必跨模块 import
// （internal 各模块间禁止互相 import，见 pkg/readonly 包注释）。
//
// 这是 W2 的基础版：仅做内存存储 + 只读闸门。后续可扩展为
// 加密落盘（密钥不进库，见 .gitignore 约束）与凭证作用域 introspection。
package credential

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"opscopilot/pkg/readonly"
)

// Credential 一条数据源访问凭证（共享只读契约）。
type Credential = readonly.Credential

// 错误定义。前两者复用共享契约的 sentinel，便于调用方用 errors.Is 统一判断。
var (
	// ErrNotReadOnly 拒绝写权限凭证。
	ErrNotReadOnly = readonly.ErrNotReadOnly
	// ErrExpired 凭证已过期。
	ErrExpired = readonly.ErrExpired
	// ErrNotFound 凭证不存在（本库独有）。
	ErrNotFound = errors.New("credential: not found")
)

// Store 内存凭证库（基础版）。
type Store struct {
	mu    sync.RWMutex
	creds map[string]Credential
}

// NewStore 构造空库。
func NewStore() *Store {
	return &Store{creds: make(map[string]Credential)}
}

// Put 写入凭证。只读闸门：非只读 / 空 ID / 已过期一律拒绝。
func (s *Store) Put(c Credential) error {
	if err := readonly.Validate(c); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.creds[c.ID] = c
	return nil
}

// Get 按 ID 取凭证。不存在返回 ErrNotFound，已过期返回包装的 ErrExpired
// （均可用 errors.Is 判断）。
func (s *Store) Get(id string) (Credential, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	c, ok := s.creds[id]
	if !ok {
		return Credential{}, ErrNotFound
	}
	if c.Expired(time.Now()) {
		return Credential{}, fmt.Errorf("credential: %q: %w", id, ErrExpired)
	}
	return c, nil
}

// Delete 删除凭证。
func (s *Store) Delete(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.creds, id)
}

// List 返回全部凭证 ID（不含 Secret）。
func (s *Store) List() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ids := make([]string, 0, len(s.creds))
	for id := range s.creds {
		ids = append(ids, id)
	}
	return ids
}

// NewReadOnlyBearer 构造一条只读 Bearer 凭证（Prometheus 等常用）。
func NewReadOnlyBearer(id, token string) Credential {
	return readonly.NewBearer(id, token)
}
