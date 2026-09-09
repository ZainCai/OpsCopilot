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
// 这是 W2 的基础版：仅做内存存储 + 只读闸门 + 容量观察点。
//
// 已知边界（全局审查 C9，勿当已完成特性）：
//   - **Secret 明文在内存**，且本库不落盘、不打日志——密钥不会因此进入
//     进程外；加密落盘前，请勿给本库加任何"导出/持久化"路径。
//   - **过期条目不会自动回收**：Put 按 ID 覆盖、Delete 才移除；已过期条目
//     在显式删除前仍占内存，长跑进程靠重复注册新 ID 会缓慢增长。
//     已提供容量观察点（Len/Stats）与清扫原语（SweepExpired）——周期
//     清扫的调度（时间轮/ticker）留到真正接线 credential 到 main 时做，
//     现在不引入无人调用的后台 goroutine。
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

// Len 返回库内全部条目数（含已过期未清理的——容量观察点，C9）。
// 长跑进程若发现 Len 只增不减，说明存在持续注册新 ID 却从不删除的路径，
// 排查后应周期调用 SweepExpired 回收。
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.creds)
}

// Stats 容量观察点（C9）：区分"库内总数"与"已过期但仍占内存"，
// 供周期任务/监控判断是否需要清扫。
type Stats struct {
	// Total 库内全部条目（含过期未清）。
	Total int
	// Expired 已过期但尚未删除的条目数（应触发 Sweep 的信号）。
	Expired int
}

// Stats 统计当前容量状态（now 为判定过期的参考时刻）。
func (s *Store) Stats(now time.Time) Stats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	st := Stats{Total: len(s.creds)}
	for _, c := range s.creds {
		if c.Expired(now) {
			st.Expired++
		}
	}
	return st
}

// SweepExpired 删除全部已过期条目并返回删除条数。
// 清扫原语已就绪；周期调度（ticker 等）留待 credential 接线 main 时挂载，
// 避免现在引入无人调用的后台 goroutine（C9）。
func (s *Store) SweepExpired(now time.Time) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for id, c := range s.creds {
		if c.Expired(now) {
			delete(s.creds, id)
			n++
		}
	}
	return n
}

// NewReadOnlyBearer 构造一条只读 Bearer 凭证（Prometheus 等常用）。
func NewReadOnlyBearer(id, token string) Credential {
	return readonly.NewBearer(id, token)
}
