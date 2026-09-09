// Package readonly 只读凭证契约（ADR-002「算子前置、只读优先」的共享实现）。
//
// 为什么放在 pkg/ 而不是 internal/credential：
// internal/connector 与 internal/credential 是两个独立模块，按 v1.3 §5.2 调用纪律，
// 跨模块禁止直接 import 对方内部包（跨模块调用须走 gRPC/transport）。
// 而"只读强制"必须能在进程内被真正校验——若绕经 gRPC 则无法在连接器构造期拦截。
// 故把凭证契约下沉到共享层，供凭证库与连接器双方引用，既守住模块边界，
// 又让只读纪律从注释固化为编译期可校验的代码。
package readonly

import (
	"errors"
	"fmt"
	"time"
)

// Credential 一条数据源访问凭证的共享契约。
// 与业务无关，仅描述"是谁、什么类型、是否只读、何时过期"。
type Credential struct {
	// ID 凭证唯一标识。
	ID string
	// Type 凭证类别，如 "prometheus-bearer" / "aws-iam-role" / "aliyun-ak"。
	Type string
	// Secret 敏感值（token / ak / 角色 ARN）。调用方负责不在日志中打印。
	Secret string
	// ReadOnly 必须为 true——这是只读强制闸门，写权限凭证一律拒绝。
	ReadOnly bool
	// Scope 可选作用域描述（如 "metrics:read"），仅作标注，不参与强制逻辑（W2）。
	Scope string
	// ExpiresAt 可选过期时间；零值表示不过期。
	ExpiresAt time.Time
}

// 只读强制相关错误（sentinel，便于调用方 errors.Is 判断）。
var (
	// ErrNotReadOnly 凭证非只读（写权限凭证被拒绝）。
	ErrNotReadOnly = errors.New("readonly: write-scoped credential rejected (read-only enforced)")
	// ErrExpired 凭证已过期。
	ErrExpired = errors.New("readonly: credential expired")
	// ErrEmptyID 凭证 ID 为空。
	ErrEmptyID = errors.New("readonly: empty credential ID")
)

// Expired 判断凭证在 now 时刻是否已过期（零值 ExpiresAt 视为不过期）。
func (c Credential) Expired(now time.Time) bool {
	return !c.ExpiresAt.IsZero() && now.After(c.ExpiresAt)
}

// Validate 校验凭证是否满足只读纪律：ID 非空、ReadOnly 为真、未过期。
// 连接器应在构造期调用，把"只读"从约定变成代码强制。
func Validate(c Credential) error {
	if c.ID == "" {
		return ErrEmptyID
	}
	if !c.ReadOnly {
		return ErrNotReadOnly
	}
	if c.Expired(time.Now()) {
		return fmt.Errorf("%w: %q", ErrExpired, c.ID)
	}
	return nil
}

// NewBearer 构造一条只读 Bearer 凭证（Prometheus 等常用）。
func NewBearer(id, token string) Credential {
	return Credential{
		ID:       id,
		Type:     "prometheus-bearer",
		Secret:   token,
		ReadOnly: true,
		Scope:    "metrics:read,alerts:read",
	}
}
