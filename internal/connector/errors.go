package connector

import "fmt"

// 注册相关错误。
var (
	// ErrNilConnector 注册了 nil 连接器。
	ErrNilConnector = fmt.Errorf("connector: nil connector")
	// ErrEmptyID 连接器 ID 为空。
	ErrEmptyID = fmt.Errorf("connector: empty ID")
)

// ErrDuplicateID 重复注册同一 ID 的连接器。
func ErrDuplicateID(id string) error {
	return fmt.Errorf("connector: duplicate ID %q", id)
}
