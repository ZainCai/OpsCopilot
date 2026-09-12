// session_envelope.go 复盘会话的 envelope/键约定层（二期池 #7 S1，
// 设计文档《sessionstore消费方与接线》§3）。
//
// 背景：本包原实现是通用 KV 会话袋（设计文档 §1 判定"名大于实"——零运行时
// 消费方）。接线 RCA 复盘会话（首发消费方）按文档结论"只加一层 envelope/
// 键约定而非重写"：底层 Save/Load 语义与 2h TTL（拍板①：不改默认、靠 PG
// 懒恢复兜真相）逐字节不动，本文件只赋予其会话语义：
//   - 键约定 SessionKey = "<tenant>:<incidentID>"（拍板③：incident 级共享，
//     非 per-user——同事件的所有参与者读写同一会话袋）；
//   - Envelope 为带 schema_ver 的 JSON：热态缓冲保存"会话真相（PG）的最近
//     全量轮次投影"，Redis 蒸发后由消费方（cmd 层编排器）从 PG 懒恢复重建
//     ——本包不认识 PG（真相源在消费方，包边界不扩）。
//
// 纪律：schema_ver 不匹配的旧/异代 envelope 一律按 ErrNotFound 对待——
// 宁可触发一次懒恢复重建，也不拿不认识的字节当合法会话用。
package sessionstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// SchemaVer 当前 envelope 结构版本（字段增删即递增；旧版本读入即弃、走重建）。
const SchemaVer = 1

// ErrSchemaVer envelope 版本不认识（等价"热态缺失"，消费方应回源重建）。
var ErrSchemaVer = errors.New("session envelope schema version mismatch")

// Role 轮次角色封闭集合（拍板②：会话轮次 ≠ 审计证据；与 PG CHECK 对齐）。
const (
	RoleUser      = "user"
	RoleAssistant = "assistant"
)

// Turn 单轮次（热态投影；真相以 PG rca_session_turn 为准）。
type Turn struct {
	Seq       int64     `json:"seq"`
	Role      string    `json:"role"`
	Content   string    `json:"content"`
	CreatedBy string    `json:"created_by,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

// Envelope 会话热态信封（Save 覆盖写即刷新 TTL——活跃会话持续续命）。
type Envelope struct {
	SchemaVer int    `json:"schema_ver"`
	SessionID string `json:"session_id"` // = SessionKey(tenant, incidentID)
	Turns     []Turn `json:"turns"`
}

// SessionKey 复盘会话键约定：incident 级共享（拍板③）。
// tenant 与 incidentID 都不该含 ':'——incident id 字符集已在 REST 侧收窄
// （validIncidentID 允许冒号，为外部 origin:ref 形态所留），冒号仅做可读
// 分隔不参与反解析：会话消费方永远按整串键存取，不拆键。
func SessionKey(tenant, incidentID string) string {
	return tenant + ":" + incidentID
}

// SaveEnvelope 序列化并覆盖写热态（刷新 TTL）。SessionID 为空时用 sessionID
// 参数回填，保证信封自描述。
func (s *Store) SaveEnvelope(ctx context.Context, sessionID string, env Envelope) error {
	env.SchemaVer = SchemaVer
	if env.SessionID == "" {
		env.SessionID = sessionID
	}
	b, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("sessionstore: marshal envelope: %w", err)
	}
	return s.Save(ctx, sessionID, b)
}

// LoadEnvelope 读取并解码热态。缺失/过期 → ErrNotFound（原语义不变）；
// schema_ver 不认识 → 包装 ErrSchemaVer 且按 ErrNotFound 可判（errors.Is
// 双链）——调用方以 ErrNotFound 统一处理"回源重建"，排障时可细分。
func (s *Store) LoadEnvelope(ctx context.Context, sessionID string) (Envelope, error) {
	b, err := s.Load(ctx, sessionID)
	if err != nil {
		return Envelope{}, err
	}
	var env Envelope
	if err := json.Unmarshal(b, &env); err != nil {
		return Envelope{}, fmt.Errorf("sessionstore: decode envelope: %w (not an envelope?)", err)
	}
	if env.SchemaVer != SchemaVer {
		return Envelope{}, fmt.Errorf("sessionstore: %w: got %d want %d: %w",
			ErrSchemaVer, env.SchemaVer, SchemaVer, ErrNotFound)
	}
	return env, nil
}
