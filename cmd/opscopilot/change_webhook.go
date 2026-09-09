// 变更关联 webhook（M1 W3）：极简 HTTP 端点，接收 JSON 格式的变更事件
// 并写入 ChangeStore。手动 curl 即可提交；后续对接 Git/Jenkins 时只需
// 把 Source 换成对应标识（或在推送侧带 Source），本端点无需改动。
//
// 为什么放在 cmd/（package main）：与 topology_sink.go 同理——webhook
// 需要同时引用 topology（ChangeStore/ChangeEvent）与未来的图持有者
// （节点存在性校验），编排装配层是模块间唯一合法汇合点（v1.3 §5.2）。
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"opscopilot/internal/topology"
)

// changeWebhookDefaultBodyLimit 请求体上限：变更事件是几行 JSON，
// 1MiB 足够，防止误用/恶意大包。
const changeWebhookDefaultBodyLimit = 1 << 20

// ChangeWebhook 变更事件接收端点。
//
// DefaultSource：请求体未带 source 时填充的默认值（如 "manual"），
// 让手动 curl 与 Git/Jenkins 推送共用同一端点、来源可区分。
type ChangeWebhook struct {
	store         *topology.ChangeStore
	DefaultSource string
	// MaxBodyBytes 请求体上限；零值取默认 1MiB。
	MaxBodyBytes int64
}

// NewChangeWebhook 构造。store 须非 nil。
func NewChangeWebhook(store *topology.ChangeStore, defaultSource string) (*ChangeWebhook, error) {
	if store == nil {
		return nil, errors.New("change webhook: nil change store")
	}
	return &ChangeWebhook{store: store, DefaultSource: defaultSource}, nil
}

// ServeHTTP 实现 http.Handler。
//
// 状态码语义（显式约定，调用方按此排障）：
//   - 200：入库成功，返回存储后的事件（含补全的默认值）；
//   - 400：JSON 解析失败 / 未知字段 / 字段校验失败（缺 id、node_key、
//     非法 type/confidence）——请求本身有问题；
//   - 405：非 POST 方法（响应带 Allow: POST）；
//   - 409：同 ID 变更重复提交（幂等冲突，重发同一事件属正常场景）；
//   - 413：请求体超过上限；
//   - 422：事件关联的节点不在拓扑图中（严格节点校验开启时）——
//     请求格式正确但语义上指向不存在的资源，区别于 400。
func (h *ChangeWebhook) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed: change webhook accepts POST only", http.StatusMethodNotAllowed)
		return
	}

	limit := h.MaxBodyBytes
	if limit <= 0 {
		limit = changeWebhookDefaultBodyLimit
	}
	r.Body = http.MaxBytesReader(w, r.Body, limit)

	var ev topology.ChangeEvent
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields() // 拼写错误的字段（如 node_ky）直接 400，不静默丢弃
	if err := dec.Decode(&ev); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			http.Error(w, fmt.Sprintf("request body exceeds limit (%d bytes)", limit), http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	// 拒绝尾部多余数据（P3）：decoder 只取第一个值，`{...}{...}` 这类
	// 粘包若不检查会静默收一半——请求体必须是且仅是一个 JSON 对象。
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		http.Error(w, "unexpected trailing data after JSON object", http.StatusBadRequest)
		return
	}

	// 请求体没带来源时用配置默认值，保证 Source 可追溯到提交通道。
	if ev.Source == "" {
		ev.Source = h.DefaultSource
	}

	stored, err := h.store.Record(ev)
	switch {
	case err == nil:
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status": "ok",
			"event":  stored,
		})
	case errors.Is(err, topology.ErrDuplicateChange):
		http.Error(w, err.Error(), http.StatusConflict)
	case errors.Is(err, topology.ErrNodeNotFound):
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
	case errors.Is(err, topology.ErrEmptyChangeID),
		errors.Is(err, topology.ErrEmptyNodeKey),
		errors.Is(err, topology.ErrUnknownChangeType),
		errors.Is(err, topology.ErrUnknownConfidence):
		// 请求字段缺失或取值非法 → 400。
		http.Error(w, err.Error(), http.StatusBadRequest)
	default:
		// 未知错误：入库侧内部问题，按 500 上抛而非吞掉。
		http.Error(w, "internal error: "+err.Error(), http.StatusInternalServerError)
	}
}
