// 变更关联 webhook（M1 W3）：极简 HTTP 端点，接收 JSON 格式的变更事件
// 并写入 ChangeStore。手动 curl 即可提交；后续对接 Git/Jenkins 时只需
// 把 Source 换成对应标识（或在推送侧带 Source），本端点无需改动。
//
// 为什么放在 cmd/（package main）：与 topology_sink.go 同理——webhook
// 需要同时引用 topology（ChangeStore/ChangeEvent）与未来的图持有者
// （节点存在性校验），编排装配层是模块间唯一合法汇合点（v1.3 §5.2）。
package main

import (
	"crypto/subtle"
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

// AuthHeader 变更 webhook 的共享密钥头。配置了 Token 的端点必须携带
// 匹配的头，否则 401（全局审查 S1：变更事件是 RCA 证据链的输入，
// 无鉴权可写端点等于允许伪造"上线/回滚"误导故障根因判断）。
const AuthHeader = "X-OpsCopilot-Token"

// ChangeWebhook 变更事件接收端点。
//
// DefaultSource：请求体未带 source 时填充的默认值（如 "manual"），
// 让手动 curl 与 Git/Jenkins 推送共用同一端点、来源可区分。
type ChangeWebhook struct {
	// store 变更库后端（内存或 PG 持久化，装配期选定，见 assembly.go）。
	store topology.ChangeBackend
	// DefaultSource 请求体未带 source 时填充的默认来源。
	DefaultSource string
	// Token 共享密钥；非空时请求必须携带匹配的 AuthHeader 头（401 拒绝）。
	// 为空表示不鉴权——仅限回环/内网部署（main.go 会在未配置时打警告）。
	Token string
	// MaxBodyBytes 请求体上限；零值取默认 1MiB。
	MaxBodyBytes int64
}

// NewChangeWebhook 构造。store 须非 nil（内存或 PG 后端均可，见 ChangeBackend）。
func NewChangeWebhook(store topology.ChangeBackend, defaultSource string) (*ChangeWebhook, error) {
	if store == nil {
		return nil, errors.New("change webhook: nil change store")
	}
	return &ChangeWebhook{store: store, DefaultSource: defaultSource}, nil
}

// ServeHTTP 实现 http.Handler。
//
// 状态码语义（显式约定，调用方按此排障）：
//   - 200：入库成功（含幂等重复，见 G5 注释），返回存储后的事件；
//   - 400：JSON 解析失败 / 未知字段 / 字段校验失败——请求本身有问题；
//   - 401：配置了 Token 但请求头缺失或不匹配；
//   - 405：非 POST 方法（响应带 Allow: POST）；
//   - 413：请求体超过上限；
//   - 422：事件关联的节点不在拓扑图中（严格节点校验开启时）——
//     请求格式正确但语义上指向不存在的资源，区别于 400。
func (h *ChangeWebhook) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed: change webhook accepts POST only", http.StatusMethodNotAllowed)
		return
	}

	// 写路径准入（S1）：constant-time 比较防时序侧信道。
	if h.Token != "" && subtle.ConstantTimeCompare([]byte(r.Header.Get(AuthHeader)), []byte(h.Token)) != 1 {
		http.Error(w, "unauthorized: missing or invalid "+AuthHeader+" header", http.StatusUnauthorized)
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
		h.writeEvent(w, map[string]any{"status": "ok", "event": stored})
	case errors.Is(err, topology.ErrDuplicateChange):
		// 幂等成功（全局审查 G5）：重复 ID 返回 200 + 原记录。
		// Git/Jenkins 等自动重试的发送方把 409 当失败会无限重发——
		// 幂等键存在的意义就是"重复提交 = 已经成功"。
		orig, ok := h.store.Get(ev.ID)
		if !ok {
			// 理论不可达（重复错误意味着库里必有原记录）；防御兜底。
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		h.writeEvent(w, map[string]any{"status": "ok", "duplicate": true, "event": orig})
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

// writeEvent 输出 JSON 成功响应（200）。
func (h *ChangeWebhook) writeEvent(w http.ResponseWriter, body map[string]any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(body)
}

// authorized 共享密钥校验（写路径统一鉴权件，R6）：
// Token 为空 = 未启用鉴权（仅限本机联调）；非空则必须常量时间匹配，
// 防止计时侧信道。供变更 webhook、Alertmanager 接收、人工建单端点共用。
func authorized(got, want string) bool {
	if want == "" {
		return true
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1
}
