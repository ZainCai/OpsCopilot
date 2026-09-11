// W9 双链路一期：链路 A 入口——Alertmanager Webhook 接收器。
//
// 设计要点（方案《双链路事件来源》）：
//   - 接收即入队（ingest_queue 表）后返回 202，**不做任何建单计算**——
//     外部源再突发也不占人工建单的连接与事务（互不阻塞保证①）；
//   - 入队幂等靠 (origin, source_ref) 唯一索引（待处理态），重推不堆积；
//   - 鉴权复用 ChangeWebhook 的共享密钥件（R6 写路径暴露应对）。
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"opscopilot/internal/config"
	"opscopilot/internal/incident"
)

// AlertmanagerWebhook AM webhook 接收器。
type AlertmanagerWebhook struct {
	Token  string // 共享密钥；非空时请求须携带匹配头（401 拒绝）
	Tenant string
	Owner  *QueueOwner // 队列写入口（nil = 只校验不入队，测试用）
	// BodyLimit 通知体字节上限（一批告警可能上百条，默认 4MiB）。
	// #10：与拉取侧响应上限共用 config 的**单一定义**
	// （config.DefaultAlertBodyLimit / OPS_INGEST_ALERT_BODY_LIMIT）；
	// 零值回退该默认（测试直接构造结构体的路径不改语义）。
	BodyLimit int64
}

// bodyLimit 生效上限（零值回退 config 默认，同源单一定义）。
func (h *AlertmanagerWebhook) bodyLimit() int64 {
	if h.BodyLimit > 0 {
		return h.BodyLimit
	}
	return config.DefaultAlertBodyLimit
}

type queueWriter = interface {
	Enqueue(origin incident.Origin, sourceRef, payload string) (queued bool, err error)
}

// QueueOwner 队列写入口占位（真实实现在 ingest_queue.go）。
type QueueOwner struct{ w queueWriter }

// SetWriter 注入队列写入实现（装配期）。
func (q *QueueOwner) SetWriter(w queueWriter) { q.w = w }

// amPayload Alertmanager 通知体（字段严格：未知字段拒绝，与既有
// webhook 同一纪律）。
type amPayload struct {
	Status string    `json:"status"`
	Alerts []amAlert `json:"alerts"`
}

type amAlert struct {
	Labels      map[string]string `json:"labels"`
	Annotations map[string]string `json:"annotations"`
	StartsAt    string            `json:"startsAt"`
	EndsAt      string            `json:"endsAt"`
	Fingerprint string            `json:"fingerprint"`
}

// Register 挂载两条接收路由：
//   - POST /api/v1/ingest/alertmanager（origin=alertmanager）
//   - POST /api/v1/ingest/webhook（origin=webhook，通用契约——同一
//     payload 形态，便于接入非 AM 的推送方；二期扩展性的最小验证）。
func (h *AlertmanagerWebhook) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/v1/ingest/alertmanager", func(w http.ResponseWriter, r *http.Request) {
		h.serveOrigin(w, r, incident.OriginAlertmanager)
	})
	mux.HandleFunc("POST /api/v1/ingest/webhook", func(w http.ResponseWriter, r *http.Request) {
		h.serveOrigin(w, r, incident.OriginWebhook)
	})
}

// serveOrigin 按来源处理：origin 经参数传递（**不用结构体字段**——
// 并发请求下共享字段会互相覆盖，属于实现级竞态）。
func (h *AlertmanagerWebhook) serveOrigin(w http.ResponseWriter, r *http.Request, origin incident.Origin) {
	h.handle(w, r, origin)
}

// ServeHTTP 接收通知 → 逐条入队 → 202（默认来源 alertmanager）。
func (h *AlertmanagerWebhook) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.handle(w, r, incident.OriginAlertmanager)
}

// handle 实际处理逻辑（来源由调用方显式传入）。
func (h *AlertmanagerWebhook) handle(w http.ResponseWriter, r *http.Request, origin incident.Origin) {
	if !authorized(r.Header.Get(AuthHeader), h.Token) {
		writeErr(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, h.bodyLimit())
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		writeErr(w, http.StatusRequestEntityTooLarge, "body too large")
		return
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var p amPayload
	if err := dec.Decode(&p); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid payload: "+err.Error())
		return
	}
	if len(p.Alerts) == 0 {
		writeErr(w, http.StatusBadRequest, "no alerts in payload")
		return
	}
	queued, skipped := 0, 0
	for _, a := range p.Alerts {
		ref := strings.TrimSpace(a.Fingerprint)
		if ref == "" {
			ref = fallbackRef(a) // 无指纹时退化：labels 归一化（弱幂等，记日志）
		}
		payload, err := json.Marshal(a)
		if err != nil {
			continue
		}
		ok, err := h.enqueue(origin, ref, string(payload))
		if err != nil {
			writeErr(w, http.StatusInternalServerError, "enqueue failed: "+err.Error())
			return
		}
		if ok {
			queued++
		} else {
			skipped++ // 幂等命中（同指纹待处理）
		}
	}
	writeJSON(w, http.StatusAccepted, map[string]any{
		"received": len(p.Alerts), "queued": queued, "skipped": skipped,
	})
}

// enqueue 入队（Owner 未注入时视为只校验）。
func (h *AlertmanagerWebhook) enqueue(origin incident.Origin, ref, payload string) (bool, error) {
	if h.Owner == nil || h.Owner.w == nil {
		return false, errors.New("ingest queue not wired")
	}
	return h.Owner.w.Enqueue(origin, ref, payload)
}

// fallbackRef 无 fingerprint 时的退化幂等键：alertname+instance+severity。
// 弱于官方指纹（同一告警重启后可能变），仅兜底。
func fallbackRef(a amAlert) string {
	parts := []string{
		a.Labels["alertname"], a.Labels["instance"], a.Labels["severity"],
	}
	return "nofp:" + strings.Join(parts, "|")
}

// amTitle 事件标题：注解摘要优先，其次 alertname。
func amTitle(a amAlert) string {
	if s := strings.TrimSpace(a.Annotations["summary"]); s != "" {
		return s
	}
	if s := strings.TrimSpace(a.Labels["alertname"]); s != "" {
		return s
	}
	return "External alert"
}

// amSeverity 严重级映射（AM 无标准字段，取 severity 标签）。
func amSeverity(a amAlert) string {
	switch strings.ToLower(strings.TrimSpace(a.Labels["severity"])) {
	case "critical", "crit", "p0", "fatal":
		return "critical"
	case "warning", "warn", "p2", "major":
		return "warning"
	case "info", "minor", "p3":
		return "info"
	}
	return "warning" // 未知按 warning（宁可高估，不低估）
}

// amResolved 告警是否已恢复（endsAt 已过）。
func amResolved(a amAlert) bool {
	if a.EndsAt == "" || a.EndsAt == "0001-01-01T00:00:00Z" {
		return false
	}
	t, err := time.Parse(time.RFC3339, a.EndsAt)
	if err != nil {
		return false
	}
	return !t.After(time.Now())
}
