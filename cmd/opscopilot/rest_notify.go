// rest_notify.go W9-2 通知渠道配置 API（读 GET 无鉴权 / 写 Token 门禁）。
//
// 端点：
//
//	GET    /api/v1/notify/channels            列表（含禁用）
//	POST   /api/v1/notify/channels            新建/更新（name 为键，幂等 upsert）
//	         body: {name, kind, url, min_severity?, enabled?}
//	         min_severity: critical|warning|info（省略 = info 全收，W9-3 路由）
//	POST   /api/v1/notify/channels/{name}/enabled  软开关 {"enabled":bool}
//	DELETE /api/v1/notify/channels/{name}     删除
//
// 写路径纪律与事件域一致：Token 鉴权 + requireJSON（CSRF 免预检阻挡）+
// body 上限 + DisallowUnknownFields；写成功后**热重载注册表**（改配置
// 不用重启）。
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
)

// notifyBodyLimit 单请求体上限（渠道配置就几行 JSON）。
const notifyBodyLimit = 64 << 10

// handleNotifyChannels GET（列表）/ POST（upsert）。
func (g *RESTGateway) handleNotifyChannels(w http.ResponseWriter, r *http.Request) {
	store := g.channels
	if store == nil {
		writeErr(w, http.StatusServiceUnavailable, "notify channel store not wired (needs OPS_DB_DSN)")
		return
	}
	switch r.Method {
	case http.MethodGet:
		rows, err := store.List(r.Context())
		if err != nil {
			g.logf("WARNING: notify channels list: %v", err)
			writeErr(w, http.StatusInternalServerError, "internal error")
			return
		}
		if rows == nil {
			rows = []ChannelRecord{}
		}
		writeJSON(w, http.StatusOK, map[string]any{"channels": rows})
	case http.MethodPost:
		if !authorized(r.Header.Get(AuthHeader), g.token) {
			writeErr(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		if !requireJSON(w, r) {
			return
		}
		var in struct {
			Name        string `json:"name"`
			Kind        string `json:"kind"`
			URL         string `json:"url"`
			MinSeverity string `json:"min_severity"` // 省略 = info（全收）
			Enabled     *bool  `json:"enabled"`      // 省略 = 启用
		}
		if !decodeStrict(w, r, notifyBodyLimit, &in) {
			return
		}
		name := strings.TrimSpace(in.Name)
		kind := strings.ToLower(strings.TrimSpace(in.Kind))
		enabled := true
		if in.Enabled != nil {
			enabled = *in.Enabled
		}
		if err := store.Upsert(r.Context(), name, kind, in.URL, in.MinSeverity, enabled); err != nil {
			if errors.Is(err, ErrChannelNotFound) {
				writeErr(w, http.StatusNotFound, err.Error())
				return
			}
			// 校验类错误（name/kind/url）回 400 带原因；DB 错误 500 脱敏。
			if isValidationError(err) {
				writeErr(w, http.StatusBadRequest, err.Error())
				return
			}
			g.logf("WARNING: notify channel upsert %q: %v", name, err)
			writeErr(w, http.StatusInternalServerError, "internal error")
			return
		}
		g.reloadAndReport(w, http.StatusOK, "channel saved")
	default:
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// handleNotifyChannelItem POST /{name}/enabled 与 DELETE /{name}。
func (g *RESTGateway) handleNotifyChannelItem(w http.ResponseWriter, r *http.Request) {
	store := g.channels
	if store == nil {
		writeErr(w, http.StatusServiceUnavailable, "notify channel store not wired (needs OPS_DB_DSN)")
		return
	}
	name := strings.TrimSpace(r.PathValue("name"))
	if name == "" {
		writeErr(w, http.StatusBadRequest, "name is required")
		return
	}
	if !authorized(r.Header.Get(AuthHeader), g.token) {
		writeErr(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	switch {
	case r.Method == http.MethodDelete:
		if err := store.Delete(r.Context(), name); err != nil {
			if errors.Is(err, ErrChannelNotFound) {
				writeErr(w, http.StatusNotFound, "channel not found")
				return
			}
			g.logf("WARNING: notify channel delete %q: %v", name, err)
			writeErr(w, http.StatusInternalServerError, "internal error")
			return
		}
		g.reloadAndReport(w, http.StatusOK, "channel deleted")
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/enabled"):
		if !requireJSON(w, r) {
			return
		}
		var in struct {
			Enabled *bool `json:"enabled"`
		}
		if !decodeStrict(w, r, notifyBodyLimit, &in) {
			return
		}
		if in.Enabled == nil {
			writeErr(w, http.StatusBadRequest, "enabled is required")
			return
		}
		if err := store.SetEnabled(r.Context(), name, *in.Enabled); err != nil {
			if errors.Is(err, ErrChannelNotFound) {
				writeErr(w, http.StatusNotFound, "channel not found")
				return
			}
			g.logf("WARNING: notify channel toggle %q: %v", name, err)
			writeErr(w, http.StatusInternalServerError, "internal error")
			return
		}
		g.reloadAndReport(w, http.StatusOK, "channel updated")
	default:
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// reloadAndReport 写成功后热重载注册表；重载失败**不回滚写**（配置已落库，
// 下次重载/重启会补上），只在响应里标注并记日志——静默失败才是真问题。
func (g *RESTGateway) reloadAndReport(w http.ResponseWriter, code int, msg string) {
	active := 0
	warn := ""
	if g.reloadChannels != nil {
		n, err := g.reloadChannels()
		active = n
		if err != nil {
			warn = "channel reload failed; retry by restart or next write"
			g.logf("WARNING: notify channels reload: %v", err)
		}
	}
	resp := map[string]any{"status": msg, "active_channels": active}
	if warn != "" {
		resp["warning"] = warn
	}
	writeJSON(w, code, resp)
}

// decodeStrict 严格解码（未知字段拒绝 + body 上限），失败写响应。
func decodeStrict(w http.ResponseWriter, r *http.Request, limit int64, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, limit)
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		writeErr(w, http.StatusRequestEntityTooLarge, "body too large")
		return false
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid body: "+err.Error())
		return false
	}
	return true
}

// isValidationError 区分"配置非法"（400，带原因）与"存储/系统故障"（500，脱敏）。
// 用类型断言而非文案匹配（第八轮审核反模式：文案必定漏）。
func isValidationError(err error) bool {
	var ve *ValidationError
	return errors.As(err, &ve)
}
