// normalize.go Prometheus 原始响应解码结构与归一化（connector.Alert/ResourceNode/MetricSample）。
package prometheus

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"opscopilot/internal/connector"
)

// ---- 归一化 ----

func normalizeAlert(a rawAlert, source string) connector.Alert {
	sev := a.Labels["severity"]
	status := ""
	if a.Status.State != "" {
		status = a.Status.State
	} else if a.StatusText != "" {
		status = a.StatusText
	}
	// 发射时刻两形态取一：Alertmanager 用 startsAt，**Prometheus 的
	// /api/v1/alerts 只给 activeAt**（告警开始 firing 的时刻）。此前只读
	// startsAt → 对接 Prometheus 时该字段恒空，下游（cmd/noise.go toEvent）
	// 只好兜底成"采集时刻"——等于把告警年龄抹成 0，W9-4 的
	// fired→verdict 延迟就没法测了（也无法区分"源侧积压"与"系统慢"）。
	// 保留 startsAt 优先：Alertmanager 形态下它才是权威值。
	startsAt := a.StartsAt
	if startsAt == "" {
		startsAt = a.ActiveAt
	}
	return connector.Alert{
		Fingerprint:  a.Fingerprint,
		GeneratorURL: a.GeneratorURL,
		Labels:       a.Labels,
		Annotations:  a.Annotations,
		StartsAt:     parseTime(startsAt),
		EndsAt:       parseTime(a.EndsAt),
		Status:       status,
		Severity:     sev,
		Source:       source,
	}
}

func normalizeTarget(t rawTarget, source string, now time.Time) connector.ResourceNode {
	key := "prometheus://" + t.ScrapePool + "/"
	if inst, ok := t.Labels["instance"]; ok {
		key += inst
	} else {
		key += t.ScrapeURL
	}
	typ := "target"
	if job, ok := t.Labels["job"]; ok && job != "" {
		typ = job
	}
	// 复制标签，避免改写解码产生的原始 map；
	// 把 target 健康状态（up/down）保留进标签，防止归一化丢信息。
	labels := make(map[string]string, len(t.Labels)+1)
	for k, v := range t.Labels {
		labels[k] = v
	}
	if t.Health != "" {
		labels["health"] = t.Health
	}
	return connector.ResourceNode{
		Key:        key,
		Type:       typ,
		Labels:     labels,
		Source:     source,
		ObservedAt: now,
	}
}

// normalizeSample 把 instant vector 的一条样本归一化为 MetricSample。
func normalizeSample(s rawSample, queryName, source string) (connector.MetricSample, error) {
	if len(s.Value) < 2 {
		return connector.MetricSample{}, fmt.Errorf("malformed sample: value length %d", len(s.Value))
	}
	var tsSec float64
	if err := json.Unmarshal(s.Value[0], &tsSec); err != nil {
		return connector.MetricSample{}, fmt.Errorf("decode sample timestamp: %w", err)
	}
	var rawVal string
	if err := json.Unmarshal(s.Value[1], &rawVal); err != nil {
		return connector.MetricSample{}, fmt.Errorf("decode sample value: %w", err)
	}
	val, err := strconv.ParseFloat(rawVal, 64)
	if err != nil {
		// 注意：Prometheus 的 NaN / +Inf 是合法值，ParseFloat 可正常解析为
		// 对应浮点值，不会走到这里；此处仅捕获真正的格式异常。
		return connector.MetricSample{}, fmt.Errorf("parse sample value %q: %w", rawVal, err)
	}
	name := queryName
	if name == "" {
		name = s.Metric["__name__"]
	}
	labels := make(map[string]string, len(s.Metric))
	for k, v := range s.Metric {
		labels[k] = v
	}
	// tsSec 是浮点秒、含亚秒小数。若直接 int64 截断（time.Unix(sec, 0)）
	// 会丢弃小数部分，导致同一时刻的多条样本精度失真；
	// 故换算为纳秒时间戳，保留数据源返回的完整精度。
	return connector.MetricSample{
		Name:      name,
		Labels:    labels,
		Value:     val,
		Timestamp: time.Unix(0, int64(tsSec*float64(time.Second))),
		Source:    source,
	}, nil
}

// ---- Prometheus API 响应结构 ----

type alertsResponse struct {
	Status string `json:"status"`
	Data   struct {
		Alerts []rawAlert `json:"alerts"`
	} `json:"data"`
}

type rawAlert struct {
	Fingerprint  string            `json:"fingerprint"`
	GeneratorURL string            `json:"generatorURL"`
	Labels       map[string]string `json:"labels"`
	Annotations  map[string]string `json:"annotations"`
	StartsAt     string            `json:"startsAt"`
	// ActiveAt Prometheus /api/v1/alerts 的告警开始时刻（该形态没有
	// startsAt/endsAt）——normalizeAlert 在 startsAt 缺失时用它兜底。
	ActiveAt string `json:"activeAt"`
	EndsAt   string `json:"endsAt"`
	// StatusRaw 先以 RawMessage 兜住两种形态（对象或字符串），避免类型冲突。
	StatusRaw json.RawMessage `json:"status"`
	// 归一化后填充：
	Status struct {
		State string `json:"state"`
	}
	StatusText string
}

// UnmarshalJSON 兼容 Prometheus 两种告警 status 表达：
//   - 新版：{"status":{"state":"firing"}}
//   - 旧版：{"status":"firing"}
//
// 标准解码遇到同 key 不同类型会失败，故先以 RawMessage 兜住再分支解析。
func (a *rawAlert) UnmarshalJSON(data []byte) error {
	type alias struct {
		Fingerprint  string            `json:"fingerprint"`
		GeneratorURL string            `json:"generatorURL"`
		Labels       map[string]string `json:"labels"`
		Annotations  map[string]string `json:"annotations"`
		StartsAt     string            `json:"startsAt"`
		ActiveAt     string            `json:"activeAt"`
		EndsAt       string            `json:"endsAt"`
		Status       json.RawMessage   `json:"status"`
	}
	var aux alias
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	a.Fingerprint = aux.Fingerprint
	a.GeneratorURL = aux.GeneratorURL
	a.Labels = aux.Labels
	a.Annotations = aux.Annotations
	a.StartsAt = aux.StartsAt
	a.ActiveAt = aux.ActiveAt
	a.EndsAt = aux.EndsAt
	if len(aux.Status) > 0 && aux.Status[0] == '{' {
		_ = json.Unmarshal(aux.Status, &a.Status)
	} else {
		var s string
		if err := json.Unmarshal(aux.Status, &s); err == nil {
			a.StatusText = s
		}
	}
	return nil
}

type targetsResponse struct {
	Status string `json:"status"`
	Data   struct {
		ActiveTargets []rawTarget `json:"activeTargets"`
	} `json:"data"`
}

type rawTarget struct {
	DiscoveredLabels map[string]string `json:"discoveredLabels"`
	Labels           map[string]string `json:"labels"`
	ScrapePool       string            `json:"scrapePool"`
	ScrapeURL        string            `json:"scrapeUrl"`
	Health           string            `json:"health"`
}

// queryResponse /api/v1/query 的 instant query 响应。
type queryResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string      `json:"resultType"`
		Result     []rawSample `json:"result"`
	} `json:"data"`
}

type rawSample struct {
	Metric map[string]string `json:"metric"`
	// Value 为 [时间戳(秒, float), 值(string)]，形态混合故用 RawMessage 延后解析。
	Value []json.RawMessage `json:"value"`
}

// parseTime 解析 RFC3339 时间戳；失败返回零值。
func parseTime(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}
	}
	return t
}
