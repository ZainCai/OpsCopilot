// W11 链路 A 拉取侧：定时从数据源轮询告警并入队。
//
// 动机：一期链路 A 只有 push（Alertmanager / 通用 webhook 主动推）。但很多
// 源不会推——Prometheus 只在被 scrape 时暴露 /api/v1/alerts，没有推送能力。
// 本文件补上"拉"：定时抓取 → 走**与 push 完全相同**的入队路径
// （Enqueue → ingest_queue → IngestWorker → UpsertExternal），不另起一套建单
// 逻辑——两条进入路径在队列处汇流，幂等/限流/审计/自动关单策略全部复用。
//
// 设计与边界：
//   - Source 可插拔（AlertSource 接口）：本期实现 Prometheus；后续接 Azure
//     Monitor / 自建 API 只需再写一个 FetchAlerts。
//   - 载荷统一映射为 amAlert（与 push 同一形态）→ worker 的 processAlertmanager
//     原样复用，不因来源不同而分叉。
//   - **不自动关单**：Prometheus /api/v1/alerts 只列"当前活跃"告警，没有
//     endsAt。据"告警从列表消失"推断恢复太危险（一次 scrape 抖动/重启会把
//     整批单误关）。故拉取侧只负责导入与刷新，恢复仍由 push（AM 的 endsAt）
//     或人工处置。后续若要做"消失即恢复"，必须加 N 次连续缺失 + 宽限期。
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"opscopilot/internal/config"
	"opscopilot/internal/incident"
)

// pullHTTPTimeout 单次拉取 HTTP 超时。
const pullHTTPTimeout = 15 * time.Second

// AlertSource 拉取型告警源：返回与 push 同形态（amAlert）的告警列表。
type AlertSource interface {
	// Name 源名（日志与观测）。
	Name() string
	// FetchAlerts 拉取**当前活跃**告警（firing/pending）。失败返回错误，
	// 调用方记日志并等下一轮（不因一轮失败而退出）。
	FetchAlerts(ctx context.Context) ([]amAlert, error)
}

// PrometheusAlertsSource 从 Prometheus /api/v1/alerts 拉取活跃告警。
//
// 该接口返回 Prometheus 内部的告警状态（不是 Alertmanager 的通知状态）：
// 每条告警有 labels / annotations / state / activeAt / value。映射为 amAlert：
// labels/annotations 直通，activeAt→StartsAt，EndsAt 留空（见文件头"不自动
// 关单"），Fingerprint 由 labels 归一化生成（Prometheus 不提供指纹）。
type PrometheusAlertsSource struct {
	BaseURL string
	Token   string // 可选 Bearer Token（无鉴权源留空）
	// BodyLimit 单次拉取响应体上限（Prometheus 活跃告警列表，默认 4MiB）。
	// #10：与入站 AM webhook **共用 config 的单一定义**
	// （config.DefaultAlertBodyLimit / OPS_INGEST_ALERT_BODY_LIMIT），
	// 不再两处各写一个 4<<20。
	BodyLimit int64
	client    *http.Client
}

// NewPrometheusAlertsSource 构造。bodyLimit<=0 回退 config 默认（同源单一定义）。
func NewPrometheusAlertsSource(baseURL, token string, bodyLimit int64) *PrometheusAlertsSource {
	if bodyLimit <= 0 {
		bodyLimit = config.DefaultAlertBodyLimit
	}
	return &PrometheusAlertsSource{
		BaseURL:   baseURL,
		Token:     token,
		BodyLimit: bodyLimit,
		client:    &http.Client{Timeout: pullHTTPTimeout},
	}
}

// Name 源名。
func (s *PrometheusAlertsSource) Name() string { return "prometheus" }

// promAlertsResp Prometheus /api/v1/alerts 响应（只取用到的字段）。
type promAlertsResp struct {
	Status string `json:"status"`
	Data   struct {
		Alerts []struct {
			Labels      map[string]string `json:"labels"`
			Annotations map[string]string `json:"annotations"`
			State       string            `json:"state"` // firing | pending
			ActiveAt    string            `json:"activeAt"`
			Value       string            `json:"value"`
		} `json:"alerts"`
	} `json:"data"`
}

// FetchAlerts 拉取并映射为 amAlert 列表。
func (s *PrometheusAlertsSource) FetchAlerts(ctx context.Context) ([]amAlert, error) {
	url := strings.TrimRight(s.BaseURL, "/") + "/api/v1/alerts"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("prometheus alerts: build request: %w", err)
	}
	if s.Token != "" {
		req.Header.Set("Authorization", "Bearer "+s.Token)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("prometheus alerts: %w", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, s.BodyLimit))
	if err != nil {
		return nil, fmt.Errorf("prometheus alerts: read body: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("prometheus alerts: http %d", resp.StatusCode)
	}
	var p promAlertsResp
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, fmt.Errorf("prometheus alerts: decode: %w", err)
	}
	if p.Status != "" && p.Status != "success" {
		return nil, fmt.Errorf("prometheus alerts: status %q", p.Status)
	}
	out := make([]amAlert, 0, len(p.Data.Alerts))
	for _, a := range p.Data.Alerts {
		out = append(out, amAlert{
			Labels:      a.Labels,
			Annotations: a.Annotations,
			StartsAt:    a.ActiveAt,
			// EndsAt 留空 → amResolved=false（拉取侧不自动关单）。
			Fingerprint: promFingerprint(a.Labels),
		})
	}
	return out, nil
}

// promFingerprint 由 labels 生成稳定指纹（sha256(sorted "k=v")）。
// 同一条告警跨轮询指纹不变 → 队列唯一索引幂等命中 → 只刷新不重开单。
func promFingerprint(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(labels[k])
		b.WriteByte('\n')
	}
	sum := sha256.Sum256([]byte(b.String()))
	return "prom:" + hex.EncodeToString(sum[:16])
}

// AlertPoller 拉取调度器：定时 FetchAlerts → 逐条 Enqueue（与 push 同路径）。
//
// **只上报变化（内容去重）**：轮询会反复看到同一批活跃告警；若无脑每轮入队，
// 队列表会持续膨胀，且 worker 每轮 UpsertExternal 都会刷 updated_at、经 SSE
// 推一条 updated——事件页会"每轮自刷新"，纯噪声。故本调度器记住"上一轮已入队
// 的 source_ref→载荷哈希"，仅在**新出现**或**内容变化**时入队；告警从列表
// 消失即遗忘该 ref（再次出现时重新入队）。
//
// 注意：这是"变化上报"，不是"状态收敛"——告警消失只做遗忘，不自动关单
// （见文件头"不自动关单"）。
type AlertPoller struct {
	Source   AlertSource     // 拉取源
	Owner    *QueueOwner     // 队列写入口（装配期注入）
	Origin   incident.Origin // 入队来源（拉取侧用 OriginPrometheus）
	Interval time.Duration   // 轮询周期
	logf     func(string, ...any)

	mu   sync.Mutex
	seen map[string]string // source_ref → 上一轮载荷哈希
}

// NewAlertPoller 构造。interval<=0 视为未启用（Run 立即返回）。
func NewAlertPoller(source AlertSource, owner *QueueOwner, origin incident.Origin, interval time.Duration, logf func(string, ...any)) *AlertPoller {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	return &AlertPoller{Source: source, Owner: owner, Origin: origin, Interval: interval,
		logf: logf, seen: map[string]string{}}
}

// Run 周期拉取，ctx 取消即退出。首轮立即拉（不等第一个 tick），
// 让服务一起来就补齐存量告警。
func (p *AlertPoller) Run(ctx context.Context) {
	if p.Interval <= 0 || p.Source == nil || p.Owner == nil || p.Owner.w == nil {
		p.logf("alert pull: not started (disabled or queue unwired)")
		return
	}
	p.logf("alert pull: started (source %s, interval %v, origin %s)", p.Source.Name(), p.Interval, p.Origin)
	p.pollOnce(ctx)
	ticker := time.NewTicker(p.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			p.logf("alert pull: stopped")
			return
		case <-ticker.C:
			p.pollOnce(ctx)
		}
	}
}

// pollOnce 拉一轮，仅对"新增/内容变化"的告警入队。单轮失败只记日志（下轮重试）。
func (p *AlertPoller) pollOnce(ctx context.Context) {
	alerts, err := p.Source.FetchAlerts(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return // 停机中的取消失败，不刷噪声
		}
		p.logf("WARNING: alert pull fetch (%s): %v", p.Source.Name(), err)
		return
	}
	enqueued, unchanged := 0, 0
	current := make(map[string]struct{}, len(alerts))
	for _, a := range alerts {
		ref := strings.TrimSpace(a.Fingerprint)
		if ref == "" {
			ref = fallbackRef(a) // 无指纹退化：labels 归一化（弱幂等）
		}
		payload, err := json.Marshal(a)
		if err != nil {
			continue
		}
		current[ref] = struct{}{}
		h := contentHash(payload)

		p.mu.Lock()
		prev, known := p.seen[ref]
		p.mu.Unlock()
		if known && prev == h {
			unchanged++ // 内容未变：不入队（避免队列膨胀与 updated_at 噪声）
			continue
		}
		ok, err := p.Owner.w.Enqueue(p.Origin, ref, string(payload))
		if err != nil {
			p.logf("WARNING: alert pull enqueue (%s): %v", p.Source.Name(), err)
			return
		}
		// 入队成功或幂等命中都记为"已见"：前者在途、后者已在队列，下轮不必再试。
		p.mu.Lock()
		p.seen[ref] = h
		p.mu.Unlock()
		if ok {
			enqueued++
		} else {
			unchanged++ // 队列中已有同 (origin,ref) 待处理
		}
	}
	// 本轮未出现的 ref 视为"已消失"：遗忘，待其再次出现时重新入队。
	p.mu.Lock()
	for ref := range p.seen {
		if _, ok := current[ref]; !ok {
			delete(p.seen, ref)
		}
	}
	p.mu.Unlock()
	p.logf("alert pull: source=%s fetched=%d enqueued=%d unchanged=%d",
		p.Source.Name(), len(alerts), enqueued, unchanged)
}

// contentHash 载荷内容哈希（变化检测用）。
func contentHash(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:16])
}
