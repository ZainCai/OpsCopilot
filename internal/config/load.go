// load.go OPS_*/REDIS_* 环境变量的唯一装载通道（优化方案 #2 配置收敛）。
//
// 统一策略（每个 key 都走同一套原语）：
//   - 缺失（未设置或空白）→ 取默认值；
//   - 非法值 → **fail-fast**：不再静默取默认、也不再多处各判各的，
//     启动时一次性列出**全部**解析错误（聚合报错），运维一轮就能改完；
//   - 例外：REDIS_ALERT_ADDR / REDIS_CACHE_ADDR 必填——双实例物理隔离
//     （v1.2 C2 / 全局审查 G1）"缺一拒绝启动"的约定不因收敛而放松。
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// env 键名（全仓唯一声明处；cmd 不再出现 os.Getenv）。
const (
	EnvRedisAlertAddr = "REDIS_ALERT_ADDR"
	EnvRedisCacheAddr = "REDIS_CACHE_ADDR"

	EnvTenant = "OPS_TENANT"

	EnvDBDSN      = "OPS_DB_DSN"
	EnvDBMaxConns = "OPS_DB_MAX_CONNS"

	EnvListenAddr           = "OPS_LISTEN_ADDR"
	EnvWebhookToken         = "OPS_WEBHOOK_TOKEN"
	EnvAllowUnauthenticated = "OPS_ALLOW_UNAUTHENTICATED"
	EnvCORSOrigin           = "OPS_CORS_ORIGIN"

	EnvPromURL      = "OPS_PROM_URL"
	EnvPromToken    = "OPS_PROM_TOKEN"
	EnvAzureSubID   = "OPS_AZURE_SUBSCRIPTION_ID"
	EnvAzureToken   = "OPS_AZURE_TOKEN"
	EnvConnInterval = "OPS_CONNECTOR_INTERVAL"
	EnvConnTimeout  = "OPS_CONNECTOR_TIMEOUT"

	EnvNoiseShadow = "OPS_NOISE_SHADOW"
	EnvNoiseWindow = "OPS_NOISE_WINDOW"
	EnvNoiseMode   = "OPS_NOISE_MODE"
	EnvDedupWindow = "OPS_DEDUP_WINDOW"

	EnvPullAlerts   = "OPS_PULL_ALERTS"
	EnvPullInterval = "OPS_PULL_INTERVAL"

	EnvIngestAutoCreate     = "OPS_INCIDENT_AUTOCREATE"
	EnvIngestInterval       = "OPS_INGEST_INTERVAL"
	EnvIngestBatch          = "OPS_INGEST_BATCH"
	EnvIngestRateLimit      = "OPS_INGEST_RATE_LIMIT"
	EnvIngestRateWindow     = "OPS_INGEST_RATE_WINDOW"
	EnvIngestBatchPerItem   = "OPS_INGEST_BATCH_TIMEOUT_PER_ITEM"
	EnvIngestAlertBodyLimit = "OPS_INGEST_ALERT_BODY_LIMIT"

	EnvEscalation         = "OPS_ESCALATION"
	EnvEscalationAfter    = "OPS_ESCALATION_AFTER"
	EnvEscalationInterval = "OPS_ESCALATION_INTERVAL"

	EnvIncidentRetention = "OPS_INCIDENT_RETENTION"

	EnvTopologyEdges       = "OPS_TOPOLOGY_EDGES"
	EnvChangeRetention     = "OPS_CHANGE_RETENTION"
	EnvChangePruneInterval = "OPS_CHANGE_PRUNE_INTERVAL"
)

// LookupFunc 与 os.LookupEnv 同签名（测试注入假环境，不碰全局 env）。
type LookupFunc func(key string) (string, bool)

// Load 从进程环境装载配置。非法值聚合报错（全部列出后一次性返回）。
func Load() (*Config, error) {
	return LoadFrom(os.LookupEnv)
}

// LoadFrom 从给定查找函数装载（缺省项取 Defaults() 的同款默认值）。
func LoadFrom(lookup LookupFunc) (*Config, error) {
	p := &parser{lookup: lookup}
	c := Defaults()

	c.Tenant = p.strOr(EnvTenant, DefaultTenant)
	c.Redis.Alert.Addr = p.required(EnvRedisAlertAddr)
	c.Redis.Cache.Addr = p.required(EnvRedisCacheAddr)

	c.DB.DSN = p.str(EnvDBDSN)
	c.DB.MaxConns = p.posInt(EnvDBMaxConns, DefaultDBMaxConns)

	c.Security.ListenAddr = p.strOr(EnvListenAddr, DefaultListenAddr)
	c.Security.WebhookToken = p.raw(EnvWebhookToken) // 令牌不 Trim：原样比对
	c.Security.AllowUnauthenticated = p.onOff(EnvAllowUnauthenticated, false)
	c.Security.CORSOrigin = p.str(EnvCORSOrigin)
	if err := ValidateCORSOrigin(c.Security.CORSOrigin); err != nil {
		p.bad(EnvCORSOrigin, err.Error())
	}

	c.Connector.PromURL = p.str(EnvPromURL)
	c.Connector.PromToken = p.raw(EnvPromToken)
	c.Connector.AzureSubscriptionID = p.str(EnvAzureSubID)
	c.Connector.AzureToken = p.raw(EnvAzureToken)
	c.Connector.Interval = p.posDur(EnvConnInterval, DefaultConnectorInterval)
	c.Connector.OpTimeout = p.posDur(EnvConnTimeout, DefaultConnectorTimeout)

	c.Noise.Enabled = !p.isOff(EnvNoiseShadow) // 默认开；off 关闭
	c.Noise.Window = p.posDur(EnvNoiseWindow, DefaultNoiseWindow)
	c.Noise.Mode = p.enum(EnvNoiseMode, NoiseModeShadow, NoiseModeShadow, NoiseModeEnforce)
	c.Noise.DedupWindow = p.posDur(EnvDedupWindow, DefaultDedupWindow)

	c.Pull.Enabled = p.onOff(EnvPullAlerts, false)
	c.Pull.Interval = p.posDur(EnvPullInterval, DefaultPullInterval)

	c.Ingest.AutoCreate = p.onOff(EnvIngestAutoCreate, false)
	c.Ingest.Interval = p.posDur(EnvIngestInterval, DefaultIngestInterval)
	c.Ingest.Batch = p.posInt(EnvIngestBatch, DefaultIngestBatch)
	c.Ingest.RateLimit = p.posInt(EnvIngestRateLimit, DefaultIngestRateLimit)
	c.Ingest.RateWindow = p.posDur(EnvIngestRateWindow, DefaultIngestRateWindow)
	c.Ingest.BatchTimeoutPerItem = p.posDur(EnvIngestBatchPerItem, DefaultIngestBatchPerItem)
	c.Ingest.AlertBodyLimit = p.posInt64(EnvIngestAlertBodyLimit, DefaultAlertBodyLimit)

	c.Notify.EscalationEnabled = p.onOff(EnvEscalation, false)
	c.Notify.EscalationAfter = p.posDur(EnvEscalationAfter, DefaultEscalationAfter)
	c.Notify.EscalationInterval = p.posDur(EnvEscalationInterval, DefaultEscalationInterval)

	if w, on, err := ParseRetention(p.str(EnvIncidentRetention), DefaultIncidentRetention); err != nil {
		p.bad(EnvIncidentRetention, err.Error())
	} else {
		c.Retention.IncidentWindow, c.Retention.IncidentEnabled = w, on
	}

	c.Topology.Edges = p.raw(EnvTopologyEdges)
	if w, on, err := ParseRetention(p.str(EnvChangeRetention), DefaultChangeRetention); err != nil {
		p.bad(EnvChangeRetention, err.Error())
	} else {
		c.Topology.ChangeWindow, c.Topology.ChangeEnabled = w, on
	}
	c.Topology.ChangePruneInterval = p.posDur(EnvChangePruneInterval, DefaultChangePruneInterval)

	if len(p.errs) > 0 {
		return nil, &Error{Errs: p.errs}
	}
	// 解析全绿后再跑结构性校验（双 Redis 角色/同址/格式）。
	// 同址与缺址都汇入聚合错误：缺址已在 required() 记过错，这里只补
	// 角色互换/同址这类跨字段约束（单条即可拒启，与其他错误合并展示）。
	if err := c.Validate(); err != nil {
		return nil, &Error{Errs: []error{err}}
	}
	return c, nil
}

// Error 聚合装载错误：一次列全，不再"修一个报一个"。
type Error struct {
	Errs []error
}

func (e *Error) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "config invalid (%d error(s)):", len(e.Errs))
	for _, err := range e.Errs {
		fmt.Fprintf(&b, "\n  - %s", err)
	}
	return b.String()
}

// Unwrap 暴露错误列表（errors.Is/As 穿透用）。
func (e *Error) Unwrap() []error { return e.Errs }

// parser 带聚合错误收集的原语集。
type parser struct {
	lookup LookupFunc
	errs   []error
}

func (p *parser) bad(key, msg string) {
	p.errs = append(p.errs, fmt.Errorf("%s: %s", key, msg))
}

// raw 原样取值（不 Trim——令牌/边声明等按原值消费）。
func (p *parser) raw(key string) string {
	v, _ := p.lookup(key)
	return v
}

// str Trim 后取值；缺失取默认（由调用方的 Defaults 基线提供，这里返回 ""
// 表示"未设置"，字段保持默认值——统一在 set 辅助里做）。
func (p *parser) str(key string) string {
	v := strings.TrimSpace(p.raw(key))
	return v
}

// strOr 非空字符串项：缺失/空白取给定默认。
func (p *parser) strOr(key, def string) string {
	if v := p.str(key); v != "" {
		return v
	}
	return def
}

// required 必填字符串（当前仅双 Redis 地址）：未设置/空白记聚合错误，
// 返回值被 Validate 再兜一层格式。
func (p *parser) required(key string) string {
	v := p.str(key)
	if v == "" {
		p.bad(key, "required (dual-redis isolation is enforced; set it explicitly)")
	}
	return v
}

// posDur 正 duration；非法/非正记错并保留默认值（默认值来自 Defaults 基线，
// 聚合报错保证该默认值永远不会"静默生效"——有错即启动失败）。
func (p *parser) posDur(key string, def time.Duration) time.Duration {
	raw := p.str(key)
	if raw == "" {
		return def
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		p.bad(key, fmt.Sprintf("invalid duration %q: %v", raw, err))
		return def
	}
	if d <= 0 {
		p.bad(key, fmt.Sprintf("must be a positive duration, got %q", raw))
		return def
	}
	return d
}

// posInt 正整数。
func (p *parser) posInt(key string, def int) int {
	raw := p.str(key)
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		p.bad(key, fmt.Sprintf("must be an integer, got %q", raw))
		return def
	}
	if n <= 0 {
		p.bad(key, fmt.Sprintf("must be > 0, got %d", n))
		return def
	}
	return n
}

// posInt64 正整数（字节上限类）。
func (p *parser) posInt64(key string, def int64) int64 {
	raw := p.str(key)
	if raw == "" {
		return def
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		p.bad(key, fmt.Sprintf("must be an integer, got %q", raw))
		return def
	}
	if n <= 0 {
		p.bad(key, fmt.Sprintf("must be > 0, got %d", n))
		return def
	}
	return n
}

// onOff 布尔开关：大小写不敏感的 on/off；缺失/空取默认；其余值记错。
// （原行为是"!= off 即真"或"EqualFold on"两种写法并存——统一后，"1"/"true"
// 这类手写值会被明确拒绝而非静默解释。）
func (p *parser) onOff(key string, def bool) bool {
	raw := p.str(key)
	switch strings.ToLower(raw) {
	case "":
		return def
	case "on":
		return true
	case "off":
		return false
	default:
		p.bad(key, fmt.Sprintf("must be \"on\" or \"off\", got %q", raw))
		return def
	}
}

// isOff 兼容旧语义的"仅 off 关闭"开关（OPS_NOISE_SHADOW）：大小写不敏感
// off → true（=关闭）；空 → false；其余非法值记错（原实现把 "OFF"/"bogus"
// 一律当"开"——统一 fail-fast 后非法值启动失败）。
func (p *parser) isOff(key string) bool {
	raw := p.str(key)
	switch strings.ToLower(raw) {
	case "":
		return false
	case "off":
		return true
	case "on":
		return false
	default:
		p.bad(key, fmt.Sprintf("must be \"on\" or \"off\", got %q", raw))
		return false
	}
}

// enum 枚举字符串：大小写不敏感，缺失取默认，白名单外记错。
func (p *parser) enum(key, def string, allowed ...string) string {
	raw := p.str(key)
	if raw == "" {
		return def
	}
	low := strings.ToLower(raw)
	for _, a := range allowed {
		if low == strings.ToLower(a) {
			return low
		}
	}
	p.bad(key, fmt.Sprintf("must be one of %s, got %q", strings.Join(allowed, "|"), raw))
	return def
}
