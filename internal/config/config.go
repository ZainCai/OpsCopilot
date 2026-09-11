// Package config 是进程配置的唯一 schema（优化方案 #2 配置收敛 / #10 去魔法数字）。
//
// 设计：
//   - 全部 OPS_* / REDIS_* 环境变量在这里声明、解析、校验——cmd/ 不再散读 os.Getenv；
//   - 统一解析策略：缺失取默认、**非法值 fail-fast 且聚合报错**（一次列出全部
//     解析错误，不再一次只报一个、也不再静默取默认）；
//   - 双 Redis 实例架构（v1.2 C2）：告警实例（持久化）与缓存实例（可逐出）
//     物理隔离——强校验并入 Load，不放松（地址必填、角色固定、归一化后同址拒绝）。
//
// 边界（scripts/check_module_boundaries.py）：internal 各模块不得 import 本包；
// 配置注入只发生在 cmd/（装配层），internal 包收显式参数。
package config

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

// RedisRole 区分两个 Redis 实例的用途。
type RedisRole string

const (
	// RedisAlert 持久化告警实例：AOF everysec、noeviction。
	// 承载：告警流（Streams）、会话状态（P1-1）、llm 调度队列（P1-6 预留）。
	RedisAlert RedisRole = "alert"
	// RedisCache 缓存实例：LRU 可逐出。
	// 仅承载可丢失数据（查询缓存、拓扑展示缓存）。禁止承载会话状态。
	RedisCache RedisRole = "cache"
)

// RedisConfig 单个 Redis 实例配置。
//
// 配置通道为 env-only（main 只读环境变量，无 YAML 装载路径），
// 因此不带 yaml tag——遗留 tag 暗示着一条并不存在的文件配置通道
// （全局审查 C6）。Password/DB 同理：redis client 由外部注入
// （sessionstore.New 收 *redis.Client），本配置尚无消费者；
// 真正接线 client 构造时随 env key 一并加回，不在无消费者时留半成品。
type RedisConfig struct {
	Addr string
	Role RedisRole
}

// Validate 校验实例角色合法性与地址必填 + host:port 格式。
// 告警实例必须启用 AOF everysec 且禁用逐出——这是 v1.2 C2 的部署约束，
// 在配置层强制，防止运维改错。
// 全局审查 G1：两个实例的地址都必须非空（与"缺一拒绝启动"的约定一致）。
func (c *RedisConfig) Validate() error {
	switch c.Role {
	case RedisAlert:
		if err := validateAddr(c.Addr); err != nil {
			return errors.New("alert redis: " + err.Error())
		}
		return nil
	case RedisCache:
		if err := validateAddr(c.Addr); err != nil {
			return errors.New("cache redis: " + err.Error())
		}
		return nil
	default:
		return errors.New("redis role must be 'alert' or 'cache', got: " + string(c.Role))
	}
}

// validateAddr 校验 host:port 形态。此前只查非空，"127.0.0.1"（缺端口）
// 这类配置会拖到运行期才由 redis client 报错，启动期就该拦住。
func validateAddr(addr string) error {
	if strings.TrimSpace(addr) == "" {
		return errors.New("addr is required")
	}
	host, port, err := net.SplitHostPort(strings.TrimSpace(addr))
	if err != nil || host == "" || port == "" {
		return errors.New("addr must be host:port, got: " + addr)
	}
	return nil
}

// normalizeRedisAddr 归一化地址，仅用于"是否同一实例"的判定：
// 去空白、统一小写、localhost → 127.0.0.1（同一台机器的两种写法）。
// 解析失败则原样返回，交由 Validate 报格式错误。
func normalizeRedisAddr(addr string) string {
	a := strings.ToLower(strings.TrimSpace(addr))
	host, port, err := net.SplitHostPort(a)
	if err != nil {
		return a
	}
	if host == "localhost" {
		host = "127.0.0.1"
	}
	return net.JoinHostPort(host, port)
}

// ---------- 分组 schema ----------

// RedisSection 双实例：两个都必须配置，缺一拒绝启动。
type RedisSection struct {
	Alert RedisConfig
	Cache RedisConfig
}

// DBSection 事件/队列/审计/变更库共享的 TimescaleDB。
type DBSection struct {
	DSN string // OPS_DB_DSN；空 = 无库（内存降级、导入队列不启用）
	// MaxConns 共享连接池上限（D7 决策 A）。原非法值静默取 pgx 默认——
	// 收敛为非法即启动失败。
	MaxConns int // OPS_DB_MAX_CONNS，默认 16
}

// SecuritySection 安全门禁（D1/D2 决策）：监听地址、写密钥、豁免与跨源白名单。
type SecuritySection struct {
	ListenAddr           string // OPS_LISTEN_ADDR，默认 127.0.0.1:8080（S1 回环）
	WebhookToken         string // OPS_WEBHOOK_TOKEN；空 = 写端点无鉴权（启动警告）
	AllowUnauthenticated bool   // OPS_ALLOW_UNAUTHENTICATED；非回环+无密钥的本机联调逃生门
	CORSOrigin           string // OPS_CORS_ORIGIN；空 = 仅同源；"*"/畸形值启动失败（D2）
}

// ConnectorSection 连接器宿主（W4-1.1）。Interval/OpTimeout 原先是 cmd 里
// 写死的 30s（#10 魔法数字），进 schema 后可 env 覆盖，默认与现状一致。
type ConnectorSection struct {
	PromURL             string        // OPS_PROM_URL；设置即注册 prometheus 连接器
	PromToken           string        // OPS_PROM_TOKEN 可选 Bearer
	AzureSubscriptionID string        // OPS_AZURE_SUBSCRIPTION_ID（与 token 齐备才注册）
	AzureToken          string        // OPS_AZURE_TOKEN（Secret，绝不打日志）
	Interval            time.Duration // OPS_CONNECTOR_INTERVAL，默认 30s（采集轮询）
	OpTimeout           time.Duration // OPS_CONNECTOR_TIMEOUT，默认 30s（单连接器单轮上限，C3）
}

// NoiseSection 降噪（W4-1.4 / W9-1 ADR-011）。
type NoiseSection struct {
	Enabled bool          // OPS_NOISE_SHADOW=off 整体关闭（默认开）
	Window  time.Duration // OPS_NOISE_WINDOW 去重/聚类窗，默认 10m
	Mode    string        // OPS_NOISE_MODE：shadow（默认）| enforce；非法启动失败
	// DedupWindow L2 事件相似度的时间邻近窗口（原 cmd 常量 dedupWindow，
	// #10 魔法数字）。归入降噪组：与告警去重同一语义。
	DedupWindow time.Duration // OPS_DEDUP_WINDOW，默认 30m
}

// PullSection 链路 A 拉取侧（W11）。源地址/令牌复用 ConnectorSection 的
// Prom 两项（与 push 采集同源，不再散读第二份 env）。
type PullSection struct {
	Enabled  bool          // OPS_PULL_ALERTS=on 才启用（默认关）
	Interval time.Duration // OPS_PULL_INTERVAL，默认 30s（原非法值静默取默认→现 fail-fast）
}

// IngestSection 链路 A 导入队列与消费 worker（W9）。
type IngestSection struct {
	AutoCreate bool          // OPS_INCIDENT_AUTOCREATE=on 才消费建单（默认影子期只排队）
	Interval   time.Duration // OPS_INGEST_INTERVAL 消费轮询，默认 5s
	Batch      int           // OPS_INGEST_BATCH 单轮批量，默认 20
	// RateLimit/RateWindow 建单风暴限流（R1）——原 burst 50/5min 是
	// ingest_queue.go 里的魔法数字（#10），默认值不变。
	RateLimit  int           // OPS_INGEST_RATE_LIMIT，默认 50（>0）
	RateWindow time.Duration // OPS_INGEST_RATE_WINDOW，默认 5m
	// BatchTimeoutPerItem 批消费超时 = 批量 × 本值（第七轮 M-2 口径），
	// 原硬编码 15s/条。
	BatchTimeoutPerItem time.Duration // OPS_INGEST_BATCH_TIMEOUT_PER_ITEM，默认 15s
	// AlertBodyLimit 告警载荷字节上限——原 4MiB 在入站 webhook 与拉取
	// 响应两处重复定义（#10），收敛为单一定义、两处共用。
	AlertBodyLimit int64 // OPS_INGEST_ALERT_BODY_LIMIT，默认 4194304（4MiB）
}

// NotifySection 通知与值班升级（W9-2/W9-3）。渠道配置在 DB（notify_channel），
// 这里只有升级调度。
type NotifySection struct {
	EscalationEnabled  bool          // OPS_ESCALATION=on 才启用（默认关）
	EscalationAfter    time.Duration // OPS_ESCALATION_AFTER，默认 15m
	EscalationInterval time.Duration // OPS_ESCALATION_INTERVAL，默认 60s
}

// RetentionSection 事件保留与归档（D8 决策 C）。
type RetentionSection struct {
	IncidentWindow  time.Duration // OPS_INCIDENT_RETENTION，默认 90d
	IncidentEnabled bool          // off/0/never = 关闭归档
}

// TopologySection 拓扑与变更（W3/W6-0/W9-5/#4）。
type TopologySection struct {
	Edges               string        // OPS_TOPOLOGY_EDGES 原样承接（"src->dst,..."，解析仍带 fail-fast 在 cmd）
	ChangeWindow        time.Duration // OPS_CHANGE_RETENTION 变更库保留窗，默认 7d
	ChangeEnabled       bool          // off/0/never = 不清理（全量保留 + 全量回放）
	ChangePruneInterval time.Duration // OPS_CHANGE_PRUNE_INTERVAL 清理周期，默认 1h
}

// MemLimitSection 内存有界化（优化方案 #6）：各无界/准无界进程内结构的
// 容量上限与告警水位。上限按"开发/演示规模不可能触发"保守设定——
// 正常规模行为与不设限完全一致；只有逼近病态增长才淘汰并计 WARN/指标。
// 消费方（装配层）把对应字段变成 pkg/memguard.Guard 注入各结构，
// internal 模块本身不认识本包（边界纪律）。
type MemLimitSection struct {
	WarnRatio        float64 // OPS_MEMLIMIT_WARN_RATIO，告警水位比例，默认 0.8（0<r<1）
	TopologyNodes    int     // OPS_MEMLIMIT_TOPOLOGY_NODES 拓扑图节点上限，默认 100000
	TopologyEdges    int     // OPS_MEMLIMIT_TOPOLOGY_EDGES 拓扑图边上限，默认 400000
	Incidents        int     // OPS_MEMLIMIT_INCIDENTS 事件内存兜底 Store 上限，默认 50000
	EscalationLedger int     // OPS_MEMLIMIT_ESCALATION_LEDGER 升级台账内存兜底上限，默认 100000
	NoiseDedup       int     // OPS_MEMLIMIT_NOISE_DEDUP 去重指纹表上限，默认 200000
	NoiseClusters    int     // OPS_MEMLIMIT_NOISE_CLUSTERS 内存簇（活跃+历史）上限，默认 50000
	NoiseSigCache    int     // OPS_MEMLIMIT_NOISE_SIGCACHE 簇落库签名缓存上限，默认 50000
	Audit            int     // OPS_MEMLIMIT_AUDIT 审计内存兜底上限，默认 50000
}

// MetricsSection 服务暴露面口径（HTTP 超时与请求体上限）。
//
// 与其余分组不同：**当前没有 env 通道**——这些是安全/稳定性口径
// （W9-5 第八轮审核 D7 的超时三件套、各写端点的体上限），改动应经代码评审
// 而不是运维改 env。进 schema 的目的是"单一定义"：SSE 依赖的 WriteTimeout
// 与响应级清除、各端点上限此前散落在 cmd 各文件，这里收敛为唯一来源，
// 消费方（main 的 newHTTPServer、REST 网关、webhook）从这里取值。
type MetricsSection struct {
	HTTPReadHeaderTimeout time.Duration // slow-loris 防御，5s
	HTTPWriteTimeout      time.Duration // 30s；SSE 在 handler 内显式清除
	HTTPIdleTimeout       time.Duration // keep-alive 空闲上限，120s
	IncidentBodyLimit     int64         // 人工建单等请求体上限，1MiB
	ChangeBodyLimit       int64         // 变更 webhook 请求体上限，1MiB
	NotifyBodyLimit       int64         // 通知渠道配置请求体上限，64KiB
}

// Config all-in-one 进程配置（schema 见各分组注释；env 清单文档镜像
// docs/配置清单-OpsEnv.md 与 .env.example）。
type Config struct {
	// Tenant 租户标识（OPS_TENANT，默认 "default"）。
	// 取代原 cmd 包级变量 DefaultTenant（#10 去全局）：构造参数显式注入。
	Tenant string

	Redis     RedisSection
	DB        DBSection
	Security  SecuritySection
	Connector ConnectorSection
	Noise     NoiseSection
	Pull      PullSection
	Ingest    IngestSection
	Notify    NotifySection
	Retention RetentionSection
	Topology  TopologySection
	MemLimit  MemLimitSection
	Metrics   MetricsSection
}

// Validate 全局校验：双 Redis 角色不得互换、地址不得相同（防止偷偷合并回
// 单实例）、两个实例地址都必须非空且格式合法（G1）。其余字段在 Load 解析期
// 已按类型校验，这里不再重复。
func (c *Config) Validate() error {
	if c.Redis.Alert.Role != RedisAlert || c.Redis.Cache.Role != RedisCache {
		return errors.New("redis instances misconfigured: alert/cache roles are fixed")
	}
	// 归一化后比较：`localhost:6380` 与 `127.0.0.1:6380` 是同一实例，
	// 单纯字符串比较会让"物理隔离"约束被写法差异绕过。
	if normalizeRedisAddr(c.Redis.Alert.Addr) == normalizeRedisAddr(c.Redis.Cache.Addr) {
		return errors.New("alert and cache redis must be physically separate instances")
	}
	if err := c.Redis.Alert.Validate(); err != nil {
		return err
	}
	if r := c.MemLimit.WarnRatio; r <= 0 || r > 1 {
		return fmt.Errorf("memlimit warn ratio must be in (0, 1], got %g", r)
	}
	return c.Redis.Cache.Validate()
}

// ---------- 默认值（单一定义，#10） ----------

const (
	// DefaultListenAddr HTTP 监听地址默认值（全局审查 S1：默认只绑回环——
	// 进程暴露了可写的变更 webhook，无鉴权服务不得默认监听全部网络接口）。
	DefaultListenAddr = "127.0.0.1:8080"
	// DefaultAlertRedisAddr / DefaultCacheRedisAddr 仅用于 Defaults()（测试/
	// 嵌入式构造基线）。**Load 通道不套用**：两个 Redis 地址 env 缺失即
	// fail-fast（v1.2 C2 / G1"缺一拒绝启动"不放松）。
	DefaultAlertRedisAddr = "127.0.0.1:6380"
	DefaultCacheRedisAddr = "127.0.0.1:6381"

	// DefaultTenant M1 单租户缺省值（alert_cluster.tenant_id 对齐）。
	DefaultTenant = "default"

	// DefaultDBMaxConns 共享连接池默认上限（D7 决策 A）：事件 Store + 导入
	// 队列 + 审计三方共用，worker 批处理持一条事务连接再做 Store 写需要第
	// 二条；pgx 默认 max(4,NumCPU) 在并发下易耗尽。
	DefaultDBMaxConns = 16

	DefaultConnectorInterval = 30 * time.Second
	DefaultConnectorTimeout  = 30 * time.Second

	DefaultNoiseWindow = 10 * time.Minute
	DefaultDedupWindow = 30 * time.Minute

	// Noise 模式枚举。
	NoiseModeShadow  = "shadow"
	NoiseModeEnforce = "enforce"

	DefaultPullInterval = 30 * time.Second

	DefaultIngestInterval           = 5 * time.Second
	DefaultIngestBatch              = 20
	DefaultIngestRateLimit          = 50 // 5 分钟内最多建 50 单，其余进聚合单
	DefaultIngestRateWindow         = 5 * time.Minute
	DefaultIngestBatchPerItem       = 15 * time.Second // 批消费超时 15s/条（第七轮 M-2）
	DefaultAlertBodyLimit     int64 = 4 << 20          // 4MiB：入站 webhook 与拉取响应共用的单一定义

	DefaultEscalationAfter    = 15 * time.Minute
	DefaultEscalationInterval = 60 * time.Second

	DefaultIncidentRetention = 90 * 24 * time.Hour // D8 决策 C：resolved 后 90d 归档
	DefaultChangeRetention   = 7 * 24 * time.Hour  // W9-5：变更事件保留窗

	DefaultChangePruneInterval = time.Hour

	// 内存有界化默认上限（优化方案 #6）。取值依据：开发/演示环境规模
	// （百台节点、每批数百告警、7 天影子期）距这些数字还差 2~3 个数量级
	// ——**正常规模行为与不设限完全一致**；触限即淘汰最久未活跃并计
	// WARN + opscopilot_mem_evictions_total，是"最后一道保险"不是日常策略。
	DefaultMemWarnRatio        = 0.8    // 告警水位 = max*ratio（size 过线先 WARN，不等淘汰）
	DefaultMemTopologyNodes    = 100000 // 拓扑图节点（Builder 过度淘汰伤降噪故障域/RCA 取证，取 1e5 保守值）
	DefaultMemTopologyEdges    = 400000 // 拓扑图边（端点必为图内节点，取节点×4）
	DefaultMemIncidents        = 50000  // 事件 MemStore（仅 DB 缺席降级路径）
	DefaultMemEscalationLedger = 100000 // 升级台账内存降级路径（幂等键条数）
	DefaultMemNoiseDedup       = 200000 // 去重指纹表（窗口清扫之外的基数洪峰保险）
	DefaultMemNoiseClusters    = 50000  // 内存簇（活跃+历史；历史区 resolved 是唯一只进不出的区）
	DefaultMemNoiseSigCache    = 50000  // 簇落库签名缓存（与簇上限同源）
	DefaultMemAudit            = 50000  // 审计内存降级路径条数

	// HTTP / 请求体口径（MetricsSection 默认值，无 env 通道，见其注释）。
	DefaultHTTPReadHeaderTimeout       = 5 * time.Second
	DefaultHTTPWriteTimeout            = 30 * time.Second
	DefaultHTTPIdleTimeout             = 120 * time.Second
	DefaultIncidentBodyLimit     int64 = 1 << 20
	DefaultChangeBodyLimit       int64 = 1 << 20
	DefaultNotifyBodyLimit       int64 = 64 << 10
)

// Defaults 返回与现状行为一致的全默认配置（Redis 地址取本机 compose 端口——
// 仅供直接构造的配置消费者/测试使用；env 装载路径上 Redis 地址必填）。
func Defaults() *Config {
	c := &Config{Tenant: DefaultTenant}
	c.Redis.Alert = RedisConfig{Addr: DefaultAlertRedisAddr, Role: RedisAlert}
	c.Redis.Cache = RedisConfig{Addr: DefaultCacheRedisAddr, Role: RedisCache}
	c.DB.MaxConns = DefaultDBMaxConns
	c.Security.ListenAddr = DefaultListenAddr
	c.Connector.Interval = DefaultConnectorInterval
	c.Connector.OpTimeout = DefaultConnectorTimeout
	c.Noise.Enabled = true
	c.Noise.Window = DefaultNoiseWindow
	c.Noise.Mode = NoiseModeShadow
	c.Noise.DedupWindow = DefaultDedupWindow
	c.Pull.Interval = DefaultPullInterval
	c.Ingest.Interval = DefaultIngestInterval
	c.Ingest.Batch = DefaultIngestBatch
	c.Ingest.RateLimit = DefaultIngestRateLimit
	c.Ingest.RateWindow = DefaultIngestRateWindow
	c.Ingest.BatchTimeoutPerItem = DefaultIngestBatchPerItem
	c.Ingest.AlertBodyLimit = DefaultAlertBodyLimit
	c.Notify.EscalationAfter = DefaultEscalationAfter
	c.Notify.EscalationInterval = DefaultEscalationInterval
	c.Retention.IncidentWindow = DefaultIncidentRetention
	c.Retention.IncidentEnabled = true
	c.Topology.ChangeWindow = DefaultChangeRetention
	c.Topology.ChangeEnabled = true
	c.Topology.ChangePruneInterval = DefaultChangePruneInterval
	c.MemLimit.WarnRatio = DefaultMemWarnRatio
	c.MemLimit.TopologyNodes = DefaultMemTopologyNodes
	c.MemLimit.TopologyEdges = DefaultMemTopologyEdges
	c.MemLimit.Incidents = DefaultMemIncidents
	c.MemLimit.EscalationLedger = DefaultMemEscalationLedger
	c.MemLimit.NoiseDedup = DefaultMemNoiseDedup
	c.MemLimit.NoiseClusters = DefaultMemNoiseClusters
	c.MemLimit.NoiseSigCache = DefaultMemNoiseSigCache
	c.MemLimit.Audit = DefaultMemAudit
	c.Metrics.HTTPReadHeaderTimeout = DefaultHTTPReadHeaderTimeout
	c.Metrics.HTTPWriteTimeout = DefaultHTTPWriteTimeout
	c.Metrics.HTTPIdleTimeout = DefaultHTTPIdleTimeout
	c.Metrics.IncidentBodyLimit = DefaultIncidentBodyLimit
	c.Metrics.ChangeBodyLimit = DefaultChangeBodyLimit
	c.Metrics.NotifyBodyLimit = DefaultNotifyBodyLimit
	return c
}

// ---------- 共享解析原语 ----------

// ParseRetention 解析保留期配置（工单归档 OPS_INCIDENT_RETENTION 与变更库
// OPS_CHANGE_RETENTION 共用，W9-5 抽出动机：两处各写一份必然漂移）：
// 接受 Go duration（"2160h"）或 "<N>d"（天数，如 "90d"）；
// "off"/"0"/"never" 表示关闭；空值取给定默认。
// 非法值返回错误——所有保留策略的消费方（Load、归档器、清理器）fail-fast。
func ParseRetention(raw string, def time.Duration) (window time.Duration, on bool, err error) {
	raw = strings.TrimSpace(strings.ToLower(raw))
	switch raw {
	case "", "-":
		return def, true, nil
	case "off", "0", "never":
		return 0, false, nil
	}
	if d, e := time.ParseDuration(raw); e == nil {
		return d, true, nil
	}
	if strings.HasSuffix(raw, "d") {
		if n, e := strconv.Atoi(strings.TrimSuffix(raw, "d")); e == nil && n > 0 {
			return time.Duration(n) * 24 * time.Hour, true, nil
		}
	}
	return 0, false, fmt.Errorf("invalid retention %q (want a duration like 168h/2160h, days like 7d/90d, or off)", raw)
}

// ValidateCORSOrigin 校验跨源放行白名单（D2 决策 B+C，原 RESTGateway.
// SetCORSOrigin 的校验规则上移到 schema 层，装载期即拒）：
// 空 = 仅同源；拒绝 "*"（恢复"任意网页跨源读事件/审计"）；其余值要求形如
// scheme://host（或 "null"——file:// 调试用法）。
func ValidateCORSOrigin(origin string) error {
	o := strings.TrimSpace(origin)
	if o == "" {
		return nil
	}
	if o == "*" {
		return errors.New(`OPS_CORS_ORIGIN=* is not allowed: it re-enables any-webpage cross-origin reads of incidents/audit (see decision D2)`)
	}
	if o != "null" && !strings.Contains(o, "://") {
		return fmt.Errorf("OPS_CORS_ORIGIN must be an origin like https://ops.example.com (or \"null\"), got %q", o)
	}
	return nil
}
