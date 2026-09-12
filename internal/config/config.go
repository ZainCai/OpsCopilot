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
	"net/url"
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
	// AutoAttach W10-6 簇→事件生产自动挂簇（OPS_AUTOATTACH，默认 off）：
	// enforce 判决 new-incident（成簇即建单阈值）联动 UpsertExternal 建单 +
	// AttachCluster 挂簇 + attach_cluster 审计。挂点在 enforce 判决出口，
	// shadow 下永不触发——Validate 强制 on ⇒ 降噪启用且 enforce（配置矛盾
	// fail-fast，同款 OPS_RCA_AUTO/OPS_SESSION）。与 OPS_INCIDENT_AUTOCREATE
	// 无硬依赖（不同链路写库同走 (origin,source_ref) 幂等键，天然收敛一单）。
	AutoAttach bool
	// DedupWindow L2 事件相似度的时间邻近窗口（原 cmd 常量 dedupWindow，
	// #10 魔法数字）。归入降噪组：与告警去重同一语义。
	DedupWindow time.Duration // OPS_DEDUP_WINDOW，默认 30m
	// ---- 判决异步落库队列（优化方案 #8；落库尽力而为，通知链路不等待）----
	// ProcessAlerts 生成判决后投递带缓冲 channel 即返回，独立 writer goroutine
	// 攒批落库（Redis 镜像 + PG 真相源出口语义不变）——慢 DB 不占采集节拍。
	// 队满取向（#8 修订）：**丢弃该持久化任务**而非阻塞回压——宁漏库存、
	// 不丢通知：判决照常进内存态与 Gate 放行链路，丢弃计数走
	// opscopilot_noise_sink_drops_total{store}。PG 判决流在极端洪峰下可
	// 缺条目，Redis/PG 镜像非强一致。停机 drain 有上限超时（SINK_DRAIN），
	// 超时残量同样计丢弃。
	SinkQueue int           // OPS_NOISE_SINK_QUEUE 队列缓冲判决条数，默认 4096（水位 gauge opscopilot_noise_sink_queue）
	SinkBatch int           // OPS_NOISE_SINK_BATCH writer 单批落库条数阈值，默认 200
	SinkFlush time.Duration // OPS_NOISE_SINK_FLUSH writer 定时刷批周期（不满一批的最长滞留），默认 2s
	SinkDrain time.Duration // OPS_NOISE_SINK_DRAIN 停机 drain 上限超时（超时残量计丢弃），默认 5s
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
	// LeaseDuration 认领租约时长（#11 水平扩展 / ADR-012）：多实例并发消费
	// 同一张 ingest_queue 时，认领行带上 locked_until=now()+本值；过期行
	// 自动可被别人重领（at-least-once，下游幂等）。应 ≥ 单批最坏处理时长——
	// 默认 2m 覆盖正常 DB 下的默认批量（20 条），慢 DB 触发重领也只是幂等重做。
	LeaseDuration time.Duration // OPS_INGEST_LEASE_DURATION，默认 2m
}

// LeaderSection leader 选举与后台循环门禁（优化方案 #11 / ADR-012）：
// 拓扑判决链路单 owner——PG advisory lock（键 0x4F43504C "OCPL"）选主，
// leader 才跑 Host 采集/降噪判决链路/AlertPoller/Retention；事件链路
// （webhook 入队、IngestWorker、REST、通知、escalation、ChangePruner）全实例常驻。
// **降级路径（ADR 原文）**：无 DSN/pool 或 OPS_LEADER_ELECTION=off → 恒为
// leader，与单实例现状逐字节一致。
type LeaderSection struct {
	Election bool // OPS_LEADER_ELECTION=on/off，默认 on（有 DB 即竞选；off 或无 DB 恒 leader）
	// RetryInterval 竞选节拍 = 持锁期同连接复检（兼连接探针）节拍 =
	// failover 接管上界（ADR-012"新 leader 在下一次竞选节拍内拿到锁"）。
	RetryInterval time.Duration // OPS_LEADER_RETRY_INTERVAL，默认 5s；非法值启动失败
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

// RCASection 按需根因分析最小链路（优化方案 #12 / ADR-014）。
// 消费方唯一在 cmd/opscopilot/rca_orchestrator.go（装配显式注入，
// internal/rca 不 import 本包——边界纪律）。
type RCASection struct {
	Enabled bool // OPS_RCA on/off，默认 on（只读按需分析，无副作用面）
	// Auto 升级联动自动触发（OPS_RCA_AUTO，二期池波二 #4）。默认 off 保持
	// "只做按需"现状；on 时 critical 事件被值班升级成功催办后，异步跑一次
	// 该事件的 RCA 并落审计（实现见 cmd/opscopilot/rca_autotrigger.go）。
	// 依赖 Enabled——Validate 强制（自动触发复用按需链路，off 时无处借力）。
	Auto    bool
	Timeout time.Duration // OPS_RCA_TIMEOUT 单次分析超时（含取证），默认 10s
	Window  time.Duration // OPS_RCA_WINDOW 证据窗（T0 前多久内的变更算嫌疑），默认 30m
	Depth   int           // OPS_RCA_DEPTH 故障域邻域取证跳数，默认 2（1..10，与 GetTopology 上限同源）
	// MaxFindings 报告 findings 截断上限（OPS_RCA_MAX_FINDINGS，默认 200，
	// 二期池波二 #6）：编排器产报告时按"置信度优先 + 同档时序"截断，被截
	// 条数记入报告 findings_truncated 与审计；REST ?all=1 返回未截断全量；
	// llm_summarizer 的 prompt 证据行数同守此上限（llmgw 输入不超限）。
	MaxFindings int
}

// LLMSection llm-gateway（二期池波二 #3 / ADR-015）：通用 OpenAI-compatible
// chat 出口，唯一消费方是 RCA conclude 步（cmd 装配 Summarizer 注入）。
// Endpoint 空 = 整体禁用——conclude 维持 ADR-014 现状（pending、
// conclusion=null），行为与未接线逐字节一致；llm-gateway 是全系统唯一
// 出站 LLM 调用点（ADR-003 单出口纪律）。
type LLMSection struct {
	Endpoint string // OPS_LLM_ENDPOINT；空=禁用；LLM 网关 base URL（补全端点路径由 llmgw 拼接）
	// APIKey Bearer 密钥（Secret：绝不打日志/不入库，.env 已被 .gitignore
	// 保护）。装载时仅 TrimRight 行尾空白（.env 编辑常见的尾部空格/回车），
	// 其余原样承接（含首空白的密钥是合法值，不能被 TrimSpace 吃掉）。
	APIKey string // OPS_LLM_API_KEY
	// Model 模型名（OPS_LLM_MODEL）：Endpoint 非空时必填——**无默认值**，
	// 不预设任何厂商的模型名（厂商中立是 ADR-015 选型纪律）。
	Model     string        // OPS_LLM_MODEL
	Timeout   time.Duration // OPS_LLM_TIMEOUT 单次 chat 请求超时，默认 8s；须 ≤ RCA 超时 − 2s 预算（validateLLM）
	MaxTokens int           // OPS_LLM_MAX_TOKENS 补全 token 上限，默认 1024
}

// SessionSection RCA 复盘会话（二期池 #7 / 设计文档《sessionstore消费方与接线》
// S1+S2）：sessionstore 热态袋 + PG 真相表（migration 000018）+ llmgw 复盘问答。
// 开关 OPS_SESSION 默认 **off**——off 时不构造会话 Redis 客户端、不注册会话端点
// 消费方（端点显式 503），全链路零行为变化（与 OPS_RCA_AUTO #4 同款默认取向）。
// 无新增超时/长度键：轮次请求体复用 OPS 建单体上限（Metrics.IncidentBodyLimit），
// LLM 出站超时复用 OPS_LLM_TIMEOUT（拍板：不加配置项）。
type SessionSection struct {
	Enabled bool // OPS_SESSION on/off，默认 off
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
	Leader    LeaderSection
	Notify    NotifySection
	Retention RetentionSection
	Topology  TopologySection
	RCA       RCASection
	LLM       LLMSection
	Session   SessionSection
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
	// 二期池波二 #4：自动触发复用按需链路（RCAOrchestrator 只在 Enabled 时
	// 装配），AUTO=on 而 RCA=off 是必然而然的配置矛盾——fail-fast 拒绝启动，
	// 不让"配了自动却永远不触发"静默生效。
	if c.RCA.Auto && !c.RCA.Enabled {
		return fmt.Errorf("%s=on requires %s=on (auto trigger reuses the on-demand RCA chain; orchestrator is not wired when it is off)", EnvRCAAuto, EnvRCAEnabled)
	}
	// 二期池 #7（设计文档拍板④）：复盘会话的 prompt 必须携带该 incident 的
	// RCA findings（取证复用 RCAOrchestrator）——RCA off 时编排器不存在，
	// "会话已开但永远拼不出证据上下文"是配置矛盾，fail-fast 同 RCA.Auto 先例。
	if c.Session.Enabled && !c.RCA.Enabled {
		return fmt.Errorf("%s=on requires %s=on (the review session embeds RCA findings in every assistant prompt; the orchestrator is not wired when RCA is off)", EnvSessionEnabled, EnvRCAEnabled)
	}
	// W10-6 自动挂簇（OPS_AUTOATTACH）：挂点在 enforce 判决 new-incident
	// 联动路径——shadow/降噪关闭时永远不触发，"配了开关却静默无效"是配置
	// 矛盾，同款 OPS_RCA_AUTO/OPS_SESSION 的 fail-fast 纪律。
	if c.Noise.AutoAttach && !(c.Noise.Enabled && c.Noise.Mode == NoiseModeEnforce) {
		return fmt.Errorf("%s=on requires %s=on and %s=enforce (auto attach hooks the enforce new-incident path; it would silently never fire otherwise)",
			EnvAutoAttach, EnvNoiseShadow, EnvNoiseMode)
	}
	if err := c.validateLLM(); err != nil {
		return err
	}
	return c.Redis.Cache.Validate()
}

// validateLLM llm-gateway 结构性校验（ADR-015）——只在 Endpoint 非空
// （= 已启用）时生效；Endpoint 空维持 ADR-014 禁用现状，不新增任何约束。
// 三条纪律：
//  1. Endpoint 必须是 http(s) 绝对 URL（不含 userinfo 凭证）；
//  2. Model 必填——不给厂商默认模型名，"配了出口却没指定模型"是配置事故
//     不是可猜的默认；
//  3. **超时预算**：LLM 超时 + LLMBudgetReserve ≤ RCA 总超时——前四步
//     （取证 IO + 规则计算）必须留有硬余量，否则慢 LLM 会把整次分析拖到
//     504，违背"LLM 挂掉绝不 5xx"的 fail-open 承诺。默认 8s + 2s = RCA
//     10s 恰好压线通过；收紧 OPS_RCA_TIMEOUT 或放大 OPS_LLM_TIMEOUT 到
//     破坏预算的组合启动即拒（聚合报错路径）。
func (c *Config) validateLLM() error {
	if c.LLM.Endpoint == "" {
		return nil
	}
	if err := ValidateLLMEndpoint(c.LLM.Endpoint); err != nil {
		return err
	}
	if c.LLM.Model == "" {
		return errors.New(EnvLLMModel + " is required when " + EnvLLMEndpoint + " is set (no vendor default model)")
	}
	if c.LLM.Timeout+LLMBudgetReserve > c.RCA.Timeout {
		return fmt.Errorf("%s (%s) plus %s budget reserve must fit within %s (%s): the four rule steps and evidence IO need guaranteed time",
			EnvLLMTimeout, c.LLM.Timeout, LLMBudgetReserve, EnvRCASTimeout, c.RCA.Timeout)
	}
	return nil
}

// ValidateLLMEndpoint llm-gateway base URL 合法性（装载期 Validate 汇入；
// llmgw.New 为守住"不 import config"的边界纪律内联同款规则）：http/https
// 绝对 URL、带 host、不含 userinfo 凭证。
func ValidateLLMEndpoint(raw string) error {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("%s must be an absolute http(s) URL, got %q", EnvLLMEndpoint, raw)
	}
	if u.User != nil {
		return fmt.Errorf("%s must not embed credentials (userinfo) in the URL", EnvLLMEndpoint)
	}
	return nil
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

	// 判决异步落库队列默认值（优化方案 #8，队满丢弃取向）。取值依据：
	// "正常规模几乎不触发丢弃路径"——百台级一轮数百告警、30s 节拍，writer
	// 每 2s 刷一批（单条判决写 PG 毫秒级），健康 DB 下队列深度常态≈0；
	// 4096 条缓冲 ≈ 20 个满批，等价于 DB 停摆近半分钟仍不触丢弃。落库是
	// 评估数据链不是通知链路——洪峰下丢持久化保节拍（宁漏库存不丢通知），
	// 丢弃面看 opscopilot_noise_sink_drops_total。drain 5s 对齐停机预算与
	// PG 语句超时（pgSinkTimeout）。
	DefaultNoiseSinkQueue = 4096
	DefaultNoiseSinkBatch = 200
	DefaultNoiseSinkFlush = 2 * time.Second
	DefaultNoiseSinkDrain = 5 * time.Second

	// Noise 模式枚举。
	NoiseModeShadow  = "shadow"
	NoiseModeEnforce = "enforce"

	// DefaultAutoAttach W10-6 簇→事件自动挂簇（OPS_AUTOATTACH）默认 off——
	// 新增"判决→建单+挂簇+审计"副作用面，显式开启才生效（off 零行为变化，
	// 与 OPS_RCA_AUTO/OPS_SESSION 同款默认取向）。
	DefaultAutoAttach = false

	DefaultPullInterval = 30 * time.Second

	DefaultIngestInterval            = 5 * time.Second
	DefaultIngestBatch               = 20
	DefaultIngestRateLimit           = 50 // 5 分钟内最多建 50 单，其余进聚合单
	DefaultIngestRateWindow          = 5 * time.Minute
	DefaultIngestBatchPerItem        = 15 * time.Second // 批消费超时 15s/条（第七轮 M-2）
	DefaultAlertBodyLimit      int64 = 4 << 20          // 4MiB：入站 webhook 与拉取响应共用的单一定义
	DefaultIngestLeaseDuration       = 2 * time.Minute  // 认领租约（#11/ADR-012）：过期可被重领

	// leader 选举（#11/ADR-012）：默认开（有 DB 即竞选）；竞选/复检节拍 5s
	// ——failover 接管上界就锁在"一个节拍"内（验收标准 3 的 15s 限时余量充足）。
	DefaultLeaderElection      = true
	DefaultLeaderRetryInterval = 5 * time.Second

	DefaultEscalationAfter    = 15 * time.Minute
	DefaultEscalationInterval = 60 * time.Second

	DefaultIncidentRetention = 90 * 24 * time.Hour // D8 决策 C：resolved 后 90d 归档
	DefaultChangeRetention   = 7 * 24 * time.Hour  // W9-5：变更事件保留窗

	DefaultChangePruneInterval = time.Hour

	// 按需 RCA 最小链路默认值（优化方案 #12 / ADR-014）。证据窗 30m 对齐
	// "告警前 30 分钟内有没有变更"的经典取证口径（change.go 文件头）；
	// 超时 10s 覆盖内存态全图 AsOf + 邻域裁剪的最坏路径；深度 2 跳足以
	// 圈住"服务→宿主→共享依赖"级别的故障域，且与 maxTopologyDepth=10 的
	// 上限校验同源收口。默认 on：只读按需分析，无副作用面。
	DefaultRCAEnabled = true
	DefaultRCATimeout = 10 * time.Second
	DefaultRCAWindow  = 30 * time.Minute
	DefaultRCADepth   = 2

	// DefaultRCAAuto 自动触发（二期池波二 #4）默认 off——保持 ADR-014"本期只
	// 做按需"的现状，升级联动跑 RCA 是新增副作用面（异步分析 + 写审计），
	// 显式 OPS_RCA_AUTO=on 才开启。
	DefaultRCAAuto = false

	// DefaultRCAMaxFindings findings 截断上限（二期池波二 #6）——ADR-014
	// "findings 回显上限 200 条"的可配置化：数值与转正前 rest_rca.go 的
	// 硬编码 rcaFindingsLimit=200 逐字节一致，默认行为零漂移。200 条覆盖
	// 绝大多数真实故障域的证据链规模；超限按置信度+时序截断，?all=1 取全量。
	DefaultRCAMaxFindings = 200

	// llm-gateway 默认值（二期池波二 #3 / ADR-015）。**Endpoint/Model 无默认**
	// ——默认禁用（Endpoint 空 = conclude 维持 pending 现状，行为与 ADR-014
	// 逐字节一致）且不做厂商中立性倒退的模型名预设。超时 8s 是预算纪律
	// （LLM ≤ RCA − LLMBudgetReserve）在默认 RCA=10s 下的最大合法取值：
	// 留 2s 硬预算给取证 IO 与前四步规则计算，LLM 再慢也只拖垮 conclude
	// 一步（fail-open 回 pending）。
	DefaultLLMTimeout   = 8 * time.Second
	DefaultLLMMaxTokens = 1024

	// DefaultSessionEnabled 复盘会话（二期池 #7，OPS_SESSION）默认 off——
	// 新增交互面（REST 读写 + LLM 出站 + PG 新表），全链路零行为变化直到显式
	// 开启；off 时会话端点显式 503（可诊断，对齐 RCA #12 取向）。
	DefaultSessionEnabled = false

	// LLMBudgetReserve RCA 超时里给"取证 IO + 前四步规则计算"预留的硬
	// 预算（ADR-015）：约束 OPS_LLM_TIMEOUT ≤ OPS_RCA_TIMEOUT − 2s。
	// 前四步实测毫秒级（内存邻域 + 进程内取证直调），2s 覆盖慢 DB 下
	// GetTopology/GetRecentChanges 的最坏路径仍富余。
	LLMBudgetReserve = 2 * time.Second

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
	c.Noise.AutoAttach = DefaultAutoAttach
	c.Noise.DedupWindow = DefaultDedupWindow
	c.Noise.SinkQueue = DefaultNoiseSinkQueue
	c.Noise.SinkBatch = DefaultNoiseSinkBatch
	c.Noise.SinkFlush = DefaultNoiseSinkFlush
	c.Noise.SinkDrain = DefaultNoiseSinkDrain
	c.Pull.Interval = DefaultPullInterval
	c.Ingest.Interval = DefaultIngestInterval
	c.Ingest.Batch = DefaultIngestBatch
	c.Ingest.RateLimit = DefaultIngestRateLimit
	c.Ingest.RateWindow = DefaultIngestRateWindow
	c.Ingest.BatchTimeoutPerItem = DefaultIngestBatchPerItem
	c.Ingest.AlertBodyLimit = DefaultAlertBodyLimit
	c.Ingest.LeaseDuration = DefaultIngestLeaseDuration
	c.Leader.Election = DefaultLeaderElection
	c.Leader.RetryInterval = DefaultLeaderRetryInterval
	c.Notify.EscalationAfter = DefaultEscalationAfter
	c.Notify.EscalationInterval = DefaultEscalationInterval
	c.Retention.IncidentWindow = DefaultIncidentRetention
	c.Retention.IncidentEnabled = true
	c.Topology.ChangeWindow = DefaultChangeRetention
	c.Topology.ChangeEnabled = true
	c.Topology.ChangePruneInterval = DefaultChangePruneInterval
	c.RCA.Enabled = DefaultRCAEnabled
	c.RCA.Auto = DefaultRCAAuto
	c.RCA.Timeout = DefaultRCATimeout
	c.RCA.Window = DefaultRCAWindow
	c.RCA.Depth = DefaultRCADepth
	c.RCA.MaxFindings = DefaultRCAMaxFindings
	c.LLM.Timeout = DefaultLLMTimeout
	c.LLM.MaxTokens = DefaultLLMMaxTokens
	c.Session.Enabled = DefaultSessionEnabled
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
