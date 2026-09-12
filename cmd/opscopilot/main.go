// OpsCopilot all-in-one 入口（M1）。
// 职责：装载并校验配置（一次 config.Load，#2 收敛）-> 装配 W3 组件 ->
// 装配连接器宿主 -> 启动 HTTP 服务。
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"opscopilot/internal/config"
	"opscopilot/internal/credential"
)

// newHTTPServer 构造 HTTP 服务（超时口径唯一来源：cfg.Metrics，
// 默认值见 config.DefaultHTTP*Timeout——SSE 的稳定性依赖 WriteTimeout
// 与响应级清除（rest_stream.go）的配合，任一侧被删都会让长连接在 30s 后
// 静默断掉，测试断言这三个超时不被误删）。
//
// **刻意不设 ReadTimeout**：Go 的 http.Server 会把读截止时间覆盖到整个请求
// （不只是读头），而长连接期间后台读超时会被当作读错误 → 取消 request
// context → SSE 在 ReadTimeout 到点时被服务端主动断开。挡 slow-loris 用
// ReadHeaderTimeout 就够，ReadTimeout 在这里只有副作用。
func newHTTPServer(m config.MetricsSection, addr string, h http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: m.HTTPReadHeaderTimeout,
		WriteTimeout:      m.HTTPWriteTimeout,
		IdleTimeout:       m.HTTPIdleTimeout,
	}
}

func main() {
	// 双 Redis 实例约束（v1.2 C2/P1-1）在启动期强制。配置一次装载：
	// 全仓 OPS_*/REDIS_* 只在这里读取；非法值聚合报错、拒绝启动（#2）。
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	logger := log.New(os.Stdout, "opscopilot ", log.LstdFlags|log.LUTC)
	logger.Printf("config validated (dual-redis enforced)")

	// W3 装配：拓扑引擎 + 发现入口（TopologySink）+ 变更事件库 + webhook。
	// TopologySink 同时实现 connector.Sink——W4 起它既是 webhook 的变更库，
	// 也是 Host 采集结果的投递终点。
	asm, err := NewAssembly(logger, cfg)
	if err != nil {
		logger.Printf("assembly failed: %v", err)
		os.Exit(1)
	}
	webhookToken := cfg.Security.WebhookToken
	if webhookToken == "" {
		logger.Printf("WARNING: OPS_WEBHOOK_TOKEN not set — %s is UNAUTHENTICATED. "+
			"Default bind is loopback-only; set the token or put an authenticating reverse proxy in front before any non-loopback exposure",
			changeWebhookPath)
	}

	// W6-0 评估环境：静态拓扑边挂起队列（故障域聚合的因果链声明）。
	// 端点节点经发现进图后自动落边（见 AttachStaticEdges）。
	edgeInputs, err := parseStaticEdges(cfg.Topology.Edges)
	if err != nil {
		logger.Printf("assembly failed: %v", err)
		os.Exit(1)
	}
	asm.Sink.AttachStaticEdges(edgeInputs)

	// W4-1.1 接线：凭证库 + 连接器宿主。连接器按配置按需注册（见 connectors.go），
	// 未配置任何数据源时 Host 空转，topology + webhook 仍照常服务。
	creds := credential.NewStore()
	host, registered, err := newConnectorHost(logger, creds, cfg.Connector, cfg.Tenant)
	if err != nil {
		logger.Printf("connector assembly failed: %v", err)
		os.Exit(1)
	}

	// W6-1 真相源：OPS_DB_DSN 设置时启用 TimescaleDB 出口（簇 upsert +
	// 逐告警 Verdict），与 Redis 镜像双写。DB 不可达不阻塞启动——
	// 运维通过日志与失败计数观察；ADR-001 语义下库缺席只影响重建能力。
	var noiseRDB *redis.Client
	if asm.Noise != nil {
		// W9-5：显式给超时（默认值不动声色的后果见 noise_store_redis.go 文件头）。
		// 客户端超时只管单次 round-trip，整体上界由 sink 的 ctx deadline 兜底。
		//
		// **ContextTimeoutEnabled 必须为 true**：go-redis v9 默认（false）会把
		// 命令收到的 ctx 换成 context.Background()（见 baseClient.context），
		// 我们设的 ctx 上界会被静默丢弃——那次"补超时"就成了假修复。
		noiseRDB = redis.NewClient(&redis.Options{
			Addr:                  cfg.Redis.Alert.Addr,
			DialTimeout:           redisDialTimeout,
			ReadTimeout:           redisIOTimeout,
			WriteTimeout:          redisIOTimeout,
			ContextTimeoutEnabled: true,
		})
		redisSink := NewRedisClusterSink(noiseRDB, cfg.Tenant)
		var pgSink *PGClusterSink
		if cfg.DB.DSN != "" {
			sink, err := NewPGClusterSink(context.Background(), cfg.DB.DSN, cfg.Tenant)
			if err != nil {
				logger.Printf("WARNING: pg sink unavailable (redis mirror only): %v", err)
			} else {
				pgSink = sink
				defer pgSink.Close()
				asm.Noise.SetRecordSink(&multiRecordSink{a: redisSink, b: pgSink})
				asm.Noise.SetVerdictSink(pgSink)
				logger.Printf("noise persistence: redis mirror + timescaledb truth source")
			}
		} else {
			asm.Noise.SetRecordSink(redisSink)
			logger.Printf("noise persistence: redis mirror only (set OPS_DB_DSN for truth source)")
		}
		// W9-1 闸门接线在装配层（attachNoiseGate）：enforce 挂闸门 + 计数
		// 真相源（共享池），shadow 只打日志。此处只负责簇恢复。
		if recs, err := redisSink.LoadClusters(context.Background()); err != nil {
			logger.Printf("noise cluster restore skipped (redis unreachable): %v", err)
		} else if len(recs) > 0 {
			if err := asm.Noise.RestoreFrom(recs); err != nil {
				logger.Printf("WARNING: noise cluster restore failed (starting empty): %v", err)
			} else {
				logger.Printf("noise clusters restored from redis mirror: %d", len(recs))
			}
		}
	}

	// D1 决策 C（安全门禁）：非回环监听 + 无写密钥 = 任何能到达端口的人都能
	// 建单/关单/合并。此前只打一行 WARNING，运维很容易漏看——改为启动失败，
	// 除非显式声明 OPS_ALLOW_UNAUTHENTICATED=on（本地联调逃生门，禁止用于生产）。
	addr := cfg.Security.ListenAddr
	if err := checkListenSecurity(addr, webhookToken, cfg.Security.AllowUnauthenticated); err != nil {
		logger.Printf("FATAL: %v", err)
		logger.Printf("  修复方式：设置 OPS_WEBHOOK_TOKEN；或绑定 127.0.0.1；" +
			"或显式设置 OPS_ALLOW_UNAUTHENTICATED=on（仅限本机联调，严禁用于生产）。")
		os.Exit(1)
	}
	srv := newHTTPServer(cfg.Metrics, addr, asm.Handler())

	// runCtx 同时管两件事的生命周期：Host 调度循环与 credential 周期清扫。
	// 优雅停机信号先 cancel 再 drain HTTP，采集循环在在途请求落地前先退出。
	runCtx, runCancel := context.WithCancel(context.Background())

	// Host.Run：周期"健康→采集→发现→投递"，结果进 TopologySink（拓扑 + 告警计数）。
	go func() {
		if err := host.Run(runCtx, asm.Sink); err != nil && !errors.Is(err, context.Canceled) {
			logger.Printf("connector host stopped: %v", err)
		}
	}()

	// W9 链路 A：外部导入队列消费者。autoCreate off（影子期默认）时 Run 立即
	// 返回——消息只堆积不建单，转正后开启即可回放历史消息。
	if asm.Worker != nil {
		go asm.Worker.Run(runCtx)
	}

	// W11 链路 A 拉取侧：定时从数据源拉告警入队（未启用则 nil）。
	if asm.Poller != nil {
		go asm.Poller.Run(runCtx)
	}

	// W9-3 值班升级：周期扫描未 ack 超时事件并升级一次（未启用则 nil）。
	if asm.Escalation != nil {
		go asm.Escalation.Run(runCtx)
	}

	// D8 决策 C：事件保留策略 —— resolved 满 N 天归档到 incident_archive
	// （事件本体 + 簇 + 审计 打包成 JSONB，不丢任何上下文）。默认 90d，
	// OPS_INCIDENT_RETENTION=off 可关闭；配置非法在 config.Load 即启动失败。
	if cfg.Retention.IncidentEnabled {
		if asm.pool == nil {
			logger.Printf("  retention: configured but no DB (set OPS_DB_DSN) — disabled")
		} else {
			logger.Printf("  retention: ON (archive resolved incidents older than %s)", cfg.Retention.IncidentWindow)
			go NewRetentionSweeper(asm.pool, cfg.Tenant, cfg.Retention.IncidentWindow, logger.Printf).Run(runCtx)
		}
	} else {
		logger.Printf("  retention: OFF (set OPS_INCIDENT_RETENTION, e.g. 90d, to enable)")
	}

	// W9-5（第八轮审核 C7）：变更事件库的保留窗清理。ChangeStore 是进程内
	// map、POST /api/v1/changes 公开可写——不清理就是一条无人察觉的内存
	// 增长路径。默认 7d / 每 1h；配置非法在 config.Load 即启动失败（fail-fast）。
	changePruner := NewChangePrunerFromConfig(asm.Changes, cfg.Topology, logger.Printf)
	go changePruner.Run(runCtx)

	// credential 周期清扫（C9 收尾）：过期条目不再是"删除前一直占内存"。
	// 周期 10 分钟——清扫是幂等原语，频率只需远小于凭证最小有效期。
	go func() {
		ticker := time.NewTicker(10 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-runCtx.Done():
				return
			case <-ticker.C:
				if n := creds.SweepExpired(time.Now()); n > 0 {
					logger.Printf("credential sweep: removed %d expired entries", n)
				}
			}
		}
	}()

	// 优雅停机：SIGINT/SIGTERM 触发后最多等 10s 让在途请求落地。
	done := make(chan struct{})
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
		<-sig
		logger.Printf("shutdown signal received, draining...")
		runCancel()
		// 优化方案 #8（有限 drain）：判决异步落库队列排空至多等
		// OPS_NOISE_SINK_DRAIN（默认 5s），超时残量计 sink_drops 后放行
		// 停机流程——停机预算优先于库存完整；停机中到达的新判决直接计
		// 丢弃，不再同步兜底（宁漏库存不丢通知）。位置不变：必须早于
		// Redis 客户端与共享池关闭（SSE 先关的理由见下一段注释）。
		if asm.Noise != nil {
			asm.Noise.StopVerdictWriter()
		}
		// SSE 是长连接：http.Server.Shutdown 只等"连接变空闲"，**不会**取消
		// 在途请求的 ctx。若先 Shutdown，每个在线控制台的 SSE 连接都要等到
		// ctx 超时才退出 → 停机固定拖满 10s。故先关广播器：Hub 一关，SSE
		// handler 立即返回、连接变空闲，Shutdown 才能迅速收尾。
		if asm.Events != nil {
			asm.Events.Close()
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		if asm.GRPC != nil {
			asm.GRPC.Stop() // R6：DialInProcess 的 Serve goroutine 只能靠 Stop 退出
		}
		if noiseRDB != nil {
			_ = noiseRDB.Close() // 第四轮扫描 F4：落库连接随停机关闭
		}
		asm.Close() // 释放事件 Store/队列/审计共享的连接池
		close(done)
	}()

	// 启动可见性：明确当前 main 装配的 W 阶段，
	// 避免运维把"骨架就绪"误读为"产品就绪"。
	logger.Printf("=== M1 stage: W4-1.5 (topology + change webhook + connector host + shadow noise + cluster persistence) ===")
	logger.Printf("  wired:    topology builder + topology sink + change store + change webhook + credential store(sweep) + connector host + shadow noise + cluster mirror(redis)")
	if len(registered) > 0 {
		logger.Printf("  connectors: %v (interval %s, conn timeout %s)", registered, cfg.Connector.Interval, cfg.Connector.OpTimeout)
	} else {
		logger.Printf("  connectors: none (set OPS_PROM_URL / OPS_AZURE_SUBSCRIPTION_ID+OPS_AZURE_TOKEN to enable)")
	}
	if asm.Noise == nil {
		logger.Printf("  noise: OFF (OPS_NOISE_SHADOW=off)")
	} else if asm.Noise.Mode() == ModeEnforce {
		logger.Printf("  noise: enforce (WouldSuppress 真拦截 + 放行通知; window %s; 回退=OPS_NOISE_MODE=shadow)",
			cfg.Noise.Window)
	} else {
		logger.Printf("  noise: shadow (alerts annotated, NOT suppressed; window %s)", cfg.Noise.Window)
	}
	if asm.Ingest != nil {
		logger.Printf("  ingest: ON (POST /api/v1/ingest/{alertmanager,webhook}; auto-create %s)",
			onOffLabel(cfg.Ingest.AutoCreate))
	} else {
		logger.Printf("  ingest: OFF (set OPS_DB_DSN to enable external import queue)")
	}
	if asm.Poller != nil {
		logger.Printf("  alert pull: ON (source %s, origin %s)", asm.Poller.Source.Name(), asm.Poller.Origin)
	} else {
		logger.Printf("  alert pull: OFF (set OPS_PULL_ALERTS=on + OPS_PROM_URL to enable)")
	}
	if asm.Escalation != nil {
		logger.Printf("  escalation: ON (unacked > %v re-notify once, scan %v)",
			asm.Escalation.After, asm.Escalation.Interval)
	} else {
		logger.Printf("  escalation: OFF (set OPS_ESCALATION=on to enable unacked-timeout re-notify)")
	}
	logger.Printf("  events: 双链路（人工建单 POST /api/v1/incidents ∥ 外部导入 push/pull）+ SSE 实时推送 + 控制台事件页 /console")
	logger.Printf("  metrics: GET /metrics (告警 fired→verdict / fired→通知 延迟分位 + 判决/闸门计数 + 内存有界结构规模/淘汰 opscopilot_mem_*)")
	logger.Printf("  not wired (M2): rca, sessionstore（预留件，见 README「预留未接线的组件」；notify 闸门随 W9-1、渠道随 W9-2、/metrics 随 W9-4 已接线）")
	logger.Printf("POST %s (change events) | GET /healthz | listening on %s",
		changeWebhookPath, addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Printf("http server: %v", err)
		os.Exit(1)
	}
	<-done
	logger.Printf("bye")
}

// onOffLabel 启动日志用的开关文案（决策 2 / R8：auto-create）。
func onOffLabel(on bool) string {
	if on {
		return "ON"
	}
	return "off (shadow: queue only)"
}
