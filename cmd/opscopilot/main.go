// OpsCopilot all-in-one 入口（M1）。
// 职责：加载配置 -> 校验双 Redis 实例约束 -> 装配 W3 组件 -> 装配连接器宿主 -> 启动 HTTP 服务。
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"opscopilot/internal/config"
	"opscopilot/internal/credential"
)

// defaultListenAddr HTTP 监听地址默认值；可用环境变量 OPS_LISTEN_ADDR 覆盖。
// 默认只绑回环（全局审查 S1）：进程暴露了可写的变更 webhook，
// 无鉴权服务不得默认监听全部网络接口。
const defaultListenAddr = "127.0.0.1:8080"

func main() {
	// 双 Redis 实例约束（v1.2 C2/P1-1）在启动期强制。
	cfg := &config.Config{
		RedisAlert: config.RedisConfig{Addr: os.Getenv("REDIS_ALERT_ADDR"), Role: config.RedisAlert},
		RedisCache: config.RedisConfig{Addr: os.Getenv("REDIS_CACHE_ADDR"), Role: config.RedisCache},
	}
	if err := cfg.Validate(); err != nil {
		fmt.Fprintln(os.Stderr, "config invalid:", err)
		os.Exit(1)
	}

	logger := log.New(os.Stdout, "opscopilot ", log.LstdFlags|log.LUTC)
	logger.Printf("config validated (dual-redis enforced)")

	// W3 装配：拓扑引擎 + 发现入口（TopologySink）+ 变更事件库 + webhook。
	// TopologySink 同时实现 connector.Sink——W4 起它既是 webhook 的变更库，
	// 也是 Host 采集结果的投递终点。
	webhookToken := os.Getenv("OPS_WEBHOOK_TOKEN")
	asm, err := NewAssembly(logger, webhookToken)
	if err != nil {
		logger.Printf("assembly failed: %v", err)
		os.Exit(1)
	}
	if webhookToken == "" {
		logger.Printf("WARNING: OPS_WEBHOOK_TOKEN not set — %s is UNAUTHENTICATED. "+
			"Default bind is loopback-only; set the token or put an authenticating reverse proxy in front before any non-loopback exposure",
			changeWebhookPath)
	}

	// W6-0 评估环境：静态拓扑边挂起队列（故障域聚合的因果链声明）。
	// 端点节点经发现进图后自动落边（见 AttachStaticEdges）。
	edgeInputs, err := parseStaticEdges(os.Getenv("OPS_TOPOLOGY_EDGES"))
	if err != nil {
		logger.Printf("assembly failed: %v", err)
		os.Exit(1)
	}
	asm.Sink.AttachStaticEdges(edgeInputs)

	// W4-1.1 接线：凭证库 + 连接器宿主。连接器按 env 按需注册（见 connectors.go），
	// 未配置任何数据源时 Host 空转，topology + webhook 仍照常服务。
	creds := credential.NewStore()
	host, registered, err := newConnectorHost(logger, creds)
	if err != nil {
		logger.Printf("connector assembly failed: %v", err)
		os.Exit(1)
	}

	// W6-1 真相源：OPS_DB_DSN 设置时启用 TimescaleDB 出口（簇 upsert +
	// 逐告警 Verdict），与 Redis 镜像双写。DB 不可达不阻塞启动——
	// 运维通过日志与失败计数观察；ADR-001 语义下库缺席只影响重建能力。
	var noiseRDB *redis.Client
	if asm.Noise != nil {
		noiseRDB = redis.NewClient(&redis.Options{Addr: os.Getenv("REDIS_ALERT_ADDR")})
		redisSink := NewRedisClusterSink(noiseRDB, DefaultTenant)
		if dsn := os.Getenv("OPS_DB_DSN"); dsn != "" {
			pgSink, err := NewPGClusterSink(context.Background(), dsn, DefaultTenant)
			if err != nil {
				logger.Printf("WARNING: pg sink unavailable (redis mirror only): %v", err)
			} else {
				defer pgSink.Close()
				asm.Noise.SetRecordSink(&multiRecordSink{a: redisSink, b: pgSink})
				asm.Noise.SetVerdictSink(pgSink)
				logger.Printf("noise persistence: redis mirror + timescaledb truth source")
			}
		} else {
			asm.Noise.SetRecordSink(redisSink)
			logger.Printf("noise persistence: redis mirror only (set OPS_DB_DSN for truth source)")
		}
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

	addr := os.Getenv("OPS_LISTEN_ADDR")
	if addr == "" {
		addr = defaultListenAddr
	}
	srv := &http.Server{
		Addr:              addr,
		Handler:           asm.Handler(),
		ReadHeaderTimeout: 5 * time.Second, // slow-loris 防御
	}

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
		logger.Printf("  connectors: %v (interval 30s, conn timeout 30s)", registered)
	} else {
		logger.Printf("  connectors: none (set OPS_PROM_URL / OPS_AZURE_SUBSCRIPTION_ID+OPS_AZURE_TOKEN to enable)")
	}
	if asm.Noise == nil {
		logger.Printf("  noise: shadow mode OFF (OPS_NOISE_SHADOW=off)")
	} else {
		logger.Printf("  noise: shadow mode ON (alerts annotated, NOT suppressed; window %s)", noiseWindowForLog())
	}
	if asm.Ingest != nil {
		logger.Printf("  ingest: ON (POST /api/v1/ingest/{alertmanager,webhook}; auto-create %s)",
			autoCreateLabel())
	} else {
		logger.Printf("  ingest: OFF (set OPS_DB_DSN to enable external import queue)")
	}
	if asm.Poller != nil {
		logger.Printf("  alert pull: ON (source %s, origin %s)", asm.Poller.Source.Name(), asm.Poller.Origin)
	} else {
		logger.Printf("  alert pull: OFF (set OPS_PULL_ALERTS=on + OPS_PROM_URL to enable)")
	}
	logger.Printf("  events: 双链路（人工建单 POST /api/v1/incidents ∥ 外部导入 push/pull）+ SSE 实时推送 + 控制台事件页 /console")
	logger.Printf("  not wired (W5+): sessionstore, /metrics")
	logger.Printf("POST %s (change events) | GET /healthz | listening on %s",
		changeWebhookPath, addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Printf("http server: %v", err)
		os.Exit(1)
	}
	<-done
	logger.Printf("bye")
}

// noiseWindowForLog 启动日志用：当前生效的降噪窗口。
// 默认值取 defaultNoiseWindow 常量——别处改默认值这里不会说谎（F4）。
func noiseWindowForLog() string {
	if raw := os.Getenv("OPS_NOISE_WINDOW"); raw != "" {
		return raw
	}
	return defaultNoiseWindow.String()
}

// autoCreateLabel 启动日志用：外部导入自动建单开关状态（决策 2 / R8）。
func autoCreateLabel() string {
	if strings.EqualFold(strings.TrimSpace(os.Getenv("OPS_INCIDENT_AUTOCREATE")), "on") {
		return "ON"
	}
	return "off (shadow: queue only)"
}
