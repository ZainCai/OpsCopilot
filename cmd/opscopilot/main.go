// OpsCopilot all-in-one 入口（M1）。
// 职责：加载配置 -> 校验双 Redis 实例约束 -> 装配 W3 组件 -> 启动 HTTP 服务。
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

	"opscopilot/internal/config"
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
	// 连接器注册进 Host 的装配在 W4 降噪接入时一起做（需要告警消费方就位）。
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

	addr := os.Getenv("OPS_LISTEN_ADDR")
	if addr == "" {
		addr = defaultListenAddr
	}
	srv := &http.Server{
		Addr:              addr,
		Handler:           asm.Handler(),
		ReadHeaderTimeout: 5 * time.Second, // slow-loris 防御
	}

	// 优雅停机：SIGINT/SIGTERM 触发后最多等 10s 让在途请求落地。
	done := make(chan struct{})
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
		<-sig
		logger.Printf("shutdown signal received, draining...")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		close(done)
	}()

	logger.Printf("W3 assembly ready: POST %s (change events) | GET /healthz | listening on %s",
		changeWebhookPath, addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Printf("http server: %v", err)
		os.Exit(1)
	}
	<-done
	logger.Printf("bye")
}
