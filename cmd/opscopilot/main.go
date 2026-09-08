// OpsCopilot all-in-one 入口（M1 骨架）。
// 职责：加载配置 -> 校验双 Redis 实例约束 -> 装配会话存储（P1-1 落位）。
// 各业务模块随 M1 迭代接入（连接器 -> 存储 -> 降噪）。
package main

import (
	"fmt"
	"os"

	"opscopilot/internal/config"
)

func main() {
	// M1 第 0 周：配置骨架。双 Redis 实例约束（v1.2 C2/P1-1）在启动期强制。
	cfg := &config.Config{
		RedisAlert: config.RedisConfig{Addr: os.Getenv("REDIS_ALERT_ADDR"), Role: config.RedisAlert},
		RedisCache: config.RedisConfig{Addr: os.Getenv("REDIS_CACHE_ADDR"), Role: config.RedisCache},
	}
	if err := cfg.Validate(); err != nil {
		fmt.Fprintln(os.Stderr, "config invalid:", err)
		os.Exit(1)
	}
	fmt.Println("opscopilot all-in-one: config validated (dual-redis enforced)")
	// TODO(M1): redis client 装配 + sessionstore.New(alertClient, "alert")（P1-1）
	// TODO(M1): transport.DialInProcess 承载进程内 gRPC（P1-2）
}
