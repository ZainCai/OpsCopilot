// Package config 定义全局配置。
// 双 Redis 实例架构（v1.2 C2）：告警实例（持久化）与缓存实例（可逐出）物理隔离。
package config

import "errors"

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

// Validate 校验实例角色合法性与地址必填。
// 告警实例必须启用 AOF everysec 且禁用逐出——这是 v1.2 C2 的部署约束，
// 在配置层强制，防止运维改错。
// 全局审查 G1：两个实例的地址都必须非空（与"缺一拒绝启动"的约定一致）。
func (c *RedisConfig) Validate() error {
	switch c.Role {
	case RedisAlert:
		if c.Addr == "" {
			return errors.New("alert redis: addr is required")
		}
		return nil
	case RedisCache:
		if c.Addr == "" {
			return errors.New("cache redis: addr is required")
		}
		return nil
	default:
		return errors.New("redis role must be 'alert' or 'cache', got: " + string(c.Role))
	}
}

// Config all-in-one 进程配置。
type Config struct {
	// 双实例：两个都必须配置，缺一拒绝启动。
	RedisAlert RedisConfig
	RedisCache RedisConfig
}

// Validate 全局校验：双实例角色不得互换、地址不得相同（防止偷偷合并回单实例）、
// 两个实例地址都必须非空（G1）。
func (c *Config) Validate() error {
	if c.RedisAlert.Role != RedisAlert || c.RedisCache.Role != RedisCache {
		return errors.New("redis instances misconfigured: alert/cache roles are fixed")
	}
	if c.RedisAlert.Addr == c.RedisCache.Addr {
		return errors.New("alert and cache redis must be physically separate instances")
	}
	if err := c.RedisAlert.Validate(); err != nil {
		return err
	}
	return c.RedisCache.Validate()
}
