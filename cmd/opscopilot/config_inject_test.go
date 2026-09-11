// 测试侧配置注入门面（#2/#10）：装配/引擎/连接器构造函数改为参数注入后，
// 测试不再用 t.Setenv 控制行为——统一从这里拿 config.Defaults() 基线，
// 按需覆写字段。config.Defaults 的 Redis 占位地址仅供直接构造，
// 不影响 env 装载通道的"双 Redis 必填"约束（那条路径用 LoadFrom 测）。
package main

import "opscopilot/internal/config"

// testTenant 测试用租户（与 config 默认一致；取代已删除的包级 DefaultTenant）。
const testTenant = config.DefaultTenant

// testAssemblyConfig 构造装配测试基线：指定写密钥、无 DB、降噪/开关均为默认。
func testAssemblyConfig(token string) *config.Config {
	c := config.Defaults()
	c.Security.WebhookToken = token
	return c
}
