// 监听安全门禁（D1 决策 C）：非回环监听 + 无写密钥 = 无鉴权写路径。
// 此前只在启动日志打一行 WARNING，运维很容易漏看——改为启动失败，
// OPS_ALLOW_UNAUTHENTICATED=on 作为本地联调的显式逃生门。
package main

import (
	"fmt"
	"net"
	"strings"
)

// checkListenSecurity 校验"监听地址 × 写密钥"组合是否允许启动。
//
// 规则：监听地址非回环（0.0.0.0 / 具体网卡 IP）且未配置 OPS_WEBHOOK_TOKEN
// 时拒绝启动——任何能到达该端口的人都能建单/关单/合并。
// token 非空、或 allowUnauthenticated=true（OPS_ALLOW_UNAUTHENTICATED=on）
// 时放行；后者仅供本机联调，禁止用于生产。
func checkListenSecurity(addr, token string, allowUnauthenticated bool) error {
	if strings.TrimSpace(token) != "" || allowUnauthenticated {
		return nil
	}
	host, _, err := net.SplitHostPort(strings.TrimSpace(addr))
	if err != nil {
		host = strings.TrimSpace(addr) // 形如只写了 host：按原样判断
	}
	if host == "" || isLoopbackHost(host) {
		return nil // 回环 = 只有本机能访问，安全默认
	}
	return fmt.Errorf(
		"listening on %q without OPS_WEBHOOK_TOKEN would expose unauthenticated write endpoints", addr)
}

// isLoopbackHost 判断主机是否回环（127.x / localhost / ::1）。
func isLoopbackHost(host string) bool {
	h := strings.Trim(strings.TrimSpace(host), "[]")
	if strings.EqualFold(h, "localhost") {
		return true
	}
	ip := net.ParseIP(h)
	return ip != nil && ip.IsLoopback()
}
