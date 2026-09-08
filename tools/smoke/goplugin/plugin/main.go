// T2 冒烟测试插件侧：由宿主作为子进程拉起，不单独运行。
// 编译：go build -o bin/plugin.exe ./plugin（在 tools/smoke/goplugin 目录下）
package main

import (
	"errors"
	"net/rpc"

	"github.com/hashicorp/go-plugin"
)

// Handshake 与宿主侧必须一致。
var handshake = plugin.HandshakeConfig{
	ProtocolVersion:  1,
	MagicCookieKey:   "OPSCOPILOT_SMOKE",
	MagicCookieValue: "smoke-test-cookie",
}

// Pinger 冒烟接口（插件侧视角；实际工程中放 contracts 包共享）。
type Pinger interface {
	Ping() (string, error)
}

// PingServer net/rpc 接收器：方法签名必须为 (args, *reply) 形式。
type PingServer struct{}

// Ping 返回 pong。
func (s *PingServer) Ping(args interface{}, resp *string) error {
	*resp = "pong"
	return nil
}

// PingPlugin 插件侧注册器（net/rpc 协议）。
type PingPlugin struct{}

// Server 暴露 RPC 接收器给宿主调用。
func (p *PingPlugin) Server(*plugin.MuxBroker) (interface{}, error) {
	return &PingServer{}, nil
}

// Client 插件进程不实现客户端侧。
func (PingPlugin) Client(*plugin.MuxBroker, *rpc.Client) (interface{}, error) {
	return nil, errors.New("plugin process: client side not implemented")
}

func main() {
	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: handshake,
		Plugins: map[string]plugin.Plugin{
			"pinger": &PingPlugin{},
		},
	})
}
