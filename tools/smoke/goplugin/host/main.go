// T2 冒烟测试宿主侧：验证 go-plugin 在 Windows 上的可用性。
// 运行方式（在 tools/smoke/goplugin 目录下）：
//   go build -o bin/plugin.exe ./plugin && go run ./host
package main

import (
	"errors"
	"fmt"
	"net/rpc"
	"os"
	"os/exec"
	"runtime"
	"time"

	"github.com/hashicorp/go-plugin"
)

// Handshake 宿主与插件必须一致。
var handshake = plugin.HandshakeConfig{
	ProtocolVersion:  1,
	MagicCookieKey:   "OPSCOPILOT_SMOKE",
	MagicCookieValue: "smoke-test-cookie",
}

// Pinger 冒烟接口（宿主侧视角）。
type Pinger interface {
	Ping() (string, error)
}

// PingerRPC 客户端桥接：net/rpc 调用转发。
type PingerRPC struct {
	client *rpc.Client
}

// Ping 调用插件侧 Plugin.Ping。
func (g *PingerRPC) Ping() (string, error) {
	var resp string
	err := g.client.Call("Plugin.Ping", new(interface{}), &resp)
	return resp, err
}

// PingPlugin 宿主侧插件注册器（net/rpc 协议）。
type PingPlugin struct{}

// Server 宿主侧不提供 server。
func (PingPlugin) Server(*plugin.MuxBroker) (interface{}, error) {
	return nil, errors.New("host process: server side not implemented")
}

// Client 返回 RPC 桥接实现。
func (p *PingPlugin) Client(m *plugin.MuxBroker, c *rpc.Client) (interface{}, error) {
	return &PingerRPC{client: c}, nil
}

func fail(format string, args ...interface{}) {
	fmt.Printf("FAIL "+format+"\n", args...)
	os.Exit(1)
}

func main() {
	pluginBin := "bin/plugin.exe"

	// 1. 启动插件子进程
	client := plugin.NewClient(&plugin.ClientConfig{
		HandshakeConfig: handshake,
		Plugins: map[string]plugin.Plugin{
			"pinger": &PingPlugin{},
		},
		Cmd: exec.Command(pluginBin),
	})
	defer client.Kill()

	rpcClient, err := client.Client()
	if err != nil {
		fail("handshake: %v", err)
	}
	raw, err := rpcClient.Dispense("pinger")
	if err != nil {
		fail("dispense: %v", err)
	}
	pinger, ok := raw.(Pinger)
	if !ok {
		fail("dispense returned unexpected type %T", raw)
	}

	// 2. RPC 调用
	msg, err := pinger.Ping()
	if err != nil || msg != "pong" {
		fail("ping: msg=%q err=%v", msg, err)
	}
	fmt.Printf("PASS handshake + rpc call on %s/%s\n", runtime.GOOS, runtime.GOARCH)

	// 3. kill 协议：插件进程应干净退出
	client.Kill()
	deadline := time.Now().Add(5 * time.Second)
	for !client.Exited() {
		if time.Now().After(deadline) {
			fail("plugin process did not exit after kill")
		}
		time.Sleep(50 * time.Millisecond)
	}
	fmt.Println("PASS kill protocol (clean plugin exit)")
	fmt.Printf("SMOKE RESULT: GO-PLUGIN %s/%s OK\n", runtime.GOOS, runtime.GOARCH)
}
