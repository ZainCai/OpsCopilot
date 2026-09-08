// Package transport 模块间通信抽象（P1-2 调用纪律守护的核心件）。
//
// 架构纪律（v1.3 §5.2，CI 强制）：
//   1. 模块间只允许通过 gRPC client 调用，禁止 import 其他模块的内部包；
//   2. all-in-one 形态用本包的 bufconn 直连承载进程内调用——
//      零网络开销、同一序列化语义，未来拆分为独立进程时仅换 Dial 实现；
//   3. 共享契约代码只允许放在 internal/contracts（生成代码）与 pkg/（通用库）。
package transport

import (
	"context"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// DialInProcess 建立进程内 gRPC 连接。
// 用法：srv := grpc.NewServer(); pb.RegisterXxxServer(srv, impl); conn := DialInProcess(srv)
// 返回的 ClientConn 与真实网络连接行为一致，业务代码零改动。
func DialInProcess(ctx context.Context, srv *grpc.Server) (*grpc.ClientConn, error) {
	serverLn, clientLn := net.Pipe()
	go func() { _ = srv.Serve(&singleListener{ln: serverLn}) }()

	dialCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return grpc.DialContext(dialCtx, "inproc",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return clientLn, nil
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
}

// singleListener 把一对 pipe 适配为 net.Listener（只接受一次连接）。
type singleListener struct {
	ln    net.Conn
	ready bool
}

func (l *singleListener) Accept() (net.Conn, error) {
	if l.ready {
		// 阻塞等关闭
		select {}
	}
	l.ready = true
	return l.ln, nil
}

func (l *singleListener) Close() error   { return l.ln.Close() }
func (l *singleListener) Addr() net.Addr { return pipeAddr{} }

type pipeAddr struct{}

func (pipeAddr) Network() string { return "inproc" }
func (pipeAddr) String() string  { return "inproc" }
