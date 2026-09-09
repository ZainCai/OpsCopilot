// Package transport 模块间通信抽象（P1-2 调用纪律守护的核心件）。
//
// 架构纪律（v1.3 §5.2，CI 强制）：
//  1. 模块间只允许通过 gRPC client 调用，禁止 import 其他模块的内部包；
//  2. all-in-one 形态用本包的 bufconn 直连承载进程内调用——
//     零网络开销、同一序列化语义，未来拆分为独立进程时仅换 Dial 实现；
//  3. 共享契约代码只允许放在 internal/contracts（生成代码）与 pkg/（通用库）。
package transport

import (
	"context"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// DialInProcess 建立进程内 gRPC 连接。
// 用法：srv := grpc.NewServer(); pb.RegisterXxxServer(srv, impl); conn := DialInProcess(srv)
// 返回的 ClientConn 与真实网络连接行为一致，业务代码零改动。
func DialInProcess(ctx context.Context, srv *grpc.Server) (*grpc.ClientConn, error) {
	serverLn, clientLn := net.Pipe()
	go func() { _ = srv.Serve(&singleListener{ln: serverLn, closed: make(chan struct{})}) }()

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
//
// 停机语义（全局审查 G2）：第二次 Accept 阻塞等待关闭——Close 时必须
// 返回 net.ErrClosed 而不是永远 select{} 卡死。否则 grpc.Server.Stop()
// 会因 accept 循环无法退出而卡死（Serve goroutine 与管道资源泄漏）。
// 测试锁定：单连接约束（第二次 Accept 阻塞）与停机（Close 后 Accept 报错）。
type singleListener struct {
	ln net.Conn
	// accepted 标记唯一一次 Accept 是否已发生。
	accepted bool
	// closed 由 Close 触发关闭；Accept 的阻塞等待监听它。
	closed chan struct{}
	// closeOnce 保证 Close 幂等（重复 Close 不能重复 close channel）。
	closeOnce sync.Once
	closeErr  error
}

func (l *singleListener) Accept() (net.Conn, error) {
	// 已关闭：立即报错（含 Close 先于 Accept 的时序）。
	select {
	case <-l.closed:
		return nil, net.ErrClosed
	default:
	}
	if !l.accepted {
		l.accepted = true
		return l.ln, nil
	}
	// 唯一连接已交付：阻塞直到 Close（这是单连接设计的约束点）。
	<-l.closed
	return nil, net.ErrClosed
}

func (l *singleListener) Close() error {
	l.closeOnce.Do(func() {
		close(l.closed)
		l.closeErr = l.ln.Close()
	})
	return l.closeErr
}

func (l *singleListener) Addr() net.Addr { return pipeAddr{} }

type pipeAddr struct{}

func (pipeAddr) Network() string { return "inproc" }
func (pipeAddr) String() string  { return "inproc" }
