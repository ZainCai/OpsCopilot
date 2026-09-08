package transport

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// TestDialInProcessRoundTrip 验证进程内 gRPC 往返可用（O9：transport 此前零测试）。
// 用官方 health 服务做探针，避免为测试引入 protobuf 生成代码。
func TestDialInProcessRoundTrip(t *testing.T) {
	srv := grpc.NewServer()
	hs := health.NewServer()
	healthpb.RegisterHealthServer(srv, hs)
	hs.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := DialInProcess(ctx, srv)
	if err != nil {
		t.Fatalf("DialInProcess failed: %v", err)
	}
	defer conn.Close()

	client := healthpb.NewHealthClient(conn)
	resp, err := client.Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("health check over in-process conn failed: %v", err)
	}
	if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("expected SERVING, got %v", resp.GetStatus())
	}
	t.Log("in-process gRPC round trip OK")
}

// TestSingleListenerSecondAcceptBlocks 固化单连接设计：第二次 Accept 必须阻塞。
// 约束含义：一个 DialInProcess 对应一个 grpc.Server 的一条连接；
// 若未来 gRPC 需要多路连接，本测试会失败并提醒重新设计，避免静默退化。
func TestSingleListenerSecondAcceptBlocks(t *testing.T) {
	serverConn, _ := net.Pipe()
	ln := &singleListener{ln: serverConn}

	got, err := ln.Accept()
	if err != nil {
		t.Fatalf("first Accept failed: %v", err)
	}
	if got != serverConn {
		t.Fatal("first Accept should return the pipe conn")
	}
	if ln.Addr().Network() != "inproc" {
		t.Fatalf("unexpected network: %s", ln.Addr().Network())
	}

	done := make(chan struct{})
	go func() {
		_, _ = ln.Accept() // 设计上应永久阻塞
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("second Accept should block under single-connection design")
	case <-time.After(200 * time.Millisecond):
		// 符合预期
	}
}
