package main

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"

	"battleworld/pb"
	"battleworld/protocol"
)

type adminTestGateway struct {
	pb.UnimplementedGatewayServiceServer
	expectedAction string
	expectedNodeID string
	response       *pb.Message
}

func (s *adminTestGateway) GameStream(stream pb.GatewayService_GameStreamServer) error {
	request, err := stream.Recv()
	if err != nil {
		return err
	}
	if request.Type != protocol.TypeAdmin || request.Action != s.expectedAction || request.NodeId != s.expectedNodeID {
		return stream.Send(&pb.Message{Type: protocol.TypeError, Error: "unexpected admin request"})
	}
	return stream.Send(s.response)
}

func TestExecuteAdminUsesGatewayGRPCStream(t *testing.T) {
	address, stop := startAdminTestGateway(t, &adminTestGateway{
		expectedAction: "status",
		expectedNodeID: "",
		response:       &pb.Message{Type: protocol.TypeAdmin, Ok: true, Text: "网关路由状态：Topology Version=2"},
	})
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	text, err := executeAdmin(ctx, address, "status", "")
	if err != nil {
		t.Fatalf("executeAdmin 返回错误: %v", err)
	}
	if text != "网关路由状态：Topology Version=2" {
		t.Fatalf("管理结果 = %q", text)
	}
}

func TestExecuteAdminReturnsGatewayError(t *testing.T) {
	address, stop := startAdminTestGateway(t, &adminTestGateway{
		expectedAction: "down",
		expectedNodeID: "node-a",
		response:       &pb.Message{Type: protocol.TypeError, Error: "节点故障与恢复管理已迁入 coordinator"},
	})
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := executeAdmin(ctx, address, "down", "node-a")
	if err == nil || !strings.Contains(err.Error(), "已迁入 coordinator") {
		t.Fatalf("管理错误 = %v，期望返回 Gateway 业务错误", err)
	}
}

func startAdminTestGateway(t *testing.T, gateway pb.GatewayServiceServer) (string, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("监听测试 gRPC 地址: %v", err)
	}
	server := grpc.NewServer()
	pb.RegisterGatewayServiceServer(server, gateway)
	go func() {
		_ = server.Serve(listener)
	}()
	return listener.Addr().String(), func() {
		server.Stop()
		_ = listener.Close()
	}
}
