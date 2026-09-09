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
	pb.UnimplementedAdminServiceServer

	expectedAction string
	response       *pb.Message
	status         *pb.GatewayStatusResponse
}

func (s *adminTestGateway) GameStream(stream pb.GatewayService_GameStreamServer) error {
	request, err := stream.Recv()
	if err != nil {
		return err
	}
	if request.GetType() != protocol.TypeAdmin || request.GetAction() != s.expectedAction {
		return stream.Send(&pb.Message{Type: protocol.TypeError, Error: "unexpected admin request"})
	}
	return stream.Send(s.response)
}

func (s *adminTestGateway) GetGatewayStatus(context.Context, *pb.GatewayStatusRequest) (*pb.GatewayStatusResponse, error) {
	return s.status, nil
}

func TestExecuteAdminUsesDedicatedAdminServiceByDefault(t *testing.T) {
	address, stop := startAdminTestGateway(t, &adminTestGateway{
		status: &pb.GatewayStatusResponse{
			RoutingReady:    true,
			TopologyVersion: 2,
			Summary:         "网关路由状态：Topology Version=2",
		},
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

func TestExecuteAdminV1RemainsExplicitFallback(t *testing.T) {
	address, stop := startAdminTestGateway(t, &adminTestGateway{
		expectedAction: "status",
		response:       &pb.Message{Type: protocol.TypeAdmin, Ok: true, Text: "V1 网关路由状态"},
	})
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	text, err := executeAdminWithVersion(ctx, address, "status", "v1")
	if err != nil {
		t.Fatalf("V1 回退执行失败: %v", err)
	}
	if text != "V1 网关路由状态" {
		t.Fatalf("V1 管理结果 = %q", text)
	}
}

func TestExecuteAdminRejectsNonStatusActions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := executeAdminWithVersion(ctx, "unused", "down", "v2")
	if err == nil || !strings.Contains(err.Error(), "已迁入 coordinator") {
		t.Fatalf("管理错误 = %v，期望本地拒绝非 status 操作", err)
	}
}

func startAdminTestGateway(t *testing.T, gateway *adminTestGateway) (string, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("监听测试 gRPC 地址: %v", err)
	}
	server := grpc.NewServer()
	pb.RegisterGatewayServiceServer(server, gateway)
	pb.RegisterAdminServiceServer(server, gateway)
	go func() {
		_ = server.Serve(listener)
	}()
	return listener.Addr().String(), func() {
		server.Stop()
		_ = listener.Close()
	}
}
