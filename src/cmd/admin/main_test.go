package main

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"

	"battleworld/pb"
)

type adminTestGateway struct {
	pb.UnimplementedAdminServiceServer

	status *pb.GatewayStatusResponse
}

func (s *adminTestGateway) GetGatewayStatus(context.Context, *pb.GatewayStatusRequest) (*pb.GatewayStatusResponse, error) {
	return s.status, nil
}

func TestExecuteAdminUsesDedicatedAdminService(t *testing.T) {
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
	text, err := executeAdmin(ctx, address, "status")
	if err != nil {
		t.Fatalf("executeAdmin 返回错误: %v", err)
	}
	if text != "网关路由状态：Topology Version=2" {
		t.Fatalf("管理结果 = %q", text)
	}
}

func TestExecuteAdminRejectsNonStatusActions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := executeAdmin(ctx, "unused", "down")
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
	pb.RegisterAdminServiceServer(server, gateway)
	go func() {
		_ = server.Serve(listener)
	}()
	return listener.Addr().String(), func() {
		server.Stop()
		_ = listener.Close()
	}
}
