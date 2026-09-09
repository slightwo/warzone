package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"battleworld/pb"
	"battleworld/protocol"
)

func main() {
	protocolVersion := flag.String("protocol-version", "v2", "Gateway 协议版本：v2（默认）或 v1（回退）")
	flag.Parse()
	if flag.NArg() < 1 {
		usage()
		os.Exit(1)
	}

	action := flag.Arg(0)
	addr := protocol.GatewayAddr
	if isStatusAction(action) && flag.NArg() >= 2 {
		addr = flag.Arg(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	text, err := executeAdminWithVersion(ctx, addr, action, *protocolVersion)
	if err != nil {
		fmt.Fprintf(os.Stderr, "管理命令失败：%v\n", err)
		os.Exit(1)
	}
	fmt.Println(text)
}

// executeAdmin 默认经独立的 AdminService 查询 Gateway 状态；它不再创建玩家双向流。
func executeAdmin(ctx context.Context, addr, action, _ string) (string, error) {
	return executeAdminWithVersion(ctx, addr, action, "v2")
}

func executeAdminWithVersion(ctx context.Context, addr, action, protocolVersion string) (string, error) {
	version := strings.ToLower(strings.TrimSpace(protocolVersion))
	if version != "v1" && version != "v2" {
		return "", fmt.Errorf("不支持的协议版本 %q：仅支持 v1 或 v2", protocolVersion)
	}
	if !isStatusAction(action) {
		return "", fmt.Errorf("管理操作 %q 已迁入 coordinator；Gateway 仅提供只读状态", action)
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return "", fmt.Errorf("创建网关 gRPC 连接: %w", err)
	}
	defer conn.Close()

	if version == "v1" {
		return executeAdminV1(ctx, conn, action)
	}
	response, err := pb.NewAdminServiceClient(conn).GetGatewayStatus(ctx, &pb.GatewayStatusRequest{})
	if err != nil {
		return "", fmt.Errorf("查询网关状态: %w", err)
	}
	return formatGatewayStatus(response), nil
}

func executeAdminV1(ctx context.Context, conn *grpc.ClientConn, action string) (string, error) {
	stream, err := pb.NewGatewayServiceClient(conn).GameStream(ctx)
	if err != nil {
		return "", fmt.Errorf("打开 V1 管理 stream: %w", err)
	}
	if err := stream.Send(&pb.Message{Type: protocol.TypeAdmin, Action: action}); err != nil {
		return "", fmt.Errorf("发送 V1 管理请求: %w", err)
	}
	reply, err := stream.Recv()
	if err != nil {
		return "", fmt.Errorf("接收 V1 管理响应: %w", err)
	}
	if reply.GetType() == protocol.TypeError || !reply.GetOk() {
		if reply.GetError() == "" {
			return "", fmt.Errorf("网关拒绝管理请求")
		}
		return "", fmt.Errorf("%s", reply.GetError())
	}
	return reply.GetText(), nil
}

func formatGatewayStatus(response *pb.GatewayStatusResponse) string {
	if response == nil {
		return ""
	}
	return response.GetSummary()
}

func isStatusAction(action string) bool {
	return action == "状态" || strings.EqualFold(action, "status")
}

func usage() {
	fmt.Println("用法：")
	fmt.Println("  go run ./cmd/admin 状态")
	fmt.Println("  go run ./cmd/admin 状态 127.0.0.1:9310")
	fmt.Println("  go run ./cmd/admin --protocol-version=v1 状态 127.0.0.1:9310")
	fmt.Println("  默认通过 AdminService.GetGatewayStatus 查询状态；节点故障与恢复管理由 coordinator 负责")
}
