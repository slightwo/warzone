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
	text, err := executeAdmin(ctx, addr, action)
	if err != nil {
		fmt.Fprintf(os.Stderr, "管理命令失败：%v\n", err)
		os.Exit(1)
	}
	fmt.Println(text)
}

// executeAdmin 仅经独立的 AdminService 查询 Gateway 状态；管理命令不再创建玩家双向流。
func executeAdmin(ctx context.Context, addr, action string) (string, error) {
	if !isStatusAction(action) {
		return "", fmt.Errorf("管理操作 %q 已迁入 coordinator；Gateway 仅提供只读状态", action)
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return "", fmt.Errorf("创建网关 gRPC 连接: %w", err)
	}
	defer conn.Close()

	response, err := pb.NewAdminServiceClient(conn).GetGatewayStatus(ctx, &pb.GatewayStatusRequest{})
	if err != nil {
		return "", fmt.Errorf("查询网关状态: %w", err)
	}
	return formatGatewayStatus(response), nil
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
	fmt.Println("  默认通过 AdminService.GetGatewayStatus 查询状态；节点故障与恢复管理由 coordinator 负责")
}
