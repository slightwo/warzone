package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"battleworld/pb"
	"battleworld/protocol"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	action := os.Args[1]
	nodeID := ""
	addr := protocol.GatewayAddr
	if action == "状态" || action == "status" {
		if len(os.Args) >= 3 {
			addr = os.Args[2]
		}
	} else if len(os.Args) >= 3 {
		nodeID = os.Args[2]
	}
	if action != "状态" && action != "status" && len(os.Args) >= 4 {
		addr = os.Args[3]
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	text, err := executeAdmin(ctx, addr, action, nodeID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "管理命令失败：%v\n", err)
		os.Exit(1)
	}
	fmt.Println(text)
}

// executeAdmin 复用 GatewayService 的 gRPC 双向流发送单次管理请求。Gateway 会返回
// 一条结果消息并关闭该 stream，因此管理工具不再依赖已废弃的裸 TCP/JSON 协议。
func executeAdmin(ctx context.Context, addr, action, nodeID string) (string, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return "", fmt.Errorf("创建网关 gRPC 连接: %w", err)
	}
	defer conn.Close()

	stream, err := pb.NewGatewayServiceClient(conn).GameStream(ctx)
	if err != nil {
		return "", fmt.Errorf("打开管理 stream: %w", err)
	}
	if err := stream.Send(&pb.Message{
		Type:   protocol.TypeAdmin,
		Action: action,
		NodeId: nodeID,
	}); err != nil {
		return "", fmt.Errorf("发送管理请求: %w", err)
	}
	reply, err := stream.Recv()
	if err != nil {
		return "", fmt.Errorf("接收管理响应: %w", err)
	}
	if reply.Type == protocol.TypeError || !reply.Ok {
		if reply.Error == "" {
			return "", fmt.Errorf("网关拒绝管理请求")
		}
		return "", fmt.Errorf("%s", reply.Error)
	}
	return reply.Text, nil
}

func usage() {
	fmt.Println("用法：")
	fmt.Println("  go run ./cmd/admin 状态")
	fmt.Println("  go run ./cmd/admin 状态 127.0.0.1:9310")
	fmt.Println("  该命令通过 Gateway gRPC 查询状态；节点故障与恢复管理由 coordinator 负责")
}
