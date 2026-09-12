package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"battleworld/config"
	"battleworld/node"
	"battleworld/pb"
	"battleworld/storage"
	"battleworld/world"
)

func main() {
	var (
		nodeID        string
		nodeAddr      string
		declaredMap   string
		lifecycleAddr string
		drainTimeout  time.Duration
	)
	flag.StringVar(&nodeID, "id", "node-a", "节点唯一标识 (如: node-a)")
	flag.StringVar(&nodeAddr, "addr", "127.0.0.1:9311", "节点监听地址 (如: 127.0.0.1:9311)")
	flag.StringVar(&declaredMap, "map", "", "节点参与 owner 选举的唯一地图 ID（如：green）")
	flag.StringVar(&lifecycleAddr, "lifecycle-addr", "", "生命周期 HTTP 监听地址；为空时禁用（如：127.0.0.1:9411）")
	flag.DurationVar(&drainTimeout, "drain-timeout", 0, "节点 drain 等待 coordinator 迁移 owner 地图的最大时长；为 0 时读取运行时配置")
	flag.Parse()

	runtime, err := config.LoadRuntime()
	if err != nil {
		log.Fatalf("加载运行时配置失败: %v", err)
	}
	if drainTimeout == 0 {
		drainTimeout = runtime.Node.DrainTimeout.Duration
	}

	declaredMap = strings.TrimSpace(declaredMap)
	if !isKnownMap(declaredMap) {
		log.Fatalf("节点 [%s] 声明了未知或空地图 %q", nodeID, declaredMap)
	}
	log.Printf("正在启动物理节点 [%s]，监听地址：%s，参与地图 %s 的 owner 选举", nodeID, nodeAddr, declaredMap)

	// 连接 Store 获取 Redis
	store, err := storage.NewStore(".")
	if err != nil {
		log.Printf("警告: Store 初始化失败，可能是PG连不上，暂时只通过gRPC等待连接: %v", err)
	}

	ns := node.NewNodeService(nodeID, nodeAddr, store, runtime.Node, declaredMap)
	if err := ns.Start(); err != nil {
		log.Fatalf("逻辑节点启动失败: %v", err)
	}
	defer ns.Stop()

	lis, err := net.Listen("tcp", nodeAddr)
	if err != nil {
		log.Fatalf("无法监听端口 %s: %v", nodeAddr, err)
	}

	grpcServer := grpc.NewServer()
	// Node 数据面仅服务带 authority 和 typed payload 的 NodeService。
	pb.RegisterNodeServiceServer(grpcServer, node.NewNodeGRPCServer(ns))

	// 如果有 Redis，则开启心跳上报线程。注册只表达本节点参与的单一地图选举和
	// 生命周期状态；owner/standby 均由 coordinator 基于这些租约和健康状态决定。
	if store != nil {
		go func() {
			ticker := time.NewTicker(runtime.NodeRegistry.HeartbeatInterval.Duration)
			defer ticker.Stop()
			for {
				info := storage.NodeRegistryInfo{
					ID:       nodeID,
					Addr:     nodeAddr,
					MapID:    declaredMap,
					Draining: ns.IsDraining(),
				}
				if err := store.RegisterNode(info, runtime.NodeRegistry.LeaseTTL.Duration); err != nil {
					log.Printf("Redis Node心跳失败: %v", err)
				}
				<-ticker.C
			}
		}()
	}

	if lifecycleAddr != "" {
		go serveLifecycle(lifecycleAddr, ns, drainTimeout, runtime)
	}

	log.Printf("物理节点 [%s] 启动完毕，开始处理网络请求...", nodeID)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("收到关闭信号，开始受控 drain...")
		ctx, cancel := context.WithTimeout(context.Background(), drainTimeout)
		defer cancel()
		if err := ns.BeginDrain(ctx); err != nil {
			log.Printf("节点 drain 未完成，拒绝退出以保护 owner 地图: %v", err)
			return
		}
		grpcServer.GracefulStop()
	}()

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("gRPC 服务异常退出: %v", err)
	}
}

func isKnownMap(mapID string) bool {
	for _, config := range world.AvailableMaps() {
		if config.ID == mapID {
			return true
		}
	}
	return false
}
