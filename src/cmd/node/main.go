package main

import (
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"battleworld/node"
	"battleworld/pb"
	"battleworld/storage"
	"battleworld/world"
)

func main() {
	var (
		nodeID                 string
		nodeAddr               string
		declaredPrimaryMapsStr string
		declaredReplicaMapsStr string
	)
	flag.StringVar(&nodeID, "id", "node-a", "节点唯一标识 (如: node-a)")
	flag.StringVar(&nodeAddr, "addr", "127.0.0.1:9311", "节点监听地址 (如: 127.0.0.1:9311)")
	flag.StringVar(&declaredPrimaryMapsStr, "maps", "", "节点声明可承载的主地图候选 ID，逗号分隔（如：green,ruins）")
	flag.StringVar(&declaredReplicaMapsStr, "replicas", "", "节点声明可承载的副本地图候选 ID，逗号分隔")
	flag.Parse()

	log.Printf("正在启动物理节点 [%s]，监听地址：%s，声明主地图候选：%s，声明副本地图候选：%s", nodeID, nodeAddr, declaredPrimaryMapsStr, declaredReplicaMapsStr)

	// 连接 Store 获取 Redis
	store, err := storage.NewStore(".")
	if err != nil {
		log.Printf("警告: Store 初始化失败，可能是PG连不上，暂时只通过gRPC等待连接: %v", err)
	}

	ns := node.NewNodeService(nodeID, nodeAddr, store)
	if err := ns.Start(); err != nil {
		log.Fatalf("逻辑节点启动失败: %v", err)
	}
	defer ns.Stop()

	// 当前启动参数仍决定节点预加载的本地地图；注册信息只将其作为候选能力上报，
	// 实际路由主权由控制面提交的 Topology 决定。
	declaredPrimaryMapIDs := strings.Split(declaredPrimaryMapsStr, ",")
	available := world.AvailableMaps()
	var declaredPrimaryMaps []string
	for _, mapID := range declaredPrimaryMapIDs {
		mapID = strings.TrimSpace(mapID)
		if mapID == "" {
			continue
		}
		for _, cfg := range available {
			if cfg.ID == mapID {
				if store != nil {
					if cp, ok := store.LoadCheckpoint(mapID); ok && cp.Version > 0 {
						ns.RestorePrimaryMap(cfg, *cp)
						log.Printf("节点 [%s] 从 checkpoint 恢复地图 %s (version %d)", nodeID, mapID, cp.Version)
					} else {
						ns.InstallPrimaryMap(cfg)
						log.Printf("节点 [%s] 本地成功加载地图: %s", nodeID, mapID)
					}
				} else {
					// store 初始化失败（PG 连不上）时仍继续，退化为起空图
					ns.InstallPrimaryMap(cfg)
					log.Printf("节点 [%s] 本地加载地图 %s（store 不可用，起空图）", nodeID, mapID)
				}
				declaredPrimaryMaps = append(declaredPrimaryMaps, mapID)
				break
			}
		}
	}

	declaredReplicaMapIDs := strings.Split(declaredReplicaMapsStr, ",")
	var declaredReplicaMaps []string
	for _, mapID := range declaredReplicaMapIDs {
		mapID = strings.TrimSpace(mapID)
		if mapID != "" {
			declaredReplicaMaps = append(declaredReplicaMaps, mapID)
			ns.AddReplicaMap(mapID)
			log.Printf("节点 [%s] 本地成功加载副本: %s", nodeID, mapID)
		}
	}

	grpcNode := node.NewNodeGRPCServer(ns)

	lis, err := net.Listen("tcp", nodeAddr)
	if err != nil {
		log.Fatalf("无法监听端口 %s: %v", nodeAddr, err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterNodeServiceServer(grpcServer, grpcNode)

	// 如果有 Redis，则开启心跳上报线程
	if store != nil {
		go func() {
			ticker := time.NewTicker(3 * time.Second)
			defer ticker.Stop()
			info := storage.NodeRegistryInfo{
				ID:       nodeID,
				Addr:     nodeAddr,
				Maps:     declaredPrimaryMaps,
				Replicas: declaredReplicaMaps,
			}
			for {
				if err := store.RegisterNode(info, 5*time.Second); err != nil {
					log.Printf("Redis Node心跳失败: %v", err)
				}
				<-ticker.C
			}
		}()
	}

	log.Printf("物理节点 [%s] 启动完毕，开始处理网络请求...", nodeID)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("收到关闭信号，正在退出...")
		grpcServer.GracefulStop()
	}()

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("gRPC 服务异常退出: %v", err)
	}
}
