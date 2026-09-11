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
		lifecycleAddr          string
		drainTimeout           time.Duration
	)
	flag.StringVar(&nodeID, "id", "node-a", "节点唯一标识 (如: node-a)")
	flag.StringVar(&nodeAddr, "addr", "127.0.0.1:9311", "节点监听地址 (如: 127.0.0.1:9311)")
	flag.StringVar(&declaredPrimaryMapsStr, "maps", "", "节点声明可承载的主地图候选 ID，逗号分隔（如：green,ruins）")
	flag.StringVar(&declaredReplicaMapsStr, "replicas", "", "节点声明可承载的副本地图候选 ID，逗号分隔")
	flag.StringVar(&lifecycleAddr, "lifecycle-addr", "", "生命周期 HTTP 监听地址；为空时禁用（如：127.0.0.1:9411）")
	flag.DurationVar(&drainTimeout, "drain-timeout", 30*time.Second, "节点 drain 等待 coordinator 迁移 owner 地图的最大时长")
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
	// 在兼容窗口内同一节点同时服务 V1 与 V2；Gateway/Coordinator 已切换到
	// NodeServiceV2，而旧二进制仍可继续调用 NodeService。
	pb.RegisterNodeServiceServer(grpcServer, grpcNode)
	pb.RegisterNodeServiceV2Server(grpcServer, node.NewNodeV2GRPCServer(ns))

	// 如果有 Redis，则开启心跳上报线程。drain 状态会随下一次租约续期被 coordinator 和
	// gateway 观察到；注册字段只表达候选能力与生命周期状态，不改变拓扑主权。
	if store != nil {
		go func() {
			ticker := time.NewTicker(3 * time.Second)
			defer ticker.Stop()
			for {
				info := storage.NodeRegistryInfo{
					ID:       nodeID,
					Addr:     nodeAddr,
					Maps:     declaredPrimaryMaps,
					Replicas: declaredReplicaMaps,
					Draining: ns.IsDraining(),
				}
				if err := store.RegisterNode(info, 5*time.Second); err != nil {
					log.Printf("Redis Node心跳失败: %v", err)
				}
				<-ticker.C
			}
		}()
	}

	if lifecycleAddr != "" {
		go serveLifecycle(lifecycleAddr, ns, drainTimeout)
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
