package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"battleworld/cluster"
	"battleworld/config"
	"battleworld/lifecycle"
	"battleworld/pb"
	"battleworld/protocol"
	"battleworld/storage"

	_ "net/http/pprof" // 注册 /debug/pprof/ 性能分析端点。
)

// defaultGameStreamStateInterval 仅供未注入运行时配置的旧测试使用。
const defaultGameStreamStateInterval = 100 * time.Millisecond

// gatewayBackend 是 GatewayService 的游戏流和 AdminService 依赖的最小业务能力集合。
// 保持传输 handler 与具体 Cluster 实现解耦，使协议行为可在不依赖 Redis 或 PostgreSQL 的
// 情况下被表征测试覆盖。
type gatewayBackend interface {
	Register(username, password, confirm string) error
	Login(username, password string) (*protocol.WorldState, error)
	QuickEnter(username, password string) (*protocol.WorldState, error)
	Logout(username string) error
	SnapshotFor(username string) (*protocol.WorldState, error)
	Move(username, dir string) (*protocol.WorldState, error)
	Attack(username string) (*protocol.WorldState, error)
	AttackBoss(username string) (*protocol.WorldState, error)
	Heal(username string) (*protocol.WorldState, error)
	BuyItem(username, item string) (*protocol.WorldState, error)
	SwitchMap(username, mapID string) (*protocol.WorldState, error)
	GatewayStatus() protocol.GatewayStatus
}

type GatewayServer struct {
	pb.UnimplementedGatewayServiceServer
	pb.UnimplementedAdminServiceServer
	gameCluster   gatewayBackend
	stateInterval time.Duration
}

func (s *GatewayServer) gameStreamStateInterval() time.Duration {
	if s.stateInterval > 0 {
		return s.stateInterval
	}
	return defaultGameStreamStateInterval
}

func main() {
	lifecycleAddr := flag.String("lifecycle-addr", "", "生命周期 HTTP 监听地址；为空时禁用（如：127.0.0.1:9413）")
	flag.Parse()

	runtime, err := config.LoadRuntime()
	if err != nil {
		fmt.Fprintf(os.Stderr, "加载运行时配置失败：%v\n", err)
		os.Exit(1)
	}

	if runtime.HTTP.PprofEnabled {
		go func() {
			_ = http.ListenAndServe(runtime.HTTP.PprofAddr, nil)
		}()
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	dataRoot := resolveDataRoot()
	store, err := storage.NewStore(dataRoot)
	if err != nil {
		fmt.Fprintf(os.Stderr, "初始化存储失败：%v\n", err)
		os.Exit(1)
	}

	gameCluster, err := cluster.NewClusterWithConfig(store, runtime.Gateway)
	if err != nil {
		fmt.Fprintf(os.Stderr, "初始化集群失败：%v\n", err)
		os.Exit(1)
	}
	if err := gameCluster.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "启动集群失败：%v\n", err)
		os.Exit(1)
	}
	defer gameCluster.Close()

	ln, err := net.Listen("tcp", protocol.GatewayAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "网关监听失败：%v\n", err)
		os.Exit(1)
	}
	defer ln.Close()

	grpcServer := grpc.NewServer()
	gatewayServer := &GatewayServer{gameCluster: gameCluster, stateInterval: runtime.Gateway.StreamStateInterval.Duration}
	pb.RegisterGatewayServiceServer(grpcServer, gatewayServer)
	pb.RegisterAdminServiceServer(grpcServer, gatewayServer)

	// draining 标志在 drain 期间返回 503，让调度器停止把新会话路由到本网关。
	var draining atomic.Bool
	if *lifecycleAddr != "" {
		go serveGatewayLifecycle(*lifecycleAddr, grpcServer, &draining, runtime)
	}

	fmt.Printf("gRPC 网关已启动：%s\n", protocol.GatewayAddr)

	go func() {
		<-sigChan
		grpcServer.GracefulStop()
	}()

	if err := grpcServer.Serve(ln); err != nil {
		fmt.Fprintf(os.Stderr, "gRPC server error: %v\n", err)
	}
}

// serveGatewayLifecycle 暴露 /healthz、/readyz、/drain。gateway 的 drain 仅停止接收
// 新会话，不会主动断开现有 stream；运维应在 drain 完成后再发送 SIGTERM。
func serveGatewayLifecycle(addr string, grpcServer *grpc.Server, draining *atomic.Bool, runtime config.RuntimeConfig) {
	handler := lifecycle.NewHandler(lifecycle.Callbacks{
		Status: func() lifecycle.Status {
			return lifecycle.Status{Draining: draining.Load()}
		},
		Ready: func() bool {
			return !draining.Load()
		},
		Drain: func(ctx context.Context) error {
			if !draining.CompareAndSwap(false, true) {
				return nil
			}
			go grpcServer.GracefulStop()
			return nil
		},
	})
	srv := &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: runtime.HTTP.LifecycleReadHeaderTimeout.Duration,
	}
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		fmt.Fprintf(os.Stderr, "gateway lifecycle 监听失败：%v\n", err)
	}
}

func resolveDataRoot() string {
	if root := strings.TrimSpace(os.Getenv("LAB3_DATA_ROOT")); root != "" {
		return root
	}
	return "."
}
