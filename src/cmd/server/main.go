package main

import (
	"context"
	"expvar"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"battleworld/cluster"
	"battleworld/lifecycle"
	"battleworld/pb"
	"battleworld/protocol"
	"battleworld/storage"
	gatewaywire "battleworld/transport/gateway"

	_ "net/http/pprof" // 注册 /debug/pprof/ 性能分析端点。
)

const defaultGameStreamStateInterval = 100 * time.Millisecond

// legacyGatewayGameStreamCalls records connections to the V1 game stream. It
// is exposed through /debug/vars and must remain unchanged for a full release
// window before the V1 Gateway RPC can be retired.
var legacyGatewayGameStreamCalls = expvar.NewInt("battleworld_gateway_v1_stream_calls_total")

// gatewayBackend 是 GatewayService 在 V1 游戏流中依赖的最小业务能力集合。
// 保持传输 handler 与具体 Cluster 实现解耦，使协议行为可在不依赖 Redis 或 PostgreSQL 的
// 情况下被表征测试覆盖。
type gatewayBackend interface {
	ExecuteAdmin(action, nodeID string) (string, error)
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

func (s *GatewayServer) GameStream(stream pb.GatewayService_GameStreamServer) error {
	legacyGatewayGameStreamCalls.Add(1)
	reqPb, err := stream.Recv()
	if err != nil {
		return err
	}

	authMsg := gatewaywire.FromLegacyMessage(reqPb)

	if authMsg.Type == protocol.TypeAdmin {
		text, err := s.gameCluster.ExecuteAdmin(authMsg.Action, authMsg.NodeID)
		if err != nil {
			_ = stream.Send(gatewaywire.ToLegacyMessage(protocol.Message{Type: protocol.TypeError, Error: err.Error()}))
			return err
		}
		_ = stream.Send(gatewaywire.ToLegacyMessage(protocol.Message{Type: protocol.TypeAdmin, OK: true, Text: text}))
		return nil
	}

	var state *protocol.WorldState
	switch authMsg.Type {
	case protocol.TypeRegister:
		if err := s.gameCluster.Register(authMsg.Username, authMsg.Password, authMsg.Confirm); err != nil {
			_ = stream.Send(gatewaywire.ToLegacyMessage(protocol.Message{Type: protocol.TypeError, Error: err.Error()}))
			return err
		}
		state, err = s.gameCluster.Login(authMsg.Username, authMsg.Password)
	case protocol.TypeLogin:
		state, err = s.gameCluster.Login(authMsg.Username, authMsg.Password)
	case protocol.TypeQuickEnter:
		state, err = s.gameCluster.QuickEnter(authMsg.Username, authMsg.Password)
	default:
		_ = stream.Send(gatewaywire.ToLegacyMessage(protocol.Message{Type: protocol.TypeError, Error: "首条消息必须是登录或注册请求"}))
		return fmt.Errorf("invalid first message type: %s", authMsg.Type)
	}

	if err != nil {
		_ = stream.Send(gatewaywire.ToLegacyMessage(protocol.Message{Type: protocol.TypeError, Error: err.Error()}))
		return err
	}

	username := authMsg.Username
	if err := stream.Send(gatewaywire.ToLegacyMessage(protocol.Message{Type: protocol.TypeAuth, OK: true, State: state})); err != nil {
		_ = s.gameCluster.Logout(username)
		return err
	}
	protocol.FreeWorldState(state)

	var once sync.Once
	done := make(chan struct{})
	stop := func() {
		once.Do(func() {
			close(done)
			_ = s.gameCluster.Logout(username)
		})
	}
	defer stop()
	//stream不是线程安全的，使用channel控制
	sendCh := make(chan *protocol.Message, 100)
	go func() {
		for {
			select {
			case msg := <-sendCh:
				err := stream.Send(gatewaywire.ToLegacyMessage(*msg))

				if msg.State != nil {
					protocol.FreeWorldState(msg.State)
				}

				if err != nil {
					stop() // 通知所有协程退出
					return
				}
			case <-done:
				// 主协程或心跳协程要求退出了，发件员也安然下班
				return
			}
		}
	}()

	ticker := time.NewTicker(s.gameStreamStateInterval())
	defer ticker.Stop()

	// 心跳/状态推送协程
	go func() {
		for {
			select {
			case <-ticker.C:
				st, err := s.gameCluster.SnapshotFor(username)
				if err != nil {
					sendCh <- &protocol.Message{Type: protocol.TypeError, Error: err.Error()}
					if strings.Contains(err.Error(), "当前不在线") || strings.Contains(err.Error(), "不存在") {
						stop()
						return
					}
					continue
				}
				sendCh <- &protocol.Message{Type: protocol.TypeState, State: st}
			case <-done:
				return
			}
		}
	}()
	// 接收指令循环
	for {
		select {
		case <-done:
			return nil
		default:
		}

		reqPb, err := stream.Recv()
		if err != nil {
			return err
		}
		msg := gatewaywire.FromLegacyMessage(reqPb)

		var next *protocol.WorldState
		switch msg.Type {
		case protocol.TypeMove:
			next, err = s.gameCluster.Move(username, msg.Dir)
		case protocol.TypeAttack:
			next, err = s.gameCluster.Attack(username)
		case protocol.TypeBossAttack:
			next, err = s.gameCluster.AttackBoss(username)
		case protocol.TypeHeal:
			next, err = s.gameCluster.Heal(username)
		case protocol.TypeShop:
			next, err = s.gameCluster.BuyItem(username, msg.Item)
		case protocol.TypeSwitchMap:
			next, err = s.gameCluster.SwitchMap(username, msg.MapID)
		case protocol.TypeLogout:
			return nil
		default:
			err = fmt.Errorf("未知指令：%q", msg.Type)
		}

		if err != nil {
			sendCh <- &protocol.Message{Type: protocol.TypeError, Error: err.Error()}

			continue
		}

		if next != nil {
			// 策略一：应用层防抖，只执行业务逻辑，不立即返回全量状态
			// 状态的下发统一交给上面 100ms 的 ticker 批量处理，降低 syscall 发包频次
			protocol.FreeWorldState(next)
		}
	}
}

func main() {
	lifecycleAddr := flag.String("lifecycle-addr", "", "生命周期 HTTP 监听地址；为空时禁用（如：127.0.0.1:9413）")
	flag.Parse()

	go func() {
		_ = http.ListenAndServe("localhost:6060", nil)
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	dataRoot := resolveDataRoot()
	store, err := storage.NewStore(dataRoot)
	if err != nil {
		fmt.Fprintf(os.Stderr, "初始化存储失败：%v\n", err)
		os.Exit(1)
	}

	gameCluster, err := cluster.NewCluster(store)
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
	gatewayServer := &GatewayServer{gameCluster: gameCluster}
	pb.RegisterGatewayServiceServer(grpcServer, gatewayServer)
	pb.RegisterAdminServiceServer(grpcServer, gatewayServer)

	// draining 标志在 drain 期间返回 503，让调度器停止把新会话路由到本网关。
	var draining atomic.Bool
	if *lifecycleAddr != "" {
		go serveGatewayLifecycle(*lifecycleAddr, grpcServer, &draining)
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
func serveGatewayLifecycle(addr string, grpcServer *grpc.Server, draining *atomic.Bool) {
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
		ReadHeaderTimeout: 5 * time.Second,
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
