package main

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"battleworld/cluster"
	"battleworld/pb"
	"battleworld/protocol"
	"battleworld/storage"
)

type GatewayServer struct {
	pb.UnimplementedGatewayServiceServer
	gameCluster *cluster.Cluster
}

func (s *GatewayServer) GameStream(stream pb.GatewayService_GameStreamServer) error {
	reqPb, err := stream.Recv()
	if err != nil {
		return err
	}

	authMsg := protocol.FromProtoMessage(reqPb)

	if authMsg.Type == protocol.TypeAdmin {
		text, err := s.gameCluster.ExecuteAdmin(authMsg.Action, authMsg.NodeID)
		if err != nil {
			_ = stream.Send(protocol.ToProtoMessage(protocol.Message{Type: protocol.TypeError, Error: err.Error()}))
			return err
		}
		_ = stream.Send(protocol.ToProtoMessage(protocol.Message{Type: protocol.TypeAdmin, OK: true, Text: text}))
		return nil
	}

	var state *protocol.WorldState
	switch authMsg.Type {
	case protocol.TypeRegister:
		if err := s.gameCluster.Register(authMsg.Username, authMsg.Password, authMsg.Confirm); err != nil {
			_ = stream.Send(protocol.ToProtoMessage(protocol.Message{Type: protocol.TypeError, Error: err.Error()}))
			return err
		}
		state, err = s.gameCluster.Login(authMsg.Username, authMsg.Password)
	case protocol.TypeLogin:
		state, err = s.gameCluster.Login(authMsg.Username, authMsg.Password)
	case protocol.TypeQuickEnter:
		state, err = s.gameCluster.QuickEnter(authMsg.Username, authMsg.Password)
	default:
		_ = stream.Send(protocol.ToProtoMessage(protocol.Message{Type: protocol.TypeError, Error: "首条消息必须是登录或注册请求"}))
		return fmt.Errorf("invalid first message type: %s", authMsg.Type)
	}

	if err != nil {
		_ = stream.Send(protocol.ToProtoMessage(protocol.Message{Type: protocol.TypeError, Error: err.Error()}))
		return err
	}

	username := authMsg.Username
	if err := stream.Send(protocol.ToProtoMessage(protocol.Message{Type: protocol.TypeAuth, OK: true, State: state})); err != nil {
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
				err := stream.Send(protocol.ToProtoMessage(*msg))

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

	ticker := time.NewTicker(400 * time.Millisecond)
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
		msg := protocol.FromProtoMessage(reqPb)

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
			sendCh <- &protocol.Message{Type: protocol.TypeState, State: next}

			//	protocol.FreeWorldState(next)
		}
	}
}

func main() {
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
	pb.RegisterGatewayServiceServer(grpcServer, &GatewayServer{
		gameCluster: gameCluster,
	})

	fmt.Printf("gRPC 网关已启动：%s\n", protocol.GatewayAddr)

	go func() {
		<-sigChan
		grpcServer.GracefulStop()
	}()

	if err := grpcServer.Serve(ln); err != nil {
		fmt.Fprintf(os.Stderr, "gRPC server error: %v\n", err)
	}
}

func resolveDataRoot() string {
	if root := strings.TrimSpace(os.Getenv("LAB3_DATA_ROOT")); root != "" {
		return root
	}
	return "."
}
