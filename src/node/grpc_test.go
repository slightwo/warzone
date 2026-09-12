package node

import (
	"context"
	"net"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"battleworld/config"
	"battleworld/pb"
	"battleworld/storage"

	"github.com/redis/go-redis/v9"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const nodeTestBufferSize = 1024 * 1024

func TestNodePingUsesTypedService(t *testing.T) {
	service := NewNodeService("node-a", "", nil, config.DefaultRuntime().Node, "")
	listener := bufconn.Listen(nodeTestBufferSize)
	server := grpc.NewServer()
	pb.RegisterNodeServiceServer(server, NewNodeGRPCServer(service))
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()
	defer listener.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conn, err := grpc.NewClient(
		"passthrough:///node-test",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("创建测试 gRPC 客户端: %v", err)
	}
	defer conn.Close()

	response, err := pb.NewNodeServiceClient(conn).Ping(ctx, &pb.NodePingRequest{})
	if err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if response.GetObservedAt() == nil || response.GetObservedAt().CheckValid() != nil {
		t.Fatalf("Ping observed_at 无效: %v", response.GetObservedAt())
	}
}

func TestNodeAcceptsCurrentAuthorityAndRejectsOldEpoch(t *testing.T) {
	store := newNodeTestStore(t)
	initial := nodeTestTopology("node-a", 1, 1)
	if err := store.CompareAndSaveTopology(0, initial); err != nil {
		t.Fatalf("提交初始 fence: %v", err)
	}
	service := newAuthorizedNodeService(t, "node-a", store, initial)
	listener := bufconn.Listen(nodeTestBufferSize)
	server := grpc.NewServer()
	pb.RegisterNodeServiceServer(server, NewNodeGRPCServer(service))
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()
	defer listener.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conn, err := grpc.NewClient(
		"passthrough:///node-authority-test",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("创建测试客户端: %v", err)
	}
	defer conn.Close()
	client := pb.NewNodeServiceClient(conn)

	_, err = client.AddPlayer(ctx, &pb.NodeAddPlayerRequest{
		Authority: &pb.MapAuthority{MapId: "green", OwnerNodeId: "node-a", MapEpoch: 1},
		Player:    &pb.PlayerState{Username: "current", LastMap: "green", LastNode: "node-a", Hp: 99, MaxHp: 100, Alive: true},
	})
	if err != nil {
		t.Fatalf("当前 owner/epoch 的 AddPlayer: %v", err)
	}
	profile, err := client.Profile(ctx, &pb.NodeProfileRequest{MapId: "green", Username: "current"})
	if err != nil || !profile.GetFound() || profile.GetPlayer().GetHp() != 99 {
		t.Fatalf("Profile = %+v, err=%v", profile, err)
	}

	next := nodeTestTopology("node-b", 2, 2)
	if err := store.CompareAndSaveTopology(1, next); err != nil {
		t.Fatalf("切换 Redis fence: %v", err)
	}
	// 故意不刷新 node-a 的 authority cache：旧 owner 仍相信 epoch=1，写入
	// 必须由写点的 Redis fence 拒绝，而不是被本地缓存提前短路。
	_, err = client.AddPlayer(ctx, &pb.NodeAddPlayerRequest{
		Authority: &pb.MapAuthority{MapId: "green", OwnerNodeId: "node-a", MapEpoch: 1},
		Player:    &pb.PlayerState{Username: "stale", LastMap: "green", LastNode: "node-a", Hp: 99, MaxHp: 100, Alive: true},
	})
	if got := status.Code(err); got != codes.FailedPrecondition {
		t.Fatalf("缓存滞后时旧 epoch 写入 code = %s，want FailedPrecondition; err=%v", got, err)
	}
	profile, err = client.Profile(ctx, &pb.NodeProfileRequest{MapId: "green", Username: "stale"})
	if err != nil || profile.GetFound() {
		t.Fatalf("缓存滞后时旧 epoch 写入改变了 world: profile=%+v err=%v", profile, err)
	}
}

func TestNodeRejectsEmptyPlayerUsername(t *testing.T) {
	store := newNodeTestStore(t)
	topology := nodeTestTopology("node-a", 1, 1)
	if err := store.CompareAndSaveTopology(0, topology); err != nil {
		t.Fatalf("提交初始 fence: %v", err)
	}
	server := NewNodeGRPCServer(newAuthorizedNodeService(t, "node-a", store, topology))
	_, err := server.AddPlayer(context.Background(), &pb.NodeAddPlayerRequest{
		Authority: &pb.MapAuthority{MapId: "green", OwnerNodeId: "node-a", MapEpoch: 1},
		Player:    &pb.PlayerState{Username: " \t "},
	})
	if got := status.Code(err); got != codes.InvalidArgument {
		t.Fatalf("空用户名 code = %s，want InvalidArgument; err=%v", got, err)
	}
	profile, err := server.Profile(context.Background(), &pb.NodeProfileRequest{MapId: "green", Username: ""})
	if err != nil || profile.GetFound() {
		t.Fatalf("空用户名意外写入 world: profile=%+v err=%v", profile, err)
	}
}

func newAuthorizedNodeService(t *testing.T, nodeID string, store *storage.Store, _ storage.Topology) *NodeService {
	t.Helper()
	service := NewNodeService(nodeID, "", store, config.DefaultRuntime().Node, "")
	if err := service.authority.refresh(store); err != nil {
		t.Fatalf("初始化测试拓扑缓存: %v", err)
	}
	if err := service.ensureOwnedMaps(); err != nil {
		t.Fatalf("初始化测试 owner 地图: %v", err)
	}
	return service
}

func newNodeTestStore(t *testing.T) *storage.Store {
	t.Helper()
	redisServer, err := exec.LookPath("redis-server")
	if err != nil {
		t.Skip("redis-server is required for Node fencing integration tests")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("预留 Redis 端口: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatalf("释放 Redis 端口: %v", err)
	}
	process := exec.Command(redisServer, "--bind", "127.0.0.1", "--port", strconv.Itoa(port), "--save", "", "--appendonly", "no")
	if err := process.Start(); err != nil {
		t.Fatalf("启动测试 Redis: %v", err)
	}
	t.Cleanup(func() {
		if process.Process != nil {
			_ = process.Process.Kill()
		}
		_ = process.Wait()
	})
	client := redis.NewClient(&redis.Options{Addr: net.JoinHostPort("127.0.0.1", strconv.Itoa(port))})
	t.Cleanup(func() { _ = client.Close() })
	deadline := time.Now().Add(3 * time.Second)
	for {
		if err := client.Ping(context.Background()).Err(); err == nil {
			break
		} else if time.Now().After(deadline) {
			t.Fatalf("测试 Redis 未就绪: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	store, err := storage.NewRedisStore(client)
	if err != nil {
		t.Fatalf("创建 Redis-only Store: %v", err)
	}
	return store
}

func nodeTestTopology(owner string, version, epoch uint64) storage.Topology {
	return storage.Topology{
		Version:   version,
		Owners:    map[string]string{"green": owner},
		MapEpochs: map[string]uint64{"green": epoch},
		UpdatedAt: time.Now().UTC(),
	}
}

func TestNodeRejectsMalformedAuthorityAndCheckpoint(t *testing.T) {
	server := NewNodeGRPCServer(NewNodeService("node-a", "", nil, config.DefaultRuntime().Node, ""))

	_, err := server.AddPlayer(context.Background(), &pb.NodeAddPlayerRequest{})
	if got := status.Code(err); got != codes.InvalidArgument {
		t.Fatalf("缺少 authority 的 code = %s，want InvalidArgument; err=%v", got, err)
	}

	_, err = server.AddPlayer(context.Background(), &pb.NodeAddPlayerRequest{
		Authority: &pb.MapAuthority{MapId: "green", OwnerNodeId: "node-b", MapEpoch: 2},
		Player:    &pb.PlayerState{Username: "tester"},
	})
	if got := status.Code(err); got != codes.FailedPrecondition {
		t.Fatalf("错误 owner 的 code = %s，want FailedPrecondition; err=%v", got, err)
	}

	// A syntactically valid mutation still reaches NodeService's authority/fence
	// check. With no topology source it fails closed as FailedPrecondition, which
	// is the same stable signal used for an old epoch or stale owner.
	_, err = server.AddPlayer(context.Background(), &pb.NodeAddPlayerRequest{
		Authority: &pb.MapAuthority{MapId: "green", OwnerNodeId: "node-a", MapEpoch: 1},
		Player:    &pb.PlayerState{Username: "tester"},
	})
	if got := status.Code(err); got != codes.FailedPrecondition {
		t.Fatalf("未授权写入的 code = %s，want FailedPrecondition; err=%v", got, err)
	}

	_, err = server.Promote(context.Background(), &pb.NodePromoteRequest{
		Authority: &pb.MapAuthority{MapId: "green", OwnerNodeId: "node-a", MapEpoch: 2},
		Checkpoint: &pb.NodeCheckpoint{
			MapId:      "green",
			NodeId:     "node-old",
			MapEpoch:   1,
			Version:    1,
			CapturedAt: &timestamppb.Timestamp{Seconds: 253402300800},
		},
	})
	if got := status.Code(err); got != codes.InvalidArgument {
		t.Fatalf("非法 Timestamp 的 code = %s，want InvalidArgument; err=%v", got, err)
	}
}
