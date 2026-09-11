package node

import (
	"context"
	"net"
	"os/exec"
	"strconv"
	"testing"
	"time"

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

const nodeV2TestBufferSize = 1024 * 1024

func TestNodeV1AndV2ServicesCoexist(t *testing.T) {
	service := NewNodeService("node-a", "", nil)
	listener := bufconn.Listen(nodeV2TestBufferSize)
	server := grpc.NewServer()
	pb.RegisterNodeServiceServer(server, NewNodeGRPCServer(service))
	pb.RegisterNodeServiceV2Server(server, NewNodeV2GRPCServer(service))
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()
	defer listener.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conn, err := grpc.NewClient(
		"passthrough:///node-v2-test",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("创建测试 gRPC 客户端: %v", err)
	}
	defer conn.Close()

	legacy, err := pb.NewNodeServiceClient(conn).Ping(ctx, &pb.PingReq{})
	if err != nil {
		t.Fatalf("V1 Ping: %v", err)
	}
	if legacy.GetTs() <= 0 {
		t.Fatalf("V1 Ping 时间戳 = %d，want positive", legacy.GetTs())
	}

	typed, err := pb.NewNodeServiceV2Client(conn).Ping(ctx, &pb.NodePingRequest{})
	if err != nil {
		t.Fatalf("V2 Ping: %v", err)
	}
	if typed.GetObservedAt() == nil || typed.GetObservedAt().CheckValid() != nil {
		t.Fatalf("V2 Ping observed_at 无效: %v", typed.GetObservedAt())
	}
}

func TestNodeV2AcceptsCurrentAuthorityAndRejectsOldEpoch(t *testing.T) {
	store := newNodeTestStore(t)
	initial := nodeTestTopology("node-a", "node-b", 1, 1)
	if err := store.CompareAndSaveTopology(0, initial); err != nil {
		t.Fatalf("提交初始 fence: %v", err)
	}
	service := newAuthorizedNodeService(t, "node-a", store, initial)
	listener := bufconn.Listen(nodeV2TestBufferSize)
	server := grpc.NewServer()
	pb.RegisterNodeServiceV2Server(server, NewNodeV2GRPCServer(service))
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()
	defer listener.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conn, err := grpc.NewClient(
		"passthrough:///node-v2-authority-test",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("创建 V2 测试客户端: %v", err)
	}
	defer conn.Close()
	client := pb.NewNodeServiceV2Client(conn)

	_, err = client.AddPlayer(ctx, &pb.NodeAddPlayerRequest{
		Authority: &pb.MapAuthority{MapId: "green", OwnerNodeId: "node-a", MapEpoch: 1},
		Player:    &pb.PlayerState{Username: "current", LastMap: "green", LastNode: "node-a", Hp: 99, MaxHp: 100, Alive: true},
	})
	if err != nil {
		t.Fatalf("当前 owner/epoch 的 V2 AddPlayer: %v", err)
	}
	profile, err := client.Profile(ctx, &pb.NodeProfileRequest{MapId: "green", Username: "current"})
	if err != nil || !profile.GetFound() || profile.GetPlayer().GetHp() != 99 {
		t.Fatalf("V2 Profile = %+v, err=%v", profile, err)
	}

	next := nodeTestTopology("node-b", "node-a", 2, 2)
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

func TestNodeV2RejectsEmptyPlayerUsername(t *testing.T) {
	store := newNodeTestStore(t)
	topology := nodeTestTopology("node-a", "node-b", 1, 1)
	if err := store.CompareAndSaveTopology(0, topology); err != nil {
		t.Fatalf("提交初始 fence: %v", err)
	}
	server := NewNodeV2GRPCServer(newAuthorizedNodeService(t, "node-a", store, topology))
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

func TestNodeV1ActionCheckpointAndPromoteRemainCompatible(t *testing.T) {
	store := newNodeTestStore(t)
	initial := nodeTestTopology("node-a", "node-b", 1, 1)
	if err := store.CompareAndSaveTopology(0, initial); err != nil {
		t.Fatalf("提交初始 fence: %v", err)
	}
	service := newAuthorizedNodeService(t, "node-a", store, initial)
	legacy := NewNodeGRPCServer(service)
	ctx := context.Background()

	_, err := legacy.AddPlayer(ctx, &pb.AddPlayerReq{
		MapId: "green", MapEpoch: 1,
		Profile: &pb.UserProfile{Username: "legacy", PasswordHash: "legacy-hash", LastMap: "green", LastNode: "node-a", Hp: 91, MaxHp: 100, Alive: true},
	})
	if err != nil {
		t.Fatalf("V1 AddPlayer: %v", err)
	}
	profile, err := legacy.Profile(ctx, &pb.ProfileReq{MapId: "green", Username: "legacy"})
	if err != nil || !profile.GetOk() || profile.GetProfile().GetUsername() != "legacy" || profile.GetProfile().GetHp() != 91 {
		t.Fatalf("V1 Profile = %+v, err=%v", profile, err)
	}
	checkpointResponse, err := legacy.Checkpoint(ctx, &pb.CheckpointReq{MapId: "green"})
	if err != nil || checkpointResponse.GetCheckpoint() == nil || checkpointResponse.GetCheckpoint().GetCheckpoint() == "" {
		t.Fatalf("V1 Checkpoint = %+v, err=%v", checkpointResponse, err)
	}
	if _, err := time.Parse(time.RFC3339, checkpointResponse.GetCheckpoint().GetCheckpoint()); err != nil {
		t.Fatalf("V1 checkpoint 时间格式错误: %v", err)
	}
	if _, err := legacy.AddPlayer(ctx, &pb.AddPlayerReq{MapId: "green", MapEpoch: 0, Profile: &pb.UserProfile{Username: "old"}}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("V1 旧 epoch code = %s，want FailedPrecondition; err=%v", status.Code(err), err)
	}

	promoteService := newAuthorizedNodeService(t, "node-b", store, initial)
	promote := NewNodeGRPCServer(promoteService)
	result, err := promote.Promote(ctx, &pb.PromoteReq{MapId: "green", MapEpoch: 2, Checkpoint: checkpointResponse.GetCheckpoint()})
	if err != nil || !result.GetOk() {
		t.Fatalf("V1 Promote = %+v, err=%v", result, err)
	}
	restored, err := promote.Profile(ctx, &pb.ProfileReq{MapId: "green", Username: "legacy"})
	if err != nil || !restored.GetOk() || restored.GetProfile().GetUsername() != "legacy" || restored.GetProfile().GetHp() != 91 {
		t.Fatalf("V1 Promote 后 profile = %+v, err=%v", restored, err)
	}
}

func newAuthorizedNodeService(t *testing.T, nodeID string, store *storage.Store, _ storage.Topology) *NodeService {
	t.Helper()
	service := NewNodeService(nodeID, "", store)
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

func nodeTestTopology(owner, replica string, version, epoch uint64) storage.Topology {
	return storage.Topology{
		Version:   version,
		Owners:    map[string]string{"green": owner},
		Replicas:  map[string]string{"green": replica},
		MapEpochs: map[string]uint64{"green": epoch},
		UpdatedAt: time.Now().UTC(),
	}
}

func TestNodeV2RejectsMalformedAuthorityAndCheckpoint(t *testing.T) {
	server := NewNodeV2GRPCServer(NewNodeService("node-a", "", nil))

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
