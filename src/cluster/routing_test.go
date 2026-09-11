package cluster

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"battleworld/pb"
	"battleworld/protocol"
	"battleworld/storage"
	"battleworld/world"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type fakeGatewayNodeClient struct {
	mu     sync.Mutex
	id     string
	closed bool
}

func (c *fakeGatewayNodeClient) NodeID() string { return c.id }
func (c *fakeGatewayNodeClient) AddPlayer(context.Context, string, uint64, *protocol.UserProfile) error {
	return nil
}
func (c *fakeGatewayNodeClient) RemovePlayer(context.Context, string, string, uint64) (protocol.UserProfile, bool, error) {
	return protocol.UserProfile{}, false, nil
}
func (c *fakeGatewayNodeClient) MovePlayer(context.Context, string, string, string, uint64) (string, protocol.UserProfile, bool, error) {
	return "", protocol.UserProfile{}, false, nil
}
func (c *fakeGatewayNodeClient) Attack(context.Context, string, string, uint64) (string, string, string, protocol.UserProfile, bool, error) {
	return "", "", "", protocol.UserProfile{}, false, nil
}
func (c *fakeGatewayNodeClient) Heal(context.Context, string, string, uint64) (string, protocol.UserProfile, bool, error) {
	return "", protocol.UserProfile{}, false, nil
}
func (c *fakeGatewayNodeClient) BuyItem(context.Context, string, string, string, uint64) (string, protocol.UserProfile, bool, error) {
	return "", protocol.UserProfile{}, false, nil
}
func (c *fakeGatewayNodeClient) AttackBoss(context.Context, string, string, uint64) (string, protocol.UserProfile, bool, error) {
	return "", protocol.UserProfile{}, false, nil
}
func (c *fakeGatewayNodeClient) Profile(context.Context, string, string) (protocol.UserProfile, bool, error) {
	return protocol.UserProfile{}, false, nil
}
func (c *fakeGatewayNodeClient) RewardPlayer(context.Context, string, string, int, int, uint64) (protocol.UserProfile, bool, error) {
	return protocol.UserProfile{}, false, nil
}
func (c *fakeGatewayNodeClient) Snapshot(context.Context, string) (protocol.MapView, error) {
	return protocol.MapView{}, nil
}
func (c *fakeGatewayNodeClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	return nil
}

func TestReconcileGatewayNodesUsesCommittedOwnersOnly(t *testing.T) {
	clients := make(map[string]*fakeGatewayNodeClient)
	cluster := &Cluster{
		nodes:       make(map[string]gatewayNodeClient),
		nodeAddrs:   make(map[string]string),
		nodeTargets: make(map[string]string),
		configs:     testMapConfigs("green"),
		activeNodes: func() ([]storage.NodeRegistryInfo, error) {
			return []storage.NodeRegistryInfo{
				{ID: "node-a", Addr: "127.0.0.1:9311"},
				{ID: "node-b", Addr: "127.0.0.1:9312"},
				{ID: "unassigned", Addr: "127.0.0.1:9313"},
			}, nil
		},
		nodeFactory: func(nodeID, _ string) (gatewayNodeClient, error) {
			client := &fakeGatewayNodeClient{id: nodeID}
			clients[nodeID] = client
			return client, nil
		},
	}

	first := testTopology(1, "node-a", "node-b")
	cluster.mu.Lock()
	if _, err := cluster.applyTopologyLocked(first); err != nil {
		cluster.mu.Unlock()
		t.Fatalf("应用首个拓扑: %v", err)
	}
	cluster.mu.Unlock()
	if err := cluster.reconcileGatewayNodes(first); err != nil {
		t.Fatalf("同步首个网关连接池: %v", err)
	}
	if len(cluster.nodes) != 1 || cluster.nodes["node-a"] == nil || clients["unassigned"] != nil {
		t.Fatalf("网关连接池未限定为 owner: %+v", cluster.nodes)
	}

	next := testTopology(2, "node-b", "node-a")
	cluster.mu.Lock()
	if _, err := cluster.applyTopologyLocked(next); err != nil {
		cluster.mu.Unlock()
		t.Fatalf("应用新拓扑: %v", err)
	}
	cluster.mu.Unlock()
	if err := cluster.reconcileGatewayNodes(next); err != nil {
		t.Fatalf("同步新网关连接池: %v", err)
	}

	clients["node-a"].mu.Lock()
	oldClosed := clients["node-a"].closed
	clients["node-a"].mu.Unlock()
	if !oldClosed {
		t.Fatal("owner 变更后未关闭旧数据面客户端")
	}
	if len(cluster.nodes) != 1 || cluster.nodes["node-b"] == nil {
		t.Fatalf("owner 变更后网关连接池错误: %+v", cluster.nodes)
	}

	if err := cluster.reconcileGatewayNodes(first); err != nil {
		t.Fatalf("同步过期拓扑不应报错: %v", err)
	}
	if len(cluster.nodes) != 1 || cluster.nodes["node-b"] == nil || cluster.nodeTargets["node-b"] == "" {
		t.Fatalf("过期拓扑回退了当前网关路由: nodes=%+v targets=%v", cluster.nodes, cluster.nodeTargets)
	}
}

func TestGatewayOwnerTargetsSkipsUnregisteredOwners(t *testing.T) {
	topology := testTopology(1, "node-a", "node-b")
	targets := gatewayOwnerTargets(topology, []storage.NodeRegistryInfo{{ID: "node-b", Addr: "127.0.0.1:9312"}})
	if len(targets) != 0 {
		t.Fatalf("未注册 owner 不应回退到 replica: %v", targets)
	}
}

func TestGatewayOwnerTargetsSkipsDrainingOwner(t *testing.T) {
	topology := testTopology(1, "node-a", "node-b")
	targets := gatewayOwnerTargets(topology, []storage.NodeRegistryInfo{{
		ID:       "node-a",
		Addr:     "127.0.0.1:9311",
		Draining: true,
	}})
	if len(targets) != 0 {
		t.Fatalf("draining owner 不应保留数据面目标: %v", targets)
	}
}

const nodeClientTestBufferSize = 1024 * 1024

type v1OnlyNodeServer struct {
	pb.UnimplementedNodeServiceServer
	pings      atomic.Int32
	addPlayers atomic.Int32
	promotes   atomic.Int32

	mu         sync.Mutex
	profile    *pb.UserProfile
	checkpoint *pb.MapCheckpoint
}

func (s *v1OnlyNodeServer) Ping(context.Context, *pb.PingReq) (*pb.PingResp, error) {
	s.pings.Add(1)
	return &pb.PingResp{Ts: 1}, nil
}

func (s *v1OnlyNodeServer) AddPlayer(_ context.Context, req *pb.AddPlayerReq) (*pb.AddPlayerResp, error) {
	s.addPlayers.Add(1)
	if req.GetProfile().GetUsername() == "" {
		return &pb.AddPlayerResp{Ok: false}, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.profile = req.GetProfile()
	return &pb.AddPlayerResp{Ok: true}, nil
}

func (s *v1OnlyNodeServer) Profile(_ context.Context, req *pb.ProfileReq) (*pb.ProfileResp, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.profile == nil || s.profile.GetUsername() != req.GetUsername() {
		return &pb.ProfileResp{}, nil
	}
	return &pb.ProfileResp{Profile: s.profile, Ok: true}, nil
}

func (s *v1OnlyNodeServer) Checkpoint(context.Context, *pb.CheckpointReq) (*pb.CheckpointResp, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.checkpoint == nil {
		s.checkpoint = &pb.MapCheckpoint{MapId: "green", NodeId: "node-a", MapEpoch: 1, Version: 3, Terrain: []string{"...."}, Checkpoint: "2026-09-10T16:00:00Z"}
	}
	return &pb.CheckpointResp{Checkpoint: s.checkpoint}, nil
}

func (s *v1OnlyNodeServer) Promote(_ context.Context, req *pb.PromoteReq) (*pb.PromoteResp, error) {
	s.promotes.Add(1)
	if req.GetCheckpoint() == nil || req.GetCheckpoint().GetCheckpoint() == "" {
		return &pb.PromoteResp{}, nil
	}
	return &pb.PromoteResp{Ok: true}, nil
}

func (s *v1OnlyNodeServer) View(context.Context, *pb.ViewReq) (*pb.ViewResp, error) {
	return &pb.ViewResp{View: &pb.NodeView{Id: "node-a", Healthy: true}}, nil
}

type rejectingV2NodeServer struct {
	pb.UnimplementedNodeServiceV2Server
	addPlayers atomic.Int32
}

func (*rejectingV2NodeServer) Ping(context.Context, *pb.NodePingRequest) (*pb.NodePingResponse, error) {
	return nil, status.Error(codes.FailedPrecondition, "topology unavailable")
}

func (s *rejectingV2NodeServer) AddPlayer(context.Context, *pb.NodeAddPlayerRequest) (*pb.NodeAddPlayerResponse, error) {
	s.addPlayers.Add(1)
	return nil, status.Error(codes.FailedPrecondition, "topology unavailable")
}

func TestNodeGRPCClientFallsBackOnlyForV1OnlyNode(t *testing.T) {
	fallbacksBefore := nodeV1FallbacksTotal.Value()
	listener := startNodeClientTestServer(t, func(server *grpc.Server) {
		pb.RegisterNodeServiceServer(server, &v1OnlyNodeServer{})
	})
	client := newNodeClientForListener(t, listener)

	if err := client.Ping(context.Background()); err != nil {
		t.Fatalf("V1 fallback Ping: %v", err)
	}
	if !client.usingV1() {
		t.Fatal("V2 Unimplemented 后未固定使用 V1")
	}
	if got := nodeV1FallbacksTotal.Value(); got != fallbacksBefore+1 {
		t.Fatalf("V1 fallback 遥测 = %d，want %d", got, fallbacksBefore+1)
	}
	if view := client.View(); view.ID != "node-a" || !view.Healthy {
		t.Fatalf("V1 fallback View = %+v", view)
	}
	if got := nodeV1FallbacksTotal.Value(); got != fallbacksBefore+1 {
		t.Fatalf("V1 fallback 后续 V1 调用不应重复计数，got %d，want %d", got, fallbacksBefore+1)
	}
}

func TestNodeGRPCClientDoesNotFallbackForV2SemanticError(t *testing.T) {
	legacy := &v1OnlyNodeServer{}
	typed := &rejectingV2NodeServer{}
	listener := startNodeClientTestServer(t, func(server *grpc.Server) {
		pb.RegisterNodeServiceServer(server, legacy)
		pb.RegisterNodeServiceV2Server(server, typed)
	})
	client := newNodeClientForListener(t, listener)

	err := client.Ping(context.Background())
	if got := status.Code(err); got != codes.FailedPrecondition {
		t.Fatalf("V2 语义错误 code = %s，want FailedPrecondition; err=%v", got, err)
	}
	if client.usingV1() {
		t.Fatal("V2 非 Unimplemented 错误不应触发 V1 回退")
	}
	if err := client.AddPlayer(context.Background(), "green", 1, &protocol.UserProfile{Username: "hero"}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("V2 写语义错误 code = %s，want FailedPrecondition; err=%v", status.Code(err), err)
	}
	if got := typed.addPlayers.Load(); got != 1 {
		t.Fatalf("V2 AddPlayer 调用次数 = %d，want 1", got)
	}
	if got := legacy.addPlayers.Load(); got != 0 {
		t.Fatalf("V2 语义错误后不应回退执行 V1 AddPlayer，got %d", got)
	}
}

func TestNodeGRPCClientFallsBackForWriteExactlyOnce(t *testing.T) {
	legacy := &v1OnlyNodeServer{}
	listener := startNodeClientTestServer(t, func(server *grpc.Server) {
		pb.RegisterNodeServiceServer(server, legacy)
	})
	client := newNodeClientForListener(t, listener)

	if err := client.AddPlayer(context.Background(), "green", 1, &protocol.UserProfile{Username: "hero"}); err != nil {
		t.Fatalf("V1 fallback AddPlayer: %v", err)
	}
	if !client.usingV1() {
		t.Fatal("V2 AddPlayer Unimplemented 后未固定使用 V1")
	}
	if got := legacy.addPlayers.Load(); got != 1 {
		t.Fatalf("V1 AddPlayer 调用次数 = %d，want exactly once", got)
	}
}

func TestNodeGRPCClientV1FallbackPreservesLegacyDataPlane(t *testing.T) {
	legacy := &v1OnlyNodeServer{}
	listener := startNodeClientTestServer(t, func(server *grpc.Server) {
		pb.RegisterNodeServiceServer(server, legacy)
	})
	client := newNodeClientForListener(t, listener)
	ctx := context.Background()
	original := &protocol.UserProfile{Username: "legacy", PasswordHash: "hash", LastMap: "green", LastNode: "node-a", HP: 91, MaxHP: 100, Alive: true}

	if err := client.AddPlayer(ctx, "green", 1, original); err != nil {
		t.Fatalf("V1 fallback AddPlayer: %v", err)
	}
	profile, found, err := client.Profile(ctx, "green", "legacy")
	if err != nil || !found || profile.PasswordHash != "hash" || profile.HP != 91 {
		t.Fatalf("V1 fallback Profile = %+v, found=%t, err=%v", profile, found, err)
	}
	checkpoint, err := client.Checkpoint(ctx, "green")
	if err != nil || checkpoint.MapID != "green" || checkpoint.Version != 3 || checkpoint.Checkpoint.IsZero() {
		t.Fatalf("V1 fallback Checkpoint = %+v, err=%v", checkpoint, err)
	}
	if err := client.Promote("green", world.MapConfig{}, checkpoint, 2); err != nil {
		t.Fatalf("V1 fallback Promote: %v", err)
	}
	if got := legacy.addPlayers.Load(); got != 1 {
		t.Fatalf("V1 AddPlayer 调用次数 = %d，want 1", got)
	}
	if got := legacy.promotes.Load(); got != 1 {
		t.Fatalf("V1 Promote 调用次数 = %d，want 1", got)
	}
}

func TestNodeGRPCClientFallbackIsSafeForConcurrentDiscovery(t *testing.T) {
	listener := startNodeClientTestServer(t, func(server *grpc.Server) {
		pb.RegisterNodeServiceServer(server, &v1OnlyNodeServer{})
	})
	client := newNodeClientForListener(t, listener)

	const workers = 16
	errs := make(chan error, workers)
	for range workers {
		go func() { errs <- client.Ping(context.Background()) }()
	}
	deadline := time.After(2 * time.Second)
	for range workers {
		select {
		case err := <-errs:
			if err != nil {
				t.Fatalf("并发 Ping: %v", err)
			}
		case <-deadline:
			t.Fatal("并发 V1 回退超时")
		}
	}
	if !client.usingV1() {
		t.Fatal("并发发现后未进入 V1 模式")
	}
}

func startNodeClientTestServer(t *testing.T, register func(*grpc.Server)) *bufconn.Listener {
	t.Helper()
	listener := bufconn.Listen(nodeClientTestBufferSize)
	server := grpc.NewServer()
	register(server)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})
	return listener
}

func newNodeClientForListener(t *testing.T, listener *bufconn.Listener) *NodeGRPCClient {
	t.Helper()
	conn, err := grpc.NewClient(
		"passthrough:///node-client-test",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("创建测试 gRPC 客户端: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return &NodeGRPCClient{
		v1:    pb.NewNodeServiceClient(conn),
		v2:    pb.NewNodeServiceV2Client(conn),
		conn:  conn,
		id:    "node-a",
		useV1: make(chan struct{}),
	}
}
