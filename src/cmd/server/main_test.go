package main

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"battleworld/pb"
	"battleworld/protocol"
)

const gatewayTestBufferSize = 1024 * 1024

type fakeGatewayBackend struct {
	mu sync.Mutex

	registerErr   error
	loginErr      error
	quickEnterErr error
	moveErr       error

	adminText string

	registerCalls   int
	loginCalls      int
	quickEnterCalls int
	logoutCalls     int
	moveCalls       int
	snapshotCalls   int
}

func (b *fakeGatewayBackend) Register(_, _, _ string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.registerCalls++
	return b.registerErr
}

func (b *fakeGatewayBackend) Login(_, _ string) (*protocol.WorldState, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.loginCalls++
	if b.loginErr != nil {
		return nil, b.loginErr
	}
	return testWorldState(), nil
}

func (b *fakeGatewayBackend) QuickEnter(_, _ string) (*protocol.WorldState, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.quickEnterCalls++
	if b.quickEnterErr != nil {
		return nil, b.quickEnterErr
	}
	return testWorldState(), nil
}

func (b *fakeGatewayBackend) Logout(string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.logoutCalls++
	return nil
}

func (b *fakeGatewayBackend) SnapshotFor(string) (*protocol.WorldState, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.snapshotCalls++
	return testWorldState(), nil
}

func (b *fakeGatewayBackend) Move(_, _ string) (*protocol.WorldState, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.moveCalls++
	if b.moveErr != nil {
		return nil, b.moveErr
	}
	return testWorldState(), nil
}

func (b *fakeGatewayBackend) Attack(string) (*protocol.WorldState, error) {
	return testWorldState(), nil
}

func (b *fakeGatewayBackend) AttackBoss(string) (*protocol.WorldState, error) {
	return testWorldState(), nil
}

func (b *fakeGatewayBackend) Heal(string) (*protocol.WorldState, error) {
	return testWorldState(), nil
}

func (b *fakeGatewayBackend) BuyItem(_, _ string) (*protocol.WorldState, error) {
	return testWorldState(), nil
}

func (b *fakeGatewayBackend) SwitchMap(_, _ string) (*protocol.WorldState, error) {
	return testWorldState(), nil
}

func (b *fakeGatewayBackend) GatewayStatus() protocol.GatewayStatus {
	b.mu.Lock()
	defer b.mu.Unlock()
	return protocol.GatewayStatus{
		Summary:         b.adminText,
		RoutingReady:    true,
		TopologyVersion: 2,
		Nodes: []protocol.NodeView{{
			ID:          "node-a",
			Addr:        "127.0.0.1:9311",
			Healthy:     true,
			PrimaryMaps: []string{"green"},
		}},
	}
}

func (b *fakeGatewayBackend) calls() (logout, move, snapshot int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.logoutCalls, b.moveCalls, b.snapshotCalls
}

func testWorldState() *protocol.WorldState {
	return &protocol.WorldState{
		Self: protocol.PlayerView{
			Username: "tester",
			MapID:    "green",
			HP:       120,
			Alive:    true,
		},
		Map: protocol.MapView{
			ID:      "green",
			Name:    "青岚要塞",
			NodeID:  "node-a",
			Terrain: []string{"...."},
			Version: 1,
		},
		SessionVersion:  1,
		TopologyVersion: 2,
		MapEpoch:        3,
	}
}

func newGatewayTestStream(t *testing.T, backend gatewayBackend) (pb.GatewayService_GameStreamClient, func()) {
	t.Helper()
	ctx, gatewayClient, _, closeClient := newGatewayTestClients(t, backend)
	stream, err := gatewayClient.GameStream(ctx)
	if err != nil {
		closeClient()
		t.Fatalf("打开测试游戏流: %v", err)
	}
	return stream, closeClient
}

func newGatewayTestClients(t *testing.T, backend gatewayBackend) (context.Context, pb.GatewayServiceClient, pb.AdminServiceClient, func()) {
	t.Helper()

	listener := bufconn.Listen(gatewayTestBufferSize)
	server := grpc.NewServer()
	gateway := &GatewayServer{
		gameCluster:   backend,
		stateInterval: 10 * time.Millisecond,
	}
	pb.RegisterGatewayServiceServer(server, gateway)
	pb.RegisterAdminServiceServer(server, gateway)
	go func() {
		_ = server.Serve(listener)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	conn, err := grpc.NewClient(
		"passthrough:///gateway-test",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		cancel()
		server.Stop()
		_ = listener.Close()
		t.Fatalf("创建测试 gRPC 客户端: %v", err)
	}

	return ctx, pb.NewGatewayServiceClient(conn), pb.NewAdminServiceClient(conn), func() {
		cancel()
		_ = conn.Close()
		server.Stop()
		_ = listener.Close()
	}
}

func waitFor(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("等待条件满足超时")
}
