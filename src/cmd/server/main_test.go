package main

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"battleworld/pb"
	"battleworld/protocol"
	gatewaywire "battleworld/transport/gateway"
)

const gatewayTestBufferSize = 1024 * 1024

type fakeGatewayBackend struct {
	mu sync.Mutex

	registerErr   error
	loginErr      error
	quickEnterErr error
	adminErr      error
	moveErr       error

	adminText string

	registerCalls   int
	loginCalls      int
	quickEnterCalls int
	logoutCalls     int
	moveCalls       int
	snapshotCalls   int
	adminAction     string
	adminNodeID     string
}

func (b *fakeGatewayBackend) ExecuteAdmin(action, nodeID string) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.adminAction = action
	b.adminNodeID = nodeID
	if b.adminErr != nil {
		return "", b.adminErr
	}
	return b.adminText, nil
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

func (b *fakeGatewayBackend) adminRequest() (action, nodeID string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.adminAction, b.adminNodeID
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

func TestGameStreamRejectsNonAuthenticationFirstMessage(t *testing.T) {
	backend := &fakeGatewayBackend{}
	stream, closeClient := newGatewayTestStream(t, backend)
	defer closeClient()

	if err := stream.Send(gatewaywire.ToLegacyMessage(protocol.Message{Type: protocol.TypeMove, Dir: protocol.DirUp})); err != nil {
		t.Fatalf("发送首条非认证消息: %v", err)
	}
	response := receiveV1Message(t, stream)
	if response.Type != protocol.TypeError || !strings.Contains(response.Error, "首条消息必须是登录或注册请求") {
		t.Fatalf("首条非认证响应 = %+v", response)
	}
	if _, err := stream.Recv(); err == nil {
		t.Fatal("首条非认证被拒后 stream 应结束")
	}
}

func TestGameStreamAuthenticationFailureSendsErrorAndCloses(t *testing.T) {
	backend := &fakeGatewayBackend{loginErr: errors.New("用户名或密码错误")}
	stream, closeClient := newGatewayTestStream(t, backend)
	defer closeClient()

	if err := stream.Send(gatewaywire.ToLegacyMessage(protocol.Message{
		Type:     protocol.TypeLogin,
		Username: "tester",
		Password: "wrong",
	})); err != nil {
		t.Fatalf("发送登录请求: %v", err)
	}
	response := receiveV1Message(t, stream)
	if response.Type != protocol.TypeError || response.Error != "用户名或密码错误" {
		t.Fatalf("认证失败响应 = %+v", response)
	}
	if _, err := stream.Recv(); err == nil {
		t.Fatal("认证失败后 stream 应结束")
	}
}

func TestGameStreamAuthenticationThenPushesStateAndLogsOutOnce(t *testing.T) {
	backend := &fakeGatewayBackend{}
	stream, closeClient := newGatewayTestStream(t, backend)
	defer closeClient()

	if err := stream.Send(gatewaywire.ToLegacyMessage(protocol.Message{
		Type:     protocol.TypeLogin,
		Username: "tester",
		Password: "password",
	})); err != nil {
		t.Fatalf("发送登录请求: %v", err)
	}

	auth := receiveV1Message(t, stream)
	if auth.Type != protocol.TypeAuth || !auth.OK || auth.State == nil {
		t.Fatalf("认证成功响应 = %+v", auth)
	}
	assertTestState(t, auth.State)

	state := receiveMessageOfType(t, stream, protocol.TypeState)
	if state.State == nil {
		t.Fatal("状态推送没有 WorldState")
	}
	assertTestState(t, state.State)

	if err := stream.Send(gatewaywire.ToLegacyMessage(protocol.Message{Type: protocol.TypeLogout})); err != nil {
		t.Fatalf("发送退出请求: %v", err)
	}
	_ = stream.CloseSend()

	waitFor(t, time.Second, func() bool {
		logoutCalls, _, snapshotCalls := backend.calls()
		return logoutCalls == 1 && snapshotCalls > 0
	})
	logoutCalls, _, _ := backend.calls()
	if logoutCalls != 1 {
		t.Fatalf("Logout 调用次数 = %d, want 1", logoutCalls)
	}
}

func TestGameStreamCommandErrorKeepsStreamOpen(t *testing.T) {
	backend := &fakeGatewayBackend{moveErr: errors.New("地图边界不可通行")}
	stream, closeClient := newGatewayTestStream(t, backend)
	defer closeClient()

	loginV1(t, stream)
	if err := stream.Send(gatewaywire.ToLegacyMessage(protocol.Message{Type: protocol.TypeMove, Dir: protocol.DirUp})); err != nil {
		t.Fatalf("发送移动请求: %v", err)
	}
	errorMessage := receiveMessageOfType(t, stream, protocol.TypeError)
	if errorMessage.Error != "地图边界不可通行" {
		t.Fatalf("移动错误 = %q", errorMessage.Error)
	}

	if err := stream.Send(gatewaywire.ToLegacyMessage(protocol.Message{Type: protocol.TypeMove, Dir: protocol.DirDown})); err != nil {
		t.Fatalf("错误后继续发送命令: %v", err)
	}
	secondError := receiveMessageOfType(t, stream, protocol.TypeError)
	if secondError.Error != "地图边界不可通行" {
		t.Fatalf("第二次移动错误 = %q", secondError.Error)
	}

	_, moveCalls, _ := backend.calls()
	if moveCalls != 2 {
		t.Fatalf("Move 调用次数 = %d, want 2", moveCalls)
	}
}

func TestGameStreamAdminStatusUsesSingleRequestStream(t *testing.T) {
	backend := &fakeGatewayBackend{adminText: "网关路由状态：Topology Version=2"}
	stream, closeClient := newGatewayTestStream(t, backend)
	defer closeClient()

	if err := stream.Send(gatewaywire.ToLegacyMessage(protocol.Message{
		Type:   protocol.TypeAdmin,
		Action: "status",
		NodeID: "node-a",
	})); err != nil {
		t.Fatalf("发送管理请求: %v", err)
	}
	response := receiveV1Message(t, stream)
	if response.Type != protocol.TypeAdmin || !response.OK || response.Text != backend.adminText {
		t.Fatalf("管理响应 = %+v", response)
	}
	if action, nodeID := backend.adminRequest(); action != "status" || nodeID != "node-a" {
		t.Fatalf("管理请求 = action=%q nodeID=%q", action, nodeID)
	}
	if _, err := stream.Recv(); err != io.EOF {
		t.Fatalf("admin 单次响应后 Recv error = %v, want EOF", err)
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

func newGatewayV2TestStream(t *testing.T, backend gatewayBackend) (pb.GatewayService_GameStreamV2Client, func()) {
	t.Helper()
	ctx, gatewayClient, _, closeClient := newGatewayTestClients(t, backend)
	stream, err := gatewayClient.GameStreamV2(ctx)
	if err != nil {
		closeClient()
		t.Fatalf("打开 V2 测试游戏流: %v", err)
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

func loginV1(t *testing.T, stream pb.GatewayService_GameStreamClient) {
	t.Helper()
	if err := stream.Send(gatewaywire.ToLegacyMessage(protocol.Message{
		Type:     protocol.TypeLogin,
		Username: "tester",
		Password: "password",
	})); err != nil {
		t.Fatalf("发送登录请求: %v", err)
	}
	response := receiveV1Message(t, stream)
	if response.Type != protocol.TypeAuth || !response.OK || response.State == nil {
		t.Fatalf("登录响应 = %+v", response)
	}
}

func receiveV1Message(t *testing.T, stream pb.GatewayService_GameStreamClient) protocol.Message {
	t.Helper()
	response, err := stream.Recv()
	if err != nil {
		t.Fatalf("接收游戏流消息: %v", err)
	}
	return gatewaywire.FromLegacyMessage(response)
}

func receiveMessageOfType(t *testing.T, stream pb.GatewayService_GameStreamClient, messageType string) protocol.Message {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		response := receiveV1Message(t, stream)
		if response.Type == messageType {
			return response
		}
	}
	t.Fatalf("在超时前未收到消息类型 %q", messageType)
	return protocol.Message{}
}

func assertTestState(t *testing.T, state *protocol.WorldState) {
	t.Helper()
	if state.SessionVersion != 1 || state.TopologyVersion != 2 || state.MapEpoch != 3 || state.Map.ID != "green" {
		t.Fatalf("状态字段 = %+v", state)
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
