package main

import (
	"errors"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"battleworld/pb"
	"battleworld/protocol"
)

func TestGameStreamRejectsNonAuthenticationFirstMessage(t *testing.T) {
	stream, closeClient := newGatewayTestStream(t, &fakeGatewayBackend{})
	defer closeClient()

	if err := stream.Send(moveRequest(11, pb.Direction_DIRECTION_UP)); err != nil {
		t.Fatalf("发送首条命令: %v", err)
	}
	response := receiveEnvelope(t, stream)
	assertError(t, response, 11, pb.ErrorCode_ERROR_CODE_INVALID_REQUEST, false)
	if _, err := stream.Recv(); err == nil {
		t.Fatal("首条非认证消息被拒后 stream 应结束")
	}
}

func TestGameStreamAuthenticationFailureUsesStableErrorCode(t *testing.T) {
	backend := &fakeGatewayBackend{loginErr: errors.New("用户名或密码错误")}
	stream, closeClient := newGatewayTestStream(t, backend)
	defer closeClient()

	if err := stream.Send(loginRequest(12)); err != nil {
		t.Fatalf("发送登录请求: %v", err)
	}
	response := receiveEnvelope(t, stream)
	assertError(t, response, 12, pb.ErrorCode_ERROR_CODE_AUTH_FAILED, false)
	if _, err := stream.Recv(); err == nil {
		t.Fatal("认证失败后 stream 应结束")
	}
}

func TestGameStreamAuthenticatesCommandsAndPushesState(t *testing.T) {
	backend := &fakeGatewayBackend{}
	stream, closeClient := newGatewayTestStream(t, backend)
	defer closeClient()

	login(t, stream, 21)
	if err := stream.Send(moveRequest(22, pb.Direction_DIRECTION_UP)); err != nil {
		t.Fatalf("发送移动命令: %v", err)
	}
	result := receiveMatching(t, stream, func(response *pb.ServerEnvelope) bool {
		return response.GetCommandResult() != nil && response.GetRequestId() == 22
	})
	if result.GetCommandResult().GetMessage() != "移动指令已接受" {
		t.Fatalf("移动确认文本 = %q", result.GetCommandResult().GetMessage())
	}

	state := receiveMatching(t, stream, func(response *pb.ServerEnvelope) bool {
		return response.GetState() != nil
	})
	if state.GetRequestId() != 0 {
		t.Fatalf("主动状态 request_id = %d, want 0", state.GetRequestId())
	}
	assertTestState(t, state.GetState())

	if err := stream.Send(&pb.ClientEnvelope{
		RequestId: 23,
		Payload:   &pb.ClientEnvelope_Logout{Logout: &pb.LogoutCommand{}},
	}); err != nil {
		t.Fatalf("发送退出命令: %v", err)
	}
	logoutResult := receiveMatching(t, stream, func(response *pb.ServerEnvelope) bool {
		return response.GetCommandResult() != nil && response.GetRequestId() == 23
	})
	if logoutResult.GetCommandResult().GetMessage() != "已退出游戏" {
		t.Fatalf("退出确认文本 = %q", logoutResult.GetCommandResult().GetMessage())
	}
	waitFor(t, time.Second, func() bool {
		logoutCalls, _, _ := backend.calls()
		return logoutCalls == 1
	})
}

func TestGameStreamCommandErrorKeepsStreamOpen(t *testing.T) {
	backend := &fakeGatewayBackend{moveErr: errors.New("移动请求被拒绝")}
	stream, closeClient := newGatewayTestStream(t, backend)
	defer closeClient()

	login(t, stream, 31)
	if err := stream.Send(moveRequest(32, pb.Direction_DIRECTION_UP)); err != nil {
		t.Fatalf("发送被拒绝移动: %v", err)
	}
	response := receiveMatching(t, stream, func(response *pb.ServerEnvelope) bool {
		return response.GetError() != nil && response.GetRequestId() == 32
	})
	assertError(t, response, 32, pb.ErrorCode_ERROR_CODE_COMMAND_REJECTED, false)

	if err := stream.Send(moveRequest(33, pb.Direction_DIRECTION_DOWN)); err != nil {
		t.Fatalf("命令错误后继续发送: %v", err)
	}
	second := receiveMatching(t, stream, func(response *pb.ServerEnvelope) bool {
		return response.GetError() != nil && response.GetRequestId() == 33
	})
	assertError(t, second, 33, pb.ErrorCode_ERROR_CODE_COMMAND_REJECTED, false)
}

func TestGameStreamRejectsInvalidCommandRequestIDAndDirection(t *testing.T) {
	stream, closeClient := newGatewayTestStream(t, &fakeGatewayBackend{})
	defer closeClient()

	login(t, stream, 41)
	if err := stream.Send(moveRequest(0, pb.Direction_DIRECTION_UP)); err != nil {
		t.Fatalf("发送零 request_id 命令: %v", err)
	}
	zeroID := receiveMatching(t, stream, func(response *pb.ServerEnvelope) bool {
		return response.GetError() != nil && response.GetRequestId() == 0
	})
	assertError(t, zeroID, 0, pb.ErrorCode_ERROR_CODE_INVALID_REQUEST, false)

	if err := stream.Send(moveRequest(42, pb.Direction_DIRECTION_UNSPECIFIED)); err != nil {
		t.Fatalf("发送无效方向命令: %v", err)
	}
	invalidDirection := receiveMatching(t, stream, func(response *pb.ServerEnvelope) bool {
		return response.GetError() != nil && response.GetRequestId() == 42
	})
	assertError(t, invalidDirection, 42, pb.ErrorCode_ERROR_CODE_INVALID_REQUEST, false)
}

func TestGameStreamMapsStaleRouteErrors(t *testing.T) {
	backend := &fakeGatewayBackend{moveErr: status.Error(codes.FailedPrecondition, "map authority denied")}
	stream, closeClient := newGatewayTestStream(t, backend)
	defer closeClient()

	login(t, stream, 51)
	if err := stream.Send(moveRequest(52, pb.Direction_DIRECTION_UP)); err != nil {
		t.Fatalf("发送过期路由命令: %v", err)
	}
	response := receiveMatching(t, stream, func(response *pb.ServerEnvelope) bool {
		return response.GetError() != nil && response.GetRequestId() == 52
	})
	assertError(t, response, 52, pb.ErrorCode_ERROR_CODE_STALE_ROUTE, true)
}

func TestGetGatewayStatusUsesDedicatedAdminService(t *testing.T) {
	backend := &fakeGatewayBackend{adminText: "网关路由状态：Topology Version=2"}
	ctx, _, adminClient, closeClient := newGatewayTestClients(t, backend)
	defer closeClient()

	response, err := adminClient.GetGatewayStatus(ctx, &pb.GatewayStatusRequest{})
	if err != nil {
		t.Fatalf("调用 GetGatewayStatus: %v", err)
	}
	if !response.GetRoutingReady() || response.GetTopologyVersion() != 2 || response.GetSummary() != backend.adminText {
		t.Fatalf("GatewayStatus = %+v", response)
	}
	if len(response.GetNodes()) != 1 || response.GetNodes()[0].GetId() != "node-a" {
		t.Fatalf("GatewayStatus 节点 = %+v", response.GetNodes())
	}
}

func TestSenderCoalescesStateAndRejectsRegression(t *testing.T) {
	stream := &recordingStream{}
	sender := newStreamSender(stream)
	sender.SubmitState(buildWorldState(4, 4, 4))
	sender.SubmitState(buildWorldState(6, 6, 6))
	sender.SubmitState(buildWorldState(5, 5, 5))
	sender.Start()
	defer func() {
		sender.Abort()
		sender.Wait()
	}()

	waitFor(t, time.Second, func() bool { return stream.count() == 1 })
	sent := stream.envelopes()
	if len(sent) != 1 || sent[0].GetState() == nil {
		t.Fatalf("状态发送 = %+v", sent)
	}
	if sent[0].GetState().GetSessionVersion() != 6 || sent[0].GetState().GetTopologyVersion() != 6 || sent[0].GetState().GetMapEpoch() != 6 {
		t.Fatalf("状态合并/回退保护失败: %+v", sent[0].GetState())
	}
}

func TestStateOrdering(t *testing.T) {
	previous := stateVersion{mapID: "green", sessionVersion: 5, mapEpoch: 7, mapVersion: 100}
	for _, test := range []struct {
		name      string
		next      stateVersion
		regresses bool
	}{
		{name: "newer map version", next: stateVersion{mapID: "green", sessionVersion: 5, mapEpoch: 7, mapVersion: 101}},
		{name: "new epoch permits map version reset", next: stateVersion{mapID: "green", sessionVersion: 5, mapEpoch: 8, mapVersion: 1}},
		{name: "new session permits epoch reset", next: stateVersion{mapID: "green", sessionVersion: 6, mapEpoch: 1, mapVersion: 1}},
		{name: "older session", next: stateVersion{mapID: "green", sessionVersion: 4, mapEpoch: 9, mapVersion: 101}, regresses: true},
		{name: "older epoch", next: stateVersion{mapID: "green", sessionVersion: 5, mapEpoch: 6, mapVersion: 101}, regresses: true},
		{name: "older map version", next: stateVersion{mapID: "green", sessionVersion: 5, mapEpoch: 7, mapVersion: 99}, regresses: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := stateRegresses(test.next, previous); got != test.regresses {
				t.Fatalf("stateRegresses(%+v, %+v) = %t, want %t", test.next, previous, got, test.regresses)
			}
		})
	}
}

func TestSenderDrainsControlMessagesOnFinish(t *testing.T) {
	stream := &recordingStream{}
	sender := newStreamSender(stream)
	if !sender.EnqueueControl(&pb.ServerEnvelope{RequestId: 61, Payload: &pb.ServerEnvelope_CommandResult{CommandResult: &pb.CommandResult{Message: "first"}}}) {
		t.Fatal("入队 first 控制消息失败")
	}
	if !sender.EnqueueControl(&pb.ServerEnvelope{RequestId: 62, Payload: &pb.ServerEnvelope_Error{Error: &pb.ErrorResponse{Code: pb.ErrorCode_ERROR_CODE_COMMAND_REJECTED}}}) {
		t.Fatal("入队 second 控制消息失败")
	}
	sender.SubmitState(buildWorldState(9, 9, 9))
	sender.Start()
	sender.Finish()
	sender.Wait()

	sent := stream.envelopes()
	if len(sent) != 2 || sent[0].GetRequestId() != 61 || sent[1].GetRequestId() != 62 {
		t.Fatalf("结束时控制消息未按可靠队列排空: %+v", sent)
	}
	for _, envelope := range sent {
		if envelope.GetState() != nil {
			t.Fatalf("结束时不应发送可合并状态: %+v", envelope)
		}
	}
}

func TestSenderAppliesBackpressureInsteadOfDroppingControlMessages(t *testing.T) {
	stream := newBlockingStream()
	sender := newStreamSender(stream)
	sender.Start()

	if !sender.EnqueueControl(&pb.ServerEnvelope{RequestId: 1, Payload: &pb.ServerEnvelope_CommandResult{CommandResult: &pb.CommandResult{}}}) {
		t.Fatal("入队首个控制消息失败")
	}
	<-stream.started
	for requestID := 2; requestID <= controlQueueCapacity+1; requestID++ {
		if !sender.EnqueueControl(&pb.ServerEnvelope{RequestId: uint64(requestID), Payload: &pb.ServerEnvelope_CommandResult{CommandResult: &pb.CommandResult{}}}) {
			t.Fatalf("控制队列在填满前拒绝 request_id=%d", requestID)
		}
	}

	blocked := make(chan bool, 1)
	go func() {
		blocked <- sender.EnqueueControl(&pb.ServerEnvelope{RequestId: 99, Payload: &pb.ServerEnvelope_CommandResult{CommandResult: &pb.CommandResult{}}})
	}()
	select {
	case result := <-blocked:
		t.Fatalf("队列饱和时控制消息不应被丢弃或提前返回，got %t", result)
	case <-time.After(20 * time.Millisecond):
	}

	sender.Abort()
	close(stream.release)
	if result := <-blocked; result {
		t.Fatal("中止后被阻塞的控制消息不应被报告为已入队")
	}
	sender.Wait()
}

func loginRequest(requestID uint64) *pb.ClientEnvelope {
	return &pb.ClientEnvelope{
		RequestId: requestID,
		Payload: &pb.ClientEnvelope_Login{Login: &pb.LoginRequest{
			Username: "tester",
			Password: "password",
		}},
	}
}

func moveRequest(requestID uint64, direction pb.Direction) *pb.ClientEnvelope {
	return &pb.ClientEnvelope{
		RequestId: requestID,
		Payload:   &pb.ClientEnvelope_Move{Move: &pb.MoveCommand{Direction: direction}},
	}
}

func login(t *testing.T, stream pb.GatewayService_GameStreamClient, requestID uint64) {
	t.Helper()
	if err := stream.Send(loginRequest(requestID)); err != nil {
		t.Fatalf("发送登录请求: %v", err)
	}
	response := receiveMatching(t, stream, func(response *pb.ServerEnvelope) bool {
		return response.GetAuthenticated() != nil && response.GetRequestId() == requestID
	})
	if response.GetAuthenticated().GetState() == nil {
		t.Fatal("认证成功响应缺少 WorldState")
	}
	assertTestState(t, response.GetAuthenticated().GetState())
}

func receiveEnvelope(t *testing.T, stream pb.GatewayService_GameStreamClient) *pb.ServerEnvelope {
	t.Helper()
	response, err := stream.Recv()
	if err != nil {
		t.Fatalf("接收游戏流消息: %v", err)
	}
	return response
}

func receiveMatching(t *testing.T, stream pb.GatewayService_GameStreamClient, predicate func(*pb.ServerEnvelope) bool) *pb.ServerEnvelope {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		response := receiveEnvelope(t, stream)
		if predicate(response) {
			return response
		}
	}
	t.Fatal("在超时前未收到匹配的消息")
	return nil
}

func assertError(t *testing.T, response *pb.ServerEnvelope, requestID uint64, code pb.ErrorCode, retryable bool) {
	t.Helper()
	if response.GetRequestId() != requestID || response.GetError() == nil || response.GetError().GetCode() != code || response.GetError().GetRetryable() != retryable {
		t.Fatalf("错误响应 = %+v, want request_id=%d code=%s retryable=%t", response, requestID, code, retryable)
	}
}

func assertTestState(t *testing.T, state *pb.WorldState) {
	t.Helper()
	if state.GetSessionVersion() != 1 || state.GetTopologyVersion() != 2 || state.GetMapEpoch() != 3 || state.GetMap().GetId() != "green" {
		t.Fatalf("状态字段 = %+v", state)
	}
}

func buildWorldState(sessionVersion int64, topologyVersion, mapEpoch uint64) *protocol.WorldState {
	state := testWorldState()
	state.SessionVersion = sessionVersion
	state.TopologyVersion = topologyVersion
	state.MapEpoch = mapEpoch
	return state
}

type recordingStream struct {
	pb.GatewayService_GameStreamServer
	mu   sync.Mutex
	sent []*pb.ServerEnvelope
}

func (s *recordingStream) Send(envelope *pb.ServerEnvelope) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sent = append(s.sent, envelope)
	return nil
}

func (s *recordingStream) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.sent)
}

func (s *recordingStream) envelopes() []*pb.ServerEnvelope {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*pb.ServerEnvelope(nil), s.sent...)
}

type blockingStream struct {
	pb.GatewayService_GameStreamServer
	started chan struct{}
	release chan struct{}
}

func newBlockingStream() *blockingStream {
	return &blockingStream{
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
}

func (s *blockingStream) Send(*pb.ServerEnvelope) error {
	select {
	case s.started <- struct{}{}:
	default:
	}
	<-s.release
	return nil
}
