package main

import (
	"net"
	"testing"

	"google.golang.org/grpc"

	"battleworld/pb"
)

type clientTestGateway struct {
	pb.UnimplementedGatewayServiceServer

	request *pb.ClientEnvelope
}

func (s *clientTestGateway) GameStream(stream pb.GatewayService_GameStreamServer) error {
	request, err := stream.Recv()
	if err != nil {
		return err
	}
	s.request = request
	if request.GetRequestId() == 0 || request.GetLogin() == nil {
		return stream.Send(&pb.ServerEnvelope{RequestId: request.GetRequestId(), Payload: &pb.ServerEnvelope_Error{Error: &pb.ErrorResponse{
			Code:    pb.ErrorCode_ERROR_CODE_INVALID_REQUEST,
			Message: "expected login",
		}}})
	}
	return stream.Send(&pb.ServerEnvelope{RequestId: request.GetRequestId(), Payload: &pb.ServerEnvelope_Authenticated{Authenticated: &pb.Authenticated{
		State: clientTestState(1, 2, 3, 4),
	}}})
}

func TestAuthUsesSelectedAddress(t *testing.T) {
	gateway := &clientTestGateway{}
	address, stop := startClientTestGateway(t, gateway)
	defer stop()

	stream, state, err := auth(address, authModeLogin, "tester", "password", "")
	if err != nil {
		t.Fatalf("auth 返回错误: %v", err)
	}
	defer stream.Close()
	if gateway.request == nil || gateway.request.GetRequestId() == 0 || gateway.request.GetLogin().GetUsername() != "tester" {
		t.Fatalf("认证请求 = %+v", gateway.request)
	}
	if state.GetSessionVersion() != 1 || state.GetMap().GetVersion() != 4 {
		t.Fatalf("认证状态 = %+v", state)
	}
}

func TestClientStateOrdering(t *testing.T) {
	currentState := clientTestState(5, 2, 7, 100)
	for _, test := range []struct {
		name  string
		next  *pb.WorldState
		apply bool
	}{
		{name: "newer map version", next: clientTestState(5, 2, 7, 101), apply: true},
		{name: "new epoch", next: clientTestState(5, 2, 8, 1), apply: true},
		{name: "new session", next: clientTestState(6, 2, 1, 1), apply: true},
		{name: "newer topology", next: clientTestState(5, 3, 7, 1), apply: true},
		{name: "older session", next: clientTestState(4, 2, 9, 101)},
		{name: "older topology", next: clientTestState(5, 1, 9, 101)},
		{name: "older epoch", next: clientTestState(5, 2, 6, 101)},
		{name: "older map version", next: clientTestState(5, 2, 7, 99)},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := shouldApplyState(currentState, test.next); got != test.apply {
				t.Fatalf("shouldApplyState = %t, want %t", got, test.apply)
			}
		})
	}
}

func TestCommandConstructorsUseRequestIDAndTypedPayload(t *testing.T) {
	move := moveCommand(pb.Direction_DIRECTION_UP)(17)
	if move.GetRequestId() != 17 || move.GetMove().GetDirection() != pb.Direction_DIRECTION_UP {
		t.Fatalf("移动请求 = %+v", move)
	}
	logout := logoutCommand()(18)
	if logout.GetRequestId() != 18 || logout.GetLogout() == nil {
		t.Fatalf("退出请求 = %+v", logout)
	}
}

func clientTestState(sessionVersion int64, topologyVersion, epoch uint64, mapVersion int64) *pb.WorldState {
	return &pb.WorldState{
		Self:            &pb.PlayerView{Username: "tester", Alive: true},
		Map:             &pb.MapView{Id: "green", Version: mapVersion},
		SessionVersion:  sessionVersion,
		TopologyVersion: topologyVersion,
		MapEpoch:        epoch,
	}
}

func startClientTestGateway(t *testing.T, gateway *clientTestGateway) (string, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("监听测试 gRPC 地址: %v", err)
	}
	server := grpc.NewServer()
	pb.RegisterGatewayServiceServer(server, gateway)
	go func() {
		_ = server.Serve(listener)
	}()
	return listener.Addr().String(), func() {
		server.Stop()
		_ = listener.Close()
	}
}
