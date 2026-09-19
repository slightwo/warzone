package main

import (
	"context"
	"fmt"
	"sync/atomic"

	"battleworld/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	authModeLogin    = "login"
	authModeRegister = "register"
)

// clientCommand constructs a fresh envelope payload for one generated request_id.
// It avoids exposing protobuf's package-private oneof interface outside pb.
type clientCommand func(uint64) *pb.ClientEnvelope

// gameClient owns the game stream and its underlying connection.
type gameClient struct {
	stream pb.GatewayService_GameStreamClient
	conn   *grpc.ClientConn
}

func (c *gameClient) Send(request *pb.ClientEnvelope) error {
	return c.stream.Send(request)
}

func (c *gameClient) Recv() (*pb.ServerEnvelope, error) {
	return c.stream.Recv()
}

func (c *gameClient) CloseSend() error {
	return c.stream.CloseSend()
}

func (c *gameClient) Close() error {
	_ = c.stream.CloseSend()
	return c.conn.Close()
}

func receiveGameUpdates(stream *gameClient, done chan<- struct{}) {
	defer close(done)
	for {
		envelope, err := stream.Recv()
		if err != nil {
			addClientNote("与网关连接已断开")
			drawUI()
			return
		}
		handleServerEnvelope(envelope)
		drawUI()
	}
}

func handleServerEnvelope(envelope *pb.ServerEnvelope) {
	if envelope == nil {
		return
	}
	if authenticated := envelope.GetAuthenticated(); authenticated != nil {
		applyState(authenticated.GetState())
		return
	}
	if state := envelope.GetState(); state != nil {
		applyState(state)
		return
	}
	if result := envelope.GetCommandResult(); result != nil {
		addClientNote(fmt.Sprintf("指令 #%d 已确认：%s", envelope.GetRequestId(), result.GetMessage()))
		return
	}
	if responseError := envelope.GetError(); responseError != nil {
		addClientNote(formatGatewayError(envelope.GetRequestId(), responseError))
		return
	}
	if notice := envelope.GetNotice(); notice != nil {
		addClientNote(notice.GetMessage())
	}
}

func sendPlayerCommand(stream *gameClient, nextRequestID *atomic.Uint64, command clientCommand) error {
	requestID := nextRequestID.Add(1)
	return stream.Send(command(requestID))
}

func commandForKey(key byte, shop bool) (clientCommand, string, bool) {
	if shop {
		switch key {
		case 'p', 'P', 27:
			return nil, "", true
		case '1':
			return buyItemCommand("potion"), "购买药剂", true
		case '2':
			return buyItemCommand("weapon"), "强化武器", true
		default:
			return nil, "", false
		}
	}

	switch key {
	case 'w', 'W':
		return moveCommand(pb.Direction_DIRECTION_UP), "向上移动", true
	case 's', 'S':
		return moveCommand(pb.Direction_DIRECTION_DOWN), "向下移动", true
	case 'a', 'A':
		return moveCommand(pb.Direction_DIRECTION_LEFT), "向左移动", true
	case 'd', 'D':
		return moveCommand(pb.Direction_DIRECTION_RIGHT), "向右移动", true
	case 'j', 'J', 'f', 'F':
		return attackCommand(), "发起近战攻击", true
	case 'k', 'K', 'h', 'H':
		return healCommand(), "使用药剂", true
	case 'b', 'B':
		return attackBossCommand(), "挑战世界首领", true
	case '1':
		return switchMapCommand("green"), "切换到青岚要塞", true
	case '2':
		return switchMapCommand("cave"), "切换到玄矿地窟", true
	case '3':
		return switchMapCommand("ruins"), "切换到残星遗迹", true
	default:
		return nil, "", false
	}
}

func moveCommand(direction pb.Direction) clientCommand {
	return func(requestID uint64) *pb.ClientEnvelope {
		return &pb.ClientEnvelope{RequestId: requestID, Payload: &pb.ClientEnvelope_Move{Move: &pb.MoveCommand{Direction: direction}}}
	}
}

func attackCommand() clientCommand {
	return func(requestID uint64) *pb.ClientEnvelope {
		return &pb.ClientEnvelope{RequestId: requestID, Payload: &pb.ClientEnvelope_Attack{Attack: &pb.AttackCommand{}}}
	}
}

func attackBossCommand() clientCommand {
	return func(requestID uint64) *pb.ClientEnvelope {
		return &pb.ClientEnvelope{RequestId: requestID, Payload: &pb.ClientEnvelope_AttackBoss{AttackBoss: &pb.AttackBossCommand{}}}
	}
}

func healCommand() clientCommand {
	return func(requestID uint64) *pb.ClientEnvelope {
		return &pb.ClientEnvelope{RequestId: requestID, Payload: &pb.ClientEnvelope_Heal{Heal: &pb.HealCommand{}}}
	}
}

func buyItemCommand(item string) clientCommand {
	return func(requestID uint64) *pb.ClientEnvelope {
		return &pb.ClientEnvelope{RequestId: requestID, Payload: &pb.ClientEnvelope_BuyItem{BuyItem: &pb.BuyItemCommand{Item: item}}}
	}
}

func switchMapCommand(mapID string) clientCommand {
	return func(requestID uint64) *pb.ClientEnvelope {
		return &pb.ClientEnvelope{RequestId: requestID, Payload: &pb.ClientEnvelope_SwitchMap{SwitchMap: &pb.SwitchMapCommand{MapId: mapID}}}
	}
}

func logoutCommand() clientCommand {
	return func(requestID uint64) *pb.ClientEnvelope {
		return &pb.ClientEnvelope{RequestId: requestID, Payload: &pb.ClientEnvelope_Logout{Logout: &pb.LogoutCommand{}}}
	}
}

func auth(addr, mode, username, password, confirm string) (*gameClient, *pb.WorldState, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, fmt.Errorf("连接网关失败：%w", err)
	}

	gameStream, err := pb.NewGatewayServiceClient(conn).GameStream(context.Background())
	if err != nil {
		_ = conn.Close()
		return nil, nil, fmt.Errorf("打开游戏流失败：%w", err)
	}
	stream := &gameClient{stream: gameStream, conn: conn}

	requestID := uint64(1)
	if err := stream.Send(authRequest(requestID, mode, username, password, confirm)); err != nil {
		_ = stream.Close()
		return nil, nil, fmt.Errorf("发送认证消息失败：%w", err)
	}

	reply, err := stream.Recv()
	if err != nil {
		_ = stream.Close()
		return nil, nil, fmt.Errorf("接收认证结果失败：%w", err)
	}
	if responseError := reply.GetError(); responseError != nil {
		_ = stream.Close()
		return nil, nil, fmt.Errorf("%s", formatGatewayError(reply.GetRequestId(), responseError))
	}
	authenticated := reply.GetAuthenticated()
	if authenticated == nil || authenticated.GetState() == nil {
		_ = stream.Close()
		return nil, nil, fmt.Errorf("网关返回了无效认证响应")
	}
	return stream, authenticated.GetState(), nil
}

func authRequest(requestID uint64, mode, username, password, confirm string) *pb.ClientEnvelope {
	request := &pb.ClientEnvelope{RequestId: requestID}
	switch mode {
	case authModeRegister:
		request.Payload = &pb.ClientEnvelope_Register{Register: &pb.RegisterRequest{
			Username:        username,
			Password:        password,
			ConfirmPassword: confirm,
		}}
	default:
		request.Payload = &pb.ClientEnvelope_Login{Login: &pb.LoginRequest{
			Username: username,
			Password: password,
		}}
	}
	return request
}

func formatGatewayError(requestID uint64, responseError *pb.ErrorResponse) string {
	if responseError == nil {
		return "网关返回未知错误"
	}
	prefix := "网关错误"
	if requestID != 0 {
		prefix = fmt.Sprintf("指令 #%d", requestID)
	}
	message := responseError.GetMessage()
	if message == "" {
		message = responseError.GetCode().String()
	}
	if responseError.GetRetryable() {
		return fmt.Sprintf("%s：%s（可重试）", prefix, message)
	}
	return fmt.Sprintf("%s：%s", prefix, message)
}
