package main

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"battleworld/pb"
	"battleworld/protocol"
	gatewaywire "battleworld/transport/gateway"
)

func (s *GatewayServer) GameStream(stream pb.GatewayService_GameStreamServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	if first.GetRequestId() == 0 {
		return sendInitialError(stream, 0, pb.ErrorCode_ERROR_CODE_INVALID_REQUEST, "request_id 必须为非零值", false)
	}

	username, initialState, err := s.authenticateStream(first)
	if err != nil {
		response := gatewayError(first.GetRequestId(), err, authError)
		_ = stream.Send(response)
		return err
	}
	if initialState == nil {
		return sendInitialError(stream, first.GetRequestId(), pb.ErrorCode_ERROR_CODE_INTERNAL, "认证成功但未返回世界状态", true)
	}

	initialStatePB := gatewaywire.ToWorldState(initialState)
	protocol.FreeWorldState(initialState)
	if err := stream.Send(&pb.ServerEnvelope{
		RequestId: first.GetRequestId(),
		Payload: &pb.ServerEnvelope_Authenticated{Authenticated: &pb.Authenticated{
			State: initialStatePB,
		}},
	}); err != nil {
		_ = s.gameCluster.Logout(username)
		return err
	}

	sender := newStreamSender(stream)
	sender.Start()
	statePumpDone := make(chan struct{})
	go s.pumpStates(sender, username, statePumpDone)

	var logoutOnce sync.Once
	logout := func() error {
		var logoutErr error
		logoutOnce.Do(func() {
			logoutErr = s.gameCluster.Logout(username)
		})
		return logoutErr
	}
	defer func() {
		sender.Abort()
		_ = logout()
		<-statePumpDone
		sender.Wait()
	}()

	inbound := make(chan streamInbound)
	go receiveInbound(stream, sender, inbound)

	for {
		select {
		case <-sender.Done():
			return nil
		case incoming, ok := <-inbound:
			if !ok {
				return nil
			}
			if incoming.err != nil {
				if errors.Is(incoming.err, io.EOF) {
					sender.Finish()
					<-statePumpDone
					sender.Wait()
					return nil
				}
				return incoming.err
			}
			if shouldClose, ok := s.handleCommand(sender, username, logout, incoming.request); !ok {
				return nil
			} else if shouldClose {
				sender.Finish()
				<-statePumpDone
				sender.Wait()
				return nil
			}
		}
	}
}

type streamInbound struct {
	request *pb.ClientEnvelope
	err     error
}

func receiveInbound(stream pb.GatewayService_GameStreamServer, sender *streamSender, inbound chan<- streamInbound) {
	defer close(inbound)
	for {
		request, err := stream.Recv()
		select {
		case inbound <- streamInbound{request: request, err: err}:
		case <-sender.Done():
			return
		case <-sender.Finished():
			return
		}
		if err != nil {
			return
		}
	}
}

func (s *GatewayServer) authenticateStream(request *pb.ClientEnvelope) (string, *protocol.WorldState, error) {
	switch payload := request.GetPayload().(type) {
	case *pb.ClientEnvelope_Login:
		if payload.Login == nil {
			return "", nil, errors.New("登录请求不能为空")
		}
		state, err := s.gameCluster.Login(payload.Login.GetUsername(), payload.Login.GetPassword())
		return payload.Login.GetUsername(), state, err
	case *pb.ClientEnvelope_Register:
		if payload.Register == nil {
			return "", nil, errors.New("注册请求不能为空")
		}
		if err := s.gameCluster.Register(payload.Register.GetUsername(), payload.Register.GetPassword(), payload.Register.GetConfirmPassword()); err != nil {
			return payload.Register.GetUsername(), nil, err
		}
		state, err := s.gameCluster.Login(payload.Register.GetUsername(), payload.Register.GetPassword())
		return payload.Register.GetUsername(), state, err
	case *pb.ClientEnvelope_QuickEnter:
		if payload.QuickEnter == nil {
			return "", nil, errors.New("快速进入请求不能为空")
		}
		state, err := s.gameCluster.QuickEnter(payload.QuickEnter.GetUsername(), payload.QuickEnter.GetPassword())
		return payload.QuickEnter.GetUsername(), state, err
	default:
		return "", nil, errors.New("首条消息必须是登录、注册或快速进入请求")
	}
}

func (s *GatewayServer) handleCommand(sender *streamSender, username string, logout func() error, request *pb.ClientEnvelope) (shouldClose bool, enqueued bool) {
	requestID := uint64(0)
	if request != nil {
		requestID = request.GetRequestId()
	}
	if request == nil || requestID == 0 {
		return false, sender.EnqueueControl(errorEnvelope(requestID, pb.ErrorCode_ERROR_CODE_INVALID_REQUEST, "request_id 必须为非零值", false))
	}

	var (
		next    *protocol.WorldState
		err     error
		message string
	)
	switch payload := request.GetPayload().(type) {
	case *pb.ClientEnvelope_Move:
		if payload.Move == nil {
			err = errors.New("移动请求不能为空")
			break
		}
		direction, directionErr := parseDirection(payload.Move.GetDirection())
		if directionErr != nil {
			err = directionErr
			break
		}
		next, err = s.gameCluster.Move(username, direction)
		message = "移动指令已接受"
	case *pb.ClientEnvelope_Attack:
		if payload.Attack == nil {
			err = errors.New("攻击请求不能为空")
			break
		}
		next, err = s.gameCluster.Attack(username)
		message = "攻击指令已接受"
	case *pb.ClientEnvelope_AttackBoss:
		if payload.AttackBoss == nil {
			err = errors.New("首领攻击请求不能为空")
			break
		}
		next, err = s.gameCluster.AttackBoss(username)
		message = "首领攻击指令已接受"
	case *pb.ClientEnvelope_Heal:
		if payload.Heal == nil {
			err = errors.New("治疗请求不能为空")
			break
		}
		next, err = s.gameCluster.Heal(username)
		message = "治疗指令已接受"
	case *pb.ClientEnvelope_BuyItem:
		if payload.BuyItem == nil || strings.TrimSpace(payload.BuyItem.GetItem()) == "" {
			err = errors.New("商品不能为空")
			break
		}
		next, err = s.gameCluster.BuyItem(username, payload.BuyItem.GetItem())
		message = "购买指令已接受"
	case *pb.ClientEnvelope_SwitchMap:
		if payload.SwitchMap == nil || strings.TrimSpace(payload.SwitchMap.GetMapId()) == "" {
			err = errors.New("目标地图不能为空")
			break
		}
		next, err = s.gameCluster.SwitchMap(username, payload.SwitchMap.GetMapId())
		message = "切换地图指令已接受"
	case *pb.ClientEnvelope_Logout:
		if payload.Logout == nil {
			err = errors.New("退出请求不能为空")
			break
		}
		if err = logout(); err == nil {
			message = "已退出游戏"
			shouldClose = true
		}
	case *pb.ClientEnvelope_Login, *pb.ClientEnvelope_Register, *pb.ClientEnvelope_QuickEnter:
		err = errors.New("认证完成后不允许再次发送认证请求")
	default:
		err = errors.New("请求缺少可识别的命令 payload")
	}

	if err != nil {
		// Backends conventionally return either a state or an error. Free a state even
		// when a faulty implementation returns both so pooled memory is never leaked.
		if next != nil {
			protocol.FreeWorldState(next)
		}
		return false, sender.EnqueueControl(gatewayError(request.GetRequestId(), err, commandError))
	}
	if !sender.EnqueueControl(&pb.ServerEnvelope{
		RequestId: request.GetRequestId(),
		Payload: &pb.ServerEnvelope_CommandResult{CommandResult: &pb.CommandResult{
			Message: message,
		}},
	}) {
		if next != nil {
			protocol.FreeWorldState(next)
		}
		return false, false
	}
	if next != nil {
		sender.SubmitState(next)
	}
	return shouldClose, true
}

func (s *GatewayServer) pumpStates(sender *streamSender, username string, done chan<- struct{}) {
	defer close(done)
	ticker := time.NewTicker(s.gameStreamStateInterval())
	defer ticker.Stop()

	lastError := ""
	for {
		select {
		case <-sender.Done():
			return
		case <-sender.Finished():
			return
		case <-ticker.C:
			state, err := s.gameCluster.SnapshotFor(username)
			if err == nil {
				lastError = ""
				sender.SubmitState(state)
				continue
			}
			if err.Error() == lastError {
				continue
			}
			lastError = err.Error()
			if !sender.EnqueueControl(gatewayError(0, err, stateError)) {
				return
			}
		}
	}
}

func parseDirection(direction pb.Direction) (string, error) {
	switch direction {
	case pb.Direction_DIRECTION_UP:
		return protocol.DirUp, nil
	case pb.Direction_DIRECTION_DOWN:
		return protocol.DirDown, nil
	case pb.Direction_DIRECTION_LEFT:
		return protocol.DirLeft, nil
	case pb.Direction_DIRECTION_RIGHT:
		return protocol.DirRight, nil
	default:
		return "", fmt.Errorf("无效移动方向：%s", direction)
	}
}
