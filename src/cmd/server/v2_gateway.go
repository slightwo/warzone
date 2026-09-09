package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"battleworld/pb"
	"battleworld/protocol"
)

const v2ControlQueueCapacity = 32

type v2Sender struct {
	stream pb.GatewayService_GameStreamV2Server

	controls   chan *pb.ServerEnvelope
	stateReady chan struct{}
	finished   chan struct{}
	done       chan struct{}
	writerDone chan struct{}

	finishOnce sync.Once
	abortOnce  sync.Once
	startOnce  sync.Once

	controlMu         sync.Mutex
	acceptingControls bool

	stateMu        sync.Mutex
	pendingState   *pb.WorldState
	latestState    v2StateVersion
	hasLatestState bool
}

type v2StateVersion struct {
	mapID          string
	sessionVersion int64
	mapEpoch       uint64
	mapVersion     int64
}

func newV2Sender(stream pb.GatewayService_GameStreamV2Server) *v2Sender {
	return &v2Sender{
		stream:            stream,
		controls:          make(chan *pb.ServerEnvelope, v2ControlQueueCapacity),
		stateReady:        make(chan struct{}, 1),
		finished:          make(chan struct{}),
		done:              make(chan struct{}),
		writerDone:        make(chan struct{}),
		acceptingControls: true,
	}
}

func (s *v2Sender) Start() {
	s.startOnce.Do(func() {
		go s.writeLoop()
	})
}

// EnqueueControl applies backpressure instead of discarding authentication results,
// command results, or errors. It returns false only once the stream is shutting down.
func (s *v2Sender) EnqueueControl(envelope *pb.ServerEnvelope) bool {
	if envelope == nil {
		return true
	}

	// Serialize control-message admission with Finish. A producer already waiting for
	// queue capacity is allowed to complete, then Finish drains every accepted frame.
	s.controlMu.Lock()
	defer s.controlMu.Unlock()
	if !s.acceptingControls {
		return false
	}
	select {
	case s.controls <- envelope:
		return true
	case <-s.done:
		return false
	}
}

// SubmitState copies a pooled domain WorldState into the protobuf boundary before
// returning the domain object to its pool. Only the newest non-regressing state is kept.
func (s *v2Sender) SubmitState(state *protocol.WorldState) {
	if state == nil {
		return
	}
	protobufState := protocol.ToProtoWorldState(state)
	protocol.FreeWorldState(state)
	if protobufState == nil {
		return
	}

	select {
	case <-s.done:
		return
	case <-s.finished:
		return
	default:
	}

	version := v2StateVersion{
		mapID:          protobufState.GetMap().GetId(),
		sessionVersion: protobufState.GetSessionVersion(),
		mapEpoch:       protobufState.GetMapEpoch(),
		mapVersion:     protobufState.GetMap().GetVersion(),
	}

	s.stateMu.Lock()
	if s.hasLatestState && v2StateRegresses(version, s.latestState) {
		s.stateMu.Unlock()
		return
	}
	s.pendingState = protobufState
	s.latestState = version
	s.hasLatestState = true
	s.stateMu.Unlock()

	select {
	case s.stateReady <- struct{}{}:
	default:
	}
}

// v2StateRegresses follows the client-side application contract: topology version is
// routing observability rather than an ordering key; a new session or map epoch is
// authoritative, while within the same session/epoch map versions must not decrease.
func v2StateRegresses(next, previous v2StateVersion) bool {
	if next.sessionVersion < previous.sessionVersion {
		return true
	}
	if next.sessionVersion > previous.sessionVersion {
		return false
	}
	if next.mapEpoch < previous.mapEpoch {
		return true
	}
	if next.mapEpoch > previous.mapEpoch || next.mapID != previous.mapID {
		return false
	}
	return next.mapVersion < previous.mapVersion
}

func (s *v2Sender) takeState() *pb.WorldState {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	state := s.pendingState
	s.pendingState = nil
	return state
}

// Finish stops accepting new messages and drains already accepted control messages.
// It intentionally discards any pending mergeable state snapshot.
func (s *v2Sender) Finish() {
	s.finishOnce.Do(func() {
		s.controlMu.Lock()
		s.acceptingControls = false
		s.controlMu.Unlock()

		s.stateMu.Lock()
		s.pendingState = nil
		s.stateMu.Unlock()
		select {
		case <-s.stateReady:
		default:
		}
		close(s.finished)
	})
}

// Abort stops the writer immediately, for example after the transport reports Send
// failure. Producers observe Done and stop without blocking.
func (s *v2Sender) Abort() {
	s.abortOnce.Do(func() {
		close(s.done)
	})
}

func (s *v2Sender) Done() <-chan struct{} {
	return s.done
}

func (s *v2Sender) Finished() <-chan struct{} {
	return s.finished
}

func (s *v2Sender) Wait() {
	<-s.writerDone
}

func (s *v2Sender) writeLoop() {
	defer close(s.writerDone)
	for {
		// Give reliable control messages priority over mergeable state frames.
		select {
		case envelope := <-s.controls:
			if !s.send(envelope) {
				return
			}
			continue
		default:
		}

		select {
		case <-s.done:
			return
		case <-s.finished:
			s.drainControls()
			return
		default:
		}

		select {
		case <-s.done:
			return
		case <-s.finished:
			s.drainControls()
			return
		case envelope := <-s.controls:
			if !s.send(envelope) {
				return
			}
		case <-s.stateReady:
			if state := s.takeState(); state != nil {
				if !s.send(&pb.ServerEnvelope{
					RequestId: 0,
					Payload:   &pb.ServerEnvelope_State{State: state},
				}) {
					return
				}
			}
		}
	}
}

func (s *v2Sender) drainControls() {
	for {
		select {
		case <-s.done:
			return
		case envelope := <-s.controls:
			if !s.send(envelope) {
				return
			}
		default:
			return
		}
	}
}

func (s *v2Sender) send(envelope *pb.ServerEnvelope) bool {
	if err := s.stream.Send(envelope); err != nil {
		s.Abort()
		return false
	}
	return true
}

func (s *GatewayServer) GameStreamV2(stream pb.GatewayService_GameStreamV2Server) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	if first.GetRequestId() == 0 {
		return sendInitialV2Error(stream, 0, pb.ErrorCode_ERROR_CODE_INVALID_REQUEST, "request_id 必须为非零值", false)
	}

	username, initialState, err := s.authenticateV2(first)
	if err != nil {
		response := gatewayV2Error(first.GetRequestId(), err, v2AuthError)
		_ = stream.Send(response)
		return err
	}
	if initialState == nil {
		return sendInitialV2Error(stream, first.GetRequestId(), pb.ErrorCode_ERROR_CODE_INTERNAL, "认证成功但未返回世界状态", true)
	}

	initialStatePB := protocol.ToProtoWorldState(initialState)
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

	sender := newV2Sender(stream)
	sender.Start()
	statePumpDone := make(chan struct{})
	go s.pumpV2States(sender, username, statePumpDone)

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

	inbound := make(chan v2Inbound)
	go receiveV2Inbound(stream, sender, inbound)

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
			if shouldClose, ok := s.handleV2Command(sender, username, logout, incoming.request); !ok {
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

type v2Inbound struct {
	request *pb.ClientEnvelope
	err     error
}

func receiveV2Inbound(stream pb.GatewayService_GameStreamV2Server, sender *v2Sender, inbound chan<- v2Inbound) {
	defer close(inbound)
	for {
		request, err := stream.Recv()
		select {
		case inbound <- v2Inbound{request: request, err: err}:
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

func (s *GatewayServer) authenticateV2(request *pb.ClientEnvelope) (string, *protocol.WorldState, error) {
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

func (s *GatewayServer) handleV2Command(sender *v2Sender, username string, logout func() error, request *pb.ClientEnvelope) (shouldClose bool, enqueued bool) {
	requestID := uint64(0)
	if request != nil {
		requestID = request.GetRequestId()
	}
	if request == nil || requestID == 0 {
		return false, sender.EnqueueControl(v2ErrorEnvelope(requestID, pb.ErrorCode_ERROR_CODE_INVALID_REQUEST, "request_id 必须为非零值", false))
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
		direction, directionErr := v2Direction(payload.Move.GetDirection())
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
		return false, sender.EnqueueControl(gatewayV2Error(request.GetRequestId(), err, v2CommandError))
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

func (s *GatewayServer) pumpV2States(sender *v2Sender, username string, done chan<- struct{}) {
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
			if !sender.EnqueueControl(gatewayV2Error(0, err, v2StateError)) {
				return
			}
		}
	}
}

type v2ErrorKind uint8

const (
	v2AuthError v2ErrorKind = iota
	v2CommandError
	v2StateError
)

func sendInitialV2Error(stream pb.GatewayService_GameStreamV2Server, requestID uint64, code pb.ErrorCode, message string, retryable bool) error {
	err := stream.Send(v2ErrorEnvelope(requestID, code, message, retryable))
	if err != nil {
		return err
	}
	return errors.New(message)
}

func gatewayV2Error(requestID uint64, err error, kind v2ErrorKind) *pb.ServerEnvelope {
	code, retryable := mapV2Error(err, kind)
	return v2ErrorEnvelope(requestID, code, err.Error(), retryable)
}

func v2ErrorEnvelope(requestID uint64, code pb.ErrorCode, message string, retryable bool) *pb.ServerEnvelope {
	return &pb.ServerEnvelope{
		RequestId: requestID,
		Payload: &pb.ServerEnvelope_Error{Error: &pb.ErrorResponse{
			Code:      code,
			Message:   message,
			Retryable: retryable,
		}},
	}
}

func mapV2Error(err error, kind v2ErrorKind) (pb.ErrorCode, bool) {
	if err == nil {
		return pb.ErrorCode_ERROR_CODE_INTERNAL, true
	}
	switch status.Code(err) {
	case codes.FailedPrecondition:
		return pb.ErrorCode_ERROR_CODE_STALE_ROUTE, true
	case codes.Unavailable, codes.DeadlineExceeded:
		return pb.ErrorCode_ERROR_CODE_ROUTE_NOT_READY, true
	case codes.InvalidArgument:
		return pb.ErrorCode_ERROR_CODE_INVALID_REQUEST, false
	}

	message := err.Error()
	switch {
	case containsAny(message, "code = FailedPrecondition", "code = FAILED_PRECONDITION"):
		return pb.ErrorCode_ERROR_CODE_STALE_ROUTE, true
	case containsAny(message, "code = Unavailable", "code = DeadlineExceeded"):
		return pb.ErrorCode_ERROR_CODE_ROUTE_NOT_READY, true
	case containsAny(message, "code = InvalidArgument"):
		return pb.ErrorCode_ERROR_CODE_INVALID_REQUEST, false
	}
	if kind == v2AuthError {
		switch {
		case containsAny(message, "不能为空", "不一致", "请求不能为空", "首条消息"):
			return pb.ErrorCode_ERROR_CODE_INVALID_REQUEST, false
		case containsAny(message, "不存在", "密码", "已经在线"):
			return pb.ErrorCode_ERROR_CODE_AUTH_FAILED, strings.Contains(message, "已经在线")
		}
	}
	if containsAny(message, "路由", "同步中", "承载节点", "topology authority unavailable") {
		return pb.ErrorCode_ERROR_CODE_ROUTE_NOT_READY, true
	}
	if containsAny(message, "authority denied", "stale topology", "当前 epoch", "epoch") {
		return pb.ErrorCode_ERROR_CODE_STALE_ROUTE, true
	}
	if kind == v2StateError {
		return pb.ErrorCode_ERROR_CODE_ROUTE_NOT_READY, true
	}
	if kind == v2CommandError {
		if containsAny(message, "不能为空", "不允许", "缺少可识别", "无效") {
			return pb.ErrorCode_ERROR_CODE_INVALID_REQUEST, false
		}
		return pb.ErrorCode_ERROR_CODE_COMMAND_REJECTED, false
	}
	return pb.ErrorCode_ERROR_CODE_INTERNAL, true
}

func containsAny(message string, fragments ...string) bool {
	for _, fragment := range fragments {
		if strings.Contains(message, fragment) {
			return true
		}
	}
	return false
}

func v2Direction(direction pb.Direction) (string, error) {
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

func (s *GatewayServer) GetGatewayStatus(_ context.Context, _ *pb.GatewayStatusRequest) (*pb.GatewayStatusResponse, error) {
	gatewayStatus := s.gameCluster.GatewayStatus()
	response := &pb.GatewayStatusResponse{
		RoutingReady:    gatewayStatus.RoutingReady,
		TopologyVersion: gatewayStatus.TopologyVersion,
		Summary:         gatewayStatus.Summary,
		Nodes:           make([]*pb.NodeView, 0, len(gatewayStatus.Nodes)),
	}
	for _, node := range gatewayStatus.Nodes {
		response.Nodes = append(response.Nodes, protocol.ToProtoNodeView(node))
	}
	return response, nil
}
