package main

import (
	"sync"

	"battleworld/pb"
	"battleworld/protocol"
	gatewaywire "battleworld/transport/gateway"
)

const controlQueueCapacity = 32

type streamSender struct {
	stream pb.GatewayService_GameStreamServer

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
	latestState    stateVersion
	hasLatestState bool
}

type stateVersion struct {
	mapID          string
	sessionVersion int64
	mapEpoch       uint64
	mapVersion     int64
}

func newStreamSender(stream pb.GatewayService_GameStreamServer) *streamSender {
	return &streamSender{
		stream:            stream,
		controls:          make(chan *pb.ServerEnvelope, controlQueueCapacity),
		stateReady:        make(chan struct{}, 1),
		finished:          make(chan struct{}),
		done:              make(chan struct{}),
		writerDone:        make(chan struct{}),
		acceptingControls: true,
	}
}

func (s *streamSender) Start() {
	s.startOnce.Do(func() {
		go s.writeLoop()
	})
}

// EnqueueControl applies backpressure instead of discarding authentication results,
// command results, or errors. It returns false only once the stream is shutting down.
func (s *streamSender) EnqueueControl(envelope *pb.ServerEnvelope) bool {
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
func (s *streamSender) SubmitState(state *protocol.WorldState) {
	if state == nil {
		return
	}
	protobufState := gatewaywire.ToWorldState(state)
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

	version := stateVersion{
		mapID:          protobufState.GetMap().GetId(),
		sessionVersion: protobufState.GetSessionVersion(),
		mapEpoch:       protobufState.GetMapEpoch(),
		mapVersion:     protobufState.GetMap().GetVersion(),
	}

	s.stateMu.Lock()
	if s.hasLatestState && stateRegresses(version, s.latestState) {
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

// stateRegresses follows the client-side application contract: topology version is
// routing observability rather than an ordering key; a new session or map epoch is
// authoritative, while within the same session/epoch map versions must not decrease.
func stateRegresses(next, previous stateVersion) bool {
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

func (s *streamSender) takeState() *pb.WorldState {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	state := s.pendingState
	s.pendingState = nil
	return state
}

// Finish stops accepting new messages and drains already accepted control messages.
// It intentionally discards any pending mergeable state snapshot.
func (s *streamSender) Finish() {
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
func (s *streamSender) Abort() {
	s.abortOnce.Do(func() {
		close(s.done)
	})
}

func (s *streamSender) Done() <-chan struct{} {
	return s.done
}

func (s *streamSender) Finished() <-chan struct{} {
	return s.finished
}

func (s *streamSender) Wait() {
	<-s.writerDone
}

func (s *streamSender) writeLoop() {
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

func (s *streamSender) drainControls() {
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

func (s *streamSender) send(envelope *pb.ServerEnvelope) bool {
	if err := s.stream.Send(envelope); err != nil {
		s.Abort()
		return false
	}
	return true
}
