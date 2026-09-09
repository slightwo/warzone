package node

import (
	"context"
	"errors"
	"time"

	"battleworld/pb"
	"battleworld/protocol"
	"battleworld/world"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// NodeGRPCServer 是 gRPC 服务的具体实现，它包装了本地的 NodeService，对接网络请求。
type NodeGRPCServer struct {
	pb.UnimplementedNodeServiceServer
	svc *NodeService
}

func NewNodeGRPCServer(svc *NodeService) *NodeGRPCServer {
	return &NodeGRPCServer{svc: svc}
}

func (s *NodeGRPCServer) Ping(ctx context.Context, req *pb.PingReq) (*pb.PingResp, error) {
	return &pb.PingResp{Ts: time.Now().UnixMilli()}, nil
}

func (s *NodeGRPCServer) AddPlayer(ctx context.Context, req *pb.AddPlayerReq) (*pb.AddPlayerResp, error) {
	profile := protocol.FromProtoUserProfile(req.Profile)
	err := s.svc.AddPlayer(ctx, req.MapId, req.MapEpoch, &profile)
	if err != nil {
		return nil, grpcAuthorityError(err)
	}
	return &pb.AddPlayerResp{Ok: true}, nil
}

func (s *NodeGRPCServer) RemovePlayer(ctx context.Context, req *pb.RemovePlayerReq) (*pb.RemovePlayerResp, error) {
	profile, ok, err := s.svc.RemovePlayer(ctx, req.MapId, req.Username, req.MapEpoch)
	if err != nil {
		return nil, grpcAuthorityError(err)
	}
	return &pb.RemovePlayerResp{
		Profile: protocol.ToProtoUserProfile(profile),
		Ok:      ok,
	}, nil
}

func (s *NodeGRPCServer) MovePlayer(ctx context.Context, req *pb.MovePlayerReq) (*pb.MovePlayerResp, error) {
	text, profile, ok, err := s.svc.MovePlayer(ctx, req.MapId, req.Username, req.Dir, req.MapEpoch)
	if err != nil {
		return nil, grpcAuthorityError(err)
	}
	return &pb.MovePlayerResp{
		Text:    text,
		Profile: protocol.ToProtoUserProfile(profile),
		Ok:      ok,
	}, nil
}

func (s *NodeGRPCServer) Attack(ctx context.Context, req *pb.AttackReq) (*pb.AttackResp, error) {
	log1, log2, log3, profile, ok, err := s.svc.Attack(ctx, req.MapId, req.Username, req.MapEpoch)
	if err != nil {
		return nil, grpcAuthorityError(err)
	}
	return &pb.AttackResp{
		Log:     log1,
		BLog:    log2,
		GmLog:   log3,
		Profile: protocol.ToProtoUserProfile(profile),
		Ok:      ok,
	}, nil
}

func (s *NodeGRPCServer) Heal(ctx context.Context, req *pb.HealReq) (*pb.HealResp, error) {
	text, profile, ok, err := s.svc.Heal(ctx, req.MapId, req.Username, req.MapEpoch)
	if err != nil {
		return nil, grpcAuthorityError(err)
	}
	return &pb.HealResp{
		Text:    text,
		Profile: protocol.ToProtoUserProfile(profile),
		Ok:      ok,
	}, nil
}

func (s *NodeGRPCServer) BuyItem(ctx context.Context, req *pb.BuyItemReq) (*pb.BuyItemResp, error) {
	text, profile, ok, err := s.svc.BuyItem(ctx, req.MapId, req.Username, req.Item, req.MapEpoch)
	if err != nil {
		return nil, grpcAuthorityError(err)
	}
	return &pb.BuyItemResp{
		Text:    text,
		Profile: protocol.ToProtoUserProfile(profile),
		Ok:      ok,
	}, nil
}

func (s *NodeGRPCServer) AttackBoss(ctx context.Context, req *pb.AttackBossReq) (*pb.AttackBossResp, error) {
	text, profile, ok, err := s.svc.AttackBoss(ctx, req.MapId, req.Username, req.MapEpoch)
	if err != nil {
		return nil, grpcAuthorityError(err)
	}
	return &pb.AttackBossResp{
		Text:    text,
		Profile: protocol.ToProtoUserProfile(profile),
		Ok:      ok,
	}, nil
}

func (s *NodeGRPCServer) Profile(ctx context.Context, req *pb.ProfileReq) (*pb.ProfileResp, error) {
	profile, ok, err := s.svc.Profile(ctx, req.MapId, req.Username)
	if err != nil {
		return nil, err
	}
	return &pb.ProfileResp{
		Profile: protocol.ToProtoUserProfile(profile),
		Ok:      ok,
	}, nil
}

func (s *NodeGRPCServer) RewardPlayer(ctx context.Context, req *pb.RewardPlayerReq) (*pb.RewardPlayerResp, error) {
	profile, ok, err := s.svc.RewardPlayer(ctx, req.MapId, req.Username, int(req.TreasureDelta), int(req.VictoryDelta), req.MapEpoch)
	if err != nil {
		return nil, grpcAuthorityError(err)
	}
	return &pb.RewardPlayerResp{
		Profile: protocol.ToProtoUserProfile(profile),
		Ok:      ok,
	}, nil
}

func (s *NodeGRPCServer) Snapshot(ctx context.Context, req *pb.SnapshotReq) (*pb.SnapshotResp, error) {
	mapView, err := s.svc.Snapshot(ctx, req.MapId)
	if err != nil {
		return nil, err
	}
	return &pb.SnapshotResp{
		Map: protocol.ToProtoMapView(mapView),
	}, nil
}

func (s *NodeGRPCServer) Counts(ctx context.Context, req *pb.CountsReq) (*pb.CountsResp, error) {
	players, npcs, treasures, version, err := s.svc.Counts(ctx, req.MapId)
	if err != nil {
		return nil, err
	}
	return &pb.CountsResp{
		Players:   int32(players),
		Npcs:      int32(npcs),
		Treasures: int32(treasures),
		Version:   version,
	}, nil
}

func (s *NodeGRPCServer) Checkpoint(ctx context.Context, req *pb.CheckpointReq) (*pb.CheckpointResp, error) {
	cp, err := s.svc.Checkpoint(ctx, req.MapId)
	if err != nil {
		return nil, err
	}
	// protocol.ToProtoMapCheckpoint 需要在 adapter.go 中补齐
	return &pb.CheckpointResp{
		Checkpoint: protocol.ToProtoMapCheckpoint(cp),
	}, nil
}

func (s *NodeGRPCServer) Promote(ctx context.Context, req *pb.PromoteReq) (*pb.PromoteResp, error) {
	available := world.AvailableMaps()
	var cfg world.MapConfig
	for _, m := range available {
		if m.ID == req.MapId {
			cfg = m
			break
		}
	}
	err := s.svc.Promote(req.MapId, cfg, protocol.FromProtoMapCheckpoint(req.Checkpoint), req.MapEpoch)
	if err != nil {
		return nil, grpcAuthorityError(err)
	}
	return &pb.PromoteResp{Ok: true}, nil
}

// grpcAuthorityError 将 owner/epoch 不匹配统一转换为明确的 FailedPrecondition，调用者
// 可据此重新加载 Topology，而不是把 fencing 错误误判为业务拒绝。
func grpcAuthorityError(err error) error {
	if errors.Is(err, ErrTopologyAuthorityUnavailable) || errors.Is(err, ErrMapAuthorityDenied) || errors.Is(err, ErrMapPromotionDenied) {
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	return err
}

func (s *NodeGRPCServer) View(ctx context.Context, req *pb.ViewReq) (*pb.ViewResp, error) {
	view := s.svc.View()
	return &pb.ViewResp{
		View: protocol.ToProtoNodeView(view),
	}, nil
}
