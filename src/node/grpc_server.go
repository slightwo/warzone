package node

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"battleworld/pb"
	nodewire "battleworld/transport/node"
	"battleworld/world"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// nodeV2GRPCServer serves the typed NodeServiceV2 endpoint.
type nodeV2GRPCServer struct {
	pb.UnimplementedNodeServiceV2Server
	svc *NodeService
}

// NewNodeV2GRPCServer returns the NodeServiceV2 endpoint backed by the local
// node service. Every data-plane mutation carries MapAuthority.
func NewNodeV2GRPCServer(svc *NodeService) pb.NodeServiceV2Server {
	return &nodeV2GRPCServer{svc: svc}
}

func (s *nodeV2GRPCServer) Ping(context.Context, *pb.NodePingRequest) (*pb.NodePingResponse, error) {
	return &pb.NodePingResponse{ObservedAt: timestamppb.Now()}, nil
}

func (s *nodeV2GRPCServer) AddPlayer(ctx context.Context, req *pb.NodeAddPlayerRequest) (*pb.NodeAddPlayerResponse, error) {
	authority, err := s.authority(req.GetAuthority())
	if err != nil {
		return nil, err
	}
	if req.GetPlayer() == nil {
		return nil, invalidNodeRequest("player 不能为空")
	}
	if strings.TrimSpace(req.GetPlayer().GetUsername()) == "" {
		return nil, invalidNodeRequest("player.username 不能为空")
	}
	profile := nodewire.FromPlayerState(req.GetPlayer())
	if err := s.svc.AddPlayer(ctx, authority.mapID, authority.epoch, &profile); err != nil {
		return nil, grpcAuthorityError(err)
	}
	return &pb.NodeAddPlayerResponse{}, nil
}

func (s *nodeV2GRPCServer) RemovePlayer(ctx context.Context, req *pb.NodeRemovePlayerRequest) (*pb.NodeRemovePlayerResponse, error) {
	authority, err := s.authority(req.GetAuthority())
	if err != nil {
		return nil, err
	}
	profile, removed, err := s.svc.RemovePlayer(ctx, authority.mapID, req.GetUsername(), authority.epoch)
	if err != nil {
		return nil, grpcAuthorityError(err)
	}
	return &pb.NodeRemovePlayerResponse{Player: nodewire.ToPlayerState(profile), Removed: removed}, nil
}

func (s *nodeV2GRPCServer) MovePlayer(ctx context.Context, req *pb.NodeMovePlayerRequest) (*pb.NodePlayerActionResponse, error) {
	authority, err := s.authority(req.GetAuthority())
	if err != nil {
		return nil, err
	}
	direction, err := nodewire.DirectionToString(req.GetDirection())
	if err != nil {
		return nil, invalidNodeRequest(err.Error())
	}
	message, profile, accepted, err := s.svc.MovePlayer(ctx, authority.mapID, req.GetUsername(), direction, authority.epoch)
	if err != nil {
		return nil, grpcAuthorityError(err)
	}
	return &pb.NodePlayerActionResponse{Message: message, Player: nodewire.ToPlayerState(profile), Accepted: accepted}, nil
}

func (s *nodeV2GRPCServer) Attack(ctx context.Context, req *pb.NodeAttackRequest) (*pb.NodeAttackResponse, error) {
	authority, err := s.authority(req.GetAuthority())
	if err != nil {
		return nil, err
	}
	message, targetMessage, globalMessage, profile, accepted, err := s.svc.Attack(ctx, authority.mapID, req.GetUsername(), authority.epoch)
	if err != nil {
		return nil, grpcAuthorityError(err)
	}
	return &pb.NodeAttackResponse{Message: message, TargetMessage: targetMessage, GlobalMessage: globalMessage, Player: nodewire.ToPlayerState(profile), Accepted: accepted}, nil
}

func (s *nodeV2GRPCServer) Heal(ctx context.Context, req *pb.NodePlayerRequest) (*pb.NodePlayerActionResponse, error) {
	authority, err := s.authority(req.GetAuthority())
	if err != nil {
		return nil, err
	}
	message, profile, accepted, err := s.svc.Heal(ctx, authority.mapID, req.GetUsername(), authority.epoch)
	if err != nil {
		return nil, grpcAuthorityError(err)
	}
	return &pb.NodePlayerActionResponse{Message: message, Player: nodewire.ToPlayerState(profile), Accepted: accepted}, nil
}

func (s *nodeV2GRPCServer) BuyItem(ctx context.Context, req *pb.NodeBuyItemRequest) (*pb.NodePlayerActionResponse, error) {
	authority, err := s.authority(req.GetAuthority())
	if err != nil {
		return nil, err
	}
	message, profile, accepted, err := s.svc.BuyItem(ctx, authority.mapID, req.GetUsername(), req.GetItem(), authority.epoch)
	if err != nil {
		return nil, grpcAuthorityError(err)
	}
	return &pb.NodePlayerActionResponse{Message: message, Player: nodewire.ToPlayerState(profile), Accepted: accepted}, nil
}

func (s *nodeV2GRPCServer) AttackBoss(ctx context.Context, req *pb.NodePlayerRequest) (*pb.NodePlayerActionResponse, error) {
	authority, err := s.authority(req.GetAuthority())
	if err != nil {
		return nil, err
	}
	message, profile, accepted, err := s.svc.AttackBoss(ctx, authority.mapID, req.GetUsername(), authority.epoch)
	if err != nil {
		return nil, grpcAuthorityError(err)
	}
	return &pb.NodePlayerActionResponse{Message: message, Player: nodewire.ToPlayerState(profile), Accepted: accepted}, nil
}

func (s *nodeV2GRPCServer) Profile(ctx context.Context, req *pb.NodeProfileRequest) (*pb.NodeProfileResponse, error) {
	profile, found, err := s.svc.Profile(ctx, req.GetMapId(), req.GetUsername())
	if err != nil {
		return nil, err
	}
	return &pb.NodeProfileResponse{Player: nodewire.ToPlayerState(profile), Found: found}, nil
}

func (s *nodeV2GRPCServer) RewardPlayer(ctx context.Context, req *pb.NodeRewardPlayerRequest) (*pb.NodeProfileResponse, error) {
	authority, err := s.authority(req.GetAuthority())
	if err != nil {
		return nil, err
	}
	profile, found, err := s.svc.RewardPlayer(ctx, authority.mapID, req.GetUsername(), int(req.GetTreasureDelta()), int(req.GetVictoryDelta()), authority.epoch)
	if err != nil {
		return nil, grpcAuthorityError(err)
	}
	return &pb.NodeProfileResponse{Player: nodewire.ToPlayerState(profile), Found: found}, nil
}

func (s *nodeV2GRPCServer) Snapshot(ctx context.Context, req *pb.NodeSnapshotRequest) (*pb.NodeSnapshotResponse, error) {
	view, err := s.svc.Snapshot(ctx, req.GetMapId())
	if err != nil {
		return nil, err
	}
	return &pb.NodeSnapshotResponse{Map: nodewire.ToMapView(view)}, nil
}

func (s *nodeV2GRPCServer) Counts(ctx context.Context, req *pb.NodeCountsRequest) (*pb.NodeCountsResponse, error) {
	players, npcs, treasures, version, err := s.svc.Counts(ctx, req.GetMapId())
	if err != nil {
		return nil, err
	}
	return &pb.NodeCountsResponse{Players: int32(players), Npcs: int32(npcs), Treasures: int32(treasures), Version: version}, nil
}

func (s *nodeV2GRPCServer) Checkpoint(ctx context.Context, req *pb.NodeCheckpointRequest) (*pb.NodeCheckpointResponse, error) {
	checkpoint, err := s.svc.Checkpoint(ctx, req.GetMapId())
	if err != nil {
		return nil, err
	}
	return &pb.NodeCheckpointResponse{Checkpoint: nodewire.ToCheckpoint(checkpoint)}, nil
}

func (s *nodeV2GRPCServer) Promote(ctx context.Context, req *pb.NodePromoteRequest) (*pb.NodePromoteResponse, error) {
	authority, err := s.authority(req.GetAuthority())
	if err != nil {
		return nil, err
	}
	checkpoint, err := nodewire.FromCheckpoint(req.GetCheckpoint())
	if err != nil {
		return nil, invalidNodeRequest(err.Error())
	}
	config, err := nodeMapConfig(authority.mapID)
	if err != nil {
		return nil, invalidNodeRequest(err.Error())
	}
	if err := s.svc.Promote(authority.mapID, config, checkpoint, authority.epoch); err != nil {
		return nil, grpcAuthorityError(err)
	}
	return &pb.NodePromoteResponse{}, nil
}

func (s *nodeV2GRPCServer) View(context.Context, *pb.NodeViewRequest) (*pb.NodeViewResponse, error) {
	return &pb.NodeViewResponse{View: nodewire.ToNodeView(s.svc.View())}, nil
}

type nodeAuthority struct {
	mapID string
	epoch uint64
}

func (s *nodeV2GRPCServer) authority(authority *pb.MapAuthority) (nodeAuthority, error) {
	if authority == nil {
		return nodeAuthority{}, invalidNodeRequest("authority 不能为空")
	}
	if authority.GetMapId() == "" {
		return nodeAuthority{}, invalidNodeRequest("authority.map_id 不能为空")
	}
	if authority.GetOwnerNodeId() != s.svc.ID {
		return nodeAuthority{}, status.Errorf(codes.FailedPrecondition, "authority owner=%q does not match node=%q", authority.GetOwnerNodeId(), s.svc.ID)
	}
	if authority.GetMapEpoch() == 0 {
		return nodeAuthority{}, invalidNodeRequest("authority.map_epoch 必须为非零值")
	}
	return nodeAuthority{mapID: authority.GetMapId(), epoch: authority.GetMapEpoch()}, nil
}

func invalidNodeRequest(message string) error { return status.Error(codes.InvalidArgument, message) }

func nodeMapConfig(mapID string) (world.MapConfig, error) {
	for _, config := range world.AvailableMaps() {
		if config.ID == mapID {
			return config, nil
		}
	}
	return world.MapConfig{}, fmt.Errorf("未知地图 %q", mapID)
}

func grpcAuthorityError(err error) error {
	if errors.Is(err, ErrTopologyAuthorityUnavailable) || errors.Is(err, ErrMapAuthorityDenied) || errors.Is(err, ErrMapPromotionDenied) || errors.Is(err, ErrNodeDraining) {
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	return err
}
