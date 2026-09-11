package node

import (
	"context"
	"errors"
	"expvar"
	"fmt"
	"strings"
	"time"

	"battleworld/pb"
	"battleworld/protocol"
	nodewire "battleworld/transport/node"
	"battleworld/world"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// legacyNodeServiceCalls counts requests handled by the V1 Node service. It
// is exposed from a node's lifecycle endpoint at /debug/vars and must remain
// unchanged for a full release window before NodeService can be retired.
var legacyNodeServiceCalls = expvar.NewInt("battleworld_node_v1_calls_total")

// NodeGRPCServer serves the wire-compatible NodeService V1 while V2 callers
// use the dedicated typed endpoint returned by NewNodeV2GRPCServer.
type NodeGRPCServer struct {
	pb.UnimplementedNodeServiceServer
	svc *NodeService
}

func NewNodeGRPCServer(svc *NodeService) *NodeGRPCServer {
	return &NodeGRPCServer{svc: svc}
}

func (s *NodeGRPCServer) recordLegacyCall() {
	legacyNodeServiceCalls.Add(1)
}

func (s *NodeGRPCServer) Ping(context.Context, *pb.PingReq) (*pb.PingResp, error) {
	s.recordLegacyCall()
	return &pb.PingResp{Ts: time.Now().UnixMilli()}, nil
}

func (s *NodeGRPCServer) AddPlayer(ctx context.Context, req *pb.AddPlayerReq) (*pb.AddPlayerResp, error) {
	s.recordLegacyCall()
	profile := legacyUserProfileFromPB(req.GetProfile())
	if err := s.svc.AddPlayer(ctx, req.GetMapId(), req.GetMapEpoch(), &profile); err != nil {
		return nil, grpcAuthorityError(err)
	}
	return &pb.AddPlayerResp{Ok: true}, nil
}

func (s *NodeGRPCServer) RemovePlayer(ctx context.Context, req *pb.RemovePlayerReq) (*pb.RemovePlayerResp, error) {
	s.recordLegacyCall()
	profile, ok, err := s.svc.RemovePlayer(ctx, req.GetMapId(), req.GetUsername(), req.GetMapEpoch())
	if err != nil {
		return nil, grpcAuthorityError(err)
	}
	return &pb.RemovePlayerResp{Profile: legacyUserProfileToPB(profile), Ok: ok}, nil
}

func (s *NodeGRPCServer) MovePlayer(ctx context.Context, req *pb.MovePlayerReq) (*pb.MovePlayerResp, error) {
	s.recordLegacyCall()
	text, profile, ok, err := s.svc.MovePlayer(ctx, req.GetMapId(), req.GetUsername(), req.GetDir(), req.GetMapEpoch())
	if err != nil {
		return nil, grpcAuthorityError(err)
	}
	return &pb.MovePlayerResp{Text: text, Profile: legacyUserProfileToPB(profile), Ok: ok}, nil
}

func (s *NodeGRPCServer) Attack(ctx context.Context, req *pb.AttackReq) (*pb.AttackResp, error) {
	s.recordLegacyCall()
	log1, log2, log3, profile, ok, err := s.svc.Attack(ctx, req.GetMapId(), req.GetUsername(), req.GetMapEpoch())
	if err != nil {
		return nil, grpcAuthorityError(err)
	}
	return &pb.AttackResp{Log: log1, BLog: log2, GmLog: log3, Profile: legacyUserProfileToPB(profile), Ok: ok}, nil
}

func (s *NodeGRPCServer) Heal(ctx context.Context, req *pb.HealReq) (*pb.HealResp, error) {
	s.recordLegacyCall()
	text, profile, ok, err := s.svc.Heal(ctx, req.GetMapId(), req.GetUsername(), req.GetMapEpoch())
	if err != nil {
		return nil, grpcAuthorityError(err)
	}
	return &pb.HealResp{Text: text, Profile: legacyUserProfileToPB(profile), Ok: ok}, nil
}

func (s *NodeGRPCServer) BuyItem(ctx context.Context, req *pb.BuyItemReq) (*pb.BuyItemResp, error) {
	s.recordLegacyCall()
	text, profile, ok, err := s.svc.BuyItem(ctx, req.GetMapId(), req.GetUsername(), req.GetItem(), req.GetMapEpoch())
	if err != nil {
		return nil, grpcAuthorityError(err)
	}
	return &pb.BuyItemResp{Text: text, Profile: legacyUserProfileToPB(profile), Ok: ok}, nil
}

func (s *NodeGRPCServer) AttackBoss(ctx context.Context, req *pb.AttackBossReq) (*pb.AttackBossResp, error) {
	s.recordLegacyCall()
	text, profile, ok, err := s.svc.AttackBoss(ctx, req.GetMapId(), req.GetUsername(), req.GetMapEpoch())
	if err != nil {
		return nil, grpcAuthorityError(err)
	}
	return &pb.AttackBossResp{Text: text, Profile: legacyUserProfileToPB(profile), Ok: ok}, nil
}

func (s *NodeGRPCServer) Profile(ctx context.Context, req *pb.ProfileReq) (*pb.ProfileResp, error) {
	s.recordLegacyCall()
	profile, ok, err := s.svc.Profile(ctx, req.GetMapId(), req.GetUsername())
	if err != nil {
		return nil, err
	}
	return &pb.ProfileResp{Profile: legacyUserProfileToPB(profile), Ok: ok}, nil
}

func (s *NodeGRPCServer) RewardPlayer(ctx context.Context, req *pb.RewardPlayerReq) (*pb.RewardPlayerResp, error) {
	s.recordLegacyCall()
	profile, ok, err := s.svc.RewardPlayer(ctx, req.GetMapId(), req.GetUsername(), int(req.GetTreasureDelta()), int(req.GetVictoryDelta()), req.GetMapEpoch())
	if err != nil {
		return nil, grpcAuthorityError(err)
	}
	return &pb.RewardPlayerResp{Profile: legacyUserProfileToPB(profile), Ok: ok}, nil
}

func (s *NodeGRPCServer) Snapshot(ctx context.Context, req *pb.SnapshotReq) (*pb.SnapshotResp, error) {
	s.recordLegacyCall()
	view, err := s.svc.Snapshot(ctx, req.GetMapId())
	if err != nil {
		return nil, err
	}
	return &pb.SnapshotResp{Map: nodewire.ToMapView(view)}, nil
}

func (s *NodeGRPCServer) Counts(ctx context.Context, req *pb.CountsReq) (*pb.CountsResp, error) {
	s.recordLegacyCall()
	players, npcs, treasures, version, err := s.svc.Counts(ctx, req.GetMapId())
	if err != nil {
		return nil, err
	}
	return &pb.CountsResp{Players: int32(players), Npcs: int32(npcs), Treasures: int32(treasures), Version: version}, nil
}

func (s *NodeGRPCServer) Checkpoint(ctx context.Context, req *pb.CheckpointReq) (*pb.CheckpointResp, error) {
	s.recordLegacyCall()
	checkpoint, err := s.svc.Checkpoint(ctx, req.GetMapId())
	if err != nil {
		return nil, err
	}
	return &pb.CheckpointResp{Checkpoint: legacyCheckpointToPB(checkpoint)}, nil
}

func (s *NodeGRPCServer) Promote(ctx context.Context, req *pb.PromoteReq) (*pb.PromoteResp, error) {
	s.recordLegacyCall()
	config, err := nodeMapConfig(req.GetMapId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := s.svc.Promote(req.GetMapId(), config, legacyCheckpointFromPB(req.GetCheckpoint()), req.GetMapEpoch()); err != nil {
		return nil, grpcAuthorityError(err)
	}
	return &pb.PromoteResp{Ok: true}, nil
}

func (s *NodeGRPCServer) View(context.Context, *pb.ViewReq) (*pb.ViewResp, error) {
	s.recordLegacyCall()
	return &pb.ViewResp{View: nodewire.ToNodeView(s.svc.View())}, nil
}

// nodeV2GRPCServer serves NodeServiceV2 separately because Ping has the same
// method name but a different request/response type in the V1 service.
type nodeV2GRPCServer struct {
	pb.UnimplementedNodeServiceV2Server
	svc *NodeService
}

// NewNodeV2GRPCServer returns the V2 endpoint backed by the same local node
// service as its legacy counterpart. Both can be registered on one grpc.Server.
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

// V1 UserProfile remains an account-bearing compatibility model. It must not be
// used by NodeServiceV2, which exclusively uses transport/node.PlayerState.
func legacyUserProfileToPB(profile protocol.UserProfile) *pb.UserProfile {
	return &pb.UserProfile{Username: profile.Username, PasswordHash: profile.PasswordHash, LastMap: profile.LastMap, LastNode: profile.LastNode, X: int32(profile.X), Y: int32(profile.Y), Hp: int32(profile.HP), MaxHp: int32(profile.MaxHP), Attack: int32(profile.Attack), Potions: int32(profile.Potions), Treasures: int32(profile.Treasures), Kills: int32(profile.Kills), Deaths: int32(profile.Deaths), Victories: int32(profile.Victories), Alive: profile.Alive}
}

func legacyUserProfileFromPB(profile *pb.UserProfile) protocol.UserProfile {
	if profile == nil {
		return protocol.UserProfile{}
	}
	return protocol.UserProfile{Username: profile.GetUsername(), PasswordHash: profile.GetPasswordHash(), LastMap: profile.GetLastMap(), LastNode: profile.GetLastNode(), X: int(profile.GetX()), Y: int(profile.GetY()), HP: int(profile.GetHp()), MaxHP: int(profile.GetMaxHp()), Attack: int(profile.GetAttack()), Potions: int(profile.GetPotions()), Treasures: int(profile.GetTreasures()), Kills: int(profile.GetKills()), Deaths: int(profile.GetDeaths()), Victories: int(profile.GetVictories()), Alive: profile.GetAlive()}
}

func legacyCheckpointToPB(checkpoint protocol.MapCheckpoint) *pb.MapCheckpoint {
	if checkpoint.MapID == "" {
		return nil
	}
	result := &pb.MapCheckpoint{MapId: checkpoint.MapID, NodeId: checkpoint.NodeID, MapEpoch: checkpoint.MapEpoch, Version: checkpoint.Version, Terrain: append([]string(nil), checkpoint.Terrain...), Checkpoint: checkpoint.Checkpoint.Format(time.RFC3339)}
	for _, player := range checkpoint.Players {
		result.Players = append(result.Players, nodewire.ToPlayerView(player))
	}
	for _, npc := range checkpoint.NPCs {
		result.Npcs = append(result.Npcs, nodewire.ToNPCView(npc))
	}
	for _, treasure := range checkpoint.Treasures {
		result.Treasures = append(result.Treasures, nodewire.ToTreasureView(treasure))
	}
	return result
}

func legacyCheckpointFromPB(checkpoint *pb.MapCheckpoint) protocol.MapCheckpoint {
	if checkpoint == nil {
		return protocol.MapCheckpoint{}
	}
	capturedAt, _ := time.Parse(time.RFC3339, checkpoint.GetCheckpoint())
	result := protocol.MapCheckpoint{MapID: checkpoint.GetMapId(), NodeID: checkpoint.GetNodeId(), MapEpoch: checkpoint.GetMapEpoch(), Version: checkpoint.GetVersion(), Terrain: append([]string(nil), checkpoint.GetTerrain()...), Checkpoint: capturedAt}
	for _, player := range checkpoint.GetPlayers() {
		result.Players = append(result.Players, nodewire.FromPlayerView(player))
	}
	for _, npc := range checkpoint.GetNpcs() {
		result.NPCs = append(result.NPCs, nodewire.FromNPCView(npc))
	}
	for _, treasure := range checkpoint.GetTreasures() {
		result.Treasures = append(result.Treasures, nodewire.FromTreasureView(treasure))
	}
	return result
}
