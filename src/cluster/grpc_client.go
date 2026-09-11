package cluster

import (
	"context"
	"fmt"
	"sync"

	"battleworld/pb"
	"battleworld/protocol"
	nodewire "battleworld/transport/node"
	"battleworld/world"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// NodeGRPCClient is a migration client for the Node data and control planes.
// It begins with NodeServiceV2 and permanently switches to V1 only when the
// remote explicitly reports Unimplemented. This avoids double-applying writes:
// semantic, transport, and authority errors are never retried through V1.
type NodeGRPCClient struct {
	v1   pb.NodeServiceClient
	v2   pb.NodeServiceV2Client
	conn *grpc.ClientConn
	id   string

	useV1  chan struct{}
	v1Once sync.Once
}

func NewNodeGRPCClient(id string, addr string) (*NodeGRPCClient, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &NodeGRPCClient{
		v1:    pb.NewNodeServiceClient(conn),
		v2:    pb.NewNodeServiceV2Client(conn),
		conn:  conn,
		id:    id,
		useV1: make(chan struct{}),
	}, nil
}

func (c *NodeGRPCClient) Close() error {
	return c.conn.Close()
}

func (c *NodeGRPCClient) NodeID() string {
	return c.id
}

func (c *NodeGRPCClient) usingV1() bool {
	select {
	case <-c.useV1:
		return true
	default:
		return false
	}
}

func (c *NodeGRPCClient) downgradeOnUnimplemented(err error) bool {
	if status.Code(err) != codes.Unimplemented {
		return false
	}
	c.v1Once.Do(func() { close(c.useV1) })
	return true
}

func (c *NodeGRPCClient) AddPlayer(ctx context.Context, mapID string, epoch uint64, profile *protocol.UserProfile) error {
	if profile == nil {
		return fmt.Errorf("player profile 不能为空")
	}
	if c.usingV1() {
		_, err := c.v1.AddPlayer(ctx, &pb.AddPlayerReq{MapId: mapID, MapEpoch: epoch, Profile: nodewire.ToLegacyUserProfile(*profile)})
		return err
	}
	_, err := c.v2.AddPlayer(ctx, &pb.NodeAddPlayerRequest{
		Authority: nodewire.NewMapAuthority(mapID, c.id, epoch),
		Player:    nodewire.ToPlayerState(*profile),
	})
	if !c.downgradeOnUnimplemented(err) {
		return err
	}
	_, err = c.v1.AddPlayer(ctx, &pb.AddPlayerReq{MapId: mapID, MapEpoch: epoch, Profile: nodewire.ToLegacyUserProfile(*profile)})
	return err
}

func (c *NodeGRPCClient) RemovePlayer(ctx context.Context, mapID, username string, epoch uint64) (protocol.UserProfile, bool, error) {
	if c.usingV1() {
		return c.removePlayerV1(ctx, mapID, username, epoch)
	}
	response, err := c.v2.RemovePlayer(ctx, &pb.NodeRemovePlayerRequest{
		Authority: nodewire.NewMapAuthority(mapID, c.id, epoch),
		Username:  username,
	})
	if !c.downgradeOnUnimplemented(err) {
		if err != nil {
			return protocol.UserProfile{}, false, err
		}
		return nodewire.FromPlayerState(response.GetPlayer()), response.GetRemoved(), nil
	}
	return c.removePlayerV1(ctx, mapID, username, epoch)
}

func (c *NodeGRPCClient) removePlayerV1(ctx context.Context, mapID, username string, epoch uint64) (protocol.UserProfile, bool, error) {
	response, err := c.v1.RemovePlayer(ctx, &pb.RemovePlayerReq{MapId: mapID, Username: username, MapEpoch: epoch})
	if err != nil {
		return protocol.UserProfile{}, false, err
	}
	return nodewire.FromLegacyUserProfile(response.GetProfile()), response.GetOk(), nil
}

func (c *NodeGRPCClient) MovePlayer(ctx context.Context, mapID, username, direction string, epoch uint64) (string, protocol.UserProfile, bool, error) {
	if c.usingV1() {
		return c.movePlayerV1(ctx, mapID, username, direction, epoch)
	}
	wireDirection, err := nodewire.DirectionFromString(direction)
	if err != nil {
		return "", protocol.UserProfile{}, false, err
	}
	response, err := c.v2.MovePlayer(ctx, &pb.NodeMovePlayerRequest{
		Authority: nodewire.NewMapAuthority(mapID, c.id, epoch),
		Username:  username,
		Direction: wireDirection,
	})
	if !c.downgradeOnUnimplemented(err) {
		if err != nil {
			return "", protocol.UserProfile{}, false, err
		}
		return response.GetMessage(), nodewire.FromPlayerState(response.GetPlayer()), response.GetAccepted(), nil
	}
	return c.movePlayerV1(ctx, mapID, username, direction, epoch)
}

func (c *NodeGRPCClient) movePlayerV1(ctx context.Context, mapID, username, direction string, epoch uint64) (string, protocol.UserProfile, bool, error) {
	response, err := c.v1.MovePlayer(ctx, &pb.MovePlayerReq{MapId: mapID, Username: username, Dir: direction, MapEpoch: epoch})
	if err != nil {
		return "", protocol.UserProfile{}, false, err
	}
	return response.GetText(), nodewire.FromLegacyUserProfile(response.GetProfile()), response.GetOk(), nil
}

func (c *NodeGRPCClient) Attack(ctx context.Context, mapID, username string, epoch uint64) (string, string, string, protocol.UserProfile, bool, error) {
	if c.usingV1() {
		return c.attackV1(ctx, mapID, username, epoch)
	}
	response, err := c.v2.Attack(ctx, &pb.NodeAttackRequest{Authority: nodewire.NewMapAuthority(mapID, c.id, epoch), Username: username})
	if !c.downgradeOnUnimplemented(err) {
		if err != nil {
			return "", "", "", protocol.UserProfile{}, false, err
		}
		return response.GetMessage(), response.GetTargetMessage(), response.GetGlobalMessage(), nodewire.FromPlayerState(response.GetPlayer()), response.GetAccepted(), nil
	}
	return c.attackV1(ctx, mapID, username, epoch)
}

func (c *NodeGRPCClient) attackV1(ctx context.Context, mapID, username string, epoch uint64) (string, string, string, protocol.UserProfile, bool, error) {
	response, err := c.v1.Attack(ctx, &pb.AttackReq{MapId: mapID, Username: username, MapEpoch: epoch})
	if err != nil {
		return "", "", "", protocol.UserProfile{}, false, err
	}
	return response.GetLog(), response.GetBLog(), response.GetGmLog(), nodewire.FromLegacyUserProfile(response.GetProfile()), response.GetOk(), nil
}

func (c *NodeGRPCClient) Heal(ctx context.Context, mapID, username string, epoch uint64) (string, protocol.UserProfile, bool, error) {
	if c.usingV1() {
		return c.healV1(ctx, mapID, username, epoch)
	}
	response, err := c.v2.Heal(ctx, &pb.NodePlayerRequest{Authority: nodewire.NewMapAuthority(mapID, c.id, epoch), Username: username})
	if !c.downgradeOnUnimplemented(err) {
		if err != nil {
			return "", protocol.UserProfile{}, false, err
		}
		return response.GetMessage(), nodewire.FromPlayerState(response.GetPlayer()), response.GetAccepted(), nil
	}
	return c.healV1(ctx, mapID, username, epoch)
}

func (c *NodeGRPCClient) healV1(ctx context.Context, mapID, username string, epoch uint64) (string, protocol.UserProfile, bool, error) {
	response, err := c.v1.Heal(ctx, &pb.HealReq{MapId: mapID, Username: username, MapEpoch: epoch})
	if err != nil {
		return "", protocol.UserProfile{}, false, err
	}
	return response.GetText(), nodewire.FromLegacyUserProfile(response.GetProfile()), response.GetOk(), nil
}

func (c *NodeGRPCClient) BuyItem(ctx context.Context, mapID, username, item string, epoch uint64) (string, protocol.UserProfile, bool, error) {
	if c.usingV1() {
		return c.buyItemV1(ctx, mapID, username, item, epoch)
	}
	response, err := c.v2.BuyItem(ctx, &pb.NodeBuyItemRequest{Authority: nodewire.NewMapAuthority(mapID, c.id, epoch), Username: username, Item: item})
	if !c.downgradeOnUnimplemented(err) {
		if err != nil {
			return "", protocol.UserProfile{}, false, err
		}
		return response.GetMessage(), nodewire.FromPlayerState(response.GetPlayer()), response.GetAccepted(), nil
	}
	return c.buyItemV1(ctx, mapID, username, item, epoch)
}

func (c *NodeGRPCClient) buyItemV1(ctx context.Context, mapID, username, item string, epoch uint64) (string, protocol.UserProfile, bool, error) {
	response, err := c.v1.BuyItem(ctx, &pb.BuyItemReq{MapId: mapID, Username: username, Item: item, MapEpoch: epoch})
	if err != nil {
		return "", protocol.UserProfile{}, false, err
	}
	return response.GetText(), nodewire.FromLegacyUserProfile(response.GetProfile()), response.GetOk(), nil
}

func (c *NodeGRPCClient) AttackBoss(ctx context.Context, mapID, username string, epoch uint64) (string, protocol.UserProfile, bool, error) {
	if c.usingV1() {
		return c.attackBossV1(ctx, mapID, username, epoch)
	}
	response, err := c.v2.AttackBoss(ctx, &pb.NodePlayerRequest{Authority: nodewire.NewMapAuthority(mapID, c.id, epoch), Username: username})
	if !c.downgradeOnUnimplemented(err) {
		if err != nil {
			return "", protocol.UserProfile{}, false, err
		}
		return response.GetMessage(), nodewire.FromPlayerState(response.GetPlayer()), response.GetAccepted(), nil
	}
	return c.attackBossV1(ctx, mapID, username, epoch)
}

func (c *NodeGRPCClient) attackBossV1(ctx context.Context, mapID, username string, epoch uint64) (string, protocol.UserProfile, bool, error) {
	response, err := c.v1.AttackBoss(ctx, &pb.AttackBossReq{MapId: mapID, Username: username, MapEpoch: epoch})
	if err != nil {
		return "", protocol.UserProfile{}, false, err
	}
	return response.GetText(), nodewire.FromLegacyUserProfile(response.GetProfile()), response.GetOk(), nil
}

func (c *NodeGRPCClient) Profile(ctx context.Context, mapID, username string) (protocol.UserProfile, bool, error) {
	if c.usingV1() {
		return c.profileV1(ctx, mapID, username)
	}
	response, err := c.v2.Profile(ctx, &pb.NodeProfileRequest{MapId: mapID, Username: username})
	if !c.downgradeOnUnimplemented(err) {
		if err != nil {
			return protocol.UserProfile{}, false, err
		}
		return nodewire.FromPlayerState(response.GetPlayer()), response.GetFound(), nil
	}
	return c.profileV1(ctx, mapID, username)
}

func (c *NodeGRPCClient) profileV1(ctx context.Context, mapID, username string) (protocol.UserProfile, bool, error) {
	response, err := c.v1.Profile(ctx, &pb.ProfileReq{MapId: mapID, Username: username})
	if err != nil {
		return protocol.UserProfile{}, false, err
	}
	return nodewire.FromLegacyUserProfile(response.GetProfile()), response.GetOk(), nil
}

func (c *NodeGRPCClient) RewardPlayer(ctx context.Context, mapID, username string, treasureDelta, victoryDelta int, epoch uint64) (protocol.UserProfile, bool, error) {
	if c.usingV1() {
		return c.rewardPlayerV1(ctx, mapID, username, treasureDelta, victoryDelta, epoch)
	}
	response, err := c.v2.RewardPlayer(ctx, &pb.NodeRewardPlayerRequest{Authority: nodewire.NewMapAuthority(mapID, c.id, epoch), Username: username, TreasureDelta: int32(treasureDelta), VictoryDelta: int32(victoryDelta)})
	if !c.downgradeOnUnimplemented(err) {
		if err != nil {
			return protocol.UserProfile{}, false, err
		}
		return nodewire.FromPlayerState(response.GetPlayer()), response.GetFound(), nil
	}
	return c.rewardPlayerV1(ctx, mapID, username, treasureDelta, victoryDelta, epoch)
}

func (c *NodeGRPCClient) rewardPlayerV1(ctx context.Context, mapID, username string, treasureDelta, victoryDelta int, epoch uint64) (protocol.UserProfile, bool, error) {
	response, err := c.v1.RewardPlayer(ctx, &pb.RewardPlayerReq{MapId: mapID, Username: username, TreasureDelta: int32(treasureDelta), VictoryDelta: int32(victoryDelta), MapEpoch: epoch})
	if err != nil {
		return protocol.UserProfile{}, false, err
	}
	return nodewire.FromLegacyUserProfile(response.GetProfile()), response.GetOk(), nil
}

func (c *NodeGRPCClient) Snapshot(ctx context.Context, mapID string) (protocol.MapView, error) {
	if c.usingV1() {
		return c.snapshotV1(ctx, mapID)
	}
	response, err := c.v2.Snapshot(ctx, &pb.NodeSnapshotRequest{MapId: mapID})
	if !c.downgradeOnUnimplemented(err) {
		if err != nil {
			return protocol.MapView{}, err
		}
		return nodewire.FromMapView(response.GetMap()), nil
	}
	return c.snapshotV1(ctx, mapID)
}

func (c *NodeGRPCClient) snapshotV1(ctx context.Context, mapID string) (protocol.MapView, error) {
	response, err := c.v1.Snapshot(ctx, &pb.SnapshotReq{MapId: mapID})
	if err != nil {
		return protocol.MapView{}, err
	}
	return nodewire.FromMapView(response.GetMap()), nil
}

func (c *NodeGRPCClient) Counts(ctx context.Context, mapID string) (int, int, int, int64, error) {
	if c.usingV1() {
		return c.countsV1(ctx, mapID)
	}
	response, err := c.v2.Counts(ctx, &pb.NodeCountsRequest{MapId: mapID})
	if !c.downgradeOnUnimplemented(err) {
		if err != nil {
			return 0, 0, 0, 0, err
		}
		return int(response.GetPlayers()), int(response.GetNpcs()), int(response.GetTreasures()), response.GetVersion(), nil
	}
	return c.countsV1(ctx, mapID)
}

func (c *NodeGRPCClient) countsV1(ctx context.Context, mapID string) (int, int, int, int64, error) {
	response, err := c.v1.Counts(ctx, &pb.CountsReq{MapId: mapID})
	if err != nil {
		return 0, 0, 0, 0, err
	}
	return int(response.GetPlayers()), int(response.GetNpcs()), int(response.GetTreasures()), response.GetVersion(), nil
}

func (c *NodeGRPCClient) Ping(ctx context.Context) error {
	if c.usingV1() {
		_, err := c.v1.Ping(ctx, &pb.PingReq{})
		return err
	}
	_, err := c.v2.Ping(ctx, &pb.NodePingRequest{})
	if !c.downgradeOnUnimplemented(err) {
		return err
	}
	_, err = c.v1.Ping(ctx, &pb.PingReq{})
	return err
}

func (c *NodeGRPCClient) Checkpoint(ctx context.Context, mapID string) (protocol.MapCheckpoint, error) {
	if c.usingV1() {
		return c.checkpointV1(ctx, mapID)
	}
	response, err := c.v2.Checkpoint(ctx, &pb.NodeCheckpointRequest{MapId: mapID})
	if !c.downgradeOnUnimplemented(err) {
		if err != nil {
			return protocol.MapCheckpoint{}, err
		}
		return nodewire.FromCheckpoint(response.GetCheckpoint())
	}
	return c.checkpointV1(ctx, mapID)
}

func (c *NodeGRPCClient) checkpointV1(ctx context.Context, mapID string) (protocol.MapCheckpoint, error) {
	response, err := c.v1.Checkpoint(ctx, &pb.CheckpointReq{MapId: mapID})
	if err != nil {
		return protocol.MapCheckpoint{}, err
	}
	return nodewire.FromLegacyCheckpoint(response.GetCheckpoint()), nil
}

func (c *NodeGRPCClient) Promote(mapID string, _ world.MapConfig, checkpoint protocol.MapCheckpoint, epoch uint64) error {
	if c.usingV1() {
		return c.promoteV1(mapID, checkpoint, epoch)
	}
	_, err := c.v2.Promote(context.Background(), &pb.NodePromoteRequest{Authority: nodewire.NewMapAuthority(mapID, c.id, epoch), Checkpoint: nodewire.ToCheckpoint(checkpoint)})
	if !c.downgradeOnUnimplemented(err) {
		return err
	}
	return c.promoteV1(mapID, checkpoint, epoch)
}

func (c *NodeGRPCClient) promoteV1(mapID string, checkpoint protocol.MapCheckpoint, epoch uint64) error {
	_, err := c.v1.Promote(context.Background(), &pb.PromoteReq{MapId: mapID, MapEpoch: epoch, Checkpoint: nodewire.ToLegacyCheckpoint(checkpoint)})
	return err
}

func (c *NodeGRPCClient) View() protocol.NodeView {
	if c.usingV1() {
		return c.viewV1()
	}
	response, err := c.v2.View(context.Background(), &pb.NodeViewRequest{})
	if !c.downgradeOnUnimplemented(err) {
		if err != nil || response.GetView() == nil {
			return protocol.NodeView{ID: c.id}
		}
		return nodewire.FromNodeView(response.GetView())
	}
	return c.viewV1()
}

func (c *NodeGRPCClient) viewV1() protocol.NodeView {
	response, err := c.v1.View(context.Background(), &pb.ViewReq{})
	if err != nil || response.GetView() == nil {
		return protocol.NodeView{ID: c.id}
	}
	return nodewire.FromNodeView(response.GetView())
}

var (
	_ GatewayNodeClient     = (*NodeGRPCClient)(nil)
	_ CoordinatorNodeClient = (*NodeGRPCClient)(nil)
	_ gatewayNodeClient     = (*NodeGRPCClient)(nil)
)
