package cluster

import (
	"context"
	"fmt"

	"battleworld/pb"
	"battleworld/protocol"
	nodewire "battleworld/transport/node"
	"battleworld/world"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// NodeGRPCClient is the typed NodeService client shared by the Gateway data
// plane and Coordinator control plane.
type NodeGRPCClient struct {
	client pb.NodeServiceClient
	conn   *grpc.ClientConn
	id     string
}

func NewNodeGRPCClient(id string, addr string) (*NodeGRPCClient, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &NodeGRPCClient{
		client: pb.NewNodeServiceClient(conn),
		conn:   conn,
		id:     id,
	}, nil
}

func (c *NodeGRPCClient) Close() error {
	return c.conn.Close()
}

func (c *NodeGRPCClient) NodeID() string {
	return c.id
}

func (c *NodeGRPCClient) AddPlayer(ctx context.Context, mapID string, epoch uint64, profile *protocol.UserProfile) error {
	if profile == nil {
		return fmt.Errorf("player profile 不能为空")
	}
	_, err := c.client.AddPlayer(ctx, &pb.NodeAddPlayerRequest{
		Authority: nodewire.NewMapAuthority(mapID, c.id, epoch),
		Player:    nodewire.ToPlayerState(*profile),
	})
	return err
}

func (c *NodeGRPCClient) RemovePlayer(ctx context.Context, mapID, username string, epoch uint64) (protocol.UserProfile, bool, error) {
	response, err := c.client.RemovePlayer(ctx, &pb.NodeRemovePlayerRequest{
		Authority: nodewire.NewMapAuthority(mapID, c.id, epoch),
		Username:  username,
	})
	if err != nil {
		return protocol.UserProfile{}, false, err
	}
	return nodewire.FromPlayerState(response.GetPlayer()), response.GetRemoved(), nil
}

func (c *NodeGRPCClient) MovePlayer(ctx context.Context, mapID, username, direction string, epoch uint64) (string, protocol.UserProfile, bool, error) {
	wireDirection, err := nodewire.DirectionFromString(direction)
	if err != nil {
		return "", protocol.UserProfile{}, false, err
	}
	response, err := c.client.MovePlayer(ctx, &pb.NodeMovePlayerRequest{
		Authority: nodewire.NewMapAuthority(mapID, c.id, epoch),
		Username:  username,
		Direction: wireDirection,
	})
	if err != nil {
		return "", protocol.UserProfile{}, false, err
	}
	return response.GetMessage(), nodewire.FromPlayerState(response.GetPlayer()), response.GetAccepted(), nil
}

func (c *NodeGRPCClient) Attack(ctx context.Context, mapID, username string, epoch uint64) (string, string, string, protocol.UserProfile, bool, error) {
	response, err := c.client.Attack(ctx, &pb.NodeAttackRequest{
		Authority: nodewire.NewMapAuthority(mapID, c.id, epoch),
		Username:  username,
	})
	if err != nil {
		return "", "", "", protocol.UserProfile{}, false, err
	}
	return response.GetMessage(), response.GetTargetMessage(), response.GetGlobalMessage(), nodewire.FromPlayerState(response.GetPlayer()), response.GetAccepted(), nil
}

func (c *NodeGRPCClient) Heal(ctx context.Context, mapID, username string, epoch uint64) (string, protocol.UserProfile, bool, error) {
	response, err := c.client.Heal(ctx, &pb.NodePlayerRequest{
		Authority: nodewire.NewMapAuthority(mapID, c.id, epoch),
		Username:  username,
	})
	if err != nil {
		return "", protocol.UserProfile{}, false, err
	}
	return response.GetMessage(), nodewire.FromPlayerState(response.GetPlayer()), response.GetAccepted(), nil
}

func (c *NodeGRPCClient) BuyItem(ctx context.Context, mapID, username, item string, epoch uint64) (string, protocol.UserProfile, bool, error) {
	response, err := c.client.BuyItem(ctx, &pb.NodeBuyItemRequest{
		Authority: nodewire.NewMapAuthority(mapID, c.id, epoch),
		Username:  username,
		Item:      item,
	})
	if err != nil {
		return "", protocol.UserProfile{}, false, err
	}
	return response.GetMessage(), nodewire.FromPlayerState(response.GetPlayer()), response.GetAccepted(), nil
}

func (c *NodeGRPCClient) AttackBoss(ctx context.Context, mapID, username string, epoch uint64) (string, protocol.UserProfile, bool, error) {
	response, err := c.client.AttackBoss(ctx, &pb.NodePlayerRequest{
		Authority: nodewire.NewMapAuthority(mapID, c.id, epoch),
		Username:  username,
	})
	if err != nil {
		return "", protocol.UserProfile{}, false, err
	}
	return response.GetMessage(), nodewire.FromPlayerState(response.GetPlayer()), response.GetAccepted(), nil
}

func (c *NodeGRPCClient) Profile(ctx context.Context, mapID, username string) (protocol.UserProfile, bool, error) {
	response, err := c.client.Profile(ctx, &pb.NodeProfileRequest{MapId: mapID, Username: username})
	if err != nil {
		return protocol.UserProfile{}, false, err
	}
	return nodewire.FromPlayerState(response.GetPlayer()), response.GetFound(), nil
}

func (c *NodeGRPCClient) RewardPlayer(ctx context.Context, mapID, username string, treasureDelta, victoryDelta int, epoch uint64) (protocol.UserProfile, bool, error) {
	response, err := c.client.RewardPlayer(ctx, &pb.NodeRewardPlayerRequest{
		Authority:     nodewire.NewMapAuthority(mapID, c.id, epoch),
		Username:      username,
		TreasureDelta: int32(treasureDelta),
		VictoryDelta:  int32(victoryDelta),
	})
	if err != nil {
		return protocol.UserProfile{}, false, err
	}
	return nodewire.FromPlayerState(response.GetPlayer()), response.GetFound(), nil
}

func (c *NodeGRPCClient) Snapshot(ctx context.Context, mapID string) (protocol.MapView, error) {
	response, err := c.client.Snapshot(ctx, &pb.NodeSnapshotRequest{MapId: mapID})
	if err != nil {
		return protocol.MapView{}, err
	}
	return nodewire.FromMapView(response.GetMap()), nil
}

func (c *NodeGRPCClient) Counts(ctx context.Context, mapID string) (int, int, int, int64, error) {
	response, err := c.client.Counts(ctx, &pb.NodeCountsRequest{MapId: mapID})
	if err != nil {
		return 0, 0, 0, 0, err
	}
	return int(response.GetPlayers()), int(response.GetNpcs()), int(response.GetTreasures()), response.GetVersion(), nil
}

func (c *NodeGRPCClient) Ping(ctx context.Context) error {
	_, err := c.client.Ping(ctx, &pb.NodePingRequest{})
	return err
}

func (c *NodeGRPCClient) Checkpoint(ctx context.Context, mapID string) (protocol.MapCheckpoint, error) {
	response, err := c.client.Checkpoint(ctx, &pb.NodeCheckpointRequest{MapId: mapID})
	if err != nil {
		return protocol.MapCheckpoint{}, err
	}
	return nodewire.FromCheckpoint(response.GetCheckpoint())
}

func (c *NodeGRPCClient) Promote(mapID string, _ world.MapConfig, checkpoint protocol.MapCheckpoint, epoch uint64) error {
	_, err := c.client.Promote(context.Background(), &pb.NodePromoteRequest{
		Authority:  nodewire.NewMapAuthority(mapID, c.id, epoch),
		Checkpoint: nodewire.ToCheckpoint(checkpoint),
	})
	return err
}

func (c *NodeGRPCClient) View() protocol.NodeView {
	response, err := c.client.View(context.Background(), &pb.NodeViewRequest{})
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
