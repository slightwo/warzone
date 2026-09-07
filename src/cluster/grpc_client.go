package cluster

import (
	"context"

	"battleworld/pb"
	"battleworld/protocol"
	"battleworld/world"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// NodeGRPCClient 是节点的 gRPC 传输实现，同时满足网关数据面与协调器控制面接口。
type NodeGRPCClient struct {
	client pb.NodeServiceClient
	conn   *grpc.ClientConn
	id     string
}

func NewNodeGRPCClient(id string, addr string) (*NodeGRPCClient, error) {
	// 暂且使用不安全的明文连接
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

func (c *NodeGRPCClient) AddPlayer(ctx context.Context, mapID string, profile *protocol.UserProfile) error {
	req := &pb.AddPlayerReq{
		MapId:   mapID,
		Profile: protocol.ToProtoUserProfile(*profile),
	}
	_, err := c.client.AddPlayer(ctx, req)
	return err
}

func (c *NodeGRPCClient) RemovePlayer(ctx context.Context, mapID, username string) (protocol.UserProfile, bool, error) {
	req := &pb.RemovePlayerReq{
		MapId:    mapID,
		Username: username,
	}
	resp, err := c.client.RemovePlayer(ctx, req)
	if err != nil {
		return protocol.UserProfile{}, false, err
	}
	return protocol.FromProtoUserProfile(resp.Profile), resp.Ok, nil
}

func (c *NodeGRPCClient) MovePlayer(ctx context.Context, mapID, username, dir string) (string, protocol.UserProfile, bool, error) {
	req := &pb.MovePlayerReq{
		MapId:    mapID,
		Username: username,
		Dir:      dir,
	}
	resp, err := c.client.MovePlayer(ctx, req)
	if err != nil {
		return "", protocol.UserProfile{}, false, err
	}
	return resp.Text, protocol.FromProtoUserProfile(resp.Profile), resp.Ok, nil
}

func (c *NodeGRPCClient) Attack(ctx context.Context, mapID, username string) (string, string, string, protocol.UserProfile, bool, error) {
	req := &pb.AttackReq{
		MapId:    mapID,
		Username: username,
	}
	resp, err := c.client.Attack(ctx, req)
	if err != nil {
		return "", "", "", protocol.UserProfile{}, false, err
	}
	return resp.Log, resp.BLog, resp.GmLog, protocol.FromProtoUserProfile(resp.Profile), resp.Ok, nil
}

func (c *NodeGRPCClient) Heal(ctx context.Context, mapID, username string) (string, protocol.UserProfile, bool, error) {
	req := &pb.HealReq{
		MapId:    mapID,
		Username: username,
	}
	resp, err := c.client.Heal(ctx, req)
	if err != nil {
		return "", protocol.UserProfile{}, false, err
	}
	return resp.Text, protocol.FromProtoUserProfile(resp.Profile), resp.Ok, nil
}

func (c *NodeGRPCClient) BuyItem(ctx context.Context, mapID, username, item string) (string, protocol.UserProfile, bool, error) {
	req := &pb.BuyItemReq{
		MapId:    mapID,
		Username: username,
		Item:     item,
	}
	resp, err := c.client.BuyItem(ctx, req)
	if err != nil {
		return "", protocol.UserProfile{}, false, err
	}
	return resp.Text, protocol.FromProtoUserProfile(resp.Profile), resp.Ok, nil
}

func (c *NodeGRPCClient) AttackBoss(ctx context.Context, mapID, username string) (string, protocol.UserProfile, bool, error) {
	req := &pb.AttackBossReq{
		MapId:    mapID,
		Username: username,
	}
	resp, err := c.client.AttackBoss(ctx, req)
	if err != nil {
		return "", protocol.UserProfile{}, false, err
	}
	return resp.Text, protocol.FromProtoUserProfile(resp.Profile), resp.Ok, nil
}

func (c *NodeGRPCClient) Profile(ctx context.Context, mapID, username string) (protocol.UserProfile, bool, error) {
	req := &pb.ProfileReq{
		MapId:    mapID,
		Username: username,
	}
	resp, err := c.client.Profile(ctx, req)
	if err != nil {
		return protocol.UserProfile{}, false, err
	}
	return protocol.FromProtoUserProfile(resp.Profile), resp.Ok, nil
}

func (c *NodeGRPCClient) RewardPlayer(ctx context.Context, mapID, username string, treasureDelta, victoryDelta int) (protocol.UserProfile, bool, error) {
	req := &pb.RewardPlayerReq{
		MapId:         mapID,
		Username:      username,
		TreasureDelta: int32(treasureDelta),
		VictoryDelta:  int32(victoryDelta),
	}
	resp, err := c.client.RewardPlayer(ctx, req)
	if err != nil {
		return protocol.UserProfile{}, false, err
	}
	return protocol.FromProtoUserProfile(resp.Profile), resp.Ok, nil
}

func (c *NodeGRPCClient) Snapshot(ctx context.Context, mapID string) (protocol.MapView, error) {
	req := &pb.SnapshotReq{
		MapId: mapID,
	}
	resp, err := c.client.Snapshot(ctx, req)
	if err != nil {
		return protocol.MapView{}, err
	}
	return protocol.FromProtoMapView(resp.Map), nil
}

func (c *NodeGRPCClient) Counts(ctx context.Context, mapID string) (int, int, int, int64, error) {
	req := &pb.CountsReq{
		MapId: mapID,
	}
	resp, err := c.client.Counts(ctx, req)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	return int(resp.Players), int(resp.Npcs), int(resp.Treasures), resp.Version, nil
}

func (c *NodeGRPCClient) Ping(ctx context.Context) error {
	req := &pb.PingReq{}
	_, err := c.client.Ping(ctx, req)
	return err
}

func (c *NodeGRPCClient) Checkpoint(ctx context.Context, mapID string) (protocol.MapCheckpoint, error) {
	req := &pb.CheckpointReq{
		MapId: mapID,
	}
	resp, err := c.client.Checkpoint(ctx, req)
	if err != nil {
		return protocol.MapCheckpoint{}, err
	}
	return protocol.FromProtoMapCheckpoint(resp.Checkpoint), nil
}

func (c *NodeGRPCClient) Promote(mapID string, cfg world.MapConfig) error {
	req := &pb.PromoteReq{
		MapId: mapID,
	}
	_, err := c.client.Promote(context.Background(), req)
	return err
}

func (c *NodeGRPCClient) View() protocol.NodeView {
	req := &pb.ViewReq{}
	resp, err := c.client.View(context.Background(), req)
	if err != nil || resp.View == nil {
		return protocol.NodeView{ID: c.id}
	}
	return protocol.FromProtoNodeView(resp.View)
}

var (
	_ GatewayNodeClient     = (*NodeGRPCClient)(nil)
	_ CoordinatorNodeClient = (*NodeGRPCClient)(nil)
	_ gatewayNodeClient     = (*NodeGRPCClient)(nil)
)
