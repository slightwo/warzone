package cluster

import (
	"context"

	"battleworld/protocol"
	"battleworld/world"
)

// GatewayNodeClient 定义网关访问节点数据面的最小能力集合。
// 网关只通过该接口执行玩家操作、查询玩家状态和获取地图快照，不负责节点生命周期、
// 健康探测或拓扑控制。
type GatewayNodeClient interface {
	NodeID() string
	AddPlayer(context.Context, string, *protocol.UserProfile) error
	RemovePlayer(context.Context, string, string) (protocol.UserProfile, bool, error)
	MovePlayer(context.Context, string, string, string) (string, protocol.UserProfile, bool, error)
	Attack(context.Context, string, string) (string, string, string, protocol.UserProfile, bool, error)
	Heal(context.Context, string, string) (string, protocol.UserProfile, bool, error)
	BuyItem(context.Context, string, string, string) (string, protocol.UserProfile, bool, error)
	AttackBoss(context.Context, string, string) (string, protocol.UserProfile, bool, error)
	Profile(context.Context, string, string) (protocol.UserProfile, bool, error)
	RewardPlayer(context.Context, string, string, int, int) (protocol.UserProfile, bool, error)
	Snapshot(context.Context, string) (protocol.MapView, error)
}

// CoordinatorNodeClient 定义协调器访问节点控制面的最小能力集合。
// 节点进程由部署系统管理，因此该接口不包含 Start 或 Stop 等伪生命周期操作。
type CoordinatorNodeClient interface {
	NodeID() string
	Ping(context.Context) error
	View() protocol.NodeView
	Checkpoint(context.Context, string) (protocol.MapCheckpoint, error)
	Promote(string, world.MapConfig) error
	Close() error
}

// managedNodeClient 是 2.2-B 拆分期间 Cluster 的内部过渡接口。B.2 将 discovery、
// heartbeat 和故障处理迁入 coordinator 后，网关只会保留 GatewayNodeClient 引用。
type managedNodeClient interface {
	GatewayNodeClient
	CoordinatorNodeClient
	IsHealthy() bool
	SetHealthy(bool) bool
}
