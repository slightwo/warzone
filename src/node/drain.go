package node

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var (
	// ErrNodeDraining 表示节点已经拒绝新的地图状态写入，正在等待主权安全移交。
	ErrNodeDraining = errors.New("node is draining")
	// ErrDrainTopologyUnavailable 表示节点无法确认自身已不再是 owner，因此不能冒险退出。
	ErrDrainTopologyUnavailable = errors.New("drain topology authority unavailable")
	// ErrDrainOwnershipPending 表示等待超时后节点仍持有地图主权；调用方必须保留进程，
	// 由 coordinator 完成带 epoch 的迁移后再重试。
	ErrDrainOwnershipPending = errors.New("drain ownership transfer pending")
)

// DrainStatus 是 node 生命周期端点使用的轻量状态快照。
type DrainStatus struct {
	Draining          bool
	ActiveConnections int64
}

// BeginDrain 先阻止新的地图状态写入，再等待 topology 刷新确认本节点不再承载 owner。
// 确认移交后，它会为最后时刻的 owner 状态保存 fenced checkpoint；若超时或拓扑过期，
// 节点保持 draining 且继续运行，绝不能在仍拥有地图时停止进程。
func (n *NodeService) BeginDrain(ctx context.Context) error {
	if ctx == nil {
		return errors.New("drain context is nil")
	}
	n.drainMu.Lock()
	n.mu.Lock()
	alreadyDraining := n.draining
	n.draining = true
	n.mu.Unlock()
	if !alreadyDraining {
		if err := n.saveFinalCheckpoints(); err != nil {
			n.drainMu.Unlock()
			return fmt.Errorf("save final fenced checkpoints: %w", err)
		}
	}
	n.drainMu.Unlock()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		ownedMapIDs, fresh := n.authority.OwnedMapIDs()
		if !fresh {
			return ErrDrainTopologyUnavailable
		}
		if len(ownedMapIDs) == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: remaining owner maps=%v: %w", ErrDrainOwnershipPending, ownedMapIDs, ctx.Err())
		case <-ticker.C:
		}
	}
}

// StopAfterDrain 只在 topology 已确认本节点不再承载任何 owner 地图时停止本地循环。
// 这使 SIGTERM 和 /drain 的调用方能够复用同一安全退出条件。
func (n *NodeService) StopAfterDrain(ctx context.Context) error {
	if err := n.BeginDrain(ctx); err != nil {
		return err
	}
	return n.Stop()
}

// DrainState 返回 health/ready 端点所需状态。Node 当前没有长连接计数器，因此该值为 0；
// 后续接入连接跟踪时不改变接口语义。
func (n *NodeService) DrainState() DrainStatus {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return DrainStatus{Draining: n.draining}
}

func (n *NodeService) IsDraining() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.draining
}

func (n *NodeService) saveFinalCheckpoints() error {
	if n.store == nil {
		return ErrDrainTopologyUnavailable
	}
	ownedMapIDs, fresh := n.authority.OwnedMapIDs()
	if !fresh {
		return ErrDrainTopologyUnavailable
	}
	for _, mapID := range ownedMapIDs {
		authority, owned := n.authority.Current(mapID)
		if !owned {
			continue
		}
		if err := n.requireAuthority(mapID, authority.Epoch); err != nil {
			return fmt.Errorf("verify owner %q: %w", mapID, err)
		}
		n.mu.RLock()
		instance := n.maps[mapID]
		n.mu.RUnlock()
		if instance == nil {
			continue
		}
		checkpoint := instance.CaptureCheckpoint(n.ID)
		checkpoint.MapEpoch = authority.Epoch
		if err := n.store.SaveCheckpointIfOwner(checkpoint, n.ID, authority.Epoch); err != nil {
			return fmt.Errorf("save map %q: %w", mapID, err)
		}
	}
	return nil
}
