package cluster

import (
	"errors"
	"fmt"
	"log"
	"time"

	"battleworld/storage"
)

const topologySyncInterval = 500 * time.Millisecond

// applyTopologyLocked 校验并应用一份已提交的拓扑快照。调用方必须持有 c.mu 写锁。
// owners 与 replicas 仅是兼容既有路由代码的过渡缓存，唯一写入入口是本方法。
func (c *Cluster) applyTopologyLocked(topology storage.Topology) (bool, error) {
	knownMapIDs := make(map[string]struct{}, len(c.configs))
	for mapID := range c.configs {
		knownMapIDs[mapID] = struct{}{}
	}
	if err := topology.Validate(knownMapIDs); err != nil {
		return false, fmt.Errorf("校验拓扑快照失败: %w", err)
	}
	if c.topologyLoaded && topology.Version <= c.topology.Version {
		return false, nil
	}

	c.topology = topology.Clone()
	c.topologyLoaded = true
	c.owners = make(map[string]string, len(c.topology.Owners))
	c.replicas = make(map[string]string, len(c.topology.Replicas))
	for mapID, nodeID := range c.topology.Owners {
		c.owners[mapID] = nodeID
	}
	for mapID, nodeID := range c.topology.Replicas {
		c.replicas[mapID] = nodeID
	}
	return true, nil
}

// loadTopology 从 Redis 加载并应用比本地更新的拓扑。找不到拓扑不视为错误，供 discovery
// 继续等待足够的节点候选能力并尝试 bootstrap。
func (c *Cluster) loadTopology() (bool, error) {
	topology, found, err := c.store.LoadTopology()
	if err != nil {
		return false, err
	}
	if !found {
		return false, nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	return c.applyTopologyLocked(*topology)
}

// topologySyncLoop 是 Pub/Sub 通知之外的兜底机制，避免网关漏掉拓扑变更通知。
func (c *Cluster) topologySyncLoop() {
	ticker := time.NewTicker(topologySyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if updated, err := c.loadTopology(); err != nil {
				log.Printf("[topology] 轮询加载拓扑失败: %v", err)
			} else if updated {
				log.Printf("[topology] 已通过轮询刷新路由快照")
			}
		case <-c.stopCh:
			return
		}
	}
}

// bootstrapTopologyIfAbsent 仅在 Redis 尚无拓扑时，根据当前活跃节点的声明候选能力
// 构造首个 Version 1 快照。提交冲突说明其他控制面已完成 bootstrap，此时重新加载即可。
func (c *Cluster) bootstrapTopologyIfAbsent(nodes []storage.NodeRegistryInfo) error {
	c.mu.RLock()
	alreadyLoaded := c.topologyLoaded
	c.mu.RUnlock()
	if alreadyLoaded {
		return nil
	}
	if _, err := c.loadTopology(); err != nil {
		return fmt.Errorf("bootstrap 前读取拓扑失败: %w", err)
	}
	c.mu.RLock()
	alreadyLoaded = c.topologyLoaded
	c.mu.RUnlock()
	if alreadyLoaded {
		return nil
	}

	c.mu.RLock()
	knownMapIDs := make(map[string]struct{}, len(c.configs))
	for mapID := range c.configs {
		knownMapIDs[mapID] = struct{}{}
	}
	c.mu.RUnlock()

	initial, ready, err := storage.BuildInitialTopology(knownMapIDs, nodes, time.Now().UTC())
	if err != nil {
		return fmt.Errorf("构造初始拓扑失败: %w", err)
	}
	if !ready {
		return nil
	}

	if err := c.store.CompareAndSaveTopology(0, initial); err != nil {
		if errors.Is(err, storage.ErrTopologyVersionConflict) {
			_, loadErr := c.loadTopology()
			if loadErr != nil {
				return fmt.Errorf("bootstrap 竞争后重新加载拓扑失败: %w", loadErr)
			}
			return nil
		}
		return fmt.Errorf("提交初始拓扑失败: %w", err)
	}

	if _, err := c.loadTopology(); err != nil {
		return fmt.Errorf("初始拓扑提交后重新加载失败: %w", err)
	}
	if err := c.store.PublishTopologyChanged(initial.Version); err != nil {
		return fmt.Errorf("初始拓扑已提交，但发布刷新通知失败: %w", err)
	}
	log.Printf("[topology] 已创建初始拓扑版本 %d", initial.Version)
	return nil
}
