package cluster

import (
	"fmt"
	"log"
	"time"

	"battleworld/storage"
)

const topologySyncInterval = 500 * time.Millisecond

// applyTopologyLocked 校验并应用一份已提交的拓扑快照。调用方必须持有 c.mu 写锁。
// owners 与 replicas 是已提交拓扑的网关只读缓存，唯一写入入口是本方法。
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
	c.mapCacheMu.Lock()
	c.mapCache = make(map[string]MapCacheData)
	c.mapCacheMu.Unlock()
	return true, nil
}

// loadTopology 从 Redis 加载并应用比本地更新的拓扑。找不到拓扑不视为错误；网关会
// 明确报告路由尚未就绪，只有 coordinator 可以根据节点候选能力创建初始拓扑。
func (c *Cluster) loadTopology() (bool, error) {
	topology, found, err := c.store.LoadTopology()
	if err != nil {
		return false, err
	}
	if !found {
		return false, nil
	}

	c.mu.Lock()
	updated, applyErr := c.applyTopologyLocked(*topology)
	activeTopology := c.topology.Clone()
	c.mu.Unlock()
	if applyErr != nil {
		return false, applyErr
	}
	if err := c.reconcileGatewayNodes(activeTopology); err != nil {
		log.Printf("[gateway/routing] 刷新数据面连接池失败: %v", err)
	}
	return updated, nil
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
