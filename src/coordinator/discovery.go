package coordinator

import (
	"context"
	"log"
	"time"

	"battleworld/storage"
)

func (c *Coordinator) discoveryLoop(leaderCtx context.Context) {
	ticker := time.NewTicker(c.discoveryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.discoverOnce()
		case <-leaderCtx.Done():
			return
		}
	}
}

func (c *Coordinator) discoverOnce() {
	if _, leader := c.currentLeaderTerm(); !leader {
		return
	}
	activeNodes, err := c.store.GetActiveNodes()
	if err != nil {
		log.Printf("[coordinator/discovery] 读取活跃节点失败: %v", err)
		return
	}
	c.discoverNodes(activeNodes)
	c.reconcileDrainingNodes(activeNodes)
	if err := c.bootstrapTopologyIfAbsent(c.connectedNodeRegistrations(activeNodes)); err != nil {
		log.Printf("[coordinator/topology] bootstrap 失败: %v", err)
	}
}

// discoverNodes 仅建立控制面连接池，不依据节点注册声明直接修改地图 owner。
func (c *Coordinator) discoverNodes(activeNodes []storage.NodeRegistryInfo) {
	c.mu.Lock()
	pending := make([]storage.NodeRegistryInfo, 0, len(activeNodes))
	if c.closed {
		c.mu.Unlock()
		return
	}
	for _, info := range activeNodes {
		if info.Draining {
			continue
		}
		if _, connected := c.nodes[info.ID]; connected {
			continue
		}
		if _, connecting := c.connectingNodes[info.ID]; connecting {
			continue
		}
		c.connectingNodes[info.ID] = struct{}{}
		pending = append(pending, info)
	}
	c.mu.Unlock()

	for _, info := range pending {
		go c.connectDiscoveredNode(info)
	}
}

func (c *Coordinator) connectDiscoveredNode(info storage.NodeRegistryInfo) {
	client, err := c.newNodeClient(info.ID, info.Addr)
	if err != nil {
		c.finishNodeConnection(info.ID)
		log.Printf("[coordinator/discovery] 创建节点 %s 客户端失败: %v", info.ID, err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.nodeRequestTimeout)
	err = client.Ping(ctx)
	cancel()
	if err != nil {
		_ = client.Close()
		c.finishNodeConnection(info.ID)
		log.Printf("[coordinator/discovery] 节点 %s 健康检查失败: %v", info.ID, err)
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.connectingNodes, info.ID)
	if c.closed {
		_ = client.Close()
		return
	}
	if _, exists := c.nodes[info.ID]; exists {
		_ = client.Close()
		return
	}
	c.nodes[info.ID] = nodeConnection{client: client, healthy: true}
	log.Printf("[coordinator/discovery] 节点 %s 已加入控制面连接池", info.ID)
}

func (c *Coordinator) finishNodeConnection(nodeID string) {
	c.mu.Lock()
	delete(c.connectingNodes, nodeID)
	c.mu.Unlock()
}

// connectedNodeRegistrations 只返回已完成连接且健康的节点，避免将 Redis 中尚未验证
// 可达性的临时或过期租约用于初始拓扑构造。
func (c *Coordinator) connectedNodeRegistrations(activeNodes []storage.NodeRegistryInfo) []storage.NodeRegistryInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	connected := make([]storage.NodeRegistryInfo, 0, len(activeNodes))
	for _, info := range activeNodes {
		if info.Draining {
			continue
		}
		connection, ok := c.nodes[info.ID]
		if !ok || !connection.healthy {
			continue
		}
		connected = append(connected, info)
	}
	return connected
}
