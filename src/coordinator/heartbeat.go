package coordinator

import (
	"context"
	"log"
	"time"
)

func (c *Coordinator) heartbeatLoop() {
	ticker := time.NewTicker(c.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.heartbeatOnce()
		case <-c.stopCh:
			return
		}
	}
}

func (c *Coordinator) heartbeatOnce() {
	c.mu.RLock()
	nodes := make([]nodeConnection, 0, len(c.nodes))
	for _, connection := range c.nodes {
		nodes = append(nodes, connection)
	}
	c.mu.RUnlock()

	for _, connection := range nodes {
		ctx, cancel := context.WithTimeout(context.Background(), nodeRequestTimeout)
		err := connection.client.Ping(ctx)
		cancel()
		c.setNodeHealth(connection.client.NodeID(), err == nil)
	}
}

func (c *Coordinator) setNodeHealth(nodeID string, healthy bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	connection, ok := c.nodes[nodeID]
	if !ok || connection.healthy == healthy {
		return
	}
	connection.healthy = healthy
	c.nodes[nodeID] = connection
	if healthy {
		log.Printf("[coordinator/heartbeat] 节点 %s 已恢复健康", nodeID)
		return
	}
	// 2.2-B 只完成故障探测；带 MapEpoch 的副本提升和拓扑 CAS 在 2.2-C 实现。
	log.Printf("[coordinator/heartbeat] 节点 %s 不健康，当前拓扑保持不变", nodeID)
}
