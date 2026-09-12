package coordinator

import (
	"context"
	"log"
	"time"
)

func (c *Coordinator) heartbeatLoop(leaderCtx context.Context) {
	ticker := time.NewTicker(c.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.heartbeatOnce()
		case <-leaderCtx.Done():
			return
		}
	}
}

func (c *Coordinator) heartbeatOnce() {
	if _, leader := c.currentLeaderTerm(); !leader {
		return
	}
	c.mu.RLock()
	nodes := make([]nodeConnection, 0, len(c.nodes))
	for _, connection := range c.nodes {
		nodes = append(nodes, connection)
	}
	c.mu.RUnlock()

	for _, connection := range nodes {
		ctx, cancel := context.WithTimeout(context.Background(), c.nodeRequestTimeout)
		err := connection.client.Ping(ctx)
		cancel()
		c.setNodeHealth(connection.client.NodeID(), err == nil)
	}
}

func (c *Coordinator) setNodeHealth(nodeID string, healthy bool) {
	if _, leader := c.currentLeaderTerm(); !leader {
		return
	}
	c.mu.Lock()
	connection, ok := c.nodes[nodeID]
	if !ok || connection.healthy == healthy {
		c.mu.Unlock()
		return
	}
	connection.healthy = healthy
	c.nodes[nodeID] = connection
	c.mu.Unlock()

	if healthy {
		log.Printf("[coordinator/heartbeat] 节点 %s 已恢复健康", nodeID)
		return
	}
	log.Printf("[coordinator/heartbeat] 节点 %s 不健康，开始评估带 MapEpoch 的同图 standby 提升", nodeID)
	// 绝不能持有 c.mu 调用 Promote 或 Redis CAS；这些操作可回调/阻塞，且 failoverMap
	// 会重新读取最新 topology，确保只切换仍由该失效节点拥有的地图。
	go c.failoverNode(nodeID)
}
