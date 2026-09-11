package cluster

import (
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"

	"battleworld/protocol"
	"battleworld/storage"
)

// reconcileGatewayNodes 仅依据已提交 Topology 的 owner 集合维护网关数据面客户端。
// 它不会 Ping 节点、写入拓扑或修改节点健康状态；这些控制面职责完全由 coordinator
// 承担。节点注册只为已获 owner 身份的节点提供连接地址。
func (c *Cluster) reconcileGatewayNodes(topology storage.Topology) error {
	if c.activeNodes == nil {
		return errors.New("gateway 节点注册读取器不能为空")
	}
	activeNodes, err := c.activeNodes()
	if err != nil {
		return fmt.Errorf("读取节点注册地址失败: %w", err)
	}
	targets := gatewayOwnerTargets(topology, activeNodes)

	c.mu.Lock()
	if c.closed || !c.topologyLoaded || c.topology.Version != topology.Version {
		c.mu.Unlock()
		return nil
	}

	c.nodeTargets = targets
	obsolete := make([]gatewayNodeClient, 0)
	pending := make(map[string]string)
	for nodeID, node := range c.nodes {
		addr, wanted := targets[nodeID]
		if !wanted || c.nodeAddrs[nodeID] != addr {
			delete(c.nodes, nodeID)
			delete(c.nodeAddrs, nodeID)
			obsolete = append(obsolete, node)
		}
	}
	for nodeID, addr := range targets {
		if _, exists := c.nodes[nodeID]; !exists {
			pending[nodeID] = addr
		}
	}
	c.mu.Unlock()

	for _, node := range obsolete {
		_ = node.Close()
	}

	for nodeID, addr := range pending {
		client, err := c.nodeFactory(nodeID, addr)
		if err != nil {
			log.Printf("[gateway/routing] 创建节点 %s (%s) 数据面客户端失败: %v", nodeID, addr, err)
			continue
		}

		c.mu.Lock()
		if c.closed || !c.gatewayNodeStillWantedLocked(nodeID, addr) {
			c.mu.Unlock()
			_ = client.Close()
			continue
		}
		if _, exists := c.nodes[nodeID]; exists {
			c.mu.Unlock()
			_ = client.Close()
			continue
		}
		c.nodes[nodeID] = client
		c.nodeAddrs[nodeID] = addr
		c.mu.Unlock()
	}
	return nil
}

func (c *Cluster) gatewayNodeStillWantedLocked(nodeID, addr string) bool {
	if !c.topologyLoaded {
		return false
	}
	return c.nodeTargets[nodeID] == addr
}

// gatewayOwnerTargets 返回已提交 owner 节点中仍具有效注册地址的集合。无地址的 owner
// 保留在 Topology 中但不会创建客户端，网关会为其地图拒绝路由而不会猜测其它节点。
func gatewayOwnerTargets(topology storage.Topology, registrations []storage.NodeRegistryInfo) map[string]string {
	owners := make(map[string]struct{}, len(topology.Owners))
	for _, nodeID := range topology.Owners {
		if nodeID != "" {
			owners[nodeID] = struct{}{}
		}
	}

	targets := make(map[string]string, len(owners))
	for _, registration := range registrations {
		if _, owner := owners[registration.ID]; !owner {
			continue
		}
		// 主动 drain 的节点保留在已提交 Topology 中直到 coordinator 完成有序移交，
		// 但 gateway 不再为其建立或复用数据面连接，避免新会话继续落到下线节点。
		if registration.Draining {
			continue
		}
		if strings.TrimSpace(registration.Addr) == "" || registration.Addr != strings.TrimSpace(registration.Addr) {
			continue
		}
		if _, exists := targets[registration.ID]; !exists {
			targets[registration.ID] = registration.Addr
		}
	}
	return targets
}

// gatewayNodeViewsLocked 生成网关本地的连接视图。Healthy 表示网关已为该 owner 建立
// 数据面客户端，不代表对节点执行过健康探测；真实健康状态由 coordinator 负责。
// 调用方必须持有 c.mu 读锁或写锁。
func (c *Cluster) gatewayNodeViewsLocked() []protocol.NodeView {
	viewsByID := make(map[string]protocol.NodeView)
	for mapID, nodeID := range c.topology.Owners {
		if nodeID == "" {
			continue
		}
		view := viewsByID[nodeID]
		view.ID = nodeID
		view.Addr = c.nodeAddrs[nodeID]
		_, view.Healthy = c.nodes[nodeID]
		view.LastHeartbeat = "由 coordinator 管理"
		view.PrimaryMaps = append(view.PrimaryMaps, mapID)
		viewsByID[nodeID] = view
	}
	for mapID, nodeID := range c.topology.Replicas {
		if nodeID == "" {
			continue
		}
		view := viewsByID[nodeID]
		view.ID = nodeID
		view.Addr = c.nodeAddrs[nodeID]
		_, view.Healthy = c.nodes[nodeID]
		view.LastHeartbeat = "由 coordinator 管理"
		view.ReplicaMaps = append(view.ReplicaMaps, mapID)
		viewsByID[nodeID] = view
	}

	nodeIDs := make([]string, 0, len(viewsByID))
	for nodeID := range viewsByID {
		nodeIDs = append(nodeIDs, nodeID)
	}
	sort.Strings(nodeIDs)
	views := make([]protocol.NodeView, 0, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		view := viewsByID[nodeID]
		sort.Strings(view.PrimaryMaps)
		sort.Strings(view.ReplicaMaps)
		views = append(views, view)
	}
	return views
}

// GatewayStatus 返回已提交拓扑和 Gateway 数据面连接的结构化只读视图。
func (c *Cluster) GatewayStatus() protocol.GatewayStatus {
	c.mu.RLock()
	defer c.mu.RUnlock()

	status := protocol.GatewayStatus{
		RoutingReady:    c.topologyLoaded,
		TopologyVersion: c.topology.Version,
	}
	if !status.RoutingReady {
		status.Summary = "网关路由尚未就绪：等待 coordinator 提交 Topology"
		return status
	}

	status.Nodes = c.gatewayNodeViewsLocked()
	lines := []string{fmt.Sprintf("网关路由状态：Topology Version=%d", status.TopologyVersion)}
	for _, view := range status.Nodes {
		nodeStatus := "等待节点注册"
		if view.Healthy {
			nodeStatus = "数据面已连接"
		}
		lines = append(lines, fmt.Sprintf("- %s %s 主分片=%v 副本=%v", view.ID, nodeStatus, view.PrimaryMaps, view.ReplicaMaps))
	}
	status.Summary = strings.Join(lines, "\n")
	return status
}
