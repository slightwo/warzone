package coordinator

import (
	"log"

	"battleworld/storage"
)

// reconcileDrainingNodes 检查所有仍在已提交 Topology 中承载 owner 地图、但已在 Redis
// 注册表里标记为 Draining 的节点，逐节点触发受控迁移。
//
// 它复用 failoverNode 的状态机：只切换仍由 draining 节点拥有的地图，并要求同图
// standby 健康、checkpoint 有效；任何一步失败会保留在拓扑中等待下一轮重试，因此一次 reconcile 可
// 安全地并发触发多张地图。draining 节点已经拒绝新写入并保存了 fenced checkpoint，
// 所以触发迁移不会和节点自身的最终落盘竞争。
func (c *Coordinator) reconcileDrainingNodes(activeNodes []storage.NodeRegistryInfo) {
	if _, leader := c.currentLeaderTerm(); !leader {
		return
	}
	topology, found, err := c.store.LoadTopology()
	if err != nil || !found || topology == nil {
		return
	}
	drainingOwners := make(map[string]struct{}, len(activeNodes))
	for _, info := range activeNodes {
		if !info.Draining {
			continue
		}
		drainingOwners[info.ID] = struct{}{}
	}
	if len(drainingOwners) == 0 {
		return
	}
	for _, ownerID := range topology.Owners {
		if _, draining := drainingOwners[ownerID]; draining {
			log.Printf("[coordinator/drain] 节点 %s 已标记 draining，触发受控 owner 迁移", ownerID)
			go c.failoverNode(ownerID)
		}
	}
}
