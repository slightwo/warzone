package coordinator

import (
	"errors"
	"fmt"
	"log"
	"sort"
	"time"

	"battleworld/cluster"
	"battleworld/storage"
	"battleworld/world"
)

// failoverNode 逐张地图处理失效节点承载的地图。单图失败不会阻塞其他地图，避免因一个
// standby 或 checkpoint 缺失而扩大故障域。
func (c *Coordinator) failoverNode(failedNodeID string) {
	if _, leader := c.currentLeaderTerm(); !leader {
		return
	}
	topology, found, err := c.store.LoadTopology()
	if err != nil {
		log.Printf("[coordinator/failover] 读取拓扑失败，节点 %s 暂不切换: %v", failedNodeID, err)
		return
	}
	if !found || topology == nil {
		return
	}

	mapIDs := make([]string, 0)
	for mapID, ownerID := range topology.Owners {
		if ownerID == failedNodeID {
			mapIDs = append(mapIDs, mapID)
		}
	}
	sort.Strings(mapIDs)
	for _, mapID := range mapIDs {
		if err := c.failoverMap(mapID, failedNodeID); err != nil {
			log.Printf("[coordinator/failover] 地图 %s 从节点 %s 切换失败: %v", mapID, failedNodeID, err)
		}
	}
}

// failoverMap 是固定的单图切换状态机：
//
// load topology(E) -> select healthy same-map standby -> validate checkpoint -> Promote(E+1)
// -> topology+fence CAS -> repair affected sessions -> publish event.
//
// standby 不再持久化在 Topology：它由活跃注册表中声明相同 MapID、但当前未持有该地图的
// 健康节点动态推导。这样 owner 变更不会留下过期副本位，下一次切换仍能在其余候选中选择。
// Promote 仅准备内存地图，不授予写权限；只有 CAS 成功写入 topology/fence 后，新 owner
// 通过自身 topology refresh 得到 E+1 主权，旧 owner 则被 Redis fence 拒绝。
func (c *Coordinator) failoverMap(mapID, failedNodeID string) error {
	term, leader := c.currentLeaderTerm()
	if !leader {
		return errors.New("当前 coordinator 不是 leader")
	}
	topology, found, err := c.store.LoadTopology()
	if err != nil {
		return fmt.Errorf("读取当前拓扑: %w", err)
	}
	if !found || topology == nil {
		return errors.New("当前没有已提交拓扑")
	}
	if topology.Owners[mapID] != failedNodeID {
		return nil
	}
	oldEpoch := topology.MapEpochs[mapID]
	if oldEpoch == 0 {
		return fmt.Errorf("地图 %q 没有有效 epoch", mapID)
	}

	standbyID, standby, err := c.selectStandby(mapID, failedNodeID)
	if err != nil {
		return err
	}
	checkpoint, checkpointOK := c.store.LoadCheckpoint(mapID)
	if !checkpointOK || checkpoint == nil || checkpoint.MapID != mapID || checkpoint.NodeID != failedNodeID || checkpoint.MapEpoch != oldEpoch || checkpoint.Version <= 0 {
		return fmt.Errorf("standby 节点 %q 可提升的 checkpoint 不可用: checkpoint=%+v", standbyID, checkpoint)
	}

	mapConfig, ok := c.mapConfig(mapID)
	if !ok {
		return fmt.Errorf("地图 %q 不在 coordinator 配置中", mapID)
	}
	nextEpoch := oldEpoch + 1
	if err := standby.Promote(mapID, mapConfig, *checkpoint, nextEpoch); err != nil {
		return fmt.Errorf("提升 standby 节点 %q 到 epoch %d: %w", standbyID, nextEpoch, err)
	}

	next := topology.Clone()
	next.Version = topology.Version + 1
	next.Owners[mapID] = standbyID
	next.MapEpochs[mapID] = nextEpoch
	next.LeaderTerm = term
	next.UpdatedAt = time.Now().UTC()
	// Promote 期间 leader lease 可能已丢失；CAS 前必须再次确认 term 仍归本实例。
	if !c.hasLeaderTerm(term) {
		return errors.New("Promote 后已失去 coordinator leader lease")
	}
	if err := c.store.CompareAndSaveTopologyAndMigrateSessionsForLeader(topology.Version, next, term, mapID, failedNodeID, standbyID); err != nil {
		if errors.Is(err, storage.ErrTopologyVersionConflict) {
			return fmt.Errorf("%w: topology changed after promote; stop and re-evaluate", err)
		}
		return fmt.Errorf("原子提交 topology/fence/session: %w", err)
	}
	if err := c.store.PublishTopologyChanged(next.Version); err != nil {
		return fmt.Errorf("topology 已提交但发布通知失败: %w", err)
	}
	log.Printf("[coordinator/failover] 地图 %s 已从 %s 切换至 %s，epoch %d -> %d，topology version=%d，leader term=%d", mapID, failedNodeID, standbyID, oldEpoch, nextEpoch, next.Version, term)
	return nil
}

// selectStandby 仅从仍持有注册租约、声明同一地图、未 drain 且控制面健康的节点中选择
// 候选。按节点 ID 排序保证故障转移在相同候选集下具有确定性。
func (c *Coordinator) selectStandby(mapID, excludedNodeID string) (string, cluster.CoordinatorNodeClient, error) {
	registrations, err := c.store.GetActiveNodes()
	if err != nil {
		return "", nil, fmt.Errorf("读取地图 %q 的 standby 候选: %w", mapID, err)
	}

	candidateIDs := make([]string, 0)
	seen := make(map[string]struct{})
	for _, registration := range registrations {
		if registration.ID == excludedNodeID || registration.Draining || registration.MapID != mapID {
			continue
		}
		if _, duplicate := seen[registration.ID]; duplicate {
			continue
		}
		if _, healthy := c.healthyNode(registration.ID); !healthy {
			continue
		}
		seen[registration.ID] = struct{}{}
		candidateIDs = append(candidateIDs, registration.ID)
	}
	sort.Strings(candidateIDs)
	if len(candidateIDs) == 0 {
		return "", nil, fmt.Errorf("地图 %q 没有可用 standby 节点", mapID)
	}

	standbyID := candidateIDs[0]
	standby, _ := c.healthyNode(standbyID)
	return standbyID, standby, nil
}

func (c *Coordinator) healthyNode(nodeID string) (cluster.CoordinatorNodeClient, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	connection, ok := c.nodes[nodeID]
	if !ok || !connection.healthy {
		return nil, false
	}
	return connection.client, true
}

func (c *Coordinator) mapConfig(mapID string) (world.MapConfig, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	mapConfig, ok := c.configs[mapID]
	return mapConfig, ok
}
