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
// 副本快照缺失而扩大故障域。
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
// load topology(E) -> validate owner/healthy replica/checkpoint -> Promote(E+1)
// -> topology+fence CAS（旧 owner 不复用为副本）-> repair affected sessions -> publish event.

//
// Promote 仅准备副本内存地图，不授予写权限；只有 CAS 成功写入 topology/fence 后，新
// owner 通过自己的 topology refresh 得到 E+1 主权，旧 owner 则被 Redis fence 拒绝。
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
	replicaID := topology.Replicas[mapID]
	if replicaID == "" || replicaID == failedNodeID {
		return fmt.Errorf("地图 %q 没有可用副本", mapID)
	}

	replica, ok := c.healthyNode(replicaID)
	if !ok {
		return fmt.Errorf("副本节点 %q 当前不健康", replicaID)
	}
	checkpoint, checkpointOK := c.store.LoadCheckpoint(mapID)
	if !checkpointOK || checkpoint == nil || checkpoint.MapID != mapID || checkpoint.NodeID != failedNodeID || checkpoint.MapEpoch != oldEpoch || checkpoint.Version <= 0 {
		return fmt.Errorf("副本 %q 可提升的 checkpoint 不可用: checkpoint=%+v", replicaID, checkpoint)
	}

	mapConfig, ok := c.mapConfig(mapID)
	if !ok {
		return fmt.Errorf("地图 %q 不在 coordinator 配置中", mapID)
	}
	nextEpoch := oldEpoch + 1
	if err := replica.Promote(mapID, mapConfig, *checkpoint, nextEpoch); err != nil {
		return fmt.Errorf("提升副本 %q 到 epoch %d: %w", replicaID, nextEpoch, err)
	}

	next := topology.Clone()
	next.Version = topology.Version + 1
	next.Owners[mapID] = replicaID
	// 旧 owner 要么已失效，要么正在 drain，均不能被标记为新的 warm standby。
	// 当前一主一备模型尚未实现自动补副本，故切换后显式留下空副本位，等待后续
	// 副本重建流程补齐，而不能伪造一个不可用的 replica。
	next.Replicas[mapID] = ""
	next.MapEpochs[mapID] = nextEpoch
	next.LeaderTerm = term
	next.UpdatedAt = time.Now().UTC()
	// Promote 期间 leader lease 可能已丢失；CAS 前必须再次确认 term 仍归本实例。
	if !c.hasLeaderTerm(term) {
		return errors.New("Promote 后已失去 coordinator leader lease")
	}
	if err := c.store.CompareAndSaveTopologyAndMigrateSessionsForLeader(topology.Version, next, term, mapID, failedNodeID, replicaID); err != nil {
		if errors.Is(err, storage.ErrTopologyVersionConflict) {
			return fmt.Errorf("%w: topology changed after promote; stop and re-evaluate", err)
		}
		return fmt.Errorf("原子提交 topology/fence/session: %w", err)
	}
	if err := c.store.PublishTopologyChanged(next.Version); err != nil {
		return fmt.Errorf("topology 已提交但发布通知失败: %w", err)
	}
	log.Printf("[coordinator/failover] 地图 %s 已从 %s 切换至 %s，epoch %d -> %d，topology version=%d，leader term=%d", mapID, failedNodeID, replicaID, oldEpoch, nextEpoch, next.Version, term)
	return nil
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
