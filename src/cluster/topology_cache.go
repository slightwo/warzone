package cluster

import (
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"battleworld/storage"
	"battleworld/world"
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
	configs := make(map[string]world.MapConfig, len(c.configs))
	for mapID, config := range c.configs {
		configs[mapID] = config
	}
	c.mu.RUnlock()

	initial, ready, err := buildInitialTopology(configs, nodes, time.Now().UTC())
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

// buildInitialTopology 根据声明能力构造初始拓扑。ready=false 表示节点尚未全部注册；
// 重复候选、未知地图或同节点主备等配置错误会返回 error，而不会任意选择最后一个节点。
func buildInitialTopology(configs map[string]world.MapConfig, nodes []storage.NodeRegistryInfo, now time.Time) (topology storage.Topology, ready bool, err error) {
	mapIDs := make([]string, 0, len(configs))
	for mapID := range configs {
		mapIDs = append(mapIDs, mapID)
	}
	sort.Strings(mapIDs)
	if len(mapIDs) == 0 {
		return storage.Topology{}, false, errors.New("没有可用于 bootstrap 的地图配置")
	}

	nodes = append([]storage.NodeRegistryInfo(nil), nodes...)
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
	knownMapIDs := make(map[string]struct{}, len(mapIDs))
	for _, mapID := range mapIDs {
		knownMapIDs[mapID] = struct{}{}
	}

	owners := make(map[string]string, len(mapIDs))
	replicas := make(map[string]string, len(mapIDs))
	seenNodes := make(map[string]struct{}, len(nodes))
	for _, node := range nodes {
		if strings.TrimSpace(node.ID) == "" || node.ID != strings.TrimSpace(node.ID) {
			return storage.Topology{}, false, fmt.Errorf("节点注册包含非法 ID %q", node.ID)
		}
		if strings.TrimSpace(node.Addr) == "" || node.Addr != strings.TrimSpace(node.Addr) {
			return storage.Topology{}, false, fmt.Errorf("节点 %q 包含非法地址 %q", node.ID, node.Addr)
		}
		if _, exists := seenNodes[node.ID]; exists {
			return storage.Topology{}, false, fmt.Errorf("节点 %q 存在重复注册", node.ID)
		}
		seenNodes[node.ID] = struct{}{}

		if err := addTopologyCandidates(owners, node.ID, node.Maps, knownMapIDs, "主"); err != nil {
			return storage.Topology{}, false, err
		}
		if err := addTopologyCandidates(replicas, node.ID, node.Replicas, knownMapIDs, "副本"); err != nil {
			return storage.Topology{}, false, err
		}
	}

	mapEpochs := make(map[string]uint64, len(mapIDs))
	for _, mapID := range mapIDs {
		ownerID, hasOwner := owners[mapID]
		replicaID, hasReplica := replicas[mapID]
		if !hasOwner || !hasReplica {
			return storage.Topology{}, false, nil
		}
		if ownerID == replicaID {
			return storage.Topology{}, false, fmt.Errorf("地图 %q 的主节点和副本节点均为 %q", mapID, ownerID)
		}
		mapEpochs[mapID] = 1
	}

	topology = storage.Topology{
		Version:    1,
		LeaderTerm: 0,
		Owners:     owners,
		Replicas:   replicas,
		MapEpochs:  mapEpochs,
		UpdatedAt:  now.UTC(),
	}
	if err := topology.Validate(knownMapIDs); err != nil {
		return storage.Topology{}, false, err
	}
	return topology, true, nil
}

func addTopologyCandidates(target map[string]string, nodeID string, declaredMapIDs []string, knownMapIDs map[string]struct{}, role string) error {
	seenMaps := make(map[string]struct{}, len(declaredMapIDs))
	for _, mapID := range declaredMapIDs {
		if strings.TrimSpace(mapID) == "" || mapID != strings.TrimSpace(mapID) {
			return fmt.Errorf("节点 %q 声明了非法%s地图 ID %q", nodeID, role, mapID)
		}
		if _, known := knownMapIDs[mapID]; !known {
			return fmt.Errorf("节点 %q 声明了未知%s地图 %q", nodeID, role, mapID)
		}
		if _, duplicate := seenMaps[mapID]; duplicate {
			return fmt.Errorf("节点 %q 重复声明%s地图 %q", nodeID, role, mapID)
		}
		seenMaps[mapID] = struct{}{}
		if existingNodeID, exists := target[mapID]; exists {
			return fmt.Errorf("地图 %q 的%s候选节点冲突: %q 与 %q", mapID, role, existingNodeID, nodeID)
		}
		target[mapID] = nodeID
	}
	return nil
}
