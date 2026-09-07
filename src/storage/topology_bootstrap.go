package storage

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

// BuildInitialTopology 根据节点声明的候选能力构造首个拓扑快照。ready=false 表示
// 候选节点尚未齐全；重复候选、未知地图或同节点主备等配置错误会返回 error，不能通过
// 任意选择最后一个节点来掩盖配置问题。
func BuildInitialTopology(knownMapIDs map[string]struct{}, nodes []NodeRegistryInfo, now time.Time) (topology Topology, ready bool, err error) {
	mapIDs := make([]string, 0, len(knownMapIDs))
	for mapID := range knownMapIDs {
		mapIDs = append(mapIDs, mapID)
	}
	sort.Strings(mapIDs)
	if len(mapIDs) == 0 {
		return Topology{}, false, errors.New("没有可用于 bootstrap 的地图配置")
	}

	nodes = append([]NodeRegistryInfo(nil), nodes...)
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
	owners := make(map[string]string, len(mapIDs))
	replicas := make(map[string]string, len(mapIDs))
	seenNodes := make(map[string]struct{}, len(nodes))
	for _, node := range nodes {
		if strings.TrimSpace(node.ID) == "" || node.ID != strings.TrimSpace(node.ID) {
			return Topology{}, false, fmt.Errorf("节点注册包含非法 ID %q", node.ID)
		}
		if strings.TrimSpace(node.Addr) == "" || node.Addr != strings.TrimSpace(node.Addr) {
			return Topology{}, false, fmt.Errorf("节点 %q 包含非法地址 %q", node.ID, node.Addr)
		}
		if _, exists := seenNodes[node.ID]; exists {
			return Topology{}, false, fmt.Errorf("节点 %q 存在重复注册", node.ID)
		}
		seenNodes[node.ID] = struct{}{}

		if err := addTopologyCandidates(owners, node.ID, node.Maps, knownMapIDs, "主"); err != nil {
			return Topology{}, false, err
		}
		if err := addTopologyCandidates(replicas, node.ID, node.Replicas, knownMapIDs, "副本"); err != nil {
			return Topology{}, false, err
		}
	}

	mapEpochs := make(map[string]uint64, len(mapIDs))
	for _, mapID := range mapIDs {
		ownerID, hasOwner := owners[mapID]
		replicaID, hasReplica := replicas[mapID]
		if !hasOwner || !hasReplica {
			return Topology{}, false, nil
		}
		if ownerID == replicaID {
			return Topology{}, false, fmt.Errorf("地图 %q 的主节点和副本节点均为 %q", mapID, ownerID)
		}
		mapEpochs[mapID] = 1
	}

	topology = Topology{
		Version:    1,
		LeaderTerm: 0,
		Owners:     owners,
		Replicas:   replicas,
		MapEpochs:  mapEpochs,
		UpdatedAt:  now.UTC(),
	}
	if err := topology.Validate(knownMapIDs); err != nil {
		return Topology{}, false, err
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
