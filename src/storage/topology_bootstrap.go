package storage

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

const minimumMapCandidates = 2

// BuildInitialTopology 根据节点声明的单一地图候选构造首个拓扑快照。每张地图必须有至少
// 两个健康候选节点：按节点 ID 稳定选出 owner，其余节点作为 standby。ready=false 表示
// 候选节点尚未齐全；非法或重复注册会返回 error。
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
	candidates := make(map[string][]string, len(mapIDs))
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

		mapID := node.MapID
		if strings.TrimSpace(mapID) == "" || mapID != strings.TrimSpace(mapID) {
			return Topology{}, false, fmt.Errorf("节点 %q 声明了非法地图 ID %q", node.ID, mapID)
		}
		if _, known := knownMapIDs[mapID]; !known {
			return Topology{}, false, fmt.Errorf("节点 %q 声明了未知地图 %q", node.ID, mapID)
		}
		candidates[mapID] = append(candidates[mapID], node.ID)
	}

	owners := make(map[string]string, len(mapIDs))
	mapEpochs := make(map[string]uint64, len(mapIDs))
	for _, mapID := range mapIDs {
		declared := candidates[mapID]
		if len(declared) < minimumMapCandidates {
			return Topology{}, false, nil
		}
		// nodes 已按 ID 排序，因此每个 map 的候选顺序也是稳定的；仍显式排序以维持
		// 该函数在调用方更改遍历方式后的确定性。
		sort.Strings(declared)
		owners[mapID] = declared[0]
		mapEpochs[mapID] = 1
	}

	topology = Topology{
		Version:    1,
		LeaderTerm: 0,
		Owners:     owners,
		MapEpochs:  mapEpochs,
		UpdatedAt:  now.UTC(),
	}
	if err := topology.Validate(knownMapIDs); err != nil {
		return Topology{}, false, err
	}
	return topology, true, nil
}
