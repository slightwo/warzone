package storage

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

const (
	// TopologyRedisKey 以单份 JSON 文档保存完整的版本化路由拓扑。
	TopologyRedisKey = "battle:topology"
	// TopologyEventChannel 用于通知消费者存在可加载的新拓扑版本。
	TopologyEventChannel = "events:topology"
	// MapFenceRedisKeyPrefix 保存地图当前 owner 和 epoch 的原子写入 fence。
	MapFenceRedisKeyPrefix = "battle:map:fence:"
	// CoordinatorLeaderTermRedisKey 是 Redis 选主成功时原子递增的持久化任期计数器。
	CoordinatorLeaderTermRedisKey = "battle:coordinator:leader:term"
)

var (
	// ErrTopologyVersionConflict 表示 CAS 提交完成前拓扑版本已发生变化。
	ErrTopologyVersionConflict = errors.New("topology version conflict")
	// ErrTopologyCorrupt 表示 Redis 中持久化的拓扑数据已损坏，不能继续信任。
	ErrTopologyCorrupt = errors.New("stored topology is corrupt")
	// ErrMapFenceRejected 表示 checkpoint 写入方不再匹配当前地图的 owner/epoch fence。
	ErrMapFenceRejected = errors.New("map fence rejected")
	// ErrGlobalSessionCorrupt 表示迁移时发现持久化会话不是合法 JSON，不能提交半完成的切换。
	ErrGlobalSessionCorrupt = errors.New("stored global session is corrupt")
	// ErrMapEpochInvariant 表示 owner/epoch 演进违反 fencing 不变量。
	ErrMapEpochInvariant = errors.New("map epoch invariant violated")
	// ErrLeaderTermInvariant 表示控制面试图以较旧的 leader term 覆盖已提交拓扑。
	ErrLeaderTermInvariant = errors.New("leader term invariant violated")
)

// Topology 是地图路由和所有权 fencing 的唯一权威数据源。
//
// 节点通过 NodeRegistryInfo 只声明一个地图候选。Coordinator 在候选节点间选择 owner；
// 未当选的健康候选节点自然构成该地图的 standby 集合，不再作为拓扑中的独立角色持久化。
// MapEpochs 是每张地图所有权变更时递增的 fencing token。
type Topology struct {
	Version    uint64            `json:"version"`
	LeaderTerm uint64            `json:"leader_term"`
	Owners     map[string]string `json:"owners"`
	MapEpochs  map[string]uint64 `json:"map_epochs"`
	UpdatedAt  time.Time         `json:"updated_at"`
}

// Clone 返回深拷贝，避免调用方修改已缓存或已持久化的拓扑数据。
func (t Topology) Clone() Topology {
	return Topology{
		Version:    t.Version,
		LeaderTerm: t.LeaderTerm,
		Owners:     cloneStringMap(t.Owners),
		MapEpochs:  cloneUint64Map(t.MapEpochs),
		UpdatedAt:  t.UpdatedAt,
	}
}

// Normalize 初始化可选 map，并将 UpdatedAt 统一转换为 UTC。该方法不会填充
// UpdatedAt，也不会改写地图或节点 ID：提交时间应由控制面决定，静默裁剪标识符则可能
// 合并本应不同的路由条目。
func (t *Topology) Normalize() {
	if t.Owners == nil {
		t.Owners = make(map[string]string)
	}
	if t.MapEpochs == nil {
		t.MapEpochs = make(map[string]uint64)
	}
	t.UpdatedAt = t.UpdatedAt.UTC()
}

// Validate 校验拓扑的结构性不变量。knownMapIDs 非 nil 时，每张已路由地图必须
// 属于该集合。存储层校验可传入 nil，因为地图配置不属于 storage 包的职责范围。
func (t Topology) Validate(knownMapIDs map[string]struct{}) error {
	if t.Version == 0 {
		return errors.New("topology version must be greater than zero")
	}
	if t.UpdatedAt.IsZero() {
		return errors.New("topology updated_at must be set")
	}

	mapIDs := make(map[string]struct{}, len(t.Owners)+len(t.MapEpochs))
	for mapID := range t.Owners {
		mapIDs[mapID] = struct{}{}
	}
	for mapID := range t.MapEpochs {
		mapIDs[mapID] = struct{}{}
	}

	sortedMapIDs := make([]string, 0, len(mapIDs))
	for mapID := range mapIDs {
		sortedMapIDs = append(sortedMapIDs, mapID)
	}
	sort.Strings(sortedMapIDs)

	for _, mapID := range sortedMapIDs {
		if strings.TrimSpace(mapID) == "" || mapID != strings.TrimSpace(mapID) {
			return fmt.Errorf("invalid topology map id %q", mapID)
		}
		if knownMapIDs != nil {
			if _, ok := knownMapIDs[mapID]; !ok {
				return fmt.Errorf("topology references unknown map %q", mapID)
			}
		}

		owner, ownerExists := t.Owners[mapID]
		if ownerExists && owner != "" && (strings.TrimSpace(owner) == "" || owner != strings.TrimSpace(owner)) {
			return fmt.Errorf("map %q has invalid owner %q", mapID, owner)
		}

		epoch, epochExists := t.MapEpochs[mapID]
		if !epochExists || epoch == 0 {
			return fmt.Errorf("map %q must have a positive ownership epoch", mapID)
		}
	}
	return nil
}

func cloneStringMap(source map[string]string) map[string]string {
	if source == nil {
		return make(map[string]string)
	}
	cloned := make(map[string]string, len(source))
	for key, value := range source {
		cloned[key] = value
	}
	return cloned
}

// MapFenceRedisKey 返回单张地图 fence 的 Redis key。
func MapFenceRedisKey(mapID string) string {
	return MapFenceRedisKeyPrefix + mapID
}

func cloneUint64Map(source map[string]uint64) map[string]uint64 {
	if source == nil {
		return make(map[string]uint64)
	}
	cloned := make(map[string]uint64, len(source))
	for key, value := range source {
		cloned[key] = value
	}
	return cloned
}

func prepareTopology(topology Topology) (Topology, error) {
	prepared := topology.Clone()
	prepared.Normalize()
	if err := prepared.Validate(nil); err != nil {
		return Topology{}, err
	}
	return prepared, nil
}
