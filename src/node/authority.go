package node

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"battleworld/storage"
)

var (
	// ErrTopologyAuthorityUnavailable 表示节点没有一份仍在宽限期内的已提交拓扑，必须
	// fail-closed，不能继续处理地图写入、tick 或保存 checkpoint。
	ErrTopologyAuthorityUnavailable = errors.New("topology authority unavailable")
	// ErrMapAuthorityDenied 表示当前节点不是请求地图在指定 epoch 下的 owner。
	ErrMapAuthorityDenied = errors.New("map authority denied")
	// ErrMapPromotionDenied 表示当前节点不是可被提升为下一 epoch owner 的健康副本。
	ErrMapPromotionDenied = errors.New("map promotion denied")
)

// MapAuthority 是节点处理地图状态写入所需的最小主权上下文。Owner 和 Epoch 均来自
// 已提交的 Topology，节点自身的启动参数只代表可实例化能力，不能代替该上下文。
type MapAuthority struct {
	MapID string
	Owner string
	Epoch uint64
}

type topologyLoader interface {
	LoadTopology() (*storage.Topology, bool, error)
}

// authorityCache 保存节点侧最近一份已提交拓扑。刷新暂时失败时可在有限宽限期内继续使用
// 最后成功快照；超过宽限期后所有写入都会 fail-closed。
type authorityCache struct {
	mu         sync.RWMutex
	nodeID     string
	grace      time.Duration
	now        func() time.Time
	topology   storage.Topology
	loadedAt   time.Time
	topologyOK bool
}

func newAuthorityCache(nodeID string, grace time.Duration) *authorityCache {
	return &authorityCache{
		nodeID: nodeID,
		grace:  grace,
		now:    time.Now,
	}
}

func (c *authorityCache) refresh(loader topologyLoader) error {
	if loader == nil {
		return fmt.Errorf("%w: topology loader is nil", ErrTopologyAuthorityUnavailable)
	}
	topology, found, err := loader.LoadTopology()
	if err != nil {
		return fmt.Errorf("load topology: %w", err)
	}
	if !found {
		return fmt.Errorf("%w: topology has not been committed", ErrTopologyAuthorityUnavailable)
	}
	if topology == nil {
		return fmt.Errorf("%w: topology is nil", ErrTopologyAuthorityUnavailable)
	}
	if err := topology.Validate(nil); err != nil {
		return fmt.Errorf("validate topology: %w", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.topologyOK && topology.Version < c.topology.Version {
		return fmt.Errorf("received stale topology version %d after %d", topology.Version, c.topology.Version)
	}
	c.topology = topology.Clone()
	c.loadedAt = c.now().UTC()
	c.topologyOK = true
	return nil
}

func (c *authorityCache) freshLocked() bool {
	if !c.topologyOK || c.loadedAt.IsZero() {
		return false
	}
	return c.now().UTC().Sub(c.loadedAt) <= c.grace
}

// Require 校验 nodeID、地图 owner 和 epoch 同时匹配，且拓扑缓存仍处于可用宽限期。
func (c *authorityCache) Require(mapID string, epoch uint64) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if !c.freshLocked() {
		return fmt.Errorf("%w: last topology refresh=%s", ErrTopologyAuthorityUnavailable, c.loadedAt.Format(time.RFC3339Nano))
	}
	owner := c.topology.Owners[mapID]
	currentEpoch := c.topology.MapEpochs[mapID]
	if owner != c.nodeID || currentEpoch == 0 || currentEpoch != epoch {
		return fmt.Errorf("%w: map=%q requested_epoch=%d owner=%q current_epoch=%d local_node=%q", ErrMapAuthorityDenied, mapID, epoch, owner, currentEpoch, c.nodeID)
	}
	return nil
}

// Current 返回本节点当前可用于自治 tick/checkpoint 的地图主权。仅返回仍在宽限期内且
// owner 为本节点的条目。
func (c *authorityCache) Current(mapID string) (MapAuthority, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if !c.freshLocked() {
		return MapAuthority{}, false
	}
	authority := MapAuthority{
		MapID: mapID,
		Owner: c.topology.Owners[mapID],
		Epoch: c.topology.MapEpochs[mapID],
	}
	if authority.Owner != c.nodeID || authority.Epoch == 0 {
		return MapAuthority{}, false
	}
	return authority, true
}

// RequirePromotionCandidate 在 topology 尚未提交前允许 coordinator 对当前副本执行
// 准备提升。它不会授予写权限；实际 tick 和玩家写入仍须等新 Topology 提交后经 Require
// 成功校验。
func (c *authorityCache) RequirePromotionCandidate(mapID string, targetEpoch uint64) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if !c.freshLocked() {
		return fmt.Errorf("%w: last topology refresh=%s", ErrTopologyAuthorityUnavailable, c.loadedAt.Format(time.RFC3339Nano))
	}
	if c.topology.Replicas[mapID] != c.nodeID || c.topology.MapEpochs[mapID] == 0 || targetEpoch != c.topology.MapEpochs[mapID]+1 {
		return fmt.Errorf("%w: map=%q target_epoch=%d replica=%q current_epoch=%d local_node=%q", ErrMapPromotionDenied, mapID, targetEpoch, c.topology.Replicas[mapID], c.topology.MapEpochs[mapID], c.nodeID)
	}
	return nil
}
