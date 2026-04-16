package node

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"sync"
	"time"

	"battleworld/protocol"
	"battleworld/world"
)

type NodeService struct {
	mu               sync.RWMutex
	ID               string
	Addr             string
	healthy          bool
	lastHeartbeat    time.Time
	maps             map[string]*world.World
	replicaSnapshots map[string]protocol.MapCheckpoint
	ln               net.Listener
}

func NewNodeService(id, addr string) *NodeService {
	return &NodeService{
		ID:               id,
		Addr:             addr,
		healthy:          true,
		lastHeartbeat:    time.Now(),
		maps:             make(map[string]*world.World),
		replicaSnapshots: make(map[string]protocol.MapCheckpoint),
	}
}

func (n *NodeService) Start() error {
	n.mu.Lock()
	if n.ln != nil {
		n.mu.RUnlock()
		return nil
	}

	n.lastHeartbeat = time.Now()
	n.healthy = true
	n.mu.Unlock()

	return nil
}

func (n *NodeService) Stop() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.ln = nil
	n.healthy = false
	return nil
}

func (n *NodeService) RemoveHostedMap(mapID string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.maps, mapID)
}

func (n *NodeService) InstallPrimaryMap(cfg world.MapConfig) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if _, ok := n.maps[cfg.ID]; ok {
		return
	}
	n.maps[cfg.ID] = world.NewWorld(cfg)
}

func (n *NodeService) RestorePrimaryMap(cfg world.MapConfig, cp protocol.MapCheckpoint) {
	n.mu.Lock()
	defer n.mu.Unlock()

	instance, ok := n.maps[cfg.ID]
	if !ok {
		instance = world.NewWorld(cfg)
		n.maps[cfg.ID] = instance
	}
	instance.RestoreCheckpoint(cp)
}

func (n *NodeService) AddPlayer(ctx context.Context, mapID string, profile *protocol.UserProfile) error {
	n.mu.RLock()
	instance := n.maps[mapID]
	n.mu.RUnlock()
	if instance == nil {
		return errors.New("add player：没有找到源节点")
	}
	instance.AddOrRestorePlayer(profile)
	return nil
}

func (n *NodeService) RemovePlayer(ctx context.Context, mapID, username string) (protocol.UserProfile, bool, error) {
	n.mu.RLock()
	instance := n.maps[mapID]
	n.mu.RUnlock()
	if instance == nil {
		return protocol.UserProfile{}, false, nil
	}
	profile, ok := instance.RemovePlayer(username)
	return profile, ok, nil
}

func (n *NodeService) MovePlayer(ctx context.Context, mapID, username, dir string) (string, protocol.UserProfile, bool, error) {
	n.mu.RLock()
	instance := n.maps[mapID]
	n.mu.RUnlock()
	if instance == nil {
		return "", protocol.UserProfile{}, false, nil
	}
	text, profile, ok := instance.MovePlayer(username, dir)
	return text, profile, ok, nil
}

func (n *NodeService) Attack(ctx context.Context, mapID, username string) (string, string, string, protocol.UserProfile, bool, error) {
	n.mu.RLock()
	instance := n.maps[mapID]
	n.mu.RUnlock()
	if instance == nil {
		return "", "", "", protocol.UserProfile{}, false, nil
	}
	log1, log2, gmLog, profile, ok := instance.Attack(username)
	return log1, log2, gmLog, profile, ok, nil
}

func (n *NodeService) Heal(ctx context.Context, mapID, username string) (string, protocol.UserProfile, bool, error) {
	n.mu.RLock()
	instance := n.maps[mapID]
	n.mu.RUnlock()
	if instance == nil {
		return "", protocol.UserProfile{}, false, nil
	}
	text, profile, ok := instance.HealPlayer(username)
	return text, profile, ok, nil
}

func (n *NodeService) BuyItem(ctx context.Context, mapID, username, item string) (string, protocol.UserProfile, bool, error) {
	n.mu.RLock()
	instance := n.maps[mapID]
	n.mu.RUnlock()
	if instance == nil {
		return "", protocol.UserProfile{}, false, nil
	}
	text, profile, ok := instance.BuyItem(username, item)
	return text, profile, ok, nil
}

func (n *NodeService) Profile(ctx context.Context, mapID, username string) (protocol.UserProfile, bool, error) {
	n.mu.RLock()
	instance := n.maps[mapID]
	n.mu.RUnlock()
	if instance == nil {
		return protocol.UserProfile{}, false, nil
	}
	profile, ok := instance.ProfileOf(username)
	return profile, ok, nil
}

func (n *NodeService) RewardPlayer(ctx context.Context, mapID, username string, treasureDelta, victoryDelta int) (protocol.UserProfile, bool, error) {
	n.mu.RLock()
	instance := n.maps[mapID]
	n.mu.RUnlock()
	if instance == nil {
		return protocol.UserProfile{}, false, nil
	}
	profile, ok := instance.RewardPlayer(username, treasureDelta, victoryDelta)
	return profile, ok, nil
}

func (n *NodeService) Snapshot(ctx context.Context, mapID string) (protocol.MapView, error) {
	n.mu.RLock()
	instance := n.maps[mapID]
	n.mu.RUnlock()
	if instance == nil {
		return protocol.MapView{}, fmt.Errorf("地图 %q 当前不在节点 %s 上", mapID, n.ID)
	}
	return instance.Snapshot(n.ID), nil
}

func (n *NodeService) Counts(ctx context.Context, mapID string) (int, int, int, int64, error) {
	n.mu.RLock()
	instance := n.maps[mapID]
	n.mu.RUnlock()
	if instance == nil {
		return 0, 0, 0, 0, fmt.Errorf("地图 %q 当前不在节点 %s 上", mapID, n.ID)
	}
	players, npcs, treasures, version := instance.Counts()
	return players, npcs, treasures, version, nil
}

func (n *NodeService) Checkpoint(ctx context.Context, mapID string) (protocol.MapCheckpoint, error) {
	n.mu.RLock()
	instance := n.maps[mapID]
	n.mu.RUnlock()
	if instance == nil {
		return protocol.MapCheckpoint{}, fmt.Errorf("地图 %q 当前不在节点 %s 上", mapID, n.ID)
	}
	return instance.CaptureCheckpoint(n.ID), nil
}

func (n *NodeService) BackgroundStep() []protocol.MapEvents {
	n.mu.RLock()
	mapIDs := make([]string, 0, len(n.maps))
	for mapID := range n.maps {
		mapIDs = append(mapIDs, mapID)
	}
	sort.Strings(mapIDs)
	instances := make([]*world.World, 0, len(mapIDs))
	for _, mapID := range mapIDs {
		instances = append(instances, n.maps[mapID])
	}
	n.mu.RUnlock()

	results := make([]protocol.MapEvents, 0, len(instances))
	for i, instance := range instances {
		events := instance.BackgroundStep()
		if len(events) == 0 {
			continue
		}
		results = append(results, protocol.MapEvents{MapID: mapIDs[i], Events: events})
	}
	return results
}

func (n *NodeService) StoreReplica(cp protocol.MapCheckpoint) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.replicaSnapshots[cp.MapID] = cp
}

func (n *NodeService) Promote(mapID string, cfg world.MapConfig) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	cp, ok := n.replicaSnapshots[mapID]
	if !ok {
		return fmt.Errorf("节点 %s 上没有地图 %q 的副本快照", n.ID, mapID)
	}
	instance := world.NewWorld(cfg)
	instance.RestoreCheckpoint(cp)
	n.maps[mapID] = instance
	delete(n.replicaSnapshots, mapID)
	return nil
}

func (n *NodeService) View() protocol.NodeView {
	n.mu.RLock()
	defer n.mu.RUnlock()

	primaryMaps := make([]string, 0, len(n.maps))
	for mapID := range n.maps {
		primaryMaps = append(primaryMaps, mapID)
	}
	replicaMaps := make([]string, 0, len(n.replicaSnapshots))
	for mapID := range n.replicaSnapshots {
		replicaMaps = append(replicaMaps, mapID)
	}
	sort.Strings(primaryMaps)
	sort.Strings(replicaMaps)

	return protocol.NodeView{
		ID:            n.ID,
		Addr:          n.Addr,
		Healthy:       n.healthy,
		PrimaryMaps:   primaryMaps,
		ReplicaMaps:   replicaMaps,
		LastHeartbeat: n.lastHeartbeat.Format(time.RFC3339),
	}
}

func (n *NodeService) IsHealthy() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.healthy
}

func (n *NodeService) SetHealthy(healthy bool) bool {
	n.mu.Lock()
	defer n.mu.Unlock()

	wasHealthy := n.healthy
	n.healthy = healthy
	if healthy {
		n.lastHeartbeat = time.Now()
	}
	return wasHealthy
}

func (n *NodeService) NodeID() string {
	return n.ID
}
