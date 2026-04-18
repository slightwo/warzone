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
	"battleworld/storage"
	"battleworld/world"
)

type NodeService struct {
	mu               sync.RWMutex
	ID               string
	Addr             string
	store            *storage.Store
	healthy          bool
	lastHeartbeat    time.Time
	maps             map[string]*world.World
	replicaSnapshots map[string]protocol.MapCheckpoint
	ln               net.Listener
	stopCh           chan struct{}
}

func NewNodeService(id, addr string, store *storage.Store) *NodeService {
	return &NodeService{
		ID:               id,
		Addr:             addr,
		store:            store,
		healthy:          true,
		lastHeartbeat:    time.Now(),
		maps:             make(map[string]*world.World),
		replicaSnapshots: make(map[string]protocol.MapCheckpoint),
		stopCh:           make(chan struct{}),
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
	select {
	case <-n.stopCh:
		n.stopCh = make(chan struct{})
	default:
	}
	n.mu.Unlock()

	go n.flushLoop()
	return nil
}

func (n *NodeService) Stop() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.ln = nil
	n.healthy = false
	select {
	case <-n.stopCh:
	default:
		close(n.stopCh)
	}
	return nil
}

// to understand
func (n *NodeService) flushLoop() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	// 记录上次 flush 的时间，只清洗有变动的玩家数据
	lastFlush := time.Now()

	for {
		select {
		case <-ticker.C:
			now := time.Now()
			since := lastFlush
			lastFlush = now

			n.mu.RLock()
			// 收集所有在线热数据
			for mapID, instance := range n.maps {
				profiles := instance.FlushProfiles(since)
				for _, profile := range profiles {
					hotData := protocol.HotSession{
						Username:       profile.Username,
						MapID:          mapID,
						NodeID:         n.ID,
						X:              profile.X,
						Y:              profile.Y,
						HP:             profile.HP,
						Treasures:      profile.Treasures,
						SessionVersion: 0, // 在节点层不需要维护递增版本号了或者交由其他部分
						UpdatedAt:      now,
					}
					// 将活跃用户的热数据以及必要的冷数据下沉保存
					_ = n.store.SaveHotSession(hotData)
					_ = n.store.SaveProfile(profile)
				}
			}
			n.mu.RUnlock()
		case <-n.stopCh:
			return
		}
	}
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
	//fmt.Println("[debug] node.go:move")
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

func manhattan(ax, ay, bx, by int) int {
	dx := ax - bx
	if dx < 0 {
		dx = -dx
	}
	dy := ay - by
	if dy < 0 {
		dy = -dy
	}
	return dx + dy
}

func (n *NodeService) AttackBoss(ctx context.Context, mapID, username string) (string, protocol.UserProfile, bool, error) {
	fmt.Println("[debug]进入node.go attackBoss")
	n.mu.RLock()
	instance := n.maps[mapID]
	n.mu.RUnlock()
	if instance == nil {
		return "", protocol.UserProfile{}, true, fmt.Errorf("地图 %q 当前不在节点 %s 上", mapID, n.ID)
	}

	profile, ok := instance.ProfileOf(username)
	if !ok {
		fmt.Println("[debug]node.go attackBoss:获取玩家profile失败")
		return "", protocol.UserProfile{}, true, errors.New("获取玩家profile失败")
	}
	if !profile.Alive {
		fmt.Println("[debug]node.go attackBoss:倒地时无法攻击")

		return "倒地时无法攻击", profile, true, nil
	}

	state, hp, err := n.store.LoadGlobalBoss()
	if err != nil || !state.Alive {
		fmt.Println("[debug]node.go attackBoss:首领未复活")
		return "首领还在复活倒计时", profile, true, nil
	}

	var bossSite *protocol.BossSite
	for _, site := range state.Sites {
		if site.MapID == mapID {
			s := site
			bossSite = &s
			break
		}
	}
	if bossSite == nil {
		fmt.Println("[debug]node.go attackBoss:当前地图无首领")
		return "当前地图没有首领的位面", profile, true, nil
	}

	if manhattan(bossSite.X, bossSite.Y, profile.X, profile.Y) > protocol.BossAtkRange {
		fmt.Println("[debug]node.go attackBoss:首领范围外")
		return fmt.Sprintf("首领在范围外,剩余HP:%d", hp), profile, true, nil
	}

	damage := profile.Attack
	newHp, err := n.store.DecrGlobalBossHP(username, damage)
	if err != nil {
		fmt.Println("[debug]node.go attackBoss:攻击首领时err：", err)
		return "", protocol.UserProfile{}, true, err
	}

	newHpMaxZero := newHp
	if newHpMaxZero < 0 {
		newHpMaxZero = 0
	}
	event := fmt.Sprintf("%s 对 %s 造成了 %d 点伤害！剩余HP：%d", username, state.Name, damage, newHpMaxZero)

	if newHp <= 0 && state.Alive {
		if n.store.TryLockBossKill() {
			state, _, _ = n.store.LoadGlobalBoss()
			if state.Alive {
				state.Alive = false
				state.LastHit = username
				state.RespawnAt = time.Now().Add(15 * time.Second)
				n.store.SaveGlobalBoss(state)
				event += fmt.Sprintf("\nboss:%s,已经死亡！终结者：%s", state.Name, username)
			}
		}
	}
	fmt.Println("[debug]node.go attackBoss:成功")
	return event, profile, true, nil
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
