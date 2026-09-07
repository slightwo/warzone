package node

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"sort"
	"sync"
	"time"

	"battleworld/protocol"
	"battleworld/storage"
	"battleworld/world"
)

const (
	topologyRefreshInterval = 500 * time.Millisecond
	topologyAuthorityGrace  = 2 * time.Second
)

type NodeService struct {
	mu               sync.RWMutex
	ID               string
	Addr             string
	store            *storage.Store
	authority        *authorityCache
	healthy          bool
	lastHeartbeat    time.Time
	maps             map[string]*world.World
	replicaSnapshots map[string]protocol.MapCheckpoint
	replicaMaps      map[string]struct{}
	ln               net.Listener
	stopCh           chan struct{}
}

func NewNodeService(id, addr string, store *storage.Store) *NodeService {
	return &NodeService{
		ID:               id,
		Addr:             addr,
		store:            store,
		authority:        newAuthorityCache(id, topologyAuthorityGrace),
		healthy:          true,
		lastHeartbeat:    time.Now(),
		maps:             make(map[string]*world.World),
		replicaSnapshots: make(map[string]protocol.MapCheckpoint),
		replicaMaps:      make(map[string]struct{}),
		stopCh:           make(chan struct{}),
	}
}

func (n *NodeService) Start() error {
	n.mu.Lock()
	if n.ln != nil {
		n.mu.Unlock()
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

	go n.topologyRefreshLoop()
	go n.flushLoop()
	go n.tickLoop()
	go n.replicaSyncLoop()
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

// topologyRefreshLoop 持续拉取已提交 Topology。刷新失败时 authorityCache 只会在
// 有限宽限期内使用最后快照；宽限期到期后节点必须 fail-closed。
func (n *NodeService) topologyRefreshLoop() {
	ticker := time.NewTicker(topologyRefreshInterval)
	defer ticker.Stop()

	for {
		if err := n.refreshTopology(); err != nil {
			log.Printf("[node/authority] 节点 %s 刷新拓扑失败: %v", n.ID, err)
		}
		select {
		case <-ticker.C:
		case <-n.stopCh:
			return
		}
	}
}

func (n *NodeService) refreshTopology() error {
	if n.store == nil {
		return ErrTopologyAuthorityUnavailable
	}
	return n.authority.refresh(n.store)
}

// RequireAuthority 在所有地图状态写入进入 world 前执行 owner/epoch fencing。
func (n *NodeService) RequireAuthority(mapID string, epoch uint64) error {
	if n.authority == nil {
		return ErrTopologyAuthorityUnavailable
	}
	if err := n.authority.Require(mapID, epoch); err != nil {
		return err
	}
	// 本地缓存只负责快速确定候选主权；真正允许 world 变更前必须再次读取 Redis
	// fence。控制面 CAS 一旦切换 owner/epoch，旧主的下一次写入立即被拒绝。
	if n.store == nil {
		return ErrTopologyAuthorityUnavailable
	}
	if err := n.store.RequireMapFence(mapID, n.ID, epoch); err != nil {
		return fmt.Errorf("%w: %v", ErrMapAuthorityDenied, err)
	}
	return nil
}

func (n *NodeService) RequirePromotionCandidate(mapID string, targetEpoch uint64) error {
	if n.authority == nil {
		return ErrTopologyAuthorityUnavailable
	}
	return n.authority.RequirePromotionCandidate(mapID, targetEpoch)
}

func (n *NodeService) flushLoop() {
	ticker := time.NewTicker(10 * time.Second)
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
			// 仅当前 owner 可下沉在线热数据；旧 owner 超过主权宽限期后完全停止写入。
			for mapID, instance := range n.maps {
				if _, owned := n.authority.Current(mapID); !owned {
					continue
				}
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
					//_ = n.store.SaveProfile(profile)
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

// AddReplicaMap 登记本节点本地预加载的副本地图（由 -replicas 启动参数传入）。
// 该本地能力不代表路由副本主权；副本归属仅由已提交的 Topology 决定。副本数据由
// replicaSyncLoop 定期从 Redis 拉取，无需协调器推送。
func (n *NodeService) AddReplicaMap(mapID string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.replicaMaps[mapID] = struct{}{}
}

// tickLoop 是节点自治的核心：驱动世界模拟、发布事件、自落盘 checkpoint。
// 取代原先协调器 backgroundLoop / checkpointLoop 的「远程驱动」。
func (n *NodeService) tickLoop() {
	ticker := time.NewTicker(700 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if n.store == nil {
				continue
			}
			n.mu.RLock()
			instances := make(map[string]*world.World, len(n.maps))
			for mapID, instance := range n.maps {
				instances[mapID] = instance
			}
			n.mu.RUnlock()

			for mapID, instance := range instances {
				authority, owned := n.authority.Current(mapID)
				if !owned || n.RequireAuthority(mapID, authority.Epoch) != nil {
					continue
				}
				// 1. world 变更前已即时校验 Redis fence；fence 失效则不会产生事件。
				events := instance.BackgroundStep()
				// 2. CAS 可能与 BackgroundStep 并发完成，因此发布事件和 checkpoint 前再次
				// 校验。失败时不发布刚才产生的本地事件，并让下一轮重新从新 owner 读取。
				if n.RequireAuthority(mapID, authority.Epoch) != nil {
					continue
				}
				for _, event := range events {
					if err := n.store.PublishEvent("events:map:"+mapID, event); err != nil {
						log.Printf("[tick] 发布地图 %s 事件失败: %v", mapID, err)
					}
				}
				cp := instance.CaptureCheckpoint(n.ID)
				cp.MapEpoch = authority.Epoch
				if err := n.store.SaveCheckpointIfOwner(cp, n.ID, authority.Epoch); err != nil {
					log.Printf("[tick] 保存地图 %s 的 fenced 快照失败: %v", mapID, err)
				}
			}
		case <-n.stopCh:
			return
		}
	}
}

// replicaSyncLoop 副本自拉：定期从 Redis 拉取副本地图的最新快照，
// 供主节点故障时 Promote 使用。对标 Redis/MySQL 主从的「从库拉取」模式。
func (n *NodeService) replicaSyncLoop() {
	ticker := time.NewTicker(700 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if n.store == nil {
				continue
			}
			n.mu.RLock()
			replicaMapIDs := make([]string, 0, len(n.replicaMaps))
			for mapID := range n.replicaMaps {
				replicaMapIDs = append(replicaMapIDs, mapID)
			}
			n.mu.RUnlock()

			for _, mapID := range replicaMapIDs {
				if cp, ok := n.store.LoadCheckpoint(mapID); ok && cp.Version > 0 {
					n.mu.Lock()
					n.replicaSnapshots[mapID] = *cp
					n.mu.Unlock()
				}
			}
		case <-n.stopCh:
			return
		}
	}
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

func (n *NodeService) AddPlayer(ctx context.Context, mapID string, epoch uint64, profile *protocol.UserProfile) error {
	if err := n.RequireAuthority(mapID, epoch); err != nil {
		return err
	}
	n.mu.RLock()
	instance := n.maps[mapID]
	n.mu.RUnlock()
	if instance == nil {
		return errors.New("add player：没有找到源节点")
	}
	instance.AddOrRestorePlayer(profile)
	return nil
}

func (n *NodeService) RemovePlayer(ctx context.Context, mapID, username string, epoch uint64) (protocol.UserProfile, bool, error) {
	if err := n.RequireAuthority(mapID, epoch); err != nil {
		return protocol.UserProfile{}, false, err
	}
	n.mu.RLock()
	instance := n.maps[mapID]
	n.mu.RUnlock()
	if instance == nil {
		return protocol.UserProfile{}, false, nil
	}
	profile, ok := instance.RemovePlayer(username)
	return profile, ok, nil
}

func (n *NodeService) MovePlayer(ctx context.Context, mapID, username, dir string, epoch uint64) (string, protocol.UserProfile, bool, error) {
	if err := n.RequireAuthority(mapID, epoch); err != nil {
		return "", protocol.UserProfile{}, false, err
	}
	n.mu.RLock()
	instance := n.maps[mapID]
	n.mu.RUnlock()
	if instance == nil {
		return "", protocol.UserProfile{}, false, nil
	}
	text, profile, ok := instance.MovePlayer(username, dir)
	return text, profile, ok, nil
}

func (n *NodeService) Attack(ctx context.Context, mapID, username string, epoch uint64) (string, string, string, protocol.UserProfile, bool, error) {
	if err := n.RequireAuthority(mapID, epoch); err != nil {
		return "", "", "", protocol.UserProfile{}, false, err
	}
	n.mu.RLock()
	instance := n.maps[mapID]
	n.mu.RUnlock()
	if instance == nil {
		return "", "", "", protocol.UserProfile{}, false, nil
	}
	log1, log2, gmLog, profile, ok := instance.Attack(username)
	return log1, log2, gmLog, profile, ok, nil
}

func (n *NodeService) Heal(ctx context.Context, mapID, username string, epoch uint64) (string, protocol.UserProfile, bool, error) {
	if err := n.RequireAuthority(mapID, epoch); err != nil {
		return "", protocol.UserProfile{}, false, err
	}
	n.mu.RLock()
	instance := n.maps[mapID]
	n.mu.RUnlock()
	if instance == nil {
		return "", protocol.UserProfile{}, false, nil
	}
	text, profile, ok := instance.HealPlayer(username)
	return text, profile, ok, nil
}

func (n *NodeService) BuyItem(ctx context.Context, mapID, username, item string, epoch uint64) (string, protocol.UserProfile, bool, error) {
	if err := n.RequireAuthority(mapID, epoch); err != nil {
		return "", protocol.UserProfile{}, false, err
	}
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

func (n *NodeService) RewardPlayer(ctx context.Context, mapID, username string, treasureDelta, victoryDelta int, epoch uint64) (protocol.UserProfile, bool, error) {
	if err := n.RequireAuthority(mapID, epoch); err != nil {
		return protocol.UserProfile{}, false, err
	}
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

func (n *NodeService) AttackBoss(ctx context.Context, mapID, username string, epoch uint64) (string, protocol.UserProfile, bool, error) {
	if err := n.RequireAuthority(mapID, epoch); err != nil {
		return "", protocol.UserProfile{}, false, err
	}
	n.mu.RLock()
	instance := n.maps[mapID]
	n.mu.RUnlock()
	if instance == nil {
		return "", protocol.UserProfile{}, true, fmt.Errorf("地图 %q 当前不在节点 %s 上", mapID, n.ID)
	}

	profile, ok := instance.ProfileOf(username)
	if !ok {
		return "", protocol.UserProfile{}, true, errors.New("获取玩家profile失败")
	}
	if !profile.Alive {
		return "倒地时无法攻击", profile, true, nil
	}

	state, hp, err := n.store.LoadGlobalBoss()
	if err != nil || !state.Alive {
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
		return "当前地图没有首领的位面", profile, true, nil
	}

	if manhattan(bossSite.X, bossSite.Y, profile.X, profile.Y) > protocol.BossAtkRange {
		return fmt.Sprintf("首领在范围外,剩余HP:%d", hp), profile, true, nil
	}

	damage := profile.Attack
	newHp, isKiller, _, err := n.store.AtomicAttackBoss(username, damage)
	if err != nil {
		return "", protocol.UserProfile{}, true, err
	}

	if newHp == -1 {
		// 如果扣血时发现 hp <= 0，说明已经被击杀
		return "首领还在复活倒计时", profile, true, nil
	}

	event := fmt.Sprintf("%s 对 %s 造成了 %d 点伤害！剩余HP：%d", username, state.Name, damage, newHp)

	if isKiller {
		// 该玩家为实际的终结者（因 Lua 内部拦截防超扣并确认了致命一击）
		state, _, _ = n.store.LoadGlobalBoss()
		if state.Alive {
			state.Alive = false
			state.LastHit = username
			state.RespawnAt = time.Now().Add(15 * time.Second)
			n.store.SaveGlobalBoss(state)
		}
		event += fmt.Sprintf("\nboss:%s,已经死亡！终结者：%s", state.Name, username)
	}

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
	checkpoint := instance.CaptureCheckpoint(n.ID)
	if authority, owned := n.authority.Current(mapID); owned {
		checkpoint.MapEpoch = authority.Epoch
	}
	return checkpoint, nil
}

func (n *NodeService) BackgroundStep(mapID string, epoch uint64) ([]string, error) {
	if err := n.RequireAuthority(mapID, epoch); err != nil {
		return nil, err
	}
	n.mu.RLock()
	instance := n.maps[mapID]
	n.mu.RUnlock()
	if instance == nil {
		return nil, fmt.Errorf("地图 %q 当前不在节点 %s 上", mapID, n.ID)
	}
	return instance.BackgroundStep(), nil
}

func (n *NodeService) StoreReplica(cp protocol.MapCheckpoint) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.replicaSnapshots[cp.MapID] = cp
}

func (n *NodeService) Promote(mapID string, cfg world.MapConfig, checkpoint protocol.MapCheckpoint, targetEpoch uint64) error {
	if err := n.RequirePromotionCandidate(mapID, targetEpoch); err != nil {
		return err
	}
	if checkpoint.MapID != mapID || checkpoint.NodeID == "" || checkpoint.Version <= 0 || checkpoint.MapEpoch != targetEpoch-1 {
		return fmt.Errorf("地图 %q 的提升 checkpoint 无效: %+v", mapID, checkpoint)
	}
	n.mu.Lock()
	defer n.mu.Unlock()

	// coordinator 已在 Promote 前验证该 checkpoint 的 owner/epoch/version；节点必须
	// 使用该精确快照恢复，禁止悄然回退到可能滞后的 replicaSnapshots。
	instance := world.NewWorld(cfg)
	instance.RestoreCheckpoint(checkpoint)
	n.maps[mapID] = instance
	n.replicaSnapshots[mapID] = checkpoint
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
