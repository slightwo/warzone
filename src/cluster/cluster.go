package cluster

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"battleworld/protocol"
	"battleworld/storage"
	"battleworld/world"

	"github.com/redis/go-redis/v9"
)

type Session struct {
	Username string
	MapID    string
	NodeID   string
	Events   []string
	Version  int64
}

// NodeClient 代表向一个节点发起命令的客户端抽象，允许未来替换成网络版 (NodeGRPCClient)
type NodeClient interface {
	NodeID() string
	Start() error
	Stop() error
	Ping(ctx context.Context) error
	AddPlayer(ctx context.Context, mapID string, profile *protocol.UserProfile) error
	RemovePlayer(ctx context.Context, mapID, username string) (protocol.UserProfile, bool, error)
	MovePlayer(ctx context.Context, mapID, username, dir string) (string, protocol.UserProfile, bool, error)
	Attack(ctx context.Context, mapID, username string) (string, string, string, protocol.UserProfile, bool, error)
	Heal(ctx context.Context, mapID, username string) (string, protocol.UserProfile, bool, error)
	BuyItem(ctx context.Context, mapID, username, item string) (string, protocol.UserProfile, bool, error)
	AttackBoss(ctx context.Context, mapID, username string) (string, protocol.UserProfile, bool, error)
	Profile(ctx context.Context, mapID, username string) (protocol.UserProfile, bool, error)
	RewardPlayer(ctx context.Context, mapID, username string, treasureDelta, victoryDelta int) (protocol.UserProfile, bool, error)
	Snapshot(ctx context.Context, mapID string) (protocol.MapView, error)
	Counts(ctx context.Context, mapID string) (int, int, int, int64, error)
	Checkpoint(ctx context.Context, mapID string) (protocol.MapCheckpoint, error)
	Promote(mapID string, cfg world.MapConfig) error
	View() protocol.NodeView
	IsHealthy() bool
	SetHealthy(healthy bool) bool
}

type MapCacheData struct {
	View  *protocol.MapView
	Brief protocol.MapBrief
}

type Cluster struct {
	mu              sync.RWMutex
	store           *storage.Store
	nodes           map[string]NodeClient
	connectingNodes map[string]struct{}
	// owners 与 replicas 是兼容既有路由逻辑的过渡缓存，只能由 applyTopologyLocked 写入。
	owners         map[string]string
	replicas       map[string]string
	topology       storage.Topology
	topologyLoaded bool
	configs        map[string]world.MapConfig
	stopCh         chan struct{}

	mapCacheMu sync.RWMutex
	mapCache   map[string]MapCacheData

	bossCacheMu sync.RWMutex
	bossCache   protocol.BossState
	bossHpCache int32

	// 事件分离：纯内存通道，各网关实例自行订阅并缓冲
	eventMu      sync.RWMutex
	globalEvents []string
	mapEvents    map[string][]string

	userEvents map[string][]string
	pubsub     *redis.PubSub

	// 本地缓存，存放活跃用户的 Session，减少访问 Redis 的开销
	localSessions sync.Map
}

func NewCluster(store *storage.Store) (*Cluster, error) {
	c := &Cluster{
		store:           store,
		nodes:           make(map[string]NodeClient),
		connectingNodes: make(map[string]struct{}),
		owners:          make(map[string]string),
		replicas:        make(map[string]string),
		configs:         make(map[string]world.MapConfig),
		stopCh:          make(chan struct{}),
		mapCache:        make(map[string]MapCacheData),
		mapEvents:       make(map[string][]string),
		userEvents:      make(map[string][]string),
	}

	for _, cfg := range world.AvailableMaps() {
		c.configs[cfg.ID] = cfg
	}

	// 从Redis加载Boss状态，如果没找到则初始化
	if _, _, err := store.LoadGlobalBoss(); err != nil {
		initialState := protocol.BossState{
			Name:      "王子文",
			Alive:     true,
			Sites:     c.buildBossSites(),
			Version:   1,
			AttackGap: 2000,
		}
		store.InitGlobalBoss(1600, initialState)
	}

	// 路由拓扑由 Redis 中已提交的 Topology 决定；discovery 只维护节点连接池。
	c.pubsub = store.SubscribeEvents()
	go c.eventLoop()
	return c, nil
}

func (c *Cluster) Start() error {
	if updated, err := c.loadTopology(); err != nil {
		log.Printf("[topology] 启动时加载拓扑失败，将保留最后成功快照: %v", err)
	} else if updated {
		log.Printf("[topology] 启动时已加载路由快照")
	}
	go c.topologySyncLoop()
	go c.mapCacheLoop()
	go c.discoveryLoop()
	go c.heartbeatLoop()
	return nil
}
func (c *Cluster) Close() {
	// 节点自治后，协调器关闭时无需再抓 checkpoint 落盘（节点自落盘、副本自拉）。
	// 仅通知各后台循环退出。
	select {
	case <-c.stopCh:
	default:
		close(c.stopCh)
	}
}
func (c *Cluster) Register(username, password, confirm string) error {
	if confirm == "" {
		return errors.New("注册时必须再次确认密码")
	}
	if password != confirm {
		return errors.New("两次输入的密码不一致")
	}
	return c.store.Register(username, password)
}

func (c *Cluster) ExecuteAdmin(action, nodeID string) (string, error) {
	switch action {
	case "status", "状态":
		return c.adminStatus(), nil
	case "fail", "down", "故障":
		if nodeID == "" {
			return "", errors.New("请指定需要模拟故障的节点")
		}
		return c.failNode(nodeID)
	case "recover", "up", "恢复":
		if nodeID == "" {
			return "", errors.New("请指定需要恢复的节点")
		}
		return c.recoverNode(nodeID)
	default:
		return "", fmt.Errorf("未知管理动作：%s", action)
	}
}

func (c *Cluster) QuickEnter(username, password string) (*protocol.WorldState, error) {
	if err := c.Register(username, password, password); err != nil {
		if !strings.Contains(err.Error(), "已存在") {
			return nil, err
		}
	}
	return c.Login(username, password)
}

func (c *Cluster) Login(username, password string) (*protocol.WorldState, error) {
	profile, err := c.store.Authenticate(username, password)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	if _, ok := c.store.LoadGlobalSession(username); ok {
		c.mu.Unlock()
		return nil, fmt.Errorf("用户 %q 已经在线", username)
	}
	mapID := profile.LastMap
	if _, ok := c.configs[mapID]; !ok {
		mapID = world.DefaultMapID()
	}
	ownerID := c.owners[mapID]
	owner := c.nodes[ownerID]
	c.mu.Unlock()

	if owner == nil || !owner.IsHealthy() {
		return nil, errors.New("目标地图当前没有可用节点")
	}

	if err := owner.AddPlayer(context.Background(), mapID, profile); err != nil {
		return nil, fmt.Errorf("节点添加玩家失败: %v", err)
	}

	c.mu.Lock()
	session := storage.GlobalSession{
		Username: username,
		MapID:    mapID,
		NodeID:   ownerID,
		Version:  1,
	}
	_ = c.store.SaveGlobalSession(session)
	c.localSessions.Store(username, &session) // 更新本地缓存
	c.mu.Unlock()
	_ = c.store.PublishEvent("events:user:"+username, fmt.Sprintf("欢迎回来，%s", username))
	_ = c.store.PublishEvent("events:user:"+username, fmt.Sprintf("当前地图 %s 由 %s 承载", mapID, ownerID))

	return c.SnapshotFor(username)
}

func (c *Cluster) Logout(username string) error {
	c.mu.Lock()
	session, ok := c.store.LoadGlobalSession(username)
	if !ok {
		c.mu.Unlock()
		return nil
	}
	_ = c.store.DeleteGlobalSession(username)
	c.localSessions.Delete(username) // 同理，也删除本地缓存
	c.mu.Unlock()

	c.mu.RLock()
	node := c.nodes[session.NodeID]
	c.mu.RUnlock()
	if node == nil || !node.IsHealthy() {
		return c.store.DeleteHotSession(username)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	profile, removed, err := node.RemovePlayer(ctx, session.MapID, username)
	if err != nil {
		log.Printf("[logout] 节点 %s 移除玩家 %s 失败: %v", session.NodeID, username, err)
	} else if removed {
		profile.LastNode = session.NodeID
		profile.LastMap = session.MapID
		_ = c.store.SaveProfile(profile)
	}
	return c.store.DeleteHotSession(username)
}

func (c *Cluster) Move(username, dir string) (*protocol.WorldState, error) {
	session, node, err := c.sessionNode(username)
	if err != nil {
		return nil, err
	}
	_, _, ok, err := node.MovePlayer(context.Background(), session.MapID, username, dir)
	if err != nil {
		return nil, fmt.Errorf("节点RPC移动调用失败: %v", err)
	}
	if !ok {
		return nil, errors.New("移动请求被拒绝")
	}
	//c.pushEvent(username, event)
	// profile.LastNode = session.NodeID
	// profile.LastMap = session.MapID
	//	_ = c.store.SaveProfile(profile)
	//	_ = c.persistSessionState(username)
	return nil, nil //c.SnapshotFor(username)
}

func (c *Cluster) Attack(username string) (*protocol.WorldState, error) {
	session, node, err := c.sessionNode(username)
	if err != nil {
		return nil, err
	}
	event, targetUsername, targetEvent, _, ok, err := node.Attack(context.Background(), session.MapID, username)
	if err != nil {
		return nil, fmt.Errorf("节点RPC攻击调用异常: %v", err)
	}
	if !ok {
		return nil, errors.New("攻击请求被拒绝")
	}
	c.pushEvent(username, event)
	if targetUsername != "" && targetEvent != "" {
		c.pushEvent(targetUsername, targetEvent)

	}
	// profile.LastNode = session.NodeID
	// profile.LastMap = session.MapID
	// _ = c.store.SaveProfile(profile)
	// _ = c.persistSessionState(username)
	return c.SnapshotFor(username)
}

func (c *Cluster) Heal(username string) (*protocol.WorldState, error) {
	session, node, err := c.sessionNode(username)
	if err != nil {
		return nil, err
	}
	event, _, ok, err := node.Heal(context.Background(), session.MapID, username)
	if err != nil {
		return nil, fmt.Errorf("节点RPC治疗调用异常: %v", err)
	}
	if !ok {
		return nil, errors.New("治疗请求被拒绝")
	}
	c.pushEvent(username, event)
	// profile.LastNode = session.NodeID
	// profile.LastMap = session.MapID
	// _ = c.store.SaveProfile(profile)
	// _ = c.persistSessionState(username)
	return c.SnapshotFor(username)
}

func (c *Cluster) BuyItem(username, item string) (*protocol.WorldState, error) {
	session, node, err := c.sessionNode(username)
	if err != nil {
		return nil, err
	}
	event, profile, ok, err := node.BuyItem(context.Background(), session.MapID, username, item)
	if err != nil {
		return nil, fmt.Errorf("节点RPC商店请求异常: %v", err)
	}
	if !ok {
		return nil, errors.New("商店请求被拒绝")
	}
	c.pushEvent(username, event)
	profile.LastNode = session.NodeID
	profile.LastMap = session.MapID
	_ = c.store.SaveProfile(profile)
	// _ = c.persistSessionState(username) // Removed
	return c.SnapshotFor(username)
}
func (c *Cluster) AttackBoss(username string) (*protocol.WorldState, error) {
	session, node, err := c.sessionNode(username)
	if err != nil {
		return nil, err
	}
	event, _, ok, err := node.AttackBoss(context.Background(), session.MapID, username)
	if err != nil || !ok {
		return nil, err
	}
	c.broadcastGlobalEvent(event)

	if strings.Contains(event, "已经死亡！终结者：") {
		bState, _, _ := c.store.LoadGlobalBoss()
		go c.respawnBossAfterCooldown(bState.Name)
	}

	return c.SnapshotFor(username)
}

func (c *Cluster) SwitchMap(username, targetMap string) (*protocol.WorldState, error) {
	session, src_node, err := c.sessionNode(username)
	if err != nil {
		return nil, err
	}
	c.mu.RLock()
	profile, ok, err := src_node.Profile(context.Background(), session.MapID, username)
	if err != nil {
		c.mu.RUnlock()
		return nil, fmt.Errorf("节点RPC获取Profile异常: %v", err)
	}
	if !ok {
		c.mu.RUnlock()
		return nil, errors.New("获取玩家profile失败")
	}
	if !profile.Alive {
		c.mu.RUnlock()
		return nil, errors.New("倒地时不可切换地图")
	}
	dstNodeID := c.owners[targetMap]
	dst := c.nodes[dstNodeID]
	c.mu.RUnlock()
	if dst == nil || !dst.IsHealthy() {
		return nil, fmt.Errorf("目标地图 %q 当前没有可用节点", targetMap)
	}

	profile, ok, err = src_node.RemovePlayer(context.Background(), session.MapID, username)
	if err != nil {
		return nil, fmt.Errorf("源节点RPC移除玩家异常 (Try失败): %v", err)
	}
	if !ok {
		return nil, errors.New("源节点移除玩家失败 (Try被拒绝)")
	}

	// 执行 TCC 逻辑的 Confirm / Cancel 阶段
	// 尝试向目标节点写入 (Confirm)
	if err := dst.AddPlayer(context.Background(), targetMap, &profile); err != nil {
		// Cancel 阶段: 目标写入失败，必须进行业务回滚，将玩家恢复至原源节点
		fmt.Printf("[!!!严重警告!!!] 玩家 %s 切换地图 %s 在目标节点写入失败 (%v)，开始执行 TCC 回滚...\n", username, targetMap, err)

		rollbackErr := src_node.AddPlayer(context.Background(), session.MapID, &profile)
		if rollbackErr != nil {
			// 如果回滚也失败了，说明遇到了罕见的网络断裂或极端的双主脑裂，玩家数据暂时丢失在以太空间，需要后续的人工或后台守护进程修复
			fmt.Printf("[FATAL 灾难] 玩家 %s TCC 回滚失败 ! 补偿写入原节点也失败了 : %v\n", username, rollbackErr)
		} else {
			fmt.Printf("[INFO] 玩家 %s TCC 回滚成功，状态已恢复至原节点\n", username)
		}
		return nil, fmt.Errorf("目标节点添加玩家失败，切换已回滚: %v", err)
	}

	// Double-check (非必须，但加固防御): 验证目标节点写入已确认
	profile, ok, err = dst.Profile(context.Background(), targetMap, username)
	if err != nil || !ok {
		// 读不到可能表明刚写入就被挤掉或者网络问题
		return nil, fmt.Errorf("获取移动后用户信息异常 (Confirm阶段校验未通过): %v", err)
	}

	profile.LastMap = targetMap
	profile.LastNode = dstNodeID

	c.mu.Lock()
	session, ok = c.store.LoadGlobalSession(username)
	if ok && session != nil {
		session.MapID = targetMap
		session.NodeID = dst.NodeID()
		session.Version++
		_ = c.store.SaveGlobalSession(*session)
		c.localSessions.Store(username, session) // 跨节点切图成功，更新本地缓存
	}
	c.mu.Unlock()

	if ok && session != nil {
		c.pushEvent(session.Username, fmt.Sprintf("切换到地图 %s", targetMap))
	}
	_ = c.store.SaveProfile(profile)

	return c.SnapshotFor(username)
}

func (c *Cluster) SnapshotFor(username string) (*protocol.WorldState, error) {
	session, node, err := c.sessionNode(username)
	if err != nil {
		return nil, err
	}
	sessionVersion := session.Version

	ws := protocol.AllocWorldState()
	ws.SessionVersion = sessionVersion

	c.eventMu.RLock()
	ws.Events = ws.Events[:0]
	ws.Events = append(ws.Events, c.globalEvents...)
	if list, ok := c.mapEvents[session.MapID]; ok {
		ws.Events = append(ws.Events, list...)
	}
	if list, ok := c.userEvents[username]; ok {
		ws.Events = append(ws.Events, list...)
	}
	c.eventMu.RUnlock()
	// 取最近不超过 8 条的事件
	if len(ws.Events) > 8 {
		start := len(ws.Events) - 8
		copy(ws.Events, ws.Events[start:])
		ws.Events = ws.Events[:8]
	}

	c.mu.RLock()
	nodes := make(map[string]NodeClient, len(c.nodes))
	for k, v := range c.nodes {
		nodes[k] = v
	}
	configs := c.configs
	c.mu.RUnlock()

	c.bossCacheMu.RLock()
	bState := c.bossCache
	hp := c.bossHpCache
	c.bossCacheMu.RUnlock()

	respawnIn := 0
	if !bState.Alive {
		respawnIn = int(time.Until(bState.RespawnAt).Seconds())
		if respawnIn < 1 {
			respawnIn = 1
		}
	}
	bossView := protocol.BossView{
		Name:      bState.Name,
		HP:        int(hp),
		MaxHP:     1600,
		Alive:     bState.Alive,
		LastHit:   bState.LastHit,
		RespawnIn: respawnIn,
		AttackGap: bState.AttackGap,
		Sites:     bState.Sites,
		Version:   bState.Version,
	}

	if node == nil {
		protocol.FreeWorldState(ws)
		return nil, errors.New("当前承载节点已不可用")
	}

	// 新增：从缓存读取地图状态，消除 O(M) 次 RPC 放大
	c.mapCacheMu.RLock()
	cachedCurrent, mapOk := c.mapCache[session.MapID]
	cachedAllBriefs := make(map[string]protocol.MapBrief)
	for _, k := range c.mapCache {
		cachedAllBriefs[k.Brief.ID] = k.Brief
	}
	c.mapCacheMu.RUnlock()

	if !mapOk || cachedCurrent.View == nil {
		protocol.FreeWorldState(ws)
		return nil, fmt.Errorf("地图状态正在同步中，请稍候")
	}
	mapView := *cachedCurrent.View

	self := protocol.PlayerView{}
	for _, player := range mapView.Players {
		if player.Username == username {
			self = player
			break
		}
	}

	mapIDs := make([]string, 0, len(configs))
	for mapID := range configs {
		mapIDs = append(mapIDs, mapID)
	}
	sort.Strings(mapIDs)

	ws.Maps = ws.Maps[:0]
	for _, mapID := range mapIDs {
		if brief, ok := cachedAllBriefs[mapID]; ok {
			brief.IsCurrent = (mapID == session.MapID)
			ws.Maps = append(ws.Maps, brief)
		}
	}

	nodeIDs := make([]string, 0, len(nodes))
	for nodeID := range nodes {
		nodeIDs = append(nodeIDs, nodeID)
	}
	sort.Strings(nodeIDs)
	ws.Nodes = ws.Nodes[:0]
	for _, nodeID := range nodeIDs {
		ws.Nodes = append(ws.Nodes, nodes[nodeID].View())
	}

	ws.Self = self
	ws.Map = mapView
	ws.Boss = bossView

	return ws, nil
}

func (c *Cluster) sessionNode(username string) (*storage.GlobalSession, NodeClient, error) {
	// 1. 优先从本地内存获取无锁化读取
	if val, ok := c.localSessions.Load(username); ok {
		session := val.(*storage.GlobalSession)

		c.mu.RLock()
		node := c.nodes[session.NodeID]
		c.mu.RUnlock()

		if node == nil || !node.IsHealthy() {
			return nil, nil, fmt.Errorf("节点 %q 当前不可用", session.NodeID)
		}

		copySession := *session
		return &copySession, node, nil
	}

	// 2. 本地没找到，进行降级读取并做缓存回填（带读锁不阻碍本节点的操作或可先解锁）
	// 这里为了简单，维持原有的 c.mu.RLock 控制即可
	c.mu.RLock()
	defer c.mu.RUnlock()

	session, ok := c.store.LoadGlobalSession(username)
	if !ok {
		return nil, nil, fmt.Errorf("用户 %q 当前不在线", username)
	}

	c.localSessions.Store(username, session)

	node := c.nodes[session.NodeID]
	if node == nil || !node.IsHealthy() {
		return nil, nil, fmt.Errorf("节点 %q 当前不可用", session.NodeID)
	}
	copySession := *session
	return &copySession, node, nil
}

func (c *Cluster) pushEvent(username, event string) {
	if event == "" {
		return
	}
	_ = c.store.PublishEvent("events:user:"+username, event)
}

func (c *Cluster) broadcastGlobalEvent(event string) {
	if event == "" {
		return
	}
	_ = c.store.PublishEvent("events:global", event)
}

func (c *Cluster) broadcastGlobalEventLocked(event string) {
	if event == "" {
		return
	}
	_ = c.store.PublishEvent("events:global", event)
}

func (c *Cluster) broadcastMapEvent(mapID, event string) {
	if event == "" {
		return
	}
	_ = c.store.PublishEvent("events:map:"+mapID, event)
}

func (c *Cluster) respawnBossAfterCooldown(name string) {
	time.Sleep(15 * time.Second)

	bState, hp, err := c.store.LoadGlobalBoss()
	if err == nil && !bState.Alive {
		if c.store.TryLockBossRespawn() {
			bState.Alive = true
			bState.LastHit = ""
			if hp <= 0 {
				hp = 1600
			}
			c.store.InitGlobalBoss(hp, bState)
			c.broadcastGlobalEvent(fmt.Sprintf("世界首领【%s】重新降临，所有服务器均可参与讨伐", name))
		}
	}
}

func (c *Cluster) discoveryLoop() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			activeNodes, err := c.store.GetActiveNodes()
			if err != nil {
				log.Printf("[discovery] 读取活跃节点失败: %v", err)
				continue
			}
			c.discoverNodes(activeNodes)

			// 只有 Redis 中尚不存在拓扑时，才允许依据候选能力执行一次 bootstrap。
			// 已提交拓扑后，后续注册/重连只能影响连接池，不得覆写路由主权。
			if err := c.bootstrapTopologyIfAbsent(c.connectedNodeRegistrations(activeNodes)); err != nil {
				log.Printf("[topology] bootstrap 失败: %v", err)
			}
		case <-c.stopCh:
			return
		}
	}
}

// discoverNodes 仅建立并维护可用节点连接，不根据节点注册声明修改路由拓扑。
func (c *Cluster) discoverNodes(activeNodes []storage.NodeRegistryInfo) {
	c.mu.Lock()
	pending := make([]storage.NodeRegistryInfo, 0, len(activeNodes))
	for _, info := range activeNodes {
		if _, connected := c.nodes[info.ID]; connected {
			continue
		}
		if _, connecting := c.connectingNodes[info.ID]; connecting {
			continue
		}
		c.connectingNodes[info.ID] = struct{}{}
		pending = append(pending, info)
	}
	c.mu.Unlock()

	for _, info := range pending {
		go c.connectDiscoveredNode(info)
	}
}

func (c *Cluster) connectDiscoveredNode(info storage.NodeRegistryInfo) {
	client, err := NewNodeGRPCClient(info.ID, info.Addr)
	if err != nil {
		c.finishNodeConnection(info.ID)
		log.Printf("[discovery] 创建节点 %s 客户端失败: %v", info.ID, err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	err = client.Ping(ctx)
	cancel()
	if err != nil {
		_ = client.Close()
		c.finishNodeConnection(info.ID)
		log.Printf("[discovery] 节点 %s 健康检查失败: %v", info.ID, err)
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.connectingNodes, info.ID)
	if _, exists := c.nodes[info.ID]; exists {
		_ = client.Close()
		return
	}
	c.nodes[info.ID] = client
	log.Printf("[discovery] 节点 %s 已加入连接池", info.ID)
}

func (c *Cluster) finishNodeConnection(nodeID string) {
	c.mu.Lock()
	delete(c.connectingNodes, nodeID)
	c.mu.Unlock()
}

// connectedNodeRegistrations 仅保留已完成连接且健康的节点注册信息，避免将 Redis 中
// 尚未验证可达性的临时/过期租约用于 bootstrap。
func (c *Cluster) connectedNodeRegistrations(activeNodes []storage.NodeRegistryInfo) []storage.NodeRegistryInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	connected := make([]storage.NodeRegistryInfo, 0, len(activeNodes))
	for _, info := range activeNodes {
		node, ok := c.nodes[info.ID]
		if !ok || !node.IsHealthy() {
			continue
		}
		connected = append(connected, info)
	}
	return connected
}

func (c *Cluster) mapCacheLoop() {
	ticker := time.NewTicker(150 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.mu.RLock()
			owners := make(map[string]string)
			for k, v := range c.owners {
				owners[k] = v
			}
			nodes := make(map[string]NodeClient)
			for k, v := range c.nodes {
				nodes[k] = v
			}
			configs := c.configs
			c.mu.RUnlock()

			var wg sync.WaitGroup
			var cacheMu sync.Mutex
			newCache := make(map[string]MapCacheData)

			wg.Add(1)
			go func() {
				defer wg.Done()
				bState, hp, err := c.store.LoadGlobalBoss()
				if err == nil {
					c.bossCacheMu.Lock()
					c.bossCache = bState
					c.bossHpCache = hp
					c.bossCacheMu.Unlock()
				}
			}()

			for mapID, ownerID := range owners {
				host := nodes[ownerID]
				if host == nil || !host.IsHealthy() {
					continue
				}
				cfg := configs[mapID]
				wg.Add(1)
				go func(mapID string, host NodeClient, name string) {
					defer wg.Done()

					// 1. 获取 Snapshot
					mapView, err := host.Snapshot(context.Background(), mapID)
					if err != nil {
						return
					}

					// 2. 获取 Counts （可以直接通过 mapView len 获取避免 RPC）
					players := len(mapView.Players)
					npcs := len(mapView.NPCs)
					treasures := len(mapView.Treasures)
					version := mapView.Version

					brief := protocol.MapBrief{
						ID:        mapID,
						Name:      name,
						NodeID:    host.NodeID(),
						Players:   players,
						NPCs:      npcs,
						Treasures: treasures,
						Version:   version,
						Primary:   true,
						// IsCurrent: 会在外面装配
					}

					cacheMu.Lock()
					newCache[mapID] = MapCacheData{
						View:  &mapView,
						Brief: brief,
					}
					cacheMu.Unlock()
				}(mapID, host, cfg.Name)
			}
			wg.Wait()

			c.mapCacheMu.Lock()
			for k, v := range newCache {
				c.mapCache[k] = v
			}
			c.mapCacheMu.Unlock()
		case <-c.stopCh:
			return
		}
	}
}

func (c *Cluster) heartbeatLoop() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			nodeIDs := make([]string, 0, len(c.nodes))
			c.mu.RLock()
			for nodeID := range c.nodes {
				nodeIDs = append(nodeIDs, nodeID)
			}
			sort.Strings(nodeIDs)
			nodes := make([]NodeClient, 0, len(nodeIDs))
			for _, nodeID := range nodeIDs {
				nodes = append(nodes, c.nodes[nodeID])
			}
			c.mu.RUnlock()

			var wg sync.WaitGroup
			for _, node := range nodes {
				wg.Add(1)
				go func(n NodeClient) {
					defer wg.Done()
					ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
					defer cancel()

					// 使用原生的 gRPC Ping 测试应用层健康程度，而不是简单测试 TCP 端口
					err := n.Ping(ctx)
					healthy := err == nil

					wasHealthy := n.SetHealthy(healthy)
					if wasHealthy && !healthy {
						c.handleNodeFailure(n.NodeID())
					}
				}(node)
			}
			wg.Wait()
		case <-c.stopCh:
			return
		}
	}
}

// handleNodeFailure 在 2.2-A 只标记节点不可用，不得在内存中提升副本或改写拓扑。
// 故障节点的客户端连接暂留在连接池中，以支持 recoverNode 的原地恢复；连接池回收会在
// 2.2-C 的带 epoch 故障转移中与拓扑切换一并实现。完整的 Promote、MapEpoch fencing、
// Topology CAS 与会话迁移也将在该阶段作为一个原子流程实现。
func (c *Cluster) handleNodeFailure(nodeID string) {
	c.mu.RLock()
	affectedMaps := make([]string, 0)
	if c.topologyLoaded {
		for mapID, ownerID := range c.topology.Owners {
			if ownerID == nodeID {
				affectedMaps = append(affectedMaps, mapID)
			}
		}
	}
	c.mu.RUnlock()
	sort.Strings(affectedMaps)

	if len(affectedMaps) == 0 {
		log.Printf("[failover] 节点 %s 不再承载任何已提交地图，拓扑保持版本不变", nodeID)
	} else {
		log.Printf("[failover] 节点 %s 不可用，受影响地图=%v；等待 2.2-C 的带 epoch 故障转移", nodeID, affectedMaps)
	}
	c.broadcastGlobalEvent(fmt.Sprintf("%q 发生故障，当前拓扑保持不变，受影响地图暂不可用", nodeID))
}

func (c *Cluster) adminStatus() string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	nodeIDs := make([]string, 0, len(c.nodes))
	for nodeID := range c.nodes {
		nodeIDs = append(nodeIDs, nodeID)
	}
	sort.Strings(nodeIDs)

	lines := []string{"集群状态总览："}
	for _, nodeID := range nodeIDs {
		view := c.nodes[nodeID].View()
		status := "离线"
		if view.Healthy {
			status = "在线"
		}
		lines = append(lines, fmt.Sprintf("- %s %s 主分片=%v 副本=%v", view.ID, status, view.PrimaryMaps, view.ReplicaMaps))
	}
	return strings.Join(lines, "\n")
}

func (c *Cluster) failNode(nodeID string) (string, error) {
	c.mu.RLock()
	node, ok := c.nodes[nodeID]
	c.mu.RUnlock()
	if !ok {
		return "", fmt.Errorf("节点 %s 不存在", nodeID)
	}
	view := node.View()
	for _, mapID := range view.PrimaryMaps {
		cp, err := node.Checkpoint(context.Background(), mapID)
		if err != nil {
			continue
		}
		// 故障转移前确保 Redis 有最新快照；副本由各自 replicaSyncLoop 自拉。
		_ = c.store.SaveCheckpoint(cp)
	}
	if err := node.Stop(); err != nil {
		return "", err
	}
	node.SetHealthy(false)
	c.handleNodeFailure(nodeID)
	c.broadcastGlobalEvent(fmt.Sprintf("管理命令：已模拟 %s 故障，拓扑保持不变", nodeID))
	return fmt.Sprintf("节点 %s 已被标记为故障；受影响地图将暂不可用，等待带 epoch 的故障转移", nodeID), nil
}

func (c *Cluster) recoverNode(nodeID string) (string, error) {
	c.mu.RLock()
	node, ok := c.nodes[nodeID]
	c.mu.RUnlock()
	if !ok {
		return "", fmt.Errorf("节点 %s 不存在", nodeID)
	}
	if err := node.Start(); err != nil {
		return "", err
	}
	node.SetHealthy(true)

	c.broadcastGlobalEvent(fmt.Sprintf("管理命令：节点 %s 已恢复在线，拓扑保持不变", nodeID))
	return fmt.Sprintf("节点 %s 已恢复；其注册声明不会修改已提交拓扑", nodeID), nil
}

func (c *Cluster) buildBossSites() []protocol.BossSite {
	mapIDs := make([]string, 0, len(c.configs))
	for mapID := range c.configs {
		mapIDs = append(mapIDs, mapID)
	} //获取所有地图名，并排序
	sort.Strings(mapIDs)

	sites := make([]protocol.BossSite, 0, len(mapIDs))
	for _, mapID := range mapIDs {
		cfg := c.configs[mapID]
		sites = append(sites, protocol.BossSite{
			MapID: mapID,
			X:     cfg.BossX,
			Y:     cfg.BossY,
		})
	}
	return sites
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

func (c *Cluster) eventLoop() {
	ch := c.pubsub.Channel()
	for {
		select {
		case msg := <-ch:
			if msg == nil {
				continue
			}
			if msg.Channel == storage.TopologyEventChannel {
				if updated, err := c.loadTopology(); err != nil {
					log.Printf("[topology] 收到刷新通知后加载失败: %v", err)
				} else if updated {
					log.Printf("[topology] 已通过通知刷新路由快照")
				}
				continue
			}

			c.eventMu.Lock()
			if msg.Channel == "events:global" {
				c.globalEvents = append(c.globalEvents, msg.Payload)
				if len(c.globalEvents) > 3 {
					c.globalEvents = c.globalEvents[len(c.globalEvents)-3:]
				}
			} else if strings.HasPrefix(msg.Channel, "events:map:") {
				mapID := strings.TrimPrefix(msg.Channel, "events:map:")
				buf := append(c.mapEvents[mapID], msg.Payload)
				if len(buf) > 3 {
					buf = buf[len(buf)-3:]
				}
				c.mapEvents[mapID] = buf
			} else if strings.HasPrefix(msg.Channel, "events:user:") {
				username := strings.TrimPrefix(msg.Channel, "events:user:")
				buf := append(c.userEvents[username], msg.Payload)
				if len(buf) > 2 {
					buf = buf[len(buf)-2:]
				}
				c.userEvents[username] = buf
			}
			c.eventMu.Unlock()
		case <-c.stopCh:
			c.pubsub.Close()
			return
		}
	}
}
