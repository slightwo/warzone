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
	mu       sync.RWMutex
	store    *storage.Store
	nodes    map[string]NodeClient
	owners   map[string]string          //key==mapID
	replicas map[string]string          //..
	configs  map[string]world.MapConfig //..
	stopCh   chan struct{}

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
		store:      store,
		nodes:      make(map[string]NodeClient),
		owners:     make(map[string]string),
		replicas:   make(map[string]string),
		configs:    make(map[string]world.MapConfig),
		stopCh:     make(chan struct{}),
		mapCache:   make(map[string]MapCacheData),
		mapEvents:  make(map[string][]string),
		userEvents: make(map[string][]string),
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

	// 拓扑结构不再硬编码，而是由 discoveryLoop 动态从 Redis 发现节点并接管
	c.pubsub = store.SubscribeEvents()
	go c.eventLoop()
	return c, nil
}

func (c *Cluster) Start() error {
	for _, node := range c.nodes {
		if err := node.Start(); err != nil {
			return err
		}
	}
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

	if owner == nil {
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

	node := c.nodes[session.NodeID]
	if node == nil {
		return c.store.DeleteHotSession(username)
	}
	profile, removed, err := node.RemovePlayer(context.Background(), session.MapID, username)
	if err != nil {
		fmt.Printf("[ERROR] RPC Node RemovePlayer failed: %v\n", err)
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
	dst_node := c.owners[targetMap]
	dst := c.nodes[dst_node]
	c.mu.RUnlock()

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
	profile.LastNode = dst_node

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

		if node == nil {
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
	if node == nil {
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
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			activeNodes, err := c.store.GetActiveNodes()
			if err != nil {
				continue
			}

			// 改进 1：先使用只读锁(RLock)快速扫描出尚未注册的“新节点”结构
			var newInfos []storage.NodeRegistryInfo
			c.mu.RLock()
			for _, info := range activeNodes {
				if _, ok := c.nodes[info.ID]; !ok {
					newInfos = append(newInfos, info)
				}
			}
			c.mu.RUnlock()

			// 改进 2：对于每一个新节点，启动独立的异步协程处理网络连接和推送快照，不再阻塞 Cluster 主循环
			for _, info := range newInfos {
				go func(nInfo storage.NodeRegistryInfo) {
					// 独立发起耗时的网络握手
					client, err := NewNodeGRPCClient(nInfo.ID, nInfo.Addr)
					if err != nil {
						// 连接失败直接返回，下次轮询时该节点如果在Redis且未注册还会被发现并重试
						return
					}

					// 【修复点】：确认新节点确实存活。因为Redis里可能有并未过期的已宕机脏数据
					ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
					defer cancel()
					if err := client.Ping(ctx); err != nil {
						return
					}

					// 调用它的 Start
					if err := client.Start(); err != nil {
						log.Printf("[节点发现] 启动节点客户端 %s 失败: %v", nInfo.ID, err)
						return
					}

					// 获取短写锁，将验证好的新节点挂入路由拓扑 (不在此锁内发 RPC)
					c.mu.Lock()
					// 需做二次校验以防御重入
					if _, exists := c.nodes[nInfo.ID]; exists {
						c.mu.Unlock()
						return
					}
					c.nodes[nInfo.ID] = client

					// 【前提假设：保证无冲突抢占】分配该节点声明的各种地图映射
					for _, mapID := range nInfo.Maps {
						cfg := c.configs[mapID]
						if cfg.ID != "" {
							c.owners[mapID] = nInfo.ID
							fmt.Printf("[Cluster 节点发现] 新节点上线: %s, 负责接管地图: %s\n", nInfo.ID, mapID)
						}
					}
					// 【新增】分配该节点声明的副本映射
					for _, mapID := range nInfo.Replicas {
						cfg := c.configs[mapID]
						if cfg.ID != "" {
							c.replicas[mapID] = nInfo.ID
							fmt.Printf("[Cluster 节点发现] 新节点上线: %s, 负责副本同步: %s\n", nInfo.ID, mapID)
						}
					}
					c.mu.Unlock()

					// 节点自治：地图数据由节点启动时自拉 checkpoint 恢复（见 cmd/node/main.go），
					// 协调器不再向节点推送快照。
				}(info)
			}

		case <-c.stopCh:
			return
		}
	}
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

func (c *Cluster) handleNodeFailure(nodeID string) {
	c.mu.Lock()
	var map2Change []string
	for mapID, ownerID := range c.owners {
		if ownerID == nodeID {
			map2Change = append(map2Change, mapID)
		}
	}

	var replicaMaps2Change []string
	for mapID, replicaID := range c.replicas {
		if replicaID == nodeID {
			replicaMaps2Change = append(replicaMaps2Change, mapID)
		}
	}

	for _, mapID := range map2Change {
		replicasID := c.replicas[mapID]
		replicasNode := c.nodes[replicasID]
		if replicasNode == nil || !replicasNode.IsHealthy() {
			continue
		}
		if err := replicasNode.Promote(mapID, c.configs[mapID]); err != nil {
			log.Printf("[failover] 副本提升 %s 失败: %v", mapID, err)
			continue
		}
		delete(c.replicas, mapID)
		newReplicaID := c.pickReplicaLocked(replicasID)
		c.replicas[mapID] = newReplicaID
		c.owners[mapID] = replicasID
	}

	for _, mapID := range replicaMaps2Change {
		newReplicaID := c.pickReplicaLocked(c.owners[mapID])
		c.replicas[mapID] = newReplicaID

	}
	sessions, err := c.store.GetAllGlobalSessions()
	if err != nil {
		log.Printf("[failover] 获取全局会话列表失败: %v", err)
	} else {
		for _, session := range sessions {
			if session.NodeID == nodeID {
				session.NodeID = c.owners[session.MapID]
				c.store.SaveGlobalSession(session)

				// 节点失效时的补救：更新内存缓存
				if val, ok := c.localSessions.Load(session.Username); ok {
					cachedSession := val.(*storage.GlobalSession)
					cachedSession.NodeID = session.NodeID
					c.localSessions.Store(session.Username, cachedSession)
				}
			}
		}
	}
	// 【新增】从注册表中物理移除该失效节点
	delete(c.nodes, nodeID)
	c.mu.Unlock()
	c.broadcastGlobalEvent(fmt.Sprintf("%q 发生故障，已转移其他节点负责", nodeID))
}

func (c *Cluster) pickReplicaLocked(ownerID string) string {
	nodeIDs := make([]string, 0, len(c.nodes))
	for nodeID := range c.nodes {
		nodeIDs = append(nodeIDs, nodeID)
	}
	sort.Strings(nodeIDs)
	for _, nodeID := range nodeIDs {
		if nodeID == ownerID {
			continue
		}
		if c.nodes[nodeID].IsHealthy() {
			return nodeID
		}
	}
	return ""
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
	replicas := make(map[string]string, len(c.replicas))
	for mapID, replicaID := range c.replicas {
		replicas[mapID] = replicaID
	}
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
	c.broadcastGlobalEvent(fmt.Sprintf("管理命令：已模拟 %s 故障，集群开始故障转移", nodeID))
	return fmt.Sprintf("节点 %s 已被标记为故障，并触发主从切换", nodeID), nil
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

	c.mu.Lock()
	for mapID, ownerID := range c.owners {
		if ownerID == nodeID {
			continue
		}
		// 副本数据由节点 replicaSyncLoop 自拉，此处只恢复副本拓扑关系。
		c.replicas[mapID] = nodeID
	}
	c.mu.Unlock()

	c.broadcastGlobalEvent(fmt.Sprintf("管理命令：节点 %s 已恢复在线，并重新接管副本同步", nodeID))
	return fmt.Sprintf("节点 %s 已恢复，最新快照已重新装载", nodeID), nil
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
