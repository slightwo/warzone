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

type MapCacheData struct {
	View  *protocol.MapView
	Brief protocol.MapBrief
}

type Cluster struct {
	mu          sync.RWMutex
	store       *storage.Store
	nodes       map[string]gatewayNodeClient
	nodeAddrs   map[string]string
	nodeTargets map[string]string
	nodeFactory gatewayNodeClientFactory
	activeNodes func() ([]storage.NodeRegistryInfo, error)
	closeOnce   sync.Once
	closed      bool
	// owners 与 replicas 是已提交拓扑的网关只读缓存，只能由 applyTopologyLocked 写入。
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
	return newCluster(store, func(nodeID, addr string) (gatewayNodeClient, error) {
		return NewNodeGRPCClient(nodeID, addr)
	})
}

func newCluster(store *storage.Store, nodeFactory gatewayNodeClientFactory) (*Cluster, error) {
	if store == nil {
		return nil, errors.New("gateway store 不能为空")
	}
	if nodeFactory == nil {
		return nil, errors.New("gateway node client factory 不能为空")
	}
	c := &Cluster{
		store:       store,
		nodes:       make(map[string]gatewayNodeClient),
		nodeAddrs:   make(map[string]string),
		nodeTargets: make(map[string]string),
		nodeFactory: nodeFactory,
		activeNodes: store.GetActiveNodes,
		owners:      make(map[string]string),
		replicas:    make(map[string]string),
		configs:     make(map[string]world.MapConfig),
		stopCh:      make(chan struct{}),
		mapCache:    make(map[string]MapCacheData),
		mapEvents:   make(map[string][]string),
		userEvents:  make(map[string][]string),
	}

	for _, cfg := range world.AvailableMaps() {
		c.configs[cfg.ID] = cfg
	}

	// Boss 初始化与复活均由 coordinator 处理；网关仅在缓存循环中读取当前状态。
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
	return nil
}
func (c *Cluster) Close() {
	c.closeOnce.Do(func() {
		close(c.stopCh)
		c.mu.Lock()
		c.closed = true
		nodes := make([]gatewayNodeClient, 0, len(c.nodes))
		for _, node := range c.nodes {
			nodes = append(nodes, node)
		}
		c.nodes = make(map[string]gatewayNodeClient)
		c.nodeAddrs = make(map[string]string)
		c.nodeTargets = make(map[string]string)
		c.mu.Unlock()
		for _, node := range nodes {
			_ = node.Close()
		}
	})
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
	epoch := c.topology.MapEpochs[mapID]
	owner := c.nodes[ownerID]
	topologyLoaded := c.topologyLoaded
	c.mu.Unlock()

	if !topologyLoaded || ownerID == "" || epoch == 0 || owner == nil {
		return nil, errors.New("路由尚未就绪，请等待 coordinator 提交拓扑")
	}

	if err := owner.AddPlayer(context.Background(), mapID, epoch, profile); err != nil {
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
	session, ok := c.store.LoadGlobalSession(username)
	if !ok {
		return nil
	}

	c.mu.RLock()
	ownerID := c.owners[session.MapID]
	epoch := c.topology.MapEpochs[session.MapID]
	node := c.nodes[ownerID]
	topologyLoaded := c.topologyLoaded
	c.mu.RUnlock()
	if topologyLoaded && ownerID != "" && epoch != 0 && node != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		profile, removed, err := node.RemovePlayer(ctx, session.MapID, username, epoch)
		cancel()
		if err != nil {
			log.Printf("[logout] 节点 %s 移除玩家 %s 失败: %v", ownerID, username, err)
		} else if removed {
			profile.LastNode = ownerID
			profile.LastMap = session.MapID
			_ = c.store.SaveProfile(profile)
		}
	}
	_ = c.store.DeleteGlobalSession(username)
	c.localSessions.Delete(username)
	return c.store.DeleteHotSession(username)
}

func (c *Cluster) Move(username, dir string) (*protocol.WorldState, error) {
	session, node, epoch, err := c.sessionNode(username)
	if err != nil {
		return nil, err
	}
	_, _, ok, err := node.MovePlayer(context.Background(), session.MapID, username, dir, epoch)
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
	session, node, epoch, err := c.sessionNode(username)
	if err != nil {
		return nil, err
	}
	event, targetUsername, targetEvent, _, ok, err := node.Attack(context.Background(), session.MapID, username, epoch)
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
	session, node, epoch, err := c.sessionNode(username)
	if err != nil {
		return nil, err
	}
	event, _, ok, err := node.Heal(context.Background(), session.MapID, username, epoch)
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
	session, node, epoch, err := c.sessionNode(username)
	if err != nil {
		return nil, err
	}
	event, profile, ok, err := node.BuyItem(context.Background(), session.MapID, username, item, epoch)
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
	session, node, epoch, err := c.sessionNode(username)
	if err != nil {
		return nil, err
	}
	event, _, ok, err := node.AttackBoss(context.Background(), session.MapID, username, epoch)
	if err != nil || !ok {
		return nil, err
	}
	c.broadcastGlobalEvent(event)

	return c.SnapshotFor(username)
}

func (c *Cluster) SwitchMap(username, targetMap string) (*protocol.WorldState, error) {
	session, srcNode, srcEpoch, err := c.sessionNode(username)
	if err != nil {
		return nil, err
	}
	c.mu.RLock()
	profile, ok, err := srcNode.Profile(context.Background(), session.MapID, username)
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
	dstEpoch := c.topology.MapEpochs[targetMap]
	dst := c.nodes[dstNodeID]
	topologyLoaded := c.topologyLoaded
	c.mu.RUnlock()
	if !topologyLoaded || dstNodeID == "" || dstEpoch == 0 || dst == nil {
		return nil, fmt.Errorf("目标地图 %q 路由尚未就绪，请等待 coordinator 提交拓扑", targetMap)
	}

	profile, ok, err = srcNode.RemovePlayer(context.Background(), session.MapID, username, srcEpoch)
	if err != nil {
		return nil, fmt.Errorf("源节点RPC移除玩家异常 (Try失败): %v", err)
	}
	if !ok {
		return nil, errors.New("源节点移除玩家失败 (Try被拒绝)")
	}

	// 执行 TCC 逻辑的 Confirm / Cancel 阶段
	// 尝试向目标节点写入 (Confirm)
	if err := dst.AddPlayer(context.Background(), targetMap, dstEpoch, &profile); err != nil {
		// Cancel 阶段: 目标写入失败，必须进行业务回滚，将玩家恢复至原源节点
		fmt.Printf("[!!!严重警告!!!] 玩家 %s 切换地图 %s 在目标节点写入失败 (%v)，开始执行 TCC 回滚...\n", username, targetMap, err)

		rollbackErr := srcNode.AddPlayer(context.Background(), session.MapID, srcEpoch, &profile)
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
	session, node, _, err := c.sessionNode(username)
	if err != nil {
		return nil, err
	}
	sessionVersion := session.Version

	c.mu.RLock()
	topologyVersion := c.topology.Version
	mapEpoch := c.topology.MapEpochs[session.MapID]
	c.mu.RUnlock()

	ws := protocol.AllocWorldState()
	ws.SessionVersion = sessionVersion
	ws.TopologyVersion = topologyVersion
	ws.MapEpoch = mapEpoch

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
	configs := c.configs
	nodeViews := c.gatewayNodeViewsLocked()
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

	ws.Nodes = append(ws.Nodes[:0], nodeViews...)

	ws.Self = self
	ws.Map = mapView
	ws.Boss = bossView

	return ws, nil
}

// sessionNode 从同一份已提交 Topology 读取当前 map owner 与 epoch。会话中的 NodeID
// 仅是持久化定位信息；写入路由永远以最新 Topology 为准，避免 failover 后继续向旧主发送。
func (c *Cluster) sessionNode(username string) (*storage.GlobalSession, GatewayNodeClient, uint64, error) {
	var session *storage.GlobalSession
	if cached, ok := c.localSessions.Load(username); ok {
		session = cached.(*storage.GlobalSession)
	} else {
		loaded, found := c.store.LoadGlobalSession(username)
		if !found {
			return nil, nil, 0, fmt.Errorf("用户 %q 当前不在线", username)
		}
		session = loaded
		c.localSessions.Store(username, loaded)
	}

	c.mu.RLock()
	ownerID := c.owners[session.MapID]
	epoch := c.topology.MapEpochs[session.MapID]
	node := c.nodes[ownerID]
	topologyLoaded := c.topologyLoaded
	c.mu.RUnlock()
	if !topologyLoaded || ownerID == "" || epoch == 0 || node == nil {
		return nil, nil, 0, fmt.Errorf("地图 %q 当前路由不可用", session.MapID)
	}
	copySession := *session
	copySession.NodeID = ownerID
	return &copySession, node, epoch, nil
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
			topologyVersion := c.topology.Version
			nodes := make(map[string]gatewayNodeClient)
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
				if host == nil {
					continue
				}
				cfg := configs[mapID]
				wg.Add(1)
				go func(mapID string, host GatewayNodeClient, name string) {
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

			// 只允许仍对应当前 topology 的快照回填缓存。旧 owner 的慢 Snapshot 在
			// failover 后返回时会被丢弃，不能覆盖已清空的新路由缓存。
			c.mu.RLock()
			versionCurrent := c.topologyLoaded && c.topology.Version == topologyVersion
			c.mu.RUnlock()
			if !versionCurrent {
				continue
			}
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
