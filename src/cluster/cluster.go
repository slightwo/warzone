package cluster

import (
	"context"
	"errors"
	"fmt"
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
	RemoveHostedMap(mapID string)
	InstallPrimaryMap(cfg world.MapConfig)
	RestorePrimaryMap(cfg world.MapConfig, cp protocol.MapCheckpoint)
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
	BackgroundStep() []protocol.MapEvents
	StoreReplica(cp protocol.MapCheckpoint)
	Promote(mapID string, cfg world.MapConfig) error
	View() protocol.NodeView
	IsHealthy() bool
	SetHealthy(healthy bool) bool
}

type Cluster struct {
	mu       sync.RWMutex
	store    *storage.Store
	nodes    map[string]NodeClient
	owners   map[string]string          //key==mapID
	replicas map[string]string          //..
	configs  map[string]world.MapConfig //..
	stopCh   chan struct{}

	// to understand事件分离：纯内存通道，各个无状态网关实例自行订阅并缓冲
	eventMu      sync.RWMutex
	globalEvents []string
	mapEvents    map[string][]string
	userEvents   map[string][]string
	pubsub       *redis.PubSub
}

var studentTodoNotice sync.Map

func NewCluster(store *storage.Store) (*Cluster, error) {
	c := &Cluster{
		store:      store,
		nodes:      make(map[string]NodeClient),
		owners:     make(map[string]string),
		replicas:   make(map[string]string),
		configs:    make(map[string]world.MapConfig),
		stopCh:     make(chan struct{}),
		mapEvents:  make(map[string][]string), //to understand
		userEvents: make(map[string][]string), //
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
	go c.discoveryLoop()
	go c.backgroundLoop()
	go c.heartbeatLoop()
	go c.checkpointLoop()
	return nil
}
func (c *Cluster) Close() {
	c.mu.RLock()
	// 获取所有地图的 owner 和 replica 映射
	owners := make(map[string]string)
	for mapID, ownerID := range c.owners {
		owners[mapID] = ownerID
	}
	replicas := make(map[string]string)
	for mapID, replicaID := range c.replicas {
		replicas[mapID] = replicaID
	}
	c.mu.RUnlock()

	for mapID, nodeID := range owners {
		owner := c.nodes[nodeID]
		if !owner.IsHealthy() {
			continue
		}
		CP, _ := owner.Checkpoint(context.Background(), mapID)
		CP.Players = []protocol.PlayerView{}
		_ = c.store.SaveCheckpoint(CP)
		replicaID := replicas[mapID]
		replica := c.nodes[replicaID]
		if replica != nil && replica.IsHealthy() {
			replica.StoreReplica(CP)
		}
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
	c.mu.Unlock()
	//to under
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
	event, _, ok, err := node.MovePlayer(context.Background(), session.MapID, username, dir)
	if err != nil {
		return nil, fmt.Errorf("节点RPC移动调用失败: %v", err)
	}
	if !ok {
		return nil, errors.New("移动请求被拒绝")
	}
	c.pushEvent(username, event)
	// profile.LastNode = session.NodeID
	// profile.LastMap = session.MapID
	//	_ = c.store.SaveProfile(profile)
	//	_ = c.persistSessionState(username)
	return c.SnapshotFor(username)
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
	//fmt.Println("[debug] 任务下发至node")
	event, _, ok, err := node.AttackBoss(context.Background(), session.MapID, username)
	if err != nil || !ok {
		//fmt.Println("[debug] 任务失败，c.attackboss,err:", err)
		return nil, err
	}
	//fmt.Println("[debug] 任务正常结束，event:", event)
	c.broadcastGlobalEvent(event)

	if strings.Contains(event, "已经死亡！终结者：") {
		bState, _, _ := c.store.LoadGlobalBoss()
		go c.respawnBossAfterCooldown(bState.Name)
	}

	return c.SnapshotFor(username)
}

func (c *Cluster) SwitchMap(username, targetMap string) (*protocol.WorldState, error) {
	// TODO(Labc.mu.Unlock()3-2):
	// 这里需要实现“跨地图切换 + 节点路由迁移”。
	// 至少要处理：

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
		return nil, fmt.Errorf("源节点RPC移除玩家异常: %v", err)
	}
	if !ok {
		return nil, errors.New("源节点移除玩家失败")
	}

	_, stillExists, err := src_node.Profile(context.Background(), session.MapID, username)
	if err != nil {
		fmt.Printf("[ERROR] 源节点RPC验证玩家存在异常: %v\n", err)
	} else if stillExists {
		fmt.Printf("[WARN] 移除后玩家仍然存在于源地图！\n")
	}

	if err := dst.AddPlayer(context.Background(), targetMap, &profile); err != nil {
		return nil, fmt.Errorf("目标节点添加玩家失败: %v", err)
	}
	profile, ok, err = dst.Profile(context.Background(), targetMap, username)
	if err != nil {
		return nil, fmt.Errorf("获取移动后用户信息异常: %v", err)
	}
	if !ok {
		return nil, errors.New("获取移动后用户信息失败")
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
	}

	c.mu.Unlock()

	if ok && session != nil {
		c.pushEventLocked(session, fmt.Sprintf("切换到地图 %s", targetMap))
	}
	_ = c.store.SaveProfile(profile)
	// _ = c.persistSessionState(username) // Removed

	return c.SnapshotFor(username)
	// 1. 从源节点摘除玩家热状态。
	// 2. 根据 owners 路由把玩家挂到目标地图主节点。
	// 3. 更新 session.MapID / session.NodeID。
	// 4. 将新的位置、地图、节点落盘到冷热数据。
	//return nil, studentTODOError("Lab3-2", "cluster.SwitchMap", "完成跨地图路由与会话迁移")
}

func (c *Cluster) SnapshotFor(username string) (*protocol.WorldState, error) {
	c.mu.RLock()
	session, ok := c.store.LoadGlobalSession(username)
	if !ok {
		c.mu.RUnlock()
		return nil, fmt.Errorf("用户 %q 当前不在线", username)
	}
	node := c.nodes[session.NodeID]
	sessionVersion := session.Version

	ws := protocol.AllocWorldState()
	ws.SessionVersion = sessionVersion

	//to under
	c.eventMu.RLock()
	ws.Events = ws.Events[:0]
	//fmt.Println("[debug]", c.globalEvents)
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
	// events := ws.Events
	//
	owners := make(map[string]string, len(c.owners))
	for k, v := range c.owners {
		owners[k] = v
	}
	nodes := make(map[string]NodeClient, len(c.nodes))
	for k, v := range c.nodes {
		nodes[k] = v
	}
	c.mu.RUnlock()

	bState, hp, _ := c.store.LoadGlobalBoss()
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
		return nil, errors.New("当前承载节点已不可用")
	}

	mapView, err := node.Snapshot(context.Background(), session.MapID)
	if err != nil {
		return nil, fmt.Errorf("节点RPC获取地图快照异常: %v", err)
	}

	self := protocol.PlayerView{}
	for _, player := range mapView.Players {
		if player.Username == username {
			self = player
			break
		}
	}

	mapIDs := make([]string, 0, len(c.configs))
	for mapID := range c.configs {
		mapIDs = append(mapIDs, mapID)
	}
	sort.Strings(mapIDs)

	ws.Maps = ws.Maps[:0]
	for _, mapID := range mapIDs {
		ownerID := owners[mapID]
		host := nodes[ownerID]
		if host == nil {
			continue
		}
		players, npcs, treasures, version, err := host.Counts(context.Background(), mapID)
		if err != nil {
			fmt.Printf("[WARN] 节点RPC获取地图状态(Counts)异常: %v\n", err)
			continue
		}
		cfg := c.configs[mapID]
		ws.Maps = append(ws.Maps, protocol.MapBrief{
			ID:        mapID,
			Name:      cfg.Name,
			NodeID:    ownerID,
			Players:   players,
			NPCs:      npcs,
			Treasures: treasures,
			Version:   version,
			Primary:   true,
			IsCurrent: mapID == session.MapID,
		})
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
	c.mu.RLock()
	defer c.mu.RUnlock()

	session, ok := c.store.LoadGlobalSession(username)
	if !ok {
		return nil, nil, fmt.Errorf("用户 %q 当前不在线", username)
	}
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
	// to under
	_ = c.store.PublishEvent("events:user:"+username, event)
}

// to under
func (c *Cluster) pushEventLocked(session *storage.GlobalSession, event string) {
	if event == "" {
		return
	}
	_ = c.store.PublishEvent("events:user:"+session.Username, event)
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

			c.mu.Lock()
			for _, info := range activeNodes {
				// 如果是新节点，则建立链接并初始化地图分布
				if _, ok := c.nodes[info.ID]; !ok {
					client, err := NewNodeGRPCClient(info.ID, info.Addr)
					if err == nil {
						c.nodes[info.ID] = client

						// 将该节点宣称自己负责的地图加入 owners 列表
						for _, mapID := range info.Maps {
							cfg := c.configs[mapID]

							// 只处理存在的合法地图类型
							if cfg.ID != "" {
								c.owners[mapID] = info.ID
								fmt.Printf("[Cluster 节点发现] 新节点上线: %s, 负责接管地图: %s\n", info.ID, mapID)

								// 把最新的全服快照推送给它
								if cp, ok := c.store.LoadCheckpoint(mapID); ok {
									client.RestorePrimaryMap(cfg, *cp)
								} else {
									client.InstallPrimaryMap(cfg)
								}
							}
						}
						// 调用它的 Start，为了符合 NodeClient 接口规范
						_ = client.Start()
					}
				}
			}
			c.mu.Unlock()

		case <-c.stopCh:
			return
		}
	}
}

func (c *Cluster) backgroundLoop() {
	ticker := time.NewTicker(700 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			nodeIDs := make([]string, 0, len(c.nodes))
			c.mu.RLock()
			for nodeID, node := range c.nodes {
				if node.IsHealthy() {
					nodeIDs = append(nodeIDs, nodeID)
				}
			}
			sort.Strings(nodeIDs)
			nodes := make([]NodeClient, 0, len(nodeIDs))
			for _, nodeID := range nodeIDs {
				nodes = append(nodes, c.nodes[nodeID])
			}
			c.mu.RUnlock()

			for _, node := range nodes {
				for _, bundle := range node.BackgroundStep() {
					for _, event := range bundle.Events {
						c.broadcastMapEvent(bundle.MapID, event)
					}
				}
			}
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

func (c *Cluster) checkpointLoop() {
	ticker := time.NewTicker(700 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:

			c.mu.RLock()
			// 获取所有地图的 owner 和 replica 映射
			owners := make(map[string]string)
			for mapID, ownerID := range c.owners {
				owners[mapID] = ownerID
			}
			replicas := make(map[string]string)
			for mapID, replicaID := range c.replicas {
				replicas[mapID] = replicaID
			}
			c.mu.RUnlock()

			for mapID, nodeID := range owners {
				owner := c.nodes[nodeID] //c.nodes是静态的，不存在被修改的风险
				if !owner.IsHealthy() {
					continue
				}
				CP, _ := owner.Checkpoint(context.Background(), mapID)

				_ = c.store.SaveCheckpoint(CP)
				replicaID := replicas[mapID]
				replica := c.nodes[replicaID]
				if replica != nil && replica.IsHealthy() {
					replica.StoreReplica(CP)
				}
			}

		case <-c.stopCh:
			return
		}
	}

	// 这里要实现“主节点定期生成检查点，并复制给副本节点”。
	// 至少要包含：
	// 1. 从 owners 找到每张地图当前主节点。
	// 2. 抓取主节点地图快照。
	// 3. 同时写入本地检查点存储与 replica 节点内存。
	// 4. 跳过故障节点，避免把坏状态继续扩散。

}

func (c *Cluster) handleNodeFailure(nodeID string) {
	// TODO(Lab3-5):
	// 这里需要完成“主节点故障 -> 副本提升 -> 会话重路由”。
	// 最关键的步骤是：
	// 1. 找到故障节点承载的所有主地图。
	// 2. 选择对应副本并提升为新主节点。
	// 3. 更新 owners / replicas 元数据。
	// 4. 修正所有受影响玩家会话的 NodeID，并广播故障切换事件。
	node := c.nodes[nodeID]
	if node == nil {
		return
	}

	map2Change := node.View().PrimaryMaps
	c.mu.Lock()
	for _, mapID := range map2Change {
		replicasID := c.replicas[mapID]
		replicasNode := c.nodes[replicasID]
		if replicasNode == nil || !replicasNode.IsHealthy() {
			continue
		}
		replicasNode.Promote(mapID, c.configs[mapID])
		delete(c.replicas, mapID)
		newReplicaID := c.pickReplicaLocked(replicasID)
		c.replicas[mapID] = newReplicaID
		c.owners[mapID] = replicasID

	}
	sessions, _ := c.store.GetAllGlobalSessions()
	for _, session := range sessions {
		if session.NodeID == nodeID {
			session.NodeID = c.owners[session.MapID]
			c.store.SaveGlobalSession(session)
		}
	}
	c.mu.Unlock()
	c.broadcastGlobalEvent(fmt.Sprintf("%q 发生故障，已转移其他节点负责", nodeID))

	//logStudentTODO("Lab3-5", "cluster.handleNodeFailure", "完成主节点故障后的副本提升与会话重路由")
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
		_ = c.store.SaveCheckpoint(cp)
		if replica, ok := c.nodes[replicas[mapID]]; ok {
			replica.StoreReplica(cp)
		}
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
		if cp, ok := c.store.LoadCheckpoint(mapID); ok {
			node.StoreReplica(*cp)
			c.replicas[mapID] = nodeID
		}
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
	//to under
	ch := c.pubsub.Channel()
	for {
		select {
		case msg := <-ch:
			if msg == nil {
				continue
			}
			c.eventMu.Lock()
			if msg.Channel == "events:global" {
				//fmt.Println("[debug]收到全局消息")
				c.globalEvents = append(c.globalEvents, msg.Payload)
				if len(c.globalEvents) > 3 {
					c.globalEvents = c.globalEvents[len(c.globalEvents)-3:]
				}
			} else if strings.HasPrefix(msg.Channel, "events:map:") {
				//fmt.Println("[debug]收到map消息")
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
