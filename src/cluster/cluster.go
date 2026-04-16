package cluster

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"battleworld/protocol"
	"battleworld/storage"
	"battleworld/world"
)

type Session struct {
	Username string
	MapID    string
	NodeID   string
	Events   []string
	Version  int64
}

type BossState struct {
	Name         string
	HP           int
	MaxHP        int
	Alive        bool
	LastHit      string
	RespawnAt    time.Time
	Sites        []protocol.BossSite
	Version      int64
	Contributors map[string]int
}

// NodeClient 代表向一个节点发起命令的客户端抽象，允许未来替换成网络版 (NodeGRPCClient)
type NodeClient interface {
	NodeID() string
	Start() error
	Stop() error
	RemoveHostedMap(mapID string)
	InstallPrimaryMap(cfg world.MapConfig)
	RestorePrimaryMap(cfg world.MapConfig, cp protocol.MapCheckpoint)
	AddPlayer(ctx context.Context, mapID string, profile *protocol.UserProfile) error
	RemovePlayer(ctx context.Context, mapID, username string) (protocol.UserProfile, bool, error)
	MovePlayer(ctx context.Context, mapID, username, dir string) (string, protocol.UserProfile, bool, error)
	Attack(ctx context.Context, mapID, username string) (string, string, string, protocol.UserProfile, bool, error)
	Heal(ctx context.Context, mapID, username string) (string, protocol.UserProfile, bool, error)
	BuyItem(ctx context.Context, mapID, username, item string) (string, protocol.UserProfile, bool, error)
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
	sessions map[string]*Session        //key==username
	owners   map[string]string          //key==mapID
	replicas map[string]string          //..
	configs  map[string]world.MapConfig //..
	boss     *BossState
	stopCh   chan struct{}
}

var studentTodoNotice sync.Map

func NewCluster(store *storage.Store) (*Cluster, error) {
	c := &Cluster{
		store:    store,
		nodes:    make(map[string]NodeClient),
		sessions: make(map[string]*Session),
		owners:   make(map[string]string),
		replicas: make(map[string]string),
		configs:  make(map[string]world.MapConfig),
		boss:     newBossState(),
		stopCh:   make(chan struct{}),
	}

	for _, cfg := range world.AvailableMaps() {
		c.configs[cfg.ID] = cfg
	}
	c.boss.Sites = c.buildBossSites()

	// TODO: Stage 5 - Read topology from config or arguments instead of hardcoding.
	// We establish gRPC clients to connect to remote node processes.
	nodeA, err := NewNodeGRPCClient("node-a", "127.0.0.1:9311")
	if err != nil {
		return nil, err
	}
	c.nodes["node-a"] = nodeA

	nodeB, err := NewNodeGRPCClient("node-b", "127.0.0.1:9312")
	if err != nil {
		return nil, err
	}
	c.nodes["node-b"] = nodeB

	nodeC, err := NewNodeGRPCClient("node-c", "127.0.0.1:9313")
	if err != nil {
		return nil, err
	}
	c.nodes["node-c"] = nodeC

	assignments := map[string]struct {
		owner   string
		replica string
	}{
		"green": {owner: "node-a", replica: "node-c"},
		"cave":  {owner: "node-b", replica: "node-c"},
		"ruins": {owner: "node-a", replica: "node-b"},
	}

	for mapID, placement := range assignments {
		cfg := c.configs[mapID]
		c.owners[mapID] = placement.owner
		c.replicas[mapID] = placement.replica
		c.nodes[placement.owner].InstallPrimaryMap(cfg)
		if cp, ok := store.LoadCheckpoint(mapID); ok {
			c.nodes[placement.owner].RestorePrimaryMap(cfg, *cp)
			c.nodes[placement.replica].StoreReplica(*cp)
		}
	}

	return c, nil
}

func (c *Cluster) Start() error {
	for _, node := range c.nodes {
		if err := node.Start(); err != nil {
			return err
		}
	}
	go c.backgroundLoop()
	go c.heartbeatLoop()
	go c.checkpointLoop()
	go c.flushLoop()
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
		if replica.IsHealthy() && replica != nil {
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
	if _, ok := c.sessions[username]; ok {
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
	session := &Session{
		Username: username,
		MapID:    mapID,
		NodeID:   ownerID,
		Version:  1,
		Events: []string{
			fmt.Sprintf("欢迎回来，%s", username),
			fmt.Sprintf("当前地图 %s 由 %s 承载", mapID, ownerID),
		},
	}
	c.sessions[username] = session
	c.mu.Unlock()

	_ = c.persistSessionState(username)
	return c.SnapshotFor(username)
}

func (c *Cluster) Logout(username string) error {
	c.mu.Lock()
	session, ok := c.sessions[username]
	if !ok {
		c.mu.Unlock()
		return nil
	}
	delete(c.sessions, username)
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
	_ = c.persistSessionState(username)
	return c.SnapshotFor(username)
}
func (c *Cluster) AttackBoss(username string) (*protocol.WorldState, error) {
	// TODO(Lab3-1):
	// 这里需要把“世界首领”做成跨地图、跨节点共享的全局热状态。
	// 要求至少完成：
	session, node, err := c.sessionNode(username)
	if err != nil {
		return nil, err
	}

	profile, ok, err := node.Profile(context.Background(), session.MapID, username)
	if err != nil {
		return nil, fmt.Errorf("节点RPC获取Profile异常: %v", err)
	}
	if !profile.Alive {
		return nil, errors.New("倒地时无法攻击")
	}
	if !ok {
		return nil, errors.New("获取玩家profile失败")
	}

	c.mu.RLock()
	if !c.boss.Alive {
		c.mu.RUnlock()
		return nil, errors.New("等待首领复活")
	}

	bossSite, ok := c.bossSite(session.MapID)
	if !ok {
		c.mu.RUnlock()
		return nil, errors.New("当前地图没有首领投影")
	}

	// 1. 校验玩家和当前地图的首领投影距离，太远则拒绝攻击。
	if manhattan(bossSite.X, bossSite.Y, profile.X, profile.Y) > protocol.BossAtkRange {
		c.mu.RUnlock()
		return nil, fmt.Errorf("首领在范围外,剩余HP:%d", c.boss.HP)
	}
	// 2. 对全局首领 HP 做单写者更新，避免多个节点并发扣血产生不一致。
	c.mu.RUnlock()
	damadge := profile.Attack
	c.mu.Lock()
	c.boss.HP -= damadge
	c.boss.Contributors[username] += damadge
	if c.boss.HP <= 0 {

		c.boss.Alive = false
		//c.boss.Contributors[username] -= (damadge - c.boss.HP)
		c.boss.HP = 0
		c.boss.LastHit = username
		c.broadcastGlobalEventLocked(fmt.Sprintf("boss:%q,已经死亡！终结者：%q", c.boss.Name, username))
		go c.respawnBossAfterCooldown()
	}

	bossTmp := c.boss
	c.mu.Unlock()

	c.broadcastGlobalEvent(fmt.Sprintf("%q 对 %q 造成了 %d 点伤害！剩余HP：%d", username, bossTmp.Name, damadge, bossTmp.HP))
	if !bossTmp.Alive {

		//奖励参与者
		for name, con := range bossTmp.Contributors {
			c.mu.RLock()
			session := c.sessions[name]
			c.mu.RUnlock()
			profile, ok, err := node.RewardPlayer(context.Background(), session.MapID, name, con, con) //,name,contri,contri)
			if err != nil {
				fmt.Printf("[ERROR] RPC RewardPlayer 异常: %v\n", err)
			} else if ok {
				c.pushEvent(name, "您参与了boss攻略战！")
				profile.LastNode = session.NodeID
				profile.LastMap = session.MapID
				_ = c.store.SaveProfile(profile)
				// _ = c.persistSessionState(name)
			}
		}
	}
	// profile.LastNode = session.NodeID
	// profile.LastMap = session.MapID
	// _ = c.store.SaveProfile(profile)
	// _ = c.persistSessionState(username)
	return c.SnapshotFor(username)
	// 3. 首领死亡时，给所有参与玩家统一结算奖励，并安排复活。
	// 4. 将结果广播到所有在线会话，而不是只发给当前地图。
	//return nil, studentTODOError("Lab3-1", "cluster.AttackBoss", "完成全服共享世界首领的协同结算")
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
	session, ok = c.sessions[username]
	session.MapID = targetMap
	session.NodeID = dst.NodeID()
	session.Version++
	// c.mu.RLock()
	// currentSession, _ := c.sessions[username]
	// fmt.Printf("[DEBUG] Session 更新后: MapID=%s, NodeID=%s, Version=%d\n",
	// 	currentSession.MapID, currentSession.NodeID, currentSession.Version)
	c.mu.Unlock()

	c.pushEventLocked(session, fmt.Sprintf("切换到地图 %s", targetMap))
	_ = c.store.SaveProfile(profile)
	_ = c.persistSessionState(username)

	return c.SnapshotFor(username)
	// 1. 从源节点摘除玩家热状态。
	// 2. 根据 owners 路由把玩家挂到目标地图主节点。
	// 3. 更新 session.MapID / session.NodeID。
	// 4. 将新的位置、地图、节点落盘到冷热数据。
	//return nil, studentTODOError("Lab3-2", "cluster.SwitchMap", "完成跨地图路由与会话迁移")
}

func (c *Cluster) SnapshotFor(username string) (*protocol.WorldState, error) {
	c.mu.RLock()
	session, ok := c.sessions[username]
	if !ok {
		c.mu.RUnlock()
		return nil, fmt.Errorf("用户 %q 当前不在线", username)
	}
	node := c.nodes[session.NodeID]
	sessionVersion := session.Version
	events := append([]string(nil), session.Events...)
	boss := c.boss.viewLocked()
	owners := make(map[string]string, len(c.owners))
	for k, v := range c.owners {
		owners[k] = v
	}
	nodes := make(map[string]NodeClient, len(c.nodes))
	for k, v := range c.nodes {
		nodes[k] = v
	}
	c.mu.RUnlock()

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

	mapBriefs := make([]protocol.MapBrief, 0, len(mapIDs))
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
		mapBriefs = append(mapBriefs, protocol.MapBrief{
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
	nodeViews := make([]protocol.NodeView, 0, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		nodeViews = append(nodeViews, nodes[nodeID].View())
	}

	ws := protocol.AllocWorldState()
	ws.Self = self
	ws.Map = mapView
	ws.Maps = mapBriefs
	ws.Nodes = nodeViews
	ws.Boss = boss
	ws.Events = events
	ws.SessionVersion = sessionVersion

	return ws, nil
}

func (c *Cluster) sessionNode(username string) (*Session, NodeClient, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	session, ok := c.sessions[username]
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

	c.mu.Lock()
	defer c.mu.Unlock()

	session, ok := c.sessions[username]
	if !ok {
		return
	}
	c.pushEventLocked(session, event)
}

func (c *Cluster) pushEventLocked(session *Session, event string) {
	session.Events = append(session.Events, event)
	if len(session.Events) > 8 {
		session.Events = session.Events[len(session.Events)-8:]
	}
	session.Version++
}

func (c *Cluster) broadcastGlobalEvent(event string) {
	if event == "" {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.broadcastGlobalEventLocked(event)
}

func (c *Cluster) broadcastGlobalEventLocked(event string) {
	for _, session := range c.sessions {
		c.pushEventLocked(session, event)
	}
}

func (c *Cluster) broadcastMapEvent(mapID, event string) {
	if event == "" {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	for _, session := range c.sessions {
		if session.MapID == mapID {
			c.pushEventLocked(session, event)
		}
	}
}

func (c *Cluster) persistSessionState(username string) error {
	// TODO(Lab3-3):
	// 这里负责把玩家当前热状态写回存储。
	// 建议区分：
	//首先从session获取必要的信息
	c.mu.RLock()
	session, ok := c.sessions[username]

	if !ok {
		c.mu.RUnlock()
		return nil
	}
	node, ok := c.nodes[session.NodeID]
	c.mu.RUnlock()
	if node == nil {
		return nil
	}

	profile, ok, err := node.Profile(context.Background(), session.MapID, username)
	if err != nil {
		return fmt.Errorf("节点RPC获取Profile异常: %v", err)
	}
	if !ok {
		return nil
	}
	hotData := &protocol.HotSession{
		Username:       username,
		MapID:          session.MapID,
		NodeID:         session.NodeID,
		X:              profile.X,
		Y:              profile.Y,
		HP:             profile.HP,
		Treasures:      profile.Treasures,
		SessionVersion: session.Version,
		UpdatedAt:      time.Now(),
	}
	// profile.LastMap = session.MapID
	// profile.LastNode = session.NodeID
	// if err := c.store.SaveProfile(profile); err != nil {
	// 	return err
	// }
	if err := c.store.SaveHotSession(*hotData); err != nil {
		return err
	}
	// 1. 冷数据：账号、密码哈希、历史战绩、最近退出位置。
	// 2. 热数据：当前地图、节点、坐标、生命值、会话版本。
	// 注意持久化时机，避免因为节点故障导致最新状态丢失。
	return nil //studentTODOError("Lab3-3", "cluster.persistSessionState", "完成会话热数据与用户冷数据持久化")
}

func (c *Cluster) respawnBossAfterCooldown() {
	time.Sleep(15 * time.Second)

	c.mu.Lock()
	defer c.mu.Unlock()

	c.boss = newBossState()
	c.broadcastGlobalEventLocked(fmt.Sprintf("世界首领【%s】重新降临，所有服务器均可参与讨伐", c.boss.Name))
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

			for _, node := range nodes {
				healthy := ping(node.View().Addr)
				wasHealthy := node.SetHealthy(healthy)
				if wasHealthy && !healthy {
					c.handleNodeFailure(node.NodeID())
				}
			}
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
	// TODO(Lab3-4):
	// 这里要实现“主节点定期生成检查点，并复制给副本节点”。
	// 至少要包含：
	// 1. 从 owners 找到每张地图当前主节点。
	// 2. 抓取主节点地图快照。
	// 3. 同时写入本地检查点存储与 replica 节点内存。
	// 4. 跳过故障节点，避免把坏状态继续扩散。
	//logStudentTODO("Lab3-4", "cluster.checkpointLoop", "完成主节点检查点复制与副本同步")
}

func (c *Cluster) flushLoop() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.mu.RLock()
			usernames := make([]string, 0, len(c.sessions))
			for username := range c.sessions {
				usernames = append(usernames, username)
			}
			c.mu.RUnlock()

			for _, username := range usernames {
				_ = c.persistSessionState(username)
			}
		case <-c.stopCh:
			return
		}
	}
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
	for _, session := range c.sessions {
		if session.NodeID == nodeID {
			session.NodeID = c.owners[session.MapID]
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

func (c *Cluster) bossSite(mapID string) (protocol.BossSite, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, site := range c.boss.Sites {
		if site.MapID == mapID {
			return site, true
		}
	}
	return protocol.BossSite{}, false
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

func newBossState() *BossState {
	return &BossState{
		Name:  "王子文",
		HP:    1600,
		MaxHP: 1600,
		Alive: true,
		Sites: []protocol.BossSite{ //考虑到随后的初始化，这里值为空也可以？
			{MapID: "green", X: 50, Y: 20},
			{MapID: "cave", X: 49, Y: 20},
			{MapID: "ruins", X: 50, Y: 20},
		},
		Version:      1,
		Contributors: make(map[string]int),
	}
}

func (b *BossState) viewLocked() protocol.BossView {
	respawnIn := 0
	if !b.Alive {
		respawnIn = int(time.Until(b.RespawnAt).Seconds())
		if respawnIn < 1 {
			respawnIn = 1
		}
	}
	return protocol.BossView{
		Name:      b.Name,
		HP:        b.HP,
		MaxHP:     b.MaxHP,
		Alive:     b.Alive,
		LastHit:   b.LastHit,
		RespawnIn: respawnIn,
		AttackGap: protocol.BossAtkRange,
		Sites:     append([]protocol.BossSite(nil), b.Sites...),
		Version:   b.Version,
	}
}

func ping(addr string) bool {
	conn, err := net.DialTimeout("tcp", addr, 250*time.Millisecond)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
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

func studentTODOError(label, funcName, detail string) error {
	logStudentTODO(label, funcName, detail)
	return fmt.Errorf("[%s] TODO 未实现：%s，需要%s", label, funcName, detail)
}

func logStudentTODO(label, funcName, detail string) {
	if _, loaded := studentTodoNotice.LoadOrStore(label, struct{}{}); loaded {
		return
	}
	fmt.Fprintf(os.Stderr, "[%s] student 待实现函数被触发：%s，需要%s\n", label, funcName, detail)
}
