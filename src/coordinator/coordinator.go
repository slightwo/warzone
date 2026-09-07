// Package coordinator 负责节点发现、健康检查和版本化拓扑初始化等控制面职责。
// 玩家路由仍由 cluster 包中的网关数据面处理。
package coordinator

import (
	"errors"
	"sync"
	"time"

	"battleworld/cluster"
	"battleworld/protocol"
	"battleworld/storage"
	"battleworld/world"
)

const (
	defaultDiscoveryInterval = time.Second
	defaultHeartbeatInterval = time.Second
	nodeRequestTimeout       = 500 * time.Millisecond
)

// ControlStore 是 coordinator 依赖的存储契约，*storage.Store 实现该接口。
type ControlStore interface {
	GetActiveNodes() ([]storage.NodeRegistryInfo, error)
	LoadTopology() (*storage.Topology, bool, error)
	CompareAndSaveTopology(uint64, storage.Topology) error
	CompareAndSaveTopologyAndMigrateSessions(uint64, storage.Topology, string, string, string) error
	LoadCheckpoint(string) (*protocol.MapCheckpoint, bool)
	PublishTopologyChanged(uint64) error
	LoadGlobalBoss() (protocol.BossState, int32, error)
	InitGlobalBoss(int32, protocol.BossState) error
	TryLockBossRespawn() bool
	PublishEvent(string, string) error
}

// NodeClientFactory 为一个已发现节点创建控制面客户端。
type NodeClientFactory func(nodeID, addr string) (cluster.CoordinatorNodeClient, error)

type nodeConnection struct {
	client  cluster.CoordinatorNodeClient
	healthy bool
}

// Coordinator 持有拓扑控制循环。2.2-B 按部署约定运行单实例；分布式选主留待 2.2-D。
type Coordinator struct {
	mu              sync.RWMutex
	store           ControlStore
	configs         map[string]world.MapConfig
	nodes                 map[string]nodeConnection
	connectingNodes       map[string]struct{}
	failureGenerations    map[string]uint64
	failoverInFlight      map[string]bool
	newNodeClient         NodeClientFactory
	stopCh          chan struct{}
	closeOnce       sync.Once
	startMu         sync.Mutex
	started         bool
	closed          bool

	discoveryInterval time.Duration
	heartbeatInterval time.Duration
}

// New 使用生产 gRPC 节点客户端工厂构造 coordinator。
func New(store ControlStore) (*Coordinator, error) {
	return newCoordinator(store, func(nodeID, addr string) (cluster.CoordinatorNodeClient, error) {
		return cluster.NewNodeGRPCClient(nodeID, addr)
	}, world.AvailableMaps())
}

func newCoordinator(store ControlStore, newNodeClient NodeClientFactory, configs []world.MapConfig) (*Coordinator, error) {
	if store == nil {
		return nil, errors.New("coordinator store 不能为空")
	}
	if newNodeClient == nil {
		return nil, errors.New("coordinator node client factory 不能为空")
	}

	configByID := make(map[string]world.MapConfig, len(configs))
	for _, config := range configs {
		configByID[config.ID] = config
	}
	return &Coordinator{
		store:             store,
		configs:           configByID,
		nodes:              make(map[string]nodeConnection),
		connectingNodes:    make(map[string]struct{}),
		failureGenerations: make(map[string]uint64),
		failoverInFlight:   make(map[string]bool),
		newNodeClient:      newNodeClient,
		stopCh:            make(chan struct{}),
		discoveryInterval: defaultDiscoveryInterval,
		heartbeatInterval: defaultHeartbeatInterval,
	}, nil
}

// Start 启动发现和健康检查循环；重复调用是安全的。
func (c *Coordinator) Start() error {
	c.startMu.Lock()
	defer c.startMu.Unlock()

	c.mu.RLock()
	closed, started := c.closed, c.started
	c.mu.RUnlock()
	if closed {
		return errors.New("coordinator 已关闭")
	}
	if started {
		return nil
	}
	if err := c.ensureBoss(); err != nil {
		return err
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return errors.New("coordinator 已关闭")
	}
	if c.started {
		c.mu.Unlock()
		return nil
	}
	c.started = true
	c.mu.Unlock()
	go c.discoveryLoop()
	go c.heartbeatLoop()
	go c.bossLoop()
	return nil
}

// Close 停止控制循环并关闭全部控制面 gRPC 客户端。
func (c *Coordinator) Close() {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		close(c.stopCh)
		clients := make([]cluster.CoordinatorNodeClient, 0, len(c.nodes))
		for _, connection := range c.nodes {
			clients = append(clients, connection.client)
		}
		c.nodes = make(map[string]nodeConnection)
		c.connectingNodes = make(map[string]struct{})
		c.failureGenerations = make(map[string]uint64)
		c.failoverInFlight = make(map[string]bool)
		c.mu.Unlock()

		for _, client := range clients {
			_ = client.Close()
		}
	})
}

func (c *Coordinator) knownMapIDs() map[string]struct{} {
	c.mu.RLock()
	defer c.mu.RUnlock()
	mapIDs := make(map[string]struct{}, len(c.configs))
	for mapID := range c.configs {
		mapIDs[mapID] = struct{}{}
	}
	return mapIDs
}
