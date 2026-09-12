// Package coordinator 负责节点发现、健康检查和版本化拓扑初始化等控制面职责。
// 玩家路由仍由 cluster 包中的网关数据面处理。
package coordinator

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"battleworld/cluster"
	"battleworld/config"
	"battleworld/elector"
	"battleworld/protocol"
	"battleworld/storage"
	"battleworld/world"

	"github.com/redis/go-redis/v9"
)

// ControlStore 是 coordinator 依赖的存储契约，*storage.Store 实现该接口。
type ControlStore interface {
	GetActiveNodes() ([]storage.NodeRegistryInfo, error)
	LoadTopology() (*storage.Topology, bool, error)
	CompareAndSaveTopologyForLeader(uint64, storage.Topology, uint64) error
	CompareAndSaveTopologyAndMigrateSessionsForLeader(uint64, storage.Topology, uint64, string, string, string) error
	LoadCheckpoint(string) (*protocol.MapCheckpoint, bool)
	PublishTopologyChanged(uint64) error
	LoadGlobalBoss() (protocol.BossState, int32, error)
	InitGlobalBoss(int32, protocol.BossState) error
	TryLockBossRespawn() bool
	PublishEvent(string, string) error
}

type redisClientProvider interface {
	RedisClient() *redis.Client
}

// NodeClientFactory 为一个已发现节点创建控制面客户端。
type NodeClientFactory func(nodeID, addr string) (cluster.CoordinatorNodeClient, error)

type nodeConnection struct {
	client  cluster.CoordinatorNodeClient
	healthy bool
}

// Coordinator 持有拓扑控制循环。所有决策循环都绑定到 elector 返回的 leader context；
// 一旦 Redis 租约失效，循环立即停止，随后仅能由新 term 的 leader 重新开始。
type Coordinator struct {
	mu                 sync.RWMutex
	store              ControlStore
	elector            elector.Elector
	configs            map[string]world.MapConfig
	nodes              map[string]nodeConnection
	connectingNodes    map[string]struct{}
	failureGenerations map[string]uint64
	failoverInFlight   map[string]bool
	newNodeClient      NodeClientFactory
	stopCh             chan struct{}
	closeOnce          sync.Once
	startMu            sync.Mutex
	started            bool
	closed             bool

	discoveryInterval     time.Duration
	heartbeatInterval     time.Duration
	nodeRequestTimeout    time.Duration
	leaderRetryInterval   time.Duration
	bossReconcileInterval time.Duration
}

// New 使用 Redis token 租约构造生产 coordinator。测试替身没有 Redis 连接时回退为
// AlwaysLeader，以保持控制面业务测试不依赖外部 Redis。
func New(store ControlStore) (*Coordinator, error) {
	return NewWithRuntimeConfig(store, config.DefaultRuntime())
}

// NewWithRuntimeConfig 使用启动时加载的运行时配置构造生产 coordinator。
func NewWithRuntimeConfig(store ControlStore, runtime config.RuntimeConfig) (*Coordinator, error) {
	if err := runtime.Validate(); err != nil {
		return nil, fmt.Errorf("校验 coordinator 运行时配置: %w", err)
	}
	selectedElector, err := productionElector(store, runtime.LeaderElection)
	if err != nil {
		return nil, err
	}
	return newCoordinatorWithRuntimeConfig(store, selectedElector, func(nodeID, addr string) (cluster.CoordinatorNodeClient, error) {
		return cluster.NewNodeGRPCClient(nodeID, addr)
	}, world.AvailableMaps(), runtime.Coordinator)
}

// NewWithElector 允许部署或测试显式注入选主实现。
func NewWithElector(store ControlStore, selectedElector elector.Elector) (*Coordinator, error) {
	return newCoordinatorWithElector(store, selectedElector, func(nodeID, addr string) (cluster.CoordinatorNodeClient, error) {
		return cluster.NewNodeGRPCClient(nodeID, addr)
	}, world.AvailableMaps())
}

func productionElector(store ControlStore, runtime config.LeaderElectionConfig) (elector.Elector, error) {
	provider, ok := store.(redisClientProvider)
	if !ok || provider.RedisClient() == nil {
		return elector.NewAlwaysLeader(), nil
	}
	instanceID := config.CoordinatorID()
	if instanceID == "" {
		hostname, err := os.Hostname()
		if err != nil {
			return nil, fmt.Errorf("读取 coordinator 主机名: %w", err)
		}
		instanceID = fmt.Sprintf("%s-%d", hostname, os.Getpid())
	}
	selectedElector, err := elector.NewRedis(provider.RedisClient(), instanceID, elector.RedisOptions{
		TTL:           runtime.LeaseTTL.Duration,
		RenewInterval: runtime.RenewInterval.Duration,
		RetryInterval: runtime.CampaignRetryInterval.Duration,
	})
	if err != nil {
		return nil, fmt.Errorf("创建 Redis coordinator elector: %w", err)
	}
	return selectedElector, nil
}

func newCoordinator(store ControlStore, newNodeClient NodeClientFactory, configs []world.MapConfig) (*Coordinator, error) {
	return newCoordinatorWithElector(store, elector.NewAlwaysLeader(), newNodeClient, configs)
}

func newCoordinatorWithElector(store ControlStore, selectedElector elector.Elector, newNodeClient NodeClientFactory, configs []world.MapConfig) (*Coordinator, error) {
	return newCoordinatorWithRuntimeConfig(store, selectedElector, newNodeClient, configs, config.DefaultRuntime().Coordinator)
}

func newCoordinatorWithRuntimeConfig(store ControlStore, selectedElector elector.Elector, newNodeClient NodeClientFactory, configs []world.MapConfig, runtime config.CoordinatorConfig) (*Coordinator, error) {
	if store == nil {
		return nil, errors.New("coordinator store 不能为空")
	}
	if selectedElector == nil {
		return nil, errors.New("coordinator elector 不能为空")
	}
	if newNodeClient == nil {
		return nil, errors.New("coordinator node client factory 不能为空")
	}

	configByID := make(map[string]world.MapConfig, len(configs))
	for _, mapConfig := range configs {
		configByID[mapConfig.ID] = mapConfig
	}
	return &Coordinator{
		store:                store,
		elector:              selectedElector,
		configs:              configByID,
		nodes:                make(map[string]nodeConnection),
		connectingNodes:      make(map[string]struct{}),
		failureGenerations:   make(map[string]uint64),
		failoverInFlight:     make(map[string]bool),
		newNodeClient:        newNodeClient,
		stopCh:               make(chan struct{}),
		discoveryInterval:    runtime.DiscoveryInterval.Duration,
		heartbeatInterval:    runtime.HealthCheckInterval.Duration,
		nodeRequestTimeout:   runtime.NodePingTimeout.Duration,
		leaderRetryInterval:  runtime.LeaderRetryInterval.Duration,
		bossReconcileInterval: runtime.BossReconcileInterval.Duration,
	}, nil
}

// Start 仅启动选主循环。discovery、heartbeat、failover 与 Boss 生命周期在成功获得
// leader lease 后才启动；重复调用是安全的。
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
	go c.leaderLoop()
	return nil
}

func (c *Coordinator) leaderLoop() {
	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-c.stopCh:
			cancel()
		case <-rootCtx.Done():
		}
	}()

	for rootCtx.Err() == nil {
		leaderCtx, err := c.elector.Campaign(rootCtx)
		if err != nil {
			if rootCtx.Err() != nil {
				return
			}
			log.Printf("[coordinator/leader] 参与选主失败: %v", err)
			if !waitForContext(rootCtx, c.leaderRetryInterval) {
				return
			}
			continue
		}
		term, ok := c.currentLeaderTerm()
		if !ok {
			continue
		}
		log.Printf("[coordinator/leader] 当前实例成为 leader，term=%d", term)
		c.runLeader(leaderCtx)
	}
}

func (c *Coordinator) runLeader(leaderCtx context.Context) {
	if err := c.ensureBoss(); err != nil {
		log.Printf("[coordinator/boss] leader 初始化世界首领失败: %v", err)
	}

	var workers sync.WaitGroup
	workers.Add(3)
	go func() {
		defer workers.Done()
		c.discoveryLoop(leaderCtx)
	}()
	go func() {
		defer workers.Done()
		c.heartbeatLoop(leaderCtx)
	}()
	go func() {
		defer workers.Done()
		c.bossLoop(leaderCtx)
	}()
	<-leaderCtx.Done()
	workers.Wait()
}

func waitForContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (c *Coordinator) currentLeaderTerm() (uint64, bool) {
	term := c.elector.Term()
	return term, term > 0 && c.elector.IsLeader()
}

// hasLeaderTerm 用于控制面写入前的最终检查。存储层会进一步拒绝 LeaderTerm 倒退，
// 因此旧 leader 即使卡在 Promote 或网络调用中，也不能以较旧 term 覆盖新决策。
func (c *Coordinator) hasLeaderTerm(term uint64) bool {
	return term > 0 && c.elector.IsLeader() && c.elector.Term() == term
}

// LeaderStatus 返回可观测的本实例选主状态。
func (c *Coordinator) LeaderStatus() elector.Status {
	if reporter, ok := c.elector.(elector.StatusReporter); ok {
		return reporter.Status()
	}
	term, leader := c.currentLeaderTerm()
	return elector.Status{Leader: leader, Term: term}
}

// Close 停止选主和控制循环，释放本实例仍持有的 Redis token 租约，并关闭全部控制面
// gRPC 客户端。
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

		if err := c.elector.Resign(); err != nil {
			log.Printf("[coordinator/leader] 释放 leader 租约失败: %v", err)
		}
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
