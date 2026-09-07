package coordinator

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"battleworld/cluster"
	"battleworld/protocol"
	"battleworld/storage"
	"battleworld/world"
)

type fakeControlStore struct {
	mu          sync.Mutex
	activeNodes []storage.NodeRegistryInfo
	topology    *storage.Topology
	loadErr     error
	casErr      error
	casCalls    int
	published   []uint64
	publishErr  error
	events      []string
	bossState   *protocol.BossState
	bossHP      int32
	bossLoadErr error
	respawnLock bool
}

func (s *fakeControlStore) GetActiveNodes() ([]storage.NodeRegistryInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]storage.NodeRegistryInfo(nil), s.activeNodes...), nil
}

func (s *fakeControlStore) LoadTopology() (*storage.Topology, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.loadErr != nil {
		return nil, false, s.loadErr
	}
	if s.topology == nil {
		return nil, false, nil
	}
	clone := s.topology.Clone()
	return &clone, true, nil
}

func (s *fakeControlStore) CompareAndSaveTopology(expectedVersion uint64, topology storage.Topology) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.casCalls++
	if s.casErr != nil {
		return s.casErr
	}
	if expectedVersion != 0 || s.topology != nil {
		return storage.ErrTopologyVersionConflict
	}
	clone := topology.Clone()
	s.topology = &clone
	return nil
}

func (s *fakeControlStore) PublishTopologyChanged(version uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.publishErr != nil {
		return s.publishErr
	}
	s.published = append(s.published, version)
	return nil
}

func (s *fakeControlStore) LoadGlobalBoss() (protocol.BossState, int32, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.bossLoadErr != nil {
		return protocol.BossState{}, 0, s.bossLoadErr
	}
	if s.bossState == nil {
		return protocol.BossState{}, 0, storage.ErrGlobalBossNotInitialized
	}
	return *s.bossState, s.bossHP, nil
}

func (s *fakeControlStore) InitGlobalBoss(hp int32, state protocol.BossState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	clone := state
	s.bossState = &clone
	s.bossHP = hp
	return nil
}

func (s *fakeControlStore) TryLockBossRespawn() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.respawnLock {
		return false
	}
	s.respawnLock = true
	return true
}

func (s *fakeControlStore) PublishEvent(channel, event string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.publishErr != nil {
		return s.publishErr
	}
	s.events = append(s.events, channel+":"+event)
	return nil
}

type fakeCoordinatorNodeClient struct {
	mu      sync.Mutex
	id      string
	pingErr error
	closed  bool
}

func (c *fakeCoordinatorNodeClient) NodeID() string { return c.id }

func (c *fakeCoordinatorNodeClient) Ping(context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.pingErr
}

func (c *fakeCoordinatorNodeClient) View() protocol.NodeView { return protocol.NodeView{ID: c.id} }

func (c *fakeCoordinatorNodeClient) Checkpoint(context.Context, string) (protocol.MapCheckpoint, error) {
	return protocol.MapCheckpoint{}, nil
}

func (c *fakeCoordinatorNodeClient) Promote(string, world.MapConfig) error { return nil }

func (c *fakeCoordinatorNodeClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	return nil
}

func (c *fakeCoordinatorNodeClient) setPingErr(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pingErr = err
}

func TestBootstrapTopologyIfAbsentCommitsAndPublishes(t *testing.T) {
	store := &fakeControlStore{}
	coordinator := newTestCoordinator(t, store)
	nodes := []storage.NodeRegistryInfo{
		{ID: "node-a", Addr: "127.0.0.1:9311", Maps: []string{"green"}},
		{ID: "node-b", Addr: "127.0.0.1:9312", Replicas: []string{"green"}},
	}

	if err := coordinator.bootstrapTopologyIfAbsent(nodes); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.topology == nil || store.topology.Version != 1 {
		t.Fatalf("提交拓扑 = %+v，want Version 1", store.topology)
	}
	if got, want := store.topology.Owners["green"], "node-a"; got != want {
		t.Fatalf("owner = %q，want %q", got, want)
	}
	if got, want := store.published, []uint64{1}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("发布版本 = %v，want %v", got, want)
	}
}

func TestBootstrapTopologyIfAbsentNeverOverwritesExistingTopology(t *testing.T) {
	existing := storage.Topology{
		Version:   4,
		Owners:    map[string]string{"green": "node-existing"},
		Replicas:  map[string]string{"green": "node-replica"},
		MapEpochs: map[string]uint64{"green": 3},
	}
	store := &fakeControlStore{topology: &existing}
	coordinator := newTestCoordinator(t, store)

	if err := coordinator.bootstrapTopologyIfAbsent(nil); err != nil {
		t.Fatalf("bootstrap with existing topology: %v", err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.casCalls != 0 || len(store.published) != 0 {
		t.Fatalf("已存在拓扑仍发生写入: cas=%d published=%v", store.casCalls, store.published)
	}
	if got := store.topology.Owners["green"]; got != "node-existing" {
		t.Fatalf("已存在 owner 被覆盖为 %q", got)
	}
}

func TestConnectedNodeRegistrationsAndHeartbeat(t *testing.T) {
	store := &fakeControlStore{}
	clientA := &fakeCoordinatorNodeClient{id: "node-a"}
	clientB := &fakeCoordinatorNodeClient{id: "node-b", pingErr: errors.New("unavailable")}
	clients := map[string]*fakeCoordinatorNodeClient{"node-a": clientA, "node-b": clientB}
	coordinator, err := newCoordinator(store, func(nodeID, _ string) (cluster.CoordinatorNodeClient, error) {
		return clients[nodeID], nil
	}, []world.MapConfig{{ID: "green"}})
	if err != nil {
		t.Fatalf("创建 coordinator: %v", err)
	}

	coordinator.connectDiscoveredNode(storage.NodeRegistryInfo{ID: "node-a", Addr: "127.0.0.1:9311"})
	coordinator.connectDiscoveredNode(storage.NodeRegistryInfo{ID: "node-b", Addr: "127.0.0.1:9312"})
	registrations := coordinator.connectedNodeRegistrations([]storage.NodeRegistryInfo{
		{ID: "node-a", Addr: "127.0.0.1:9311"},
		{ID: "node-b", Addr: "127.0.0.1:9312"},
	})
	if len(registrations) != 1 || registrations[0].ID != "node-a" {
		t.Fatalf("健康连接注册 = %+v，want 仅 node-a", registrations)
	}

	clientA.setPingErr(errors.New("unavailable"))
	coordinator.heartbeatOnce()
	coordinator.mu.RLock()
	healthy := coordinator.nodes["node-a"].healthy
	coordinator.mu.RUnlock()
	if healthy {
		t.Fatal("心跳失败后 node-a 仍标记为健康")
	}

	clientA.setPingErr(nil)
	coordinator.heartbeatOnce()
	coordinator.mu.RLock()
	healthy = coordinator.nodes["node-a"].healthy
	coordinator.mu.RUnlock()
	if !healthy {
		t.Fatal("心跳恢复后 node-a 未恢复健康标记")
	}
}

func TestCoordinatorInitializesAndRestoresBoss(t *testing.T) {
	store := &fakeControlStore{}
	coordinator := newTestCoordinator(t, store)

	if err := coordinator.ensureBoss(); err != nil {
		t.Fatalf("初始化 Boss: %v", err)
	}
	store.mu.Lock()
	if store.bossState == nil || !store.bossState.Alive || store.bossHP != defaultBossHP {
		store.mu.Unlock()
		t.Fatalf("初始 Boss 状态错误: state=%+v hp=%d", store.bossState, store.bossHP)
	}
	if len(store.bossState.Sites) != 1 || store.bossState.Sites[0].MapID != "green" {
		store.mu.Unlock()
		t.Fatalf("初始 Boss 站点错误: %+v", store.bossState.Sites)
	}
	dead := *store.bossState
	dead.Alive = false
	dead.LastHit = "player-a"
	dead.RespawnAt = time.Now().Add(-time.Second)
	store.bossState = &dead
	store.bossHP = 0
	store.mu.Unlock()

	coordinator.restoreBossIfDue()
	store.mu.Lock()
	defer store.mu.Unlock()
	if !store.bossState.Alive || store.bossState.LastHit != "" || !store.bossState.RespawnAt.IsZero() {
		t.Fatalf("Boss 未正确复活: %+v", store.bossState)
	}
	if store.bossHP != defaultBossHP || store.bossState.Version != dead.Version+1 {
		t.Fatalf("Boss 复活版本或血量错误: version=%d hp=%d", store.bossState.Version, store.bossHP)
	}
	if len(store.events) != 1 {
		t.Fatalf("Boss 复活未发布全局事件: %v", store.events)
	}
}

func TestCloseClosesControlPlaneClients(t *testing.T) {
	store := &fakeControlStore{}
	client := &fakeCoordinatorNodeClient{id: "node-a"}
	coordinator, err := newCoordinator(store, func(string, string) (cluster.CoordinatorNodeClient, error) {
		return client, nil
	}, []world.MapConfig{{ID: "green"}})
	if err != nil {
		t.Fatalf("创建 coordinator: %v", err)
	}
	coordinator.mu.Lock()
	coordinator.nodes[client.id] = nodeConnection{client: client, healthy: true}
	coordinator.mu.Unlock()

	coordinator.Close()
	client.mu.Lock()
	closed := client.closed
	client.mu.Unlock()
	if !closed {
		t.Fatal("Close 未关闭控制面客户端")
	}
	if err := coordinator.Start(); err == nil {
		t.Fatal("关闭后 Start 未返回错误")
	}
}

func newTestCoordinator(t *testing.T, store ControlStore) *Coordinator {
	t.Helper()
	coordinator, err := newCoordinator(store, func(nodeID, _ string) (cluster.CoordinatorNodeClient, error) {
		return &fakeCoordinatorNodeClient{id: nodeID}, nil
	}, []world.MapConfig{{ID: "green"}})
	if err != nil {
		t.Fatalf("创建 coordinator: %v", err)
	}
	return coordinator
}
