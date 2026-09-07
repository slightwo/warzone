package cluster

import (
	"context"
	"sync"
	"testing"

	"battleworld/protocol"
	"battleworld/storage"
)

type fakeGatewayNodeClient struct {
	mu     sync.Mutex
	id     string
	closed bool
}

func (c *fakeGatewayNodeClient) NodeID() string { return c.id }
func (c *fakeGatewayNodeClient) AddPlayer(context.Context, string, uint64, *protocol.UserProfile) error {
	return nil
}
func (c *fakeGatewayNodeClient) RemovePlayer(context.Context, string, string, uint64) (protocol.UserProfile, bool, error) {
	return protocol.UserProfile{}, false, nil
}
func (c *fakeGatewayNodeClient) MovePlayer(context.Context, string, string, string, uint64) (string, protocol.UserProfile, bool, error) {
	return "", protocol.UserProfile{}, false, nil
}
func (c *fakeGatewayNodeClient) Attack(context.Context, string, string, uint64) (string, string, string, protocol.UserProfile, bool, error) {
	return "", "", "", protocol.UserProfile{}, false, nil
}
func (c *fakeGatewayNodeClient) Heal(context.Context, string, string, uint64) (string, protocol.UserProfile, bool, error) {
	return "", protocol.UserProfile{}, false, nil
}
func (c *fakeGatewayNodeClient) BuyItem(context.Context, string, string, string, uint64) (string, protocol.UserProfile, bool, error) {
	return "", protocol.UserProfile{}, false, nil
}
func (c *fakeGatewayNodeClient) AttackBoss(context.Context, string, string, uint64) (string, protocol.UserProfile, bool, error) {
	return "", protocol.UserProfile{}, false, nil
}
func (c *fakeGatewayNodeClient) Profile(context.Context, string, string) (protocol.UserProfile, bool, error) {
	return protocol.UserProfile{}, false, nil
}
func (c *fakeGatewayNodeClient) RewardPlayer(context.Context, string, string, int, int, uint64) (protocol.UserProfile, bool, error) {
	return protocol.UserProfile{}, false, nil
}
func (c *fakeGatewayNodeClient) Snapshot(context.Context, string) (protocol.MapView, error) {
	return protocol.MapView{}, nil
}
func (c *fakeGatewayNodeClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	return nil
}

func TestReconcileGatewayNodesUsesCommittedOwnersOnly(t *testing.T) {
	clients := make(map[string]*fakeGatewayNodeClient)
	cluster := &Cluster{
		nodes:       make(map[string]gatewayNodeClient),
		nodeAddrs:   make(map[string]string),
		nodeTargets: make(map[string]string),
		configs:     testMapConfigs("green"),
		activeNodes: func() ([]storage.NodeRegistryInfo, error) {
			return []storage.NodeRegistryInfo{
				{ID: "node-a", Addr: "127.0.0.1:9311"},
				{ID: "node-b", Addr: "127.0.0.1:9312"},
				{ID: "unassigned", Addr: "127.0.0.1:9313"},
			}, nil
		},
		nodeFactory: func(nodeID, _ string) (gatewayNodeClient, error) {
			client := &fakeGatewayNodeClient{id: nodeID}
			clients[nodeID] = client
			return client, nil
		},
	}

	first := testTopology(1, "node-a", "node-b")
	cluster.mu.Lock()
	if _, err := cluster.applyTopologyLocked(first); err != nil {
		cluster.mu.Unlock()
		t.Fatalf("应用首个拓扑: %v", err)
	}
	cluster.mu.Unlock()
	if err := cluster.reconcileGatewayNodes(first); err != nil {
		t.Fatalf("同步首个网关连接池: %v", err)
	}
	if len(cluster.nodes) != 1 || cluster.nodes["node-a"] == nil || clients["unassigned"] != nil {
		t.Fatalf("网关连接池未限定为 owner: %+v", cluster.nodes)
	}

	next := testTopology(2, "node-b", "node-a")
	cluster.mu.Lock()
	if _, err := cluster.applyTopologyLocked(next); err != nil {
		cluster.mu.Unlock()
		t.Fatalf("应用新拓扑: %v", err)
	}
	cluster.mu.Unlock()
	if err := cluster.reconcileGatewayNodes(next); err != nil {
		t.Fatalf("同步新网关连接池: %v", err)
	}

	clients["node-a"].mu.Lock()
	oldClosed := clients["node-a"].closed
	clients["node-a"].mu.Unlock()
	if !oldClosed {
		t.Fatal("owner 变更后未关闭旧数据面客户端")
	}
	if len(cluster.nodes) != 1 || cluster.nodes["node-b"] == nil {
		t.Fatalf("owner 变更后网关连接池错误: %+v", cluster.nodes)
	}

	if err := cluster.reconcileGatewayNodes(first); err != nil {
		t.Fatalf("同步过期拓扑不应报错: %v", err)
	}
	if len(cluster.nodes) != 1 || cluster.nodes["node-b"] == nil || cluster.nodeTargets["node-b"] == "" {
		t.Fatalf("过期拓扑回退了当前网关路由: nodes=%+v targets=%v", cluster.nodes, cluster.nodeTargets)
	}
}

func TestGatewayOwnerTargetsSkipsUnregisteredOwners(t *testing.T) {
	topology := testTopology(1, "node-a", "node-b")
	targets := gatewayOwnerTargets(topology, []storage.NodeRegistryInfo{{ID: "node-b", Addr: "127.0.0.1:9312"}})
	if len(targets) != 0 {
		t.Fatalf("未注册 owner 不应回退到 replica: %v", targets)
	}
}

func TestGatewayOwnerTargetsSkipsDrainingOwner(t *testing.T) {
	topology := testTopology(1, "node-a", "node-b")
	targets := gatewayOwnerTargets(topology, []storage.NodeRegistryInfo{{
		ID:       "node-a",
		Addr:     "127.0.0.1:9311",
		Draining: true,
	}})
	if len(targets) != 0 {
		t.Fatalf("draining owner 不应保留数据面目标: %v", targets)
	}
}
