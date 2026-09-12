package node

import (
	"errors"
	"testing"
	"time"

	"battleworld/config"
	"battleworld/storage"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakeTopologyLoader struct {
	topology *storage.Topology
	found    bool
	err      error
}

func (l fakeTopologyLoader) LoadTopology() (*storage.Topology, bool, error) {
	if l.err != nil {
		return nil, false, l.err
	}
	if !l.found || l.topology == nil {
		return nil, false, nil
	}
	clone := l.topology.Clone()
	return &clone, true, nil
}

func TestAuthorityCacheRejectsStaleEpochAndFailsClosedAfterGrace(t *testing.T) {
	now := time.Date(2026, time.September, 7, 0, 0, 0, 0, time.UTC)
	topology := storage.Topology{
		Version:   3,
		Owners:    map[string]string{"green": "node-a"},
		MapEpochs: map[string]uint64{"green": 2},
		UpdatedAt: now,
	}
	cache := newAuthorityCache("node-a", time.Second)
	cache.now = func() time.Time { return now }
	if err := cache.refresh(fakeTopologyLoader{topology: &topology, found: true}); err != nil {
		t.Fatalf("刷新拓扑: %v", err)
	}
	if err := cache.Require("green", 2); err != nil {
		t.Fatalf("当前 owner/epoch 被拒绝: %v", err)
	}
	if err := cache.Require("green", 1); !errors.Is(err, ErrMapAuthorityDenied) {
		t.Fatalf("旧 epoch 错误 = %v，want ErrMapAuthorityDenied", err)
	}

	now = now.Add(2 * time.Second)
	if err := cache.Require("green", 2); !errors.Is(err, ErrTopologyAuthorityUnavailable) {
		t.Fatalf("宽限期后错误 = %v，want ErrTopologyAuthorityUnavailable", err)
	}
}

func TestEnsureOwnedMapsCreatesRuntimeForTopologyOwnerAfterRestart(t *testing.T) {
	now := time.Date(2026, time.September, 8, 0, 0, 0, 0, time.UTC)
	topology := storage.Topology{
		Version:   4,
		Owners:    map[string]string{"green": "node-c"},
		MapEpochs: map[string]uint64{"green": 3},
		UpdatedAt: now,
	}
	service := NewNodeService("node-c", "", nil, config.DefaultRuntime().Node, "")
	service.authority.now = func() time.Time { return now }
	if err := service.authority.refresh(fakeTopologyLoader{topology: &topology, found: true}); err != nil {
		t.Fatalf("刷新拓扑: %v", err)
	}

	if err := service.ensureOwnedMaps(); err != nil {
		t.Fatalf("按拓扑装载 owner 地图: %v", err)
	}
	service.mu.RLock()
	instance := service.maps["green"]
	service.mu.RUnlock()
	if instance == nil {
		t.Fatal("拓扑 owner 地图未创建本地运行时")
	}
}

func TestGRPCAuthorityErrorsUseFailedPrecondition(t *testing.T) {
	for _, err := range []error{ErrTopologyAuthorityUnavailable, ErrMapAuthorityDenied, ErrMapPromotionDenied} {
		if got := status.Code(grpcAuthorityError(err)); got != codes.FailedPrecondition {
			t.Fatalf("%v 映射为 %s，want FailedPrecondition", err, got)
		}
	}
}

func TestAuthorityCacheAllowsNonOwnerCandidatePromotion(t *testing.T) {
	now := time.Date(2026, time.September, 7, 0, 0, 0, 0, time.UTC)
	topology := storage.Topology{
		Version:   3,
		Owners:    map[string]string{"green": "node-a"},
		MapEpochs: map[string]uint64{"green": 2},
		UpdatedAt: now,
	}
	standby := newAuthorityCache("node-b", time.Second)
	standby.now = func() time.Time { return now }
	if err := standby.refresh(fakeTopologyLoader{topology: &topology, found: true}); err != nil {
		t.Fatalf("刷新拓扑: %v", err)
	}
	if err := standby.RequirePromotionCandidate("green", 3); err != nil {
		t.Fatalf("同图非 owner 候选提升被拒绝: %v", err)
	}
	if err := standby.RequirePromotionCandidate("green", 2); !errors.Is(err, ErrMapPromotionDenied) {
		t.Fatalf("错误目标 epoch = %v，want ErrMapPromotionDenied", err)
	}

	owner := newAuthorityCache("node-a", time.Second)
	owner.now = func() time.Time { return now }
	if err := owner.refresh(fakeTopologyLoader{topology: &topology, found: true}); err != nil {
		t.Fatalf("刷新 owner 拓扑: %v", err)
	}
	if err := owner.RequirePromotionCandidate("green", 3); !errors.Is(err, ErrMapPromotionDenied) {
		t.Fatalf("当前 owner 不应自我提升: %v", err)
	}
}

func TestNodeServiceRejectsPromotionForAnotherDeclaredMap(t *testing.T) {
	now := time.Date(2026, time.September, 7, 0, 0, 0, 0, time.UTC)
	topology := storage.Topology{
		Version:   3,
		Owners:    map[string]string{"green": "node-a"},
		MapEpochs: map[string]uint64{"green": 2},
		UpdatedAt: now,
	}
	service := NewNodeService("node-b", "", nil, config.DefaultRuntime().Node, "cave")
	service.authority.now = func() time.Time { return now }
	if err := service.authority.refresh(fakeTopologyLoader{topology: &topology, found: true}); err != nil {
		t.Fatalf("刷新拓扑: %v", err)
	}
	if err := service.RequirePromotionCandidate("green", 3); !errors.Is(err, ErrMapPromotionDenied) {
		t.Fatalf("节点声明 cave 仍可提升 green: %v", err)
	}
}
