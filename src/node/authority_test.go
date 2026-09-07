package node

import (
	"errors"
	"testing"
	"time"

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
		Replicas:  map[string]string{"green": "node-b"},
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

func TestGRPCAuthorityErrorsUseFailedPrecondition(t *testing.T) {
	for _, err := range []error{ErrTopologyAuthorityUnavailable, ErrMapAuthorityDenied, ErrMapPromotionDenied} {
		if got := status.Code(grpcAuthorityError(err)); got != codes.FailedPrecondition {
			t.Fatalf("%v 映射为 %s，want FailedPrecondition", err, got)
		}
	}
}

func TestAuthorityCacheOnlyAllowsCurrentReplicaPromotion(t *testing.T) {
	now := time.Date(2026, time.September, 7, 0, 0, 0, 0, time.UTC)
	topology := storage.Topology{
		Version:   3,
		Owners:    map[string]string{"green": "node-a"},
		Replicas:  map[string]string{"green": "node-b"},
		MapEpochs: map[string]uint64{"green": 2},
		UpdatedAt: now,
	}
	cache := newAuthorityCache("node-b", time.Second)
	cache.now = func() time.Time { return now }
	if err := cache.refresh(fakeTopologyLoader{topology: &topology, found: true}); err != nil {
		t.Fatalf("刷新拓扑: %v", err)
	}
	if err := cache.RequirePromotionCandidate("green", 3); err != nil {
		t.Fatalf("当前副本提升被拒绝: %v", err)
	}
	if err := cache.RequirePromotionCandidate("green", 2); !errors.Is(err, ErrMapPromotionDenied) {
		t.Fatalf("错误目标 epoch = %v，want ErrMapPromotionDenied", err)
	}
}
