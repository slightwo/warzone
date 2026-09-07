package storage

import (
	"strings"
	"testing"
	"time"
)

func TestBuildInitialTopology(t *testing.T) {
	knownMapIDs := map[string]struct{}{
		"green": {},
		"cave":  {},
		"ruins": {},
	}
	nodes := []NodeRegistryInfo{
		{ID: "node-c", Addr: "127.0.0.1:9313", Replicas: []string{"green", "ruins"}},
		{ID: "node-a", Addr: "127.0.0.1:9311", Maps: []string{"green"}, Replicas: []string{"cave"}},
		{ID: "node-b", Addr: "127.0.0.1:9312", Maps: []string{"cave", "ruins"}},
	}

	topology, ready, err := BuildInitialTopology(knownMapIDs, nodes, time.Date(2026, time.September, 7, 1, 2, 3, 0, time.FixedZone("UTC+8", 8*60*60)))
	if err != nil {
		t.Fatalf("构造初始拓扑: %v", err)
	}
	if !ready {
		t.Fatal("完整候选集被判定为未就绪")
	}
	if topology.Version != 1 || topology.LeaderTerm != 0 {
		t.Fatalf("初始版本或任期不符合预期: %+v", topology)
	}
	if got, want := topology.Owners["green"], "node-a"; got != want {
		t.Fatalf("green owner = %q，want %q", got, want)
	}
	if got, want := topology.Owners["cave"], "node-b"; got != want {
		t.Fatalf("cave owner = %q，want %q", got, want)
	}
	if got, want := topology.Replicas["cave"], "node-a"; got != want {
		t.Fatalf("cave replica = %q，want %q", got, want)
	}
	for _, mapID := range []string{"green", "cave", "ruins"} {
		if got := topology.MapEpochs[mapID]; got != 1 {
			t.Fatalf("%s epoch = %d，want 1", mapID, got)
		}
	}
	if topology.UpdatedAt.Location() != time.UTC {
		t.Fatalf("updated_at location = %s，want UTC", topology.UpdatedAt.Location())
	}
}

func TestBuildInitialTopologyWaitsForCompleteCandidates(t *testing.T) {
	topology, ready, err := BuildInitialTopology(map[string]struct{}{"green": {}, "cave": {}}, []NodeRegistryInfo{
		{ID: "node-a", Addr: "127.0.0.1:9311", Maps: []string{"green"}},
		{ID: "node-b", Addr: "127.0.0.1:9312", Replicas: []string{"green"}},
	}, time.Now())
	if err != nil {
		t.Fatalf("候选集不完整应继续等待，而非报错: %v", err)
	}
	if ready {
		t.Fatalf("不完整候选集生成了拓扑: %+v", topology)
	}
}

func TestBuildInitialTopologyRejectsCandidateConflicts(t *testing.T) {
	_, ready, err := BuildInitialTopology(map[string]struct{}{"green": {}}, []NodeRegistryInfo{
		{ID: "node-a", Addr: "127.0.0.1:9311", Maps: []string{"green"}},
		{ID: "node-b", Addr: "127.0.0.1:9312", Maps: []string{"green"}, Replicas: []string{"green"}},
	}, time.Now())
	if ready {
		t.Fatal("冲突候选被判定为已就绪")
	}
	if err == nil || !strings.Contains(err.Error(), "冲突") {
		t.Fatalf("冲突候选错误 = %v，want 包含冲突", err)
	}
}

func TestBuildInitialTopologyRejectsPrimaryReplicaOnSameNode(t *testing.T) {
	_, ready, err := BuildInitialTopology(map[string]struct{}{"green": {}}, []NodeRegistryInfo{
		{ID: "node-a", Addr: "127.0.0.1:9311", Maps: []string{"green"}, Replicas: []string{"green"}},
	}, time.Now())
	if ready {
		t.Fatal("主副本同节点被判定为已就绪")
	}
	if err == nil || !strings.Contains(err.Error(), "均为") {
		t.Fatalf("主副本同节点错误 = %v", err)
	}
}
