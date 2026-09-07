package cluster

import (
	"strings"
	"testing"
	"time"

	"battleworld/storage"
	"battleworld/world"
)

func TestBuildInitialTopology(t *testing.T) {
	configs := testMapConfigs("green", "cave", "ruins")
	nodes := []storage.NodeRegistryInfo{
		{ID: "node-c", Addr: "127.0.0.1:9313", Replicas: []string{"green", "ruins"}},
		{ID: "node-a", Addr: "127.0.0.1:9311", Maps: []string{"green"}, Replicas: []string{"cave"}},
		{ID: "node-b", Addr: "127.0.0.1:9312", Maps: []string{"cave", "ruins"}},
	}

	topology, ready, err := buildInitialTopology(configs, nodes, time.Date(2026, time.September, 7, 1, 2, 3, 0, time.FixedZone("UTC+8", 8*60*60)))
	if err != nil {
		t.Fatalf("build initial topology: %v", err)
	}
	if !ready {
		t.Fatal("complete candidate set reported as not ready")
	}
	if topology.Version != 1 || topology.LeaderTerm != 0 {
		t.Fatalf("unexpected initial version or term: %+v", topology)
	}
	if got, want := topology.Owners["green"], "node-a"; got != want {
		t.Fatalf("green owner = %q, want %q", got, want)
	}
	if got, want := topology.Owners["cave"], "node-b"; got != want {
		t.Fatalf("cave owner = %q, want %q", got, want)
	}
	if got, want := topology.Replicas["cave"], "node-a"; got != want {
		t.Fatalf("cave replica = %q, want %q", got, want)
	}
	for _, mapID := range []string{"green", "cave", "ruins"} {
		if got := topology.MapEpochs[mapID]; got != 1 {
			t.Fatalf("%s epoch = %d, want 1", mapID, got)
		}
	}
	if topology.UpdatedAt.Location() != time.UTC {
		t.Fatalf("updated_at location = %s, want UTC", topology.UpdatedAt.Location())
	}
}

func TestBuildInitialTopologyWaitsForCompleteCandidates(t *testing.T) {
	configs := testMapConfigs("green", "cave")
	topology, ready, err := buildInitialTopology(configs, []storage.NodeRegistryInfo{
		{ID: "node-a", Addr: "127.0.0.1:9311", Maps: []string{"green"}},
		{ID: "node-b", Addr: "127.0.0.1:9312", Replicas: []string{"green"}},
	}, time.Now())
	if err != nil {
		t.Fatalf("incomplete candidates should wait, got error: %v", err)
	}
	if ready {
		t.Fatalf("incomplete candidates created topology: %+v", topology)
	}
}

func TestBuildInitialTopologyRejectsCandidateConflicts(t *testing.T) {
	configs := testMapConfigs("green")
	_, ready, err := buildInitialTopology(configs, []storage.NodeRegistryInfo{
		{ID: "node-a", Addr: "127.0.0.1:9311", Maps: []string{"green"}},
		{ID: "node-b", Addr: "127.0.0.1:9312", Maps: []string{"green"}, Replicas: []string{"green"}},
	}, time.Now())
	if ready {
		t.Fatal("conflicting candidates reported as ready")
	}
	if err == nil || !strings.Contains(err.Error(), "冲突") {
		t.Fatalf("conflicting candidates error = %v, want conflict", err)
	}
}

func TestBuildInitialTopologyRejectsPrimaryReplicaOnSameNode(t *testing.T) {
	configs := testMapConfigs("green")
	_, ready, err := buildInitialTopology(configs, []storage.NodeRegistryInfo{
		{ID: "node-a", Addr: "127.0.0.1:9311", Maps: []string{"green"}, Replicas: []string{"green"}},
	}, time.Now())
	if ready {
		t.Fatal("same-node primary and replica reported as ready")
	}
	if err == nil || !strings.Contains(err.Error(), "均为") {
		t.Fatalf("same-node candidates error = %v", err)
	}
}

func TestApplyTopologyLockedOnlyAdvancesVersion(t *testing.T) {
	cluster := newTopologyCacheTestCluster()
	first := testTopology(1, "node-a", "node-b")
	applied, err := cluster.applyTopologyLocked(first)
	if err != nil || !applied {
		t.Fatalf("apply first topology = applied=%t err=%v", applied, err)
	}
	if got := cluster.owners["green"]; got != "node-a" {
		t.Fatalf("owner = %q, want node-a", got)
	}

	stale := testTopology(1, "node-c", "node-b")
	applied, err = cluster.applyTopologyLocked(stale)
	if err != nil || applied {
		t.Fatalf("apply same version = applied=%t err=%v, want false nil", applied, err)
	}
	if got := cluster.owners["green"]; got != "node-a" {
		t.Fatalf("stale topology replaced owner with %q", got)
	}

	next := testTopology(2, "node-c", "node-b")
	applied, err = cluster.applyTopologyLocked(next)
	if err != nil || !applied {
		t.Fatalf("apply next topology = applied=%t err=%v", applied, err)
	}
	if got := cluster.owners["green"]; got != "node-c" {
		t.Fatalf("owner = %q, want node-c after advance", got)
	}

	next.Owners["green"] = "mutated-input"
	if got := cluster.owners["green"]; got != "node-c" {
		t.Fatalf("cluster retained mutable topology input: owner=%q", got)
	}
}

func testMapConfigs(mapIDs ...string) map[string]world.MapConfig {
	configs := make(map[string]world.MapConfig, len(mapIDs))
	for _, mapID := range mapIDs {
		configs[mapID] = world.MapConfig{ID: mapID}
	}
	return configs
}

func newTopologyCacheTestCluster() *Cluster {
	return &Cluster{
		configs:  testMapConfigs("green"),
		owners:   make(map[string]string),
		replicas: make(map[string]string),
	}
}

func testTopology(version uint64, owner, replica string) storage.Topology {
	return storage.Topology{
		Version:   version,
		Owners:    map[string]string{"green": owner},
		Replicas:  map[string]string{"green": replica},
		MapEpochs: map[string]uint64{"green": 1},
		UpdatedAt: time.Now().UTC(),
	}
}
