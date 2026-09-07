package cluster

import (
	"testing"
	"time"

	"battleworld/storage"
	"battleworld/world"
)

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
