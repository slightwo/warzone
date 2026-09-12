package storage

import (
	"strings"
	"testing"
	"time"
)

func TestBuildInitialTopologyElectsDeterministicOwnersFromSameMapCandidates(t *testing.T) {
	knownMapIDs := map[string]struct{}{
		"green": {},
		"cave":  {},
		"ruins": {},
	}
	nodes := []NodeRegistryInfo{
		{ID: "node-f", Addr: "127.0.0.1:9316", MapID: "ruins"},
		{ID: "node-d", Addr: "127.0.0.1:9314", MapID: "cave"},
		{ID: "node-b", Addr: "127.0.0.1:9312", MapID: "green"},
		{ID: "node-a", Addr: "127.0.0.1:9311", MapID: "green"},
		{ID: "node-e", Addr: "127.0.0.1:9315", MapID: "ruins"},
		{ID: "node-c", Addr: "127.0.0.1:9313", MapID: "cave"},
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
	for mapID, wantOwner := range map[string]string{"green": "node-a", "cave": "node-c", "ruins": "node-e"} {
		if got := topology.Owners[mapID]; got != wantOwner {
			t.Fatalf("%s owner = %q，want %q", mapID, got, wantOwner)
		}
		if got := topology.MapEpochs[mapID]; got != 1 {
			t.Fatalf("%s epoch = %d，want 1", mapID, got)
		}
	}
	if topology.UpdatedAt.Location() != time.UTC {
		t.Fatalf("updated_at location = %s，want UTC", topology.UpdatedAt.Location())
	}
}

func TestBuildInitialTopologyWaitsForTwoCandidatesPerMap(t *testing.T) {
	topology, ready, err := BuildInitialTopology(map[string]struct{}{"green": {}, "cave": {}}, []NodeRegistryInfo{
		{ID: "node-a", Addr: "127.0.0.1:9311", MapID: "green"},
		{ID: "node-b", Addr: "127.0.0.1:9312", MapID: "green"},
		{ID: "node-c", Addr: "127.0.0.1:9313", MapID: "cave"},
	}, time.Now())
	if err != nil {
		t.Fatalf("候选集不完整应继续等待，而非报错: %v", err)
	}
	if ready {
		t.Fatalf("单候选地图仍生成了拓扑: %+v", topology)
	}
}

func TestBuildInitialTopologyRejectsInvalidSingleMapRegistration(t *testing.T) {
	for _, test := range []struct {
		name  string
		nodes []NodeRegistryInfo
		want  string
	}{
		{name: "empty map", nodes: []NodeRegistryInfo{{ID: "node-a", Addr: "127.0.0.1:9311"}}, want: "非法地图"},
		{name: "unknown map", nodes: []NodeRegistryInfo{{ID: "node-a", Addr: "127.0.0.1:9311", MapID: "lava"}}, want: "未知地图"},
		{name: "duplicate node", nodes: []NodeRegistryInfo{{ID: "node-a", Addr: "127.0.0.1:9311", MapID: "green"}, {ID: "node-a", Addr: "127.0.0.1:9312", MapID: "green"}}, want: "重复注册"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, ready, err := BuildInitialTopology(map[string]struct{}{"green": {}}, test.nodes, time.Now())
			if ready {
				t.Fatal("非法节点注册被判定为已就绪")
			}
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("注册错误 = %v，want 包含 %q", err, test.want)
			}
		})
	}
}
