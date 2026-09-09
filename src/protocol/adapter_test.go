package protocol

import (
	"reflect"
	"testing"
	"time"

	"battleworld/pb"

	"google.golang.org/protobuf/proto"
)

func TestMessageProtoRoundTripPreservesWorldState(t *testing.T) {
	original := Message{
		Type:     TypeState,
		Action:   "status",
		Username: "tester",
		Password: "not-used-by-state",
		Dir:      DirUp,
		MapID:    "green",
		NodeID:   "node-a",
		Confirm:  "confirm",
		Item:     "potion",
		Text:     "state refreshed",
		OK:       true,
		Error:    "",
		State: &WorldState{
			Self: PlayerView{
				Username:   "tester",
				MapID:      "green",
				X:          3,
				Y:          4,
				HP:         120,
				MaxHP:      120,
				Attack:     35,
				Potions:    2,
				Treasures:  8,
				Kills:      4,
				Deaths:     1,
				Victories:  2,
				Alive:      true,
				RespawnIn:  0,
				LastUpdate: "2026-09-09T16:00:00Z",
			},
			Map: MapView{
				ID:        "green",
				Name:      "青岚要塞",
				NodeID:    "node-a",
				Width:     56,
				Height:    24,
				Terrain:   []string{"....", "####"},
				Players:   []PlayerView{{Username: "tester", MapID: "green", X: 3, Y: 4, HP: 120, Alive: true}},
				NPCs:      []NPCView{{ID: "npc-1", Name: "守卫", X: 9, Y: 8, HP: 40, MaxHP: 40, Attack: 8, Alive: true}},
				Treasures: []TreasureView{{ID: "treasure-1", Kind: "gold", X: 5, Y: 6, Value: 3}},
				Version:   17,
			},
			Maps:  []MapBrief{{ID: "green", Name: "青岚要塞", NodeID: "node-a", Players: 1, NPCs: 1, Treasures: 1, Version: 17, Primary: true, IsCurrent: true, Checkpoint: 123}},
			Nodes: []NodeView{{ID: "node-a", Addr: "127.0.0.1:9311", Healthy: true, PrimaryMaps: []string{"green"}, ReplicaMaps: []string{"cave"}, LastHeartbeat: "2026-09-09T16:00:00Z"}},
			Boss: BossView{
				Name:      "世界首领",
				HP:        1500,
				MaxHP:     1600,
				Alive:     true,
				LastHit:   "tester",
				RespawnIn: 0,
				AttackGap: 3,
				Sites:     []BossSite{{MapID: "green", X: 8, Y: 8}},
				Version:   5,
			},
			Events:          []string{"获得宝箱", "地图 tick"},
			SessionVersion:  11,
			TopologyVersion: 12,
			MapEpoch:        13,
		},
	}

	wire := ToProtoMessage(original)
	encoded, err := proto.Marshal(wire)
	if err != nil {
		t.Fatalf("marshal V1 Message: %v", err)
	}
	var decoded pb.Message
	if err := proto.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshal V1 Message: %v", err)
	}
	actual := FromProtoMessage(&decoded)

	if !reflect.DeepEqual(actual, original) {
		t.Fatalf("V1 Message protobuf 往返后不一致:\n got:  %#v\n want: %#v", actual, original)
	}
}

func TestMapCheckpointProtoRoundTripPreservesAuthorityAndTimestamp(t *testing.T) {
	checkpointAt := time.Date(2026, time.September, 9, 16, 0, 0, 0, time.UTC)
	original := MapCheckpoint{
		MapID:      "green",
		NodeID:     "node-a",
		MapEpoch:   7,
		Version:    42,
		Terrain:    []string{"....", "####"},
		Players:    []PlayerView{{Username: "tester", MapID: "green", X: 3, Y: 4, HP: 120, Alive: true}},
		NPCs:       []NPCView{{ID: "npc-1", Name: "守卫", X: 9, Y: 8, HP: 40, MaxHP: 40, Attack: 8, Alive: true}},
		Treasures:  []TreasureView{{ID: "treasure-1", Kind: "gold", X: 5, Y: 6, Value: 3}},
		Checkpoint: checkpointAt,
	}

	wire := ToProtoMapCheckpoint(original)
	encoded, err := proto.Marshal(wire)
	if err != nil {
		t.Fatalf("marshal MapCheckpoint: %v", err)
	}
	var decoded pb.MapCheckpoint
	if err := proto.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshal MapCheckpoint: %v", err)
	}
	actual := FromProtoMapCheckpoint(&decoded)

	if !reflect.DeepEqual(actual, original) {
		t.Fatalf("MapCheckpoint protobuf 往返后不一致:\n got:  %#v\n want: %#v", actual, original)
	}
}
