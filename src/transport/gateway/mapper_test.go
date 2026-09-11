package gateway

import (
	"reflect"
	"testing"

	"battleworld/protocol"
)

func TestWorldStateRoundTripPreservesCompleteStateAndIsolation(t *testing.T) {
	original := gatewayTestWorldState()

	wire := ToWorldState(original)
	if wire == nil {
		t.Fatal("WorldState 未转换为 wire 消息")
	}
	if wire.GetSelf().GetUsername() != "hero" || wire.GetMap().GetPlayers()[0].GetUsername() != "hero" || wire.GetBoss().GetSites()[0].GetMapId() != "green" {
		t.Fatalf("WorldState wire 字段缺失: %+v", wire)
	}
	original.Events[0] = "mutated-source-event"
	original.Map.Terrain[0] = "mutated-source-terrain"
	original.Nodes[0].PrimaryMaps[0] = "mutated-source-map"
	if wire.GetEvents()[0] == original.Events[0] || wire.GetMap().GetTerrain()[0] == original.Map.Terrain[0] || wire.GetNodes()[0].GetPrimaryMaps()[0] == original.Nodes[0].PrimaryMaps[0] {
		t.Fatal("协议转换与领域输入共享可变切片")
	}

	restored := FromWorldState(wire)
	if restored == nil {
		t.Fatal("wire WorldState 未还原为领域模型")
	}
	want := gatewayTestWorldState()
	if !reflect.DeepEqual(restored, want) {
		t.Fatalf("WorldState 往返不一致:\n got:  %#v\n want: %#v", restored, want)
	}
	wire.Events[0] = "mutated-wire-event"
	wire.Map.Terrain[0] = "mutated-wire-terrain"
	wire.Nodes[0].PrimaryMaps[0] = "mutated-wire-map"
	if restored.Events[0] == wire.Events[0] || restored.Map.Terrain[0] == wire.Map.Terrain[0] || restored.Nodes[0].PrimaryMaps[0] == wire.Nodes[0].PrimaryMaps[0] {
		t.Fatal("领域还原与协议输入共享可变切片")
	}
}

func TestLegacyMessageRoundTripPreservesCredentialsAndState(t *testing.T) {
	state := gatewayTestWorldState()
	original := protocol.Message{
		Type:     protocol.TypeMove,
		Action:   "move",
		Username: "hero",
		Password: "plaintext-is-v1-only",
		Dir:      protocol.DirLeft,
		MapID:    "green",
		NodeID:   "node-a",
		Confirm:  "yes",
		Item:     "potion",
		Text:     "moved",
		OK:       true,
		Error:    "",
		State:    state,
	}

	wire := ToLegacyMessage(original)
	if wire.GetPassword() != original.Password || wire.GetState().GetTopologyVersion() != original.State.TopologyVersion {
		t.Fatalf("V1 message wire 字段缺失: %+v", wire)
	}
	original.State.Events[0] = "mutated-source-event"
	actual := FromLegacyMessage(wire)
	want := protocol.Message{
		Type:     protocol.TypeMove,
		Action:   "move",
		Username: "hero",
		Password: "plaintext-is-v1-only",
		Dir:      protocol.DirLeft,
		MapID:    "green",
		NodeID:   "node-a",
		Confirm:  "yes",
		Item:     "potion",
		Text:     "moved",
		OK:       true,
		Error:    "",
		State:    gatewayTestWorldState(),
	}
	if !reflect.DeepEqual(actual, want) {
		t.Fatalf("V1 Message 往返不一致:\n got:  %#v\n want: %#v", actual, want)
	}
}

func gatewayTestWorldState() *protocol.WorldState {
	return &protocol.WorldState{
		Self: protocol.PlayerView{Username: "hero", MapID: "green", X: 3, Y: 4, HP: 95, MaxHP: 100, Attack: 12, Potions: 2, Treasures: 7, Kills: 8, Deaths: 1, Victories: 4, Alive: true, RespawnIn: 5, LastUpdate: "2026-09-10T16:00:00Z"},
		Map: protocol.MapView{
			ID: "green", Name: "翡翠原野", NodeID: "node-a", Width: 20, Height: 10, Terrain: []string{"....", "####"}, Version: 44,
			Players:   []protocol.PlayerView{{Username: "hero", MapID: "green", X: 3, Y: 4, HP: 95, MaxHP: 100, Attack: 12, Potions: 2, Treasures: 7, Kills: 8, Deaths: 1, Victories: 4, Alive: true, RespawnIn: 5, LastUpdate: "2026-09-10T16:00:00Z"}},
			NPCs:      []protocol.NPCView{{ID: "npc-1", Name: "守卫", X: 6, Y: 7, HP: 40, MaxHP: 40, Attack: 9, Alive: true}},
			Treasures: []protocol.TreasureView{{ID: "gold-1", Kind: "gold", X: 8, Y: 2, Value: 3}},
		},
		Maps:            []protocol.MapBrief{{ID: "green", Name: "翡翠原野", NodeID: "node-a", Players: 1, NPCs: 1, Treasures: 1, Version: 44, Primary: true, IsCurrent: true, Checkpoint: 41}},
		Nodes:           []protocol.NodeView{{ID: "node-a", Addr: "127.0.0.1:9311", Healthy: true, PrimaryMaps: []string{"green"}, ReplicaMaps: []string{"cave"}, LastHeartbeat: "2026-09-10T16:00:00Z"}},
		Boss:            protocol.BossView{Name: "巨龙", HP: 900, MaxHP: 1000, Alive: true, LastHit: "hero", RespawnIn: 10, AttackGap: 2, Version: 6, Sites: []protocol.BossSite{{MapID: "green", X: 9, Y: 3}}},
		Events:          []string{"boss appeared", "hero moved"},
		SessionVersion:  12,
		TopologyVersion: 15,
		MapEpoch:        4,
	}
}
