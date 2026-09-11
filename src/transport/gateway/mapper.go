// Package gateway owns protobuf mappings at the Gateway transport boundary.
// Domain packages stay protobuf-free; this package is the only place that maps
// protocol read models to Gateway V1/V2 wire messages.
package gateway

import (
	"battleworld/pb"
	"battleworld/protocol"
)

func ToPlayerView(player protocol.PlayerView) *pb.PlayerView {
	return &pb.PlayerView{
		Username: player.Username, MapId: player.MapID, X: int32(player.X), Y: int32(player.Y),
		Hp: int32(player.HP), MaxHp: int32(player.MaxHP), Attack: int32(player.Attack), Potions: int32(player.Potions),
		Treasures: int32(player.Treasures), Kills: int32(player.Kills), Deaths: int32(player.Deaths), Victories: int32(player.Victories),
		Alive: player.Alive, RespawnIn: int32(player.RespawnIn), LastUpdate: player.LastUpdate,
	}
}

func FromPlayerView(player *pb.PlayerView) protocol.PlayerView {
	if player == nil {
		return protocol.PlayerView{}
	}
	return protocol.PlayerView{
		Username: player.GetUsername(), MapID: player.GetMapId(), X: int(player.GetX()), Y: int(player.GetY()),
		HP: int(player.GetHp()), MaxHP: int(player.GetMaxHp()), Attack: int(player.GetAttack()), Potions: int(player.GetPotions()),
		Treasures: int(player.GetTreasures()), Kills: int(player.GetKills()), Deaths: int(player.GetDeaths()), Victories: int(player.GetVictories()),
		Alive: player.GetAlive(), RespawnIn: int(player.GetRespawnIn()), LastUpdate: player.GetLastUpdate(),
	}
}

func ToNPCView(npc protocol.NPCView) *pb.NPCView {
	return &pb.NPCView{Id: npc.ID, Name: npc.Name, X: int32(npc.X), Y: int32(npc.Y), Hp: int32(npc.HP), MaxHp: int32(npc.MaxHP), Attack: int32(npc.Attack), Alive: npc.Alive}
}

func FromNPCView(npc *pb.NPCView) protocol.NPCView {
	if npc == nil {
		return protocol.NPCView{}
	}
	return protocol.NPCView{ID: npc.GetId(), Name: npc.GetName(), X: int(npc.GetX()), Y: int(npc.GetY()), HP: int(npc.GetHp()), MaxHP: int(npc.GetMaxHp()), Attack: int(npc.GetAttack()), Alive: npc.GetAlive()}
}

func ToTreasureView(treasure protocol.TreasureView) *pb.TreasureView {
	return &pb.TreasureView{Id: treasure.ID, Kind: treasure.Kind, X: int32(treasure.X), Y: int32(treasure.Y), Value: int32(treasure.Value)}
}

func FromTreasureView(treasure *pb.TreasureView) protocol.TreasureView {
	if treasure == nil {
		return protocol.TreasureView{}
	}
	return protocol.TreasureView{ID: treasure.GetId(), Kind: treasure.GetKind(), X: int(treasure.GetX()), Y: int(treasure.GetY()), Value: int(treasure.GetValue())}
}

func ToMapBrief(brief protocol.MapBrief) *pb.MapBrief {
	return &pb.MapBrief{Id: brief.ID, Name: brief.Name, NodeId: brief.NodeID, Players: int32(brief.Players), Npcs: int32(brief.NPCs), Treasures: int32(brief.Treasures), Version: brief.Version, Primary: brief.Primary, IsCurrent: brief.IsCurrent, Checkpoint: brief.Checkpoint}
}

func FromMapBrief(brief *pb.MapBrief) protocol.MapBrief {
	if brief == nil {
		return protocol.MapBrief{}
	}
	return protocol.MapBrief{ID: brief.GetId(), Name: brief.GetName(), NodeID: brief.GetNodeId(), Players: int(brief.GetPlayers()), NPCs: int(brief.GetNpcs()), Treasures: int(brief.GetTreasures()), Version: brief.GetVersion(), Primary: brief.GetPrimary(), IsCurrent: brief.GetIsCurrent(), Checkpoint: brief.GetCheckpoint()}
}

func ToNodeView(view protocol.NodeView) *pb.NodeView {
	return &pb.NodeView{Id: view.ID, Addr: view.Addr, Healthy: view.Healthy, PrimaryMaps: append([]string(nil), view.PrimaryMaps...), ReplicaMaps: append([]string(nil), view.ReplicaMaps...), LastHeartbeat: view.LastHeartbeat}
}

func FromNodeView(view *pb.NodeView) protocol.NodeView {
	if view == nil {
		return protocol.NodeView{}
	}
	return protocol.NodeView{ID: view.GetId(), Addr: view.GetAddr(), Healthy: view.GetHealthy(), PrimaryMaps: append([]string(nil), view.GetPrimaryMaps()...), ReplicaMaps: append([]string(nil), view.GetReplicaMaps()...), LastHeartbeat: view.GetLastHeartbeat()}
}

func ToMapView(view protocol.MapView) *pb.MapView {
	result := &pb.MapView{Id: view.ID, Name: view.Name, NodeId: view.NodeID, Width: int32(view.Width), Height: int32(view.Height), Terrain: append([]string(nil), view.Terrain...), Version: view.Version, Players: make([]*pb.PlayerView, 0, len(view.Players)), Npcs: make([]*pb.NPCView, 0, len(view.NPCs)), Treasures: make([]*pb.TreasureView, 0, len(view.Treasures))}
	for _, player := range view.Players {
		result.Players = append(result.Players, ToPlayerView(player))
	}
	for _, npc := range view.NPCs {
		result.Npcs = append(result.Npcs, ToNPCView(npc))
	}
	for _, treasure := range view.Treasures {
		result.Treasures = append(result.Treasures, ToTreasureView(treasure))
	}
	return result
}

func FromMapView(view *pb.MapView) protocol.MapView {
	if view == nil {
		return protocol.MapView{}
	}
	result := protocol.MapView{ID: view.GetId(), Name: view.GetName(), NodeID: view.GetNodeId(), Width: int(view.GetWidth()), Height: int(view.GetHeight()), Terrain: append([]string(nil), view.GetTerrain()...), Version: view.GetVersion(), Players: make([]protocol.PlayerView, 0, len(view.GetPlayers())), NPCs: make([]protocol.NPCView, 0, len(view.GetNpcs())), Treasures: make([]protocol.TreasureView, 0, len(view.GetTreasures()))}
	for _, player := range view.GetPlayers() {
		result.Players = append(result.Players, FromPlayerView(player))
	}
	for _, npc := range view.GetNpcs() {
		result.NPCs = append(result.NPCs, FromNPCView(npc))
	}
	for _, treasure := range view.GetTreasures() {
		result.Treasures = append(result.Treasures, FromTreasureView(treasure))
	}
	return result
}

func ToBossSite(site protocol.BossSite) *pb.BossSite {
	return &pb.BossSite{MapId: site.MapID, X: int32(site.X), Y: int32(site.Y)}
}

func FromBossSite(site *pb.BossSite) protocol.BossSite {
	if site == nil {
		return protocol.BossSite{}
	}
	return protocol.BossSite{MapID: site.GetMapId(), X: int(site.GetX()), Y: int(site.GetY())}
}

func ToBossView(view protocol.BossView) *pb.BossView {
	result := &pb.BossView{Name: view.Name, Hp: int32(view.HP), MaxHp: int32(view.MaxHP), Alive: view.Alive, LastHit: view.LastHit, RespawnIn: int32(view.RespawnIn), AttackGap: int32(view.AttackGap), Version: view.Version, Sites: make([]*pb.BossSite, 0, len(view.Sites))}
	for _, site := range view.Sites {
		result.Sites = append(result.Sites, ToBossSite(site))
	}
	return result
}

func FromBossView(view *pb.BossView) protocol.BossView {
	if view == nil {
		return protocol.BossView{}
	}
	result := protocol.BossView{Name: view.GetName(), HP: int(view.GetHp()), MaxHP: int(view.GetMaxHp()), Alive: view.GetAlive(), LastHit: view.GetLastHit(), RespawnIn: int(view.GetRespawnIn()), AttackGap: int(view.GetAttackGap()), Version: view.GetVersion(), Sites: make([]protocol.BossSite, 0, len(view.GetSites()))}
	for _, site := range view.GetSites() {
		result.Sites = append(result.Sites, FromBossSite(site))
	}
	return result
}

func ToWorldState(state *protocol.WorldState) *pb.WorldState {
	if state == nil {
		return nil
	}
	result := &pb.WorldState{Self: ToPlayerView(state.Self), Map: ToMapView(state.Map), Boss: ToBossView(state.Boss), Events: append([]string(nil), state.Events...), SessionVersion: state.SessionVersion, TopologyVersion: state.TopologyVersion, MapEpoch: state.MapEpoch, Maps: make([]*pb.MapBrief, 0, len(state.Maps)), Nodes: make([]*pb.NodeView, 0, len(state.Nodes))}
	for _, brief := range state.Maps {
		result.Maps = append(result.Maps, ToMapBrief(brief))
	}
	for _, node := range state.Nodes {
		result.Nodes = append(result.Nodes, ToNodeView(node))
	}
	return result
}

func FromWorldState(state *pb.WorldState) *protocol.WorldState {
	if state == nil {
		return nil
	}
	result := &protocol.WorldState{Self: FromPlayerView(state.GetSelf()), Map: FromMapView(state.GetMap()), Boss: FromBossView(state.GetBoss()), Events: append([]string(nil), state.GetEvents()...), SessionVersion: state.GetSessionVersion(), TopologyVersion: state.GetTopologyVersion(), MapEpoch: state.GetMapEpoch(), Maps: make([]protocol.MapBrief, 0, len(state.GetMaps())), Nodes: make([]protocol.NodeView, 0, len(state.GetNodes()))}
	for _, brief := range state.GetMaps() {
		result.Maps = append(result.Maps, FromMapBrief(brief))
	}
	for _, node := range state.GetNodes() {
		result.Nodes = append(result.Nodes, FromNodeView(node))
	}
	return result
}

// ToLegacyMessage and FromLegacyMessage exist only for the Gateway V1
// compatibility stream. No V2 caller should build pb.Message.
func ToLegacyMessage(message protocol.Message) *pb.Message {
	return &pb.Message{Type: message.Type, Action: message.Action, Username: message.Username, Password: message.Password, Dir: message.Dir, MapId: message.MapID, NodeId: message.NodeID, Confirm: message.Confirm, Item: message.Item, Text: message.Text, Ok: message.OK, Error: message.Error, State: ToWorldState(message.State)}
}

func FromLegacyMessage(message *pb.Message) protocol.Message {
	if message == nil {
		return protocol.Message{}
	}
	return protocol.Message{Type: message.GetType(), Action: message.GetAction(), Username: message.GetUsername(), Password: message.GetPassword(), Dir: message.GetDir(), MapID: message.GetMapId(), NodeID: message.GetNodeId(), Confirm: message.GetConfirm(), Item: message.GetItem(), Text: message.GetText(), OK: message.GetOk(), Error: message.GetError(), State: FromWorldState(message.GetState())}
}
