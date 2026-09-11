// Package node contains protobuf/domain mappings owned by the Node RPC boundary.
// It intentionally depends on both protocol and pb; neither domain package may
// import this package or generated protobuf code.
package node

import (
	"fmt"
	"time"

	"battleworld/pb"
	"battleworld/protocol"

	"google.golang.org/protobuf/types/known/timestamppb"
)

func NewMapAuthority(mapID, ownerNodeID string, epoch uint64) *pb.MapAuthority {
	return &pb.MapAuthority{MapId: mapID, OwnerNodeId: ownerNodeID, MapEpoch: epoch}
}

func ToPlayerState(profile protocol.UserProfile) *pb.PlayerState {
	return &pb.PlayerState{
		Username:  profile.Username,
		LastMap:   profile.LastMap,
		LastNode:  profile.LastNode,
		X:         int32(profile.X),
		Y:         int32(profile.Y),
		Hp:        int32(profile.HP),
		MaxHp:     int32(profile.MaxHP),
		Attack:    int32(profile.Attack),
		Potions:   int32(profile.Potions),
		Treasures: int32(profile.Treasures),
		Kills:     int32(profile.Kills),
		Deaths:    int32(profile.Deaths),
		Victories: int32(profile.Victories),
		Alive:     profile.Alive,
	}
}

func FromPlayerState(profile *pb.PlayerState) protocol.UserProfile {
	if profile == nil {
		return protocol.UserProfile{}
	}
	return protocol.UserProfile{
		Username:  profile.GetUsername(),
		LastMap:   profile.GetLastMap(),
		LastNode:  profile.GetLastNode(),
		X:         int(profile.GetX()),
		Y:         int(profile.GetY()),
		HP:        int(profile.GetHp()),
		MaxHP:     int(profile.GetMaxHp()),
		Attack:    int(profile.GetAttack()),
		Potions:   int(profile.GetPotions()),
		Treasures: int(profile.GetTreasures()),
		Kills:     int(profile.GetKills()),
		Deaths:    int(profile.GetDeaths()),
		Victories: int(profile.GetVictories()),
		Alive:     profile.GetAlive(),
	}
}

// ToLegacyUserProfile and FromLegacyUserProfile are isolated V1 compatibility
// mappings. PasswordHash must never flow through PlayerState or NodeServiceV2.
func ToLegacyUserProfile(profile protocol.UserProfile) *pb.UserProfile {
	return &pb.UserProfile{
		Username:     profile.Username,
		PasswordHash: profile.PasswordHash,
		LastMap:      profile.LastMap,
		LastNode:     profile.LastNode,
		X:            int32(profile.X),
		Y:            int32(profile.Y),
		Hp:           int32(profile.HP),
		MaxHp:        int32(profile.MaxHP),
		Attack:       int32(profile.Attack),
		Potions:      int32(profile.Potions),
		Treasures:    int32(profile.Treasures),
		Kills:        int32(profile.Kills),
		Deaths:       int32(profile.Deaths),
		Victories:    int32(profile.Victories),
		Alive:        profile.Alive,
	}
}

func FromLegacyUserProfile(profile *pb.UserProfile) protocol.UserProfile {
	if profile == nil {
		return protocol.UserProfile{}
	}
	return protocol.UserProfile{
		Username:     profile.GetUsername(),
		PasswordHash: profile.GetPasswordHash(),
		LastMap:      profile.GetLastMap(),
		LastNode:     profile.GetLastNode(),
		X:            int(profile.GetX()),
		Y:            int(profile.GetY()),
		HP:           int(profile.GetHp()),
		MaxHP:        int(profile.GetMaxHp()),
		Attack:       int(profile.GetAttack()),
		Potions:      int(profile.GetPotions()),
		Treasures:    int(profile.GetTreasures()),
		Kills:        int(profile.GetKills()),
		Deaths:       int(profile.GetDeaths()),
		Victories:    int(profile.GetVictories()),
		Alive:        profile.GetAlive(),
	}
}

func ToPlayerView(player protocol.PlayerView) *pb.PlayerView {
	return &pb.PlayerView{
		Username:   player.Username,
		MapId:      player.MapID,
		X:          int32(player.X),
		Y:          int32(player.Y),
		Hp:         int32(player.HP),
		MaxHp:      int32(player.MaxHP),
		Attack:     int32(player.Attack),
		Potions:    int32(player.Potions),
		Treasures:  int32(player.Treasures),
		Kills:      int32(player.Kills),
		Deaths:     int32(player.Deaths),
		Victories:  int32(player.Victories),
		Alive:      player.Alive,
		RespawnIn:  int32(player.RespawnIn),
		LastUpdate: player.LastUpdate,
	}
}

func FromPlayerView(player *pb.PlayerView) protocol.PlayerView {
	if player == nil {
		return protocol.PlayerView{}
	}
	return protocol.PlayerView{
		Username:   player.GetUsername(),
		MapID:      player.GetMapId(),
		X:          int(player.GetX()),
		Y:          int(player.GetY()),
		HP:         int(player.GetHp()),
		MaxHP:      int(player.GetMaxHp()),
		Attack:     int(player.GetAttack()),
		Potions:    int(player.GetPotions()),
		Treasures:  int(player.GetTreasures()),
		Kills:      int(player.GetKills()),
		Deaths:     int(player.GetDeaths()),
		Victories:  int(player.GetVictories()),
		Alive:      player.GetAlive(),
		RespawnIn:  int(player.GetRespawnIn()),
		LastUpdate: player.GetLastUpdate(),
	}
}

func ToNPCView(npc protocol.NPCView) *pb.NPCView {
	return &pb.NPCView{
		Id:     npc.ID,
		Name:   npc.Name,
		X:      int32(npc.X),
		Y:      int32(npc.Y),
		Hp:     int32(npc.HP),
		MaxHp:  int32(npc.MaxHP),
		Attack: int32(npc.Attack),
		Alive:  npc.Alive,
	}
}

func FromNPCView(npc *pb.NPCView) protocol.NPCView {
	if npc == nil {
		return protocol.NPCView{}
	}
	return protocol.NPCView{
		ID:     npc.GetId(),
		Name:   npc.GetName(),
		X:      int(npc.GetX()),
		Y:      int(npc.GetY()),
		HP:     int(npc.GetHp()),
		MaxHP:  int(npc.GetMaxHp()),
		Attack: int(npc.GetAttack()),
		Alive:  npc.GetAlive(),
	}
}

func ToTreasureView(treasure protocol.TreasureView) *pb.TreasureView {
	return &pb.TreasureView{
		Id:    treasure.ID,
		Kind:  treasure.Kind,
		X:     int32(treasure.X),
		Y:     int32(treasure.Y),
		Value: int32(treasure.Value),
	}
}

func FromTreasureView(treasure *pb.TreasureView) protocol.TreasureView {
	if treasure == nil {
		return protocol.TreasureView{}
	}
	return protocol.TreasureView{
		ID:    treasure.GetId(),
		Kind:  treasure.GetKind(),
		X:     int(treasure.GetX()),
		Y:     int(treasure.GetY()),
		Value: int(treasure.GetValue()),
	}
}

func ToMapView(view protocol.MapView) *pb.MapView {
	result := &pb.MapView{
		Id:        view.ID,
		Name:      view.Name,
		NodeId:    view.NodeID,
		Width:     int32(view.Width),
		Height:    int32(view.Height),
		Terrain:   append([]string(nil), view.Terrain...),
		Version:   view.Version,
		Players:   make([]*pb.PlayerView, 0, len(view.Players)),
		Npcs:      make([]*pb.NPCView, 0, len(view.NPCs)),
		Treasures: make([]*pb.TreasureView, 0, len(view.Treasures)),
	}
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
	result := protocol.MapView{
		ID:        view.GetId(),
		Name:      view.GetName(),
		NodeID:    view.GetNodeId(),
		Width:     int(view.GetWidth()),
		Height:    int(view.GetHeight()),
		Terrain:   append([]string(nil), view.GetTerrain()...),
		Version:   view.GetVersion(),
		Players:   make([]protocol.PlayerView, 0, len(view.GetPlayers())),
		NPCs:      make([]protocol.NPCView, 0, len(view.GetNpcs())),
		Treasures: make([]protocol.TreasureView, 0, len(view.GetTreasures())),
	}
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

func ToCheckpoint(checkpoint protocol.MapCheckpoint) *pb.NodeCheckpoint {
	if checkpoint.MapID == "" {
		return nil
	}
	result := &pb.NodeCheckpoint{
		MapId:      checkpoint.MapID,
		NodeId:     checkpoint.NodeID,
		MapEpoch:   checkpoint.MapEpoch,
		Version:    checkpoint.Version,
		Terrain:    append([]string(nil), checkpoint.Terrain...),
		CapturedAt: timestamppb.New(checkpoint.Checkpoint),
		Players:    make([]*pb.PlayerView, 0, len(checkpoint.Players)),
		Npcs:       make([]*pb.NPCView, 0, len(checkpoint.NPCs)),
		Treasures:  make([]*pb.TreasureView, 0, len(checkpoint.Treasures)),
	}
	for _, player := range checkpoint.Players {
		result.Players = append(result.Players, ToPlayerView(player))
	}
	for _, npc := range checkpoint.NPCs {
		result.Npcs = append(result.Npcs, ToNPCView(npc))
	}
	for _, treasure := range checkpoint.Treasures {
		result.Treasures = append(result.Treasures, ToTreasureView(treasure))
	}
	return result
}

func FromCheckpoint(checkpoint *pb.NodeCheckpoint) (protocol.MapCheckpoint, error) {
	if checkpoint == nil {
		return protocol.MapCheckpoint{}, fmt.Errorf("checkpoint 不能为空")
	}
	capturedAt := checkpoint.GetCapturedAt()
	if capturedAt == nil {
		return protocol.MapCheckpoint{}, fmt.Errorf("checkpoint.captured_at 不能为空")
	}
	if err := capturedAt.CheckValid(); err != nil {
		return protocol.MapCheckpoint{}, fmt.Errorf("checkpoint.captured_at 无效: %w", err)
	}
	result := protocol.MapCheckpoint{
		MapID:      checkpoint.GetMapId(),
		NodeID:     checkpoint.GetNodeId(),
		MapEpoch:   checkpoint.GetMapEpoch(),
		Version:    checkpoint.GetVersion(),
		Terrain:    append([]string(nil), checkpoint.GetTerrain()...),
		Checkpoint: capturedAt.AsTime(),
		Players:    make([]protocol.PlayerView, 0, len(checkpoint.GetPlayers())),
		NPCs:       make([]protocol.NPCView, 0, len(checkpoint.GetNpcs())),
		Treasures:  make([]protocol.TreasureView, 0, len(checkpoint.GetTreasures())),
	}
	for _, player := range checkpoint.GetPlayers() {
		result.Players = append(result.Players, FromPlayerView(player))
	}
	for _, npc := range checkpoint.GetNpcs() {
		result.NPCs = append(result.NPCs, FromNPCView(npc))
	}
	for _, treasure := range checkpoint.GetTreasures() {
		result.Treasures = append(result.Treasures, FromTreasureView(treasure))
	}
	return result, nil
}

func ToNodeView(view protocol.NodeView) *pb.NodeView {
	return &pb.NodeView{
		Id:            view.ID,
		Addr:          view.Addr,
		Healthy:       view.Healthy,
		PrimaryMaps:   append([]string(nil), view.PrimaryMaps...),
		ReplicaMaps:   append([]string(nil), view.ReplicaMaps...),
		LastHeartbeat: view.LastHeartbeat,
	}
}

func FromNodeView(view *pb.NodeView) protocol.NodeView {
	if view == nil {
		return protocol.NodeView{}
	}
	return protocol.NodeView{
		ID:            view.GetId(),
		Addr:          view.GetAddr(),
		Healthy:       view.GetHealthy(),
		PrimaryMaps:   append([]string(nil), view.GetPrimaryMaps()...),
		ReplicaMaps:   append([]string(nil), view.GetReplicaMaps()...),
		LastHeartbeat: view.GetLastHeartbeat(),
	}
}

// ToLegacyCheckpoint and FromLegacyCheckpoint preserve the V1 RFC3339 string
// wire contract until all callers have been migrated to NodeServiceV2.
func ToLegacyCheckpoint(checkpoint protocol.MapCheckpoint) *pb.MapCheckpoint {
	if checkpoint.MapID == "" {
		return nil
	}
	result := &pb.MapCheckpoint{
		MapId:      checkpoint.MapID,
		NodeId:     checkpoint.NodeID,
		MapEpoch:   checkpoint.MapEpoch,
		Version:    checkpoint.Version,
		Terrain:    append([]string(nil), checkpoint.Terrain...),
		Checkpoint: checkpoint.Checkpoint.Format(time.RFC3339),
		Players:    make([]*pb.PlayerView, 0, len(checkpoint.Players)),
		Npcs:       make([]*pb.NPCView, 0, len(checkpoint.NPCs)),
		Treasures:  make([]*pb.TreasureView, 0, len(checkpoint.Treasures)),
	}
	for _, player := range checkpoint.Players {
		result.Players = append(result.Players, ToPlayerView(player))
	}
	for _, npc := range checkpoint.NPCs {
		result.Npcs = append(result.Npcs, ToNPCView(npc))
	}
	for _, treasure := range checkpoint.Treasures {
		result.Treasures = append(result.Treasures, ToTreasureView(treasure))
	}
	return result
}

func FromLegacyCheckpoint(checkpoint *pb.MapCheckpoint) protocol.MapCheckpoint {
	if checkpoint == nil {
		return protocol.MapCheckpoint{}
	}
	capturedAt, _ := time.Parse(time.RFC3339, checkpoint.GetCheckpoint())
	result := protocol.MapCheckpoint{
		MapID:      checkpoint.GetMapId(),
		NodeID:     checkpoint.GetNodeId(),
		MapEpoch:   checkpoint.GetMapEpoch(),
		Version:    checkpoint.GetVersion(),
		Terrain:    append([]string(nil), checkpoint.GetTerrain()...),
		Checkpoint: capturedAt,
		Players:    make([]protocol.PlayerView, 0, len(checkpoint.GetPlayers())),
		NPCs:       make([]protocol.NPCView, 0, len(checkpoint.GetNpcs())),
		Treasures:  make([]protocol.TreasureView, 0, len(checkpoint.GetTreasures())),
	}
	for _, player := range checkpoint.GetPlayers() {
		result.Players = append(result.Players, FromPlayerView(player))
	}
	for _, npc := range checkpoint.GetNpcs() {
		result.NPCs = append(result.NPCs, FromNPCView(npc))
	}
	for _, treasure := range checkpoint.GetTreasures() {
		result.Treasures = append(result.Treasures, FromTreasureView(treasure))
	}
	return result
}

func DirectionFromString(direction string) (pb.Direction, error) {
	switch direction {
	case protocol.DirUp:
		return pb.Direction_DIRECTION_UP, nil
	case protocol.DirDown:
		return pb.Direction_DIRECTION_DOWN, nil
	case protocol.DirLeft:
		return pb.Direction_DIRECTION_LEFT, nil
	case protocol.DirRight:
		return pb.Direction_DIRECTION_RIGHT, nil
	default:
		return pb.Direction_DIRECTION_UNSPECIFIED, fmt.Errorf("无效移动方向：%q", direction)
	}
}

func DirectionToString(direction pb.Direction) (string, error) {
	switch direction {
	case pb.Direction_DIRECTION_UP:
		return protocol.DirUp, nil
	case pb.Direction_DIRECTION_DOWN:
		return protocol.DirDown, nil
	case pb.Direction_DIRECTION_LEFT:
		return protocol.DirLeft, nil
	case pb.Direction_DIRECTION_RIGHT:
		return protocol.DirRight, nil
	default:
		return "", fmt.Errorf("无效移动方向：%s", direction)
	}
}
