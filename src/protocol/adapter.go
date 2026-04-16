package protocol

import (
	"battleworld/pb"
	"time"
)

func ToProtoPlayerView(p PlayerView) *pb.PlayerView {
	return &pb.PlayerView{
		Username:   p.Username,
		MapId:      p.MapID,
		X:          int32(p.X),
		Y:          int32(p.Y),
		Hp:         int32(p.HP),
		MaxHp:      int32(p.MaxHP),
		Attack:     int32(p.Attack),
		Potions:    int32(p.Potions),
		Treasures:  int32(p.Treasures),
		Kills:      int32(p.Kills),
		Deaths:     int32(p.Deaths),
		Victories:  int32(p.Victories),
		Alive:      p.Alive,
		RespawnIn:  int32(p.RespawnIn),
		LastUpdate: p.LastUpdate,
	}
}

func FromProtoPlayerView(p *pb.PlayerView) PlayerView {
	if p == nil {
		return PlayerView{}
	}
	return PlayerView{
		Username:   p.Username,
		MapID:      p.MapId,
		X:          int(p.X),
		Y:          int(p.Y),
		HP:         int(p.Hp),
		MaxHP:      int(p.MaxHp),
		Attack:     int(p.Attack),
		Potions:    int(p.Potions),
		Treasures:  int(p.Treasures),
		Kills:      int(p.Kills),
		Deaths:     int(p.Deaths),
		Victories:  int(p.Victories),
		Alive:      p.Alive,
		RespawnIn:  int(p.RespawnIn),
		LastUpdate: p.LastUpdate,
	}
}

func ToProtoNPCView(n NPCView) *pb.NPCView {
	return &pb.NPCView{
		Id:     n.ID,
		Name:   n.Name,
		X:      int32(n.X),
		Y:      int32(n.Y),
		Hp:     int32(n.HP),
		MaxHp:  int32(n.MaxHP),
		Attack: int32(n.Attack),
		Alive:  n.Alive,
	}
}

func FromProtoNPCView(n *pb.NPCView) NPCView {
	if n == nil {
		return NPCView{}
	}
	return NPCView{
		ID:     n.Id,
		Name:   n.Name,
		X:      int(n.X),
		Y:      int(n.Y),
		HP:     int(n.Hp),
		MaxHP:  int(n.MaxHp),
		Attack: int(n.Attack),
		Alive:  n.Alive,
	}
}

func ToProtoTreasureView(t TreasureView) *pb.TreasureView {
	return &pb.TreasureView{
		Id:    t.ID,
		Kind:  t.Kind,
		X:     int32(t.X),
		Y:     int32(t.Y),
		Value: int32(t.Value),
	}
}

func FromProtoTreasureView(t *pb.TreasureView) TreasureView {
	if t == nil {
		return TreasureView{}
	}
	return TreasureView{
		ID:    t.Id,
		Kind:  t.Kind,
		X:     int(t.X),
		Y:     int(t.Y),
		Value: int(t.Value),
	}
}

func ToProtoMapBrief(m MapBrief) *pb.MapBrief {
	return &pb.MapBrief{
		Id:         m.ID,
		Name:       m.Name,
		NodeId:     m.NodeID,
		Players:    int32(m.Players),
		Npcs:       int32(m.NPCs),
		Treasures:  int32(m.Treasures),
		Version:    m.Version,
		Primary:    m.Primary,
		IsCurrent:  m.IsCurrent,
		Checkpoint: m.Checkpoint,
	}
}

func FromProtoMapBrief(m *pb.MapBrief) MapBrief {
	if m == nil {
		return MapBrief{}
	}
	return MapBrief{
		ID:         m.Id,
		Name:       m.Name,
		NodeID:     m.NodeId,
		Players:    int(m.Players),
		NPCs:       int(m.Npcs),
		Treasures:  int(m.Treasures),
		Version:    m.Version,
		Primary:    m.Primary,
		IsCurrent:  m.IsCurrent,
		Checkpoint: m.Checkpoint,
	}
}

func ToProtoNodeView(n NodeView) *pb.NodeView {
	return &pb.NodeView{
		Id:            n.ID,
		Addr:          n.Addr,
		Healthy:       n.Healthy,
		PrimaryMaps:   n.PrimaryMaps,
		ReplicaMaps:   n.ReplicaMaps,
		LastHeartbeat: n.LastHeartbeat,
	}
}

func FromProtoNodeView(n *pb.NodeView) NodeView {
	if n == nil {
		return NodeView{}
	}
	return NodeView{
		ID:            n.Id,
		Addr:          n.Addr,
		Healthy:       n.Healthy,
		PrimaryMaps:   n.PrimaryMaps,
		ReplicaMaps:   n.ReplicaMaps,
		LastHeartbeat: n.LastHeartbeat,
	}
}

func ToProtoMapView(m MapView) *pb.MapView {
	res := &pb.MapView{
		Id:      m.ID,
		Name:    m.Name,
		NodeId:  m.NodeID,
		Width:   int32(m.Width),
		Height:  int32(m.Height),
		Terrain: m.Terrain,
		Version: m.Version,
	}
	for _, p := range m.Players {
		res.Players = append(res.Players, ToProtoPlayerView(p))
	}
	for _, n := range m.NPCs {
		res.Npcs = append(res.Npcs, ToProtoNPCView(n))
	}
	for _, t := range m.Treasures {
		res.Treasures = append(res.Treasures, ToProtoTreasureView(t))
	}
	return res
}

func FromProtoMapView(m *pb.MapView) MapView {
	if m == nil {
		return MapView{}
	}
	res := MapView{
		ID:      m.Id,
		Name:    m.Name,
		NodeID:  m.NodeId,
		Width:   int(m.Width),
		Height:  int(m.Height),
		Terrain: m.Terrain,
		Version: m.Version,
	}
	for _, p := range m.Players {
		res.Players = append(res.Players, FromProtoPlayerView(p))
	}
	for _, n := range m.Npcs {
		res.NPCs = append(res.NPCs, FromProtoNPCView(n))
	}
	for _, t := range m.Treasures {
		res.Treasures = append(res.Treasures, FromProtoTreasureView(t))
	}
	return res
}

func ToProtoBossSite(b BossSite) *pb.BossSite {
	return &pb.BossSite{
		MapId: b.MapID,
		X:     int32(b.X),
		Y:     int32(b.Y),
	}
}

func FromProtoBossSite(b *pb.BossSite) BossSite {
	if b == nil {
		return BossSite{}
	}
	return BossSite{
		MapID: b.MapId,
		X:     int(b.X),
		Y:     int(b.Y),
	}
}

func ToProtoBossView(b BossView) *pb.BossView {
	res := &pb.BossView{
		Name:      b.Name,
		Hp:        int32(b.HP),
		MaxHp:     int32(b.MaxHP),
		Alive:     b.Alive,
		LastHit:   b.LastHit,
		RespawnIn: int32(b.RespawnIn),
		AttackGap: int32(b.AttackGap),
		Version:   b.Version,
	}
	for _, s := range b.Sites {
		res.Sites = append(res.Sites, ToProtoBossSite(s))
	}
	return res
}

func FromProtoBossView(b *pb.BossView) BossView {
	if b == nil {
		return BossView{}
	}
	res := BossView{
		Name:      b.Name,
		HP:        int(b.Hp),
		MaxHP:     int(b.MaxHp),
		Alive:     b.Alive,
		LastHit:   b.LastHit,
		RespawnIn: int(b.RespawnIn),
		AttackGap: int(b.AttackGap),
		Version:   b.Version,
	}
	for _, s := range b.Sites {
		res.Sites = append(res.Sites, FromProtoBossSite(s))
	}
	return res
}

func ToProtoWorldState(w *WorldState) *pb.WorldState {
	if w == nil {
		return nil
	}
	res := &pb.WorldState{
		Self:           ToProtoPlayerView(w.Self),
		Map:            ToProtoMapView(w.Map),
		Boss:           ToProtoBossView(w.Boss),
		Events:         w.Events,
		SessionVersion: w.SessionVersion,
	}
	for _, m := range w.Maps {
		res.Maps = append(res.Maps, ToProtoMapBrief(m))
	}
	for _, n := range w.Nodes {
		res.Nodes = append(res.Nodes, ToProtoNodeView(n))
	}
	return res
}

func FromProtoWorldState(w *pb.WorldState) *WorldState {
	if w == nil {
		return nil
	}
	res := &WorldState{
		Self:           FromProtoPlayerView(w.Self),
		Map:            FromProtoMapView(w.Map),
		Boss:           FromProtoBossView(w.Boss),
		Events:         w.Events,
		SessionVersion: w.SessionVersion,
	}
	for _, m := range w.Maps {
		res.Maps = append(res.Maps, FromProtoMapBrief(m))
	}
	for _, n := range w.Nodes {
		res.Nodes = append(res.Nodes, FromProtoNodeView(n))
	}
	return res
}

func ToProtoMessage(m Message) *pb.Message {
	return &pb.Message{
		Type:     m.Type,
		Action:   m.Action,
		Username: m.Username,
		Password: m.Password,
		Dir:      m.Dir,
		MapId:    m.MapID,
		NodeId:   m.NodeID,
		Confirm:  m.Confirm,
		Item:     m.Item,
		Text:     m.Text,
		Ok:       m.OK,
		Error:    m.Error,
		State:    ToProtoWorldState(m.State),
	}
}

func FromProtoMessage(m *pb.Message) Message {
	if m == nil {
		return Message{}
	}
	return Message{
		Type:     m.Type,
		Action:   m.Action,
		Username: m.Username,
		Password: m.Password,
		Dir:      m.Dir,
		MapID:    m.MapId,
		NodeID:   m.NodeId,
		Confirm:  m.Confirm,
		Item:     m.Item,
		Text:     m.Text,
		OK:       m.Ok,
		Error:    m.Error,
		State:    FromProtoWorldState(m.State),
	}
}

func ToProtoUserProfile(u UserProfile) *pb.UserProfile {
	return &pb.UserProfile{
		Username:     u.Username,
		PasswordHash: u.PasswordHash,
		LastMap:      u.LastMap,
		LastNode:     u.LastNode,
		X:            int32(u.X),
		Y:            int32(u.Y),
		Hp:           int32(u.HP),
		MaxHp:        int32(u.MaxHP),
		Attack:       int32(u.Attack),
		Potions:      int32(u.Potions),
		Treasures:    int32(u.Treasures),
		Kills:        int32(u.Kills),
		Deaths:       int32(u.Deaths),
		Victories:    int32(u.Victories),
		Alive:        u.Alive,
	}
}

func FromProtoUserProfile(u *pb.UserProfile) UserProfile {
	if u == nil {
		return UserProfile{}
	}
	return UserProfile{
		Username:     u.Username,
		PasswordHash: u.PasswordHash,
		LastMap:      u.LastMap,
		LastNode:     u.LastNode,
		X:            int(u.X),
		Y:            int(u.Y),
		HP:           int(u.Hp),
		MaxHP:        int(u.MaxHp),
		Attack:       int(u.Attack),
		Potions:      int(u.Potions),
		Treasures:    int(u.Treasures),
		Kills:        int(u.Kills),
		Deaths:       int(u.Deaths),
		Victories:    int(u.Victories),
		Alive:        u.Alive,
	}
}

func ToProtoMapCheckpoint(c MapCheckpoint) *pb.MapCheckpoint {
	if c.MapID == "" {
		return nil
	}
	cp := &pb.MapCheckpoint{
		MapId:      c.MapID,
		NodeId:     c.NodeID,
		Version:    c.Version,
		Terrain:    c.Terrain,
		Checkpoint: c.Checkpoint.Format(time.RFC3339),
	}
	for _, p := range c.Players {
		cp.Players = append(cp.Players, ToProtoPlayerView(p))
	}
	for _, n := range c.NPCs {
		cp.Npcs = append(cp.Npcs, ToProtoNPCView(n))
	}
	for _, t := range c.Treasures {
		cp.Treasures = append(cp.Treasures, ToProtoTreasureView(t))
	}
	return cp
}

func FromProtoMapCheckpoint(c *pb.MapCheckpoint) MapCheckpoint {
	if c == nil {
		return MapCheckpoint{}
	}
	t, _ := time.Parse(time.RFC3339, c.Checkpoint)
	cp := MapCheckpoint{
		MapID:      c.MapId,
		NodeID:     c.NodeId,
		Version:    c.Version,
		Terrain:    c.Terrain,
		Checkpoint: t,
	}
	for _, p := range c.Players {
		cp.Players = append(cp.Players, FromProtoPlayerView(p))
	}
	for _, n := range c.Npcs {
		cp.NPCs = append(cp.NPCs, FromProtoNPCView(n))
	}
	for _, t := range c.Treasures {
		cp.Treasures = append(cp.Treasures, FromProtoTreasureView(t))
	}
	return cp
}
