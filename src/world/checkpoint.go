package world

import (
	"time"

	"battleworld/protocol"
)

func (w *World) CaptureCheckpoint(nodeID string) protocol.MapCheckpoint {
	snapshot := w.Snapshot(nodeID)
	return protocol.MapCheckpoint{
		MapID:      snapshot.ID,
		NodeID:     snapshot.NodeID,
		Version:    snapshot.Version,
		Terrain:    snapshot.Terrain,
		Players:    snapshot.Players,
		NPCs:       snapshot.NPCs,
		Treasures:  snapshot.Treasures,
		Checkpoint: time.Now(),
	}
}

func (w *World) RestoreCheckpoint(cp protocol.MapCheckpoint) {
	if cp.MapID != w.cfg.ID || cp.Version == 0 {
		return
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if len(cp.Terrain) > 0 {
		w.terrain = stringsToGrid(cp.Terrain)
	}
	w.players = make(map[string]*Player)
	for _, view := range cp.Players {
		w.players[view.Username] = &Player{
			Username:   view.Username,
			MapID:      w.cfg.ID,
			X:          view.X,
			Y:          view.Y,
			HP:         view.HP,
			MaxHP:      view.MaxHP,
			Attack:     view.Attack,
			Potions:    valueOr(view.Potions, protocol.MaxPotions),
			Treasures:  view.Treasures,
			Kills:      view.Kills,
			Deaths:     view.Deaths,
			Victories:  view.Victories,
			Alive:      view.Alive,
			LastUpdate: time.Now(),
		}
	}
	w.npcs = make(map[string]*NPC)
	for _, view := range cp.NPCs {
		w.npcs[view.ID] = &NPC{
			ID:     view.ID,
			Name:   view.Name,
			X:      view.X,
			Y:      view.Y,
			HP:     view.HP,
			MaxHP:  view.MaxHP,
			Attack: view.Attack,
			Alive:  view.Alive,
		}
	}
	w.treasures = make(map[string]*Treasure)
	for _, view := range cp.Treasures {
		w.treasures[view.ID] = &Treasure{
			ID:    view.ID,
			Kind:  view.Kind,
			X:     view.X,
			Y:     view.Y,
			Value: view.Value,
		}
	}
	w.version = cp.Version
	w.nextNPC = len(w.npcs) + 1
	w.nextTreasure = len(w.treasures) + 1
}
