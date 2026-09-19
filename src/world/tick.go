package world

import (
	"fmt"
	"sort"
	"time"

	"battleworld/protocol"
)

func (w *World) BackgroundStep() []string {
	w.mu.Lock()
	defer w.mu.Unlock()

	events := make([]string, 0, 6)

	for _, player := range w.players {
		wasAlive := player.Alive
		w.refreshPlayerStateLocked(player)
		if !wasAlive && player.Alive {
			events = append(events, fmt.Sprintf("%s 已在营地复活", player.Username))
		}
	}

	for len(w.npcs) < protocol.MinNPCs {
		if npc := w.spawnNPCLocked(); npc != nil {
			events = append(events, fmt.Sprintf("%s 刷新了 %s，位置 (%d,%d)", w.cfg.Name, npc.Name, npc.X, npc.Y))
		} else {
			break
		}
	}
	if len(w.treasures) < protocol.MaxTreasures && w.rng.Intn(100) < 45 {
		if treasure := w.spawnTreasureLocked("野外宝箱"); treasure != nil {
			events = append(events, fmt.Sprintf("%s 刷新了宝物，位置 (%d,%d)", w.cfg.Name, treasure.X, treasure.Y))
		}
	}

	ids := make([]string, 0, len(w.npcs))
	for id := range w.npcs {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	for _, id := range ids {
		npc := w.npcs[id]
		if npc == nil || !npc.Alive {
			continue
		}
		target := w.closestPlayerLocked(npc.X, npc.Y, 1)
		if target != nil {
			target.HP -= npc.Attack
			target.LastUpdate = time.Now()
			if target.HP <= 0 {
				w.knockDownPlayerLocked(target)
				events = append(events, fmt.Sprintf("%s 被 %s 击倒了", target.Username, npc.Name))
			} else {
				events = append(events, fmt.Sprintf("%s 对 %s 造成了 %d 点伤害", npc.Name, target.Username, npc.Attack))
			}
			w.version++
			continue
		}

		base := w.rng.Intn(4)
		for step := 0; step < 4; step++ {
			nx, ny := npc.X, npc.Y
			switch (base + step) % 4 {
			case 0:
				ny--
			case 1:
				ny++
			case 2:
				nx--
			default:
				nx++
			}
			if w.walkableForLocked(nx, ny, "") {
				npc.X = nx
				npc.Y = ny
				w.version++
				break
			}
		}
	}

	return events
}

func (w *World) bootstrap() {
	w.mu.Lock()
	defer w.mu.Unlock()

	for len(w.npcs) < protocol.MinNPCs {
		w.spawnNPCLocked()
	}
	for len(w.treasures) < protocol.MaxTreasures/2 {
		w.spawnTreasureLocked("遗迹补给")
	}
}

func (w *World) respawnPlayer(username string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	player, ok := w.players[username]
	if !ok || player.Alive {
		return
	}
	player.X, player.Y = w.findSafePositionLocked(w.cfg.SpawnX, w.cfg.SpawnY, username)
	player.HP = player.MaxHP
	player.Potions = protocol.MaxPotions
	player.Alive = true
	player.RespawnAt = time.Time{}
	player.LastUpdate = time.Now()
	w.version++
}

func (w *World) RewardPlayer(username string, treasureDelta, victoryDelta int) (protocol.UserProfile, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	player, ok := w.players[username]
	if !ok {
		return protocol.UserProfile{}, false
	}
	player.Treasures += treasureDelta
	player.Victories += victoryDelta
	player.LastUpdate = time.Now()
	w.version++
	return w.profileLocked(player), true
}

func (w *World) closestPlayerLocked(x, y, radius int) *Player {
	var best *Player
	for _, player := range w.players {
		w.refreshPlayerStateLocked(player)
		if !player.Alive || distance(x, y, player.X, player.Y) > radius {
			continue
		}
		if best == nil || player.HP < best.HP || (player.HP == best.HP && player.Username < best.Username) {
			best = player
		}
	}
	return best
}

func (w *World) spawnNPCLocked() *NPC {
	x, y, ok := w.bestSpawnCellLocked("", true)
	if !ok {
		return nil
	}
	names := []string{"史莱姆", "荒原狼", "流寇", "宝匣怪", "石像魔", "潜行兽"}
	id := fmt.Sprintf("%s-npc-%d", w.cfg.ID, w.nextNPC)
	npc := &NPC{
		ID:     id,
		Name:   names[w.nextNPC%len(names)],
		X:      x,
		Y:      y,
		HP:     70 + w.rng.Intn(30),
		MaxHP:  90,
		Attack: protocol.NPCDamage,
		Alive:  true,
	}
	w.nextNPC++
	w.npcs[id] = npc
	w.version++
	return npc
}

func (w *World) spawnTreasureLocked(kind string) *Treasure {
	x, y, ok := w.bestSpawnCellLocked("", false)
	if !ok {
		return nil
	}
	return w.dropTreasureLocked(x, y, 1+w.rng.Intn(4), kind)
}

func (w *World) dropTreasureLocked(x, y, value int, kind string) *Treasure {
	if x < 0 || y < 0 || y >= len(w.terrain) || x >= len(w.terrain[y]) {
		return nil
	}
	if _, treasure, ok := w.treasureAtLocked(x, y); ok {
		treasure.Value += value
		w.version++
		return treasure
	}
	id := fmt.Sprintf("%s-t-%d", w.cfg.ID, w.nextTreasure)
	w.nextTreasure++
	treasure := &Treasure{
		ID:    id,
		Kind:  kind,
		X:     x,
		Y:     y,
		Value: value,
	}
	w.treasures[id] = treasure
	w.version++
	return treasure
}

func (w *World) knockDownPlayerLocked(player *Player) {
	player.HP = 0
	player.Alive = false
	player.Deaths++
	player.RespawnAt = time.Now().Add(4 * time.Second)
	player.LastUpdate = time.Now()
}

func (w *World) refreshPlayerStateLocked(player *Player) {
	if player == nil || player.Alive || player.RespawnAt.IsZero() || time.Now().Before(player.RespawnAt) {
		return
	}
	player.X, player.Y = w.findSafePositionLocked(w.cfg.SpawnX, w.cfg.SpawnY, player.Username)
	player.HP = player.MaxHP
	player.Potions = max(player.Potions, protocol.MaxPotions)
	player.Alive = true
	player.RespawnAt = time.Time{}
	player.LastUpdate = time.Now()
	w.version++
}

func (w *World) playerRespawnInLocked(player *Player) int {
	if player == nil || player.Alive || player.RespawnAt.IsZero() {
		return 0
	}
	remain := int(time.Until(player.RespawnAt).Seconds())
	if remain < 1 {
		return 1
	}
	return remain
}
