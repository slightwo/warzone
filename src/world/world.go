package world

import (
	"fmt"
	"hash/fnv"
	"math"
	"math/rand"
	"sort"
	"sync"
	"time"

	"battleworld/protocol"
)

type MapConfig struct {
	ID     string
	Name   string
	Layout []string
	SpawnX int
	SpawnY int
	BossX  int
	BossY  int
}

type Player struct {
	Username   string
	MapID      string
	X          int
	Y          int
	HP         int
	MaxHP      int
	Attack     int
	Potions    int
	Treasures  int
	Kills      int
	Deaths     int
	Victories  int
	Alive      bool
	RespawnAt  time.Time
	LastUpdate time.Time
}

type NPC struct {
	ID     string
	Name   string
	X      int
	Y      int
	HP     int
	MaxHP  int
	Attack int
	Alive  bool
}

type Treasure struct {
	ID    string
	Kind  string
	X     int
	Y     int
	Value int
}

type World struct {
	mu           sync.RWMutex
	cfg          MapConfig
	terrain      [][]rune
	players      map[string]*Player
	npcs         map[string]*NPC
	treasures    map[string]*Treasure
	rng          *rand.Rand
	version      int64
	nextNPC      int
	nextTreasure int
}

func NewWorld(cfg MapConfig) *World {
	h := fnv.New64a()
	_, _ = h.Write([]byte(cfg.ID))
	w := &World{
		cfg:       cfg,
		terrain:   stringsToGrid(cfg.Layout),
		players:   make(map[string]*Player),
		npcs:      make(map[string]*NPC),
		treasures: make(map[string]*Treasure),
		rng:       rand.New(rand.NewSource(int64(h.Sum64()))),
	}
	w.bootstrap()
	return w
}

func (w *World) MapID() string {
	return w.cfg.ID
}

func (w *World) MapName() string {
	return w.cfg.Name
}

func (w *World) AddOrRestorePlayer(profile *protocol.UserProfile) protocol.PlayerView {
	w.mu.Lock()
	defer w.mu.Unlock()

	x, y := profile.X, profile.Y
	if profile.LastMap != w.cfg.ID {
		x, y = w.cfg.SpawnX, w.cfg.SpawnY
	}
	x, y = w.findSafePositionLocked(x, y, "")
	player := &Player{
		Username:   profile.Username,
		MapID:      w.cfg.ID,
		X:          x,
		Y:          y,
		HP:         valueOr(profile.HP, protocol.InitHP),
		MaxHP:      valueOr(profile.MaxHP, protocol.InitHP),
		Attack:     valueOr(profile.Attack, protocol.InitAttack),
		Potions:    valueOr(profile.Potions, protocol.MaxPotions),
		Treasures:  profile.Treasures,
		Kills:      profile.Kills,
		Deaths:     profile.Deaths,
		Victories:  profile.Victories,
		Alive:      profile.Alive,
		LastUpdate: time.Now(),
	}
	if profile.HP == 0 && !profile.Alive {
		player.Alive = false
	}
	if player.HP <= 0 {
		player.HP = player.MaxHP
		player.Alive = true
	}
	w.players[player.Username] = player
	w.version++
	return w.playerViewLocked(player)
}

func (w *World) RemovePlayer(username string) (protocol.UserProfile, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	player, ok := w.players[username]
	if !ok {
		return protocol.UserProfile{}, false
	}
	profile := w.profileLocked(player)
	delete(w.players, username)
	w.version++
	return profile, true
}

func (w *World) ProfileOf(username string) (protocol.UserProfile, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	player, ok := w.players[username]
	if !ok {
		return protocol.UserProfile{}, false
	}
	w.refreshPlayerStateLocked(player)
	return w.profileLocked(player), true
}

func (w *World) FlushProfiles(since time.Time) []protocol.UserProfile {
	w.mu.Lock()
	defer w.mu.Unlock()

	var profiles []protocol.UserProfile
	for _, player := range w.players {
		w.refreshPlayerStateLocked(player)
		if player.LastUpdate.After(since) {
			profiles = append(profiles, w.profileLocked(player))
		}
	}
	return profiles
}

func (w *World) MovePlayer(username, dir string) (string, protocol.UserProfile, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	player, ok := w.players[username]
	if !ok {
		return "", protocol.UserProfile{}, false
	}
	w.refreshPlayerStateLocked(player)
	if !player.Alive {
		return fmt.Sprintf("%s 当前倒地，%d 秒后复活", username, w.playerRespawnInLocked(player)), w.profileLocked(player), true
	}

	nx, ny := player.X, player.Y
	switch dir {
	case protocol.DirUp:
		ny--
	case protocol.DirDown:
		ny++
	case protocol.DirLeft:
		nx--
	case protocol.DirRight:
		nx++
	}
	if !w.walkableForLocked(nx, ny, player.Username) {
		return fmt.Sprintf("%s 被墙体或单位挡住了", username), w.profileLocked(player), true
	}

	player.X = nx
	player.Y = ny
	player.LastUpdate = time.Now()
	event := fmt.Sprintf("%s 移动到了 (%d,%d)", username, nx, ny)

	if treasureID, treasure, ok := w.treasureAtLocked(nx, ny); ok {
		player.Treasures += treasure.Value
		delete(w.treasures, treasureID)
		event = fmt.Sprintf("%s 在 (%d,%d) 拾取了%s，战利品 +%d", username, nx, ny, treasure.Kind, treasure.Value)
	}

	w.version++
	return event, w.profileLocked(player), true
}

func (w *World) HealPlayer(username string) (string, protocol.UserProfile, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	player, ok := w.players[username]
	if !ok {
		return "", protocol.UserProfile{}, false
	}
	w.refreshPlayerStateLocked(player)
	if !player.Alive {
		return fmt.Sprintf("%s 倒地时无法使用药剂，%d 秒后复活", username, w.playerRespawnInLocked(player)), w.profileLocked(player), true
	}
	if player.Potions <= 0 {
		return fmt.Sprintf("%s 的治疗药剂已经用完", username), w.profileLocked(player), true
	}
	player.Potions--
	before := player.HP
	player.HP += protocol.HealAmount
	if player.HP > player.MaxHP {
		player.HP = player.MaxHP
	}
	player.LastUpdate = time.Now()
	w.version++
	return fmt.Sprintf("%s 回复了 %d 点生命", username, player.HP-before), w.profileLocked(player), true
}

func (w *World) Attack(username string) (string, string, string, protocol.UserProfile, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	player, ok := w.players[username]
	if !ok {
		return "", "", "", protocol.UserProfile{}, false
	}
	w.refreshPlayerStateLocked(player)
	if !player.Alive {
		return fmt.Sprintf("%s 倒地时无法发起攻击，%d 秒后复活", username, w.playerRespawnInLocked(player)), "", "", w.profileLocked(player), true
	}

	var npcTarget *NPC
	for _, npc := range w.npcs {
		if !npc.Alive || distance(player.X, player.Y, npc.X, npc.Y) > protocol.AttackRange {
			continue
		}
		if npcTarget == nil || npc.HP < npcTarget.HP || (npc.HP == npcTarget.HP && npc.ID < npcTarget.ID) {
			npcTarget = npc
		}
	}
	if npcTarget != nil {
		npcTarget.HP -= player.Attack
		player.LastUpdate = time.Now()
		event := fmt.Sprintf("%s 对 %s 造成了 %d 点伤害", username, npcTarget.Name, player.Attack)
		if npcTarget.HP <= 0 {
			npcTarget.HP = 0
			npcTarget.Alive = false
			delete(w.npcs, npcTarget.ID)
			player.Kills++
			w.dropTreasureLocked(npcTarget.X, npcTarget.Y, 2+w.rng.Intn(3), "怪物战利品")
			event = fmt.Sprintf("%s 击败了 %s，掉落了一份战利品", username, npcTarget.Name)
		}
		w.version++
		return event, "", "", w.profileLocked(player), true
	}

	var playerTarget *Player
	for _, other := range w.players {
		if other.Username == username || !other.Alive || distance(player.X, player.Y, other.X, other.Y) > protocol.AttackRange {
			continue
		}
		if playerTarget == nil || other.HP < playerTarget.HP || (other.HP == playerTarget.HP && other.Username < playerTarget.Username) {
			playerTarget = other
		}
	}
	if playerTarget == nil {
		return fmt.Sprintf("%s 的攻击范围内没有目标", username), "", "", w.profileLocked(player), true
	}

	playerTarget.HP -= player.Attack
	player.LastUpdate = time.Now()
	event := fmt.Sprintf("%s 对 %s 造成了 %d 点伤害", username, playerTarget.Username, player.Attack)
	targetEvent := fmt.Sprintf("你遭到了 %s 的攻击，生命 -%d", username, player.Attack)
	if playerTarget.HP <= 0 {
		player.Kills++
		w.knockDownPlayerLocked(playerTarget)
		event = fmt.Sprintf("%s 击败了 %s", username, playerTarget.Username)
		targetEvent = fmt.Sprintf("你被 %s 击倒了，%d 秒后复活", username, w.playerRespawnInLocked(playerTarget))
	}

	w.version++
	return event, playerTarget.Username, targetEvent, w.profileLocked(player), true
}

func (w *World) BuyItem(username, item string) (string, protocol.UserProfile, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	player, ok := w.players[username]
	if !ok {
		return "", protocol.UserProfile{}, false
	}
	w.refreshPlayerStateLocked(player)
	if !player.Alive {
		return fmt.Sprintf("%s 倒地时无法打开商店，%d 秒后复活", username, w.playerRespawnInLocked(player)), w.profileLocked(player), true
	}

	switch item {
	case "potion":
		if player.Treasures < protocol.PotionPrice {
			return fmt.Sprintf("战利品不足，购买药剂需要 %d", protocol.PotionPrice), w.profileLocked(player), true
		}
		if player.Potions >= protocol.PotionCap {
			return fmt.Sprintf("药剂携带已达上限 %d", protocol.PotionCap), w.profileLocked(player), true
		}
		player.Treasures -= protocol.PotionPrice
		player.Potions++
		player.LastUpdate = time.Now()
		w.version++
		return fmt.Sprintf("%s 在商店购入药剂，药剂 +1，战利品 -%d", username, protocol.PotionPrice), w.profileLocked(player), true
	case "weapon":
		if player.Treasures < protocol.WeaponPrice {
			return fmt.Sprintf("战利品不足，强化武器需要 %d", protocol.WeaponPrice), w.profileLocked(player), true
		}
		player.Treasures -= protocol.WeaponPrice
		player.Attack += protocol.WeaponBoost
		player.LastUpdate = time.Now()
		w.version++
		return fmt.Sprintf("%s 在商店强化武器，攻击 +%d，战利品 -%d", username, protocol.WeaponBoost, protocol.WeaponPrice), w.profileLocked(player), true
	default:
		return "商店中没有这个商品", w.profileLocked(player), true
	}
}

func (w *World) Sl2_WithinRange(username string) bool {
	w.mu.RLock()
	defer w.mu.RUnlock()

	player, ok := w.players[username]
	if !ok {
		return false
	}
	return distance(w.cfg.BossX, w.cfg.BossY, player.X, player.Y) <= protocol.BossAtkRange
}

func (w *World) Sl2_GetDamadge(username string) int {
	w.mu.RLock()
	defer w.mu.RUnlock()

	player, ok := w.players[username]
	if !ok {
		return 0
	}
	return player.Attack
}

func (w *World) Snapshot(nodeID string) protocol.MapView {
	w.mu.Lock()
	defer w.mu.Unlock()

	players := make([]protocol.PlayerView, 0, len(w.players))
	for _, player := range w.players {
		w.refreshPlayerStateLocked(player)
		players = append(players, w.playerViewLocked(player))
	}
	sort.Slice(players, func(i, j int) bool { return players[i].Username < players[j].Username })

	npcs := make([]protocol.NPCView, 0, len(w.npcs))
	for _, npc := range w.npcs {
		npcs = append(npcs, protocol.NPCView{
			ID:     npc.ID,
			Name:   npc.Name,
			X:      npc.X,
			Y:      npc.Y,
			HP:     npc.HP,
			MaxHP:  npc.MaxHP,
			Attack: npc.Attack,
			Alive:  npc.Alive,
		})
	}
	sort.Slice(npcs, func(i, j int) bool { return npcs[i].ID < npcs[j].ID })

	treasures := make([]protocol.TreasureView, 0, len(w.treasures))
	for _, treasure := range w.treasures {
		treasures = append(treasures, protocol.TreasureView{
			ID:    treasure.ID,
			Kind:  treasure.Kind,
			X:     treasure.X,
			Y:     treasure.Y,
			Value: treasure.Value,
		})
	}
	sort.Slice(treasures, func(i, j int) bool { return treasures[i].ID < treasures[j].ID })

	return protocol.MapView{
		ID:        w.cfg.ID,
		Name:      w.cfg.Name,
		NodeID:    nodeID,
		Width:     len(w.cfg.Layout[0]),
		Height:    len(w.cfg.Layout),
		Terrain:   gridToStrings(w.terrain),
		Players:   players,
		NPCs:      npcs,
		Treasures: treasures,
		Version:   w.version,
	}
}

func (w *World) Counts() (players, npcs, treasures int, version int64) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return len(w.players), len(w.npcs), len(w.treasures), w.version
}

func (w *World) playerViewLocked(player *Player) protocol.PlayerView {
	return protocol.PlayerView{
		Username:   player.Username,
		MapID:      player.MapID,
		X:          player.X,
		Y:          player.Y,
		HP:         player.HP,
		MaxHP:      player.MaxHP,
		Attack:     player.Attack,
		Potions:    player.Potions,
		Treasures:  player.Treasures,
		Kills:      player.Kills,
		Deaths:     player.Deaths,
		Victories:  player.Victories,
		Alive:      player.Alive,
		RespawnIn:  w.playerRespawnInLocked(player),
		LastUpdate: player.LastUpdate.Format(time.RFC3339),
	}
}

func (w *World) profileLocked(player *Player) protocol.UserProfile {
	return protocol.UserProfile{
		Username:  player.Username,
		LastMap:   w.cfg.ID,
		X:         player.X,
		Y:         player.Y,
		HP:        player.HP,
		MaxHP:     player.MaxHP,
		Attack:    player.Attack,
		Potions:   player.Potions,
		Treasures: player.Treasures,
		Kills:     player.Kills,
		Deaths:    player.Deaths,
		Victories: player.Victories,
		Alive:     player.Alive,
	}
}

func (w *World) treasureAtLocked(x, y int) (string, *Treasure, bool) {
	for id, treasure := range w.treasures {
		if treasure.X == x && treasure.Y == y {
			return id, treasure, true
		}
	}
	return "", nil, false
}

func (w *World) walkableForLocked(x, y int, ignorePlayer string) bool {
	if y < 0 || y >= len(w.terrain) || x < 0 || x >= len(w.terrain[y]) {
		return false
	}
	if w.terrain[y][x] == '#' {
		return false
	}
	for _, player := range w.players {
		if player.Username != ignorePlayer && player.Alive && player.X == x && player.Y == y {
			return false
		}
	}
	for _, npc := range w.npcs {
		if npc.Alive && npc.X == x && npc.Y == y {
			return false
		}
	}
	return true
}

func (w *World) randomOpenCellLocked(ignorePlayer string) (int, int, bool) {
	for tries := 0; tries < 256; tries++ {
		x := w.rng.Intn(len(w.terrain[0]))
		y := w.rng.Intn(len(w.terrain))
		if w.walkableForLocked(x, y, ignorePlayer) {
			return x, y, true
		}
	}
	for y := 0; y < len(w.terrain); y++ {
		for x := 0; x < len(w.terrain[y]); x++ {
			if w.walkableForLocked(x, y, ignorePlayer) {
				return x, y, true
			}
		}
	}
	return 0, 0, false
}

func (w *World) findSafePositionLocked(preferredX, preferredY int, ignorePlayer string) (int, int) {
	if w.walkableForLocked(preferredX, preferredY, ignorePlayer) {
		return preferredX, preferredY
	}
	x, y, ok := w.bestSpawnCellLocked(ignorePlayer, true)
	if ok {
		return x, y
	}
	return w.cfg.SpawnX, w.cfg.SpawnY
}

func (w *World) bestSpawnCellLocked(ignorePlayer string, preferNPC bool) (int, int, bool) {
	type candidate struct {
		x     int
		y     int
		score int
	}
	candidates := make([]candidate, 0, len(w.terrain)*len(w.terrain[0]))
	for y := 0; y < len(w.terrain); y++ {
		for x := 0; x < len(w.terrain[y]); x++ {
			if !w.walkableForLocked(x, y, ignorePlayer) {
				continue
			}
			candidates = append(candidates, candidate{x: x, y: y, score: w.spawnScoreLocked(x, y, preferNPC)})
		}
	}
	if len(candidates) == 0 {
		return 0, 0, false
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].score == candidates[j].score {
			if candidates[i].y == candidates[j].y {
				return candidates[i].x < candidates[j].x
			}
			return candidates[i].y < candidates[j].y
		}
		return candidates[i].score > candidates[j].score
	})
	limit := 12
	if len(candidates) < limit {
		limit = len(candidates)
	}
	pick := candidates[w.rng.Intn(limit)]
	return pick.x, pick.y, true
}

func (w *World) spawnScoreLocked(x, y int, preferNPC bool) int {
	bestNPC := protocol.MapWidth + protocol.MapHeight
	for _, npc := range w.npcs {
		if !npc.Alive {
			continue
		}
		if d := distance(x, y, npc.X, npc.Y); d < bestNPC {
			bestNPC = d
		}
	}
	bestPlayer := protocol.MapWidth + protocol.MapHeight
	for _, player := range w.players {
		if !player.Alive {
			continue
		}
		if d := distance(x, y, player.X, player.Y); d < bestPlayer {
			bestPlayer = d
		}
	}
	bestTreasure := protocol.MapWidth + protocol.MapHeight
	for _, treasure := range w.treasures {
		if d := distance(x, y, treasure.X, treasure.Y); d < bestTreasure {
			bestTreasure = d
		}
	}
	width := len(w.terrain[0])
	height := len(w.terrain)
	edgeMargin := min(min(x, width-1-x), min(y, height-1-y))
	centerDist := distance(x, y, width/2, height/2)
	score := bestNPC*120 + bestPlayer*20 + bestTreasure*15 + edgeMargin*30 - centerDist*3
	if preferNPC {
		score += distance(x, y, w.cfg.SpawnX, w.cfg.SpawnY) * 2
	}
	return score
}

func valueOr(v, fallback int) int {
	if v == 0 {
		return fallback
	}
	return v
}

func distance(ax, ay, bx, by int) int {
	return int(math.Abs(float64(ax-bx)) + math.Abs(float64(ay-by)))
}
