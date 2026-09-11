package protocol

import (
	"sync"
	"time"
)

const (
	GatewayAddr = "127.0.0.1:9310"

	MapWidth  = 56
	MapHeight = 24

	InitHP       = 120
	InitAttack   = 35
	AttackRange  = 2
	BossAtkRange = 4
	NPCDamage    = 18
	HealAmount   = 30
	MaxPotions   = 3
	PotionCap    = 8
	PotionPrice  = 3
	WeaponPrice  = 8
	WeaponBoost  = 6
	MinNPCs      = 5
	MaxTreasures = 8
)

const (
	DirUp    = "up"
	DirDown  = "down"
	DirLeft  = "left"
	DirRight = "right"
)

type PlayerView struct {
	Username   string `json:"username"`
	MapID      string `json:"map_id"`
	X          int    `json:"x"`
	Y          int    `json:"y"`
	HP         int    `json:"hp"`
	MaxHP      int    `json:"max_hp"`
	Attack     int    `json:"attack"`
	Potions    int    `json:"potions"`
	Treasures  int    `json:"treasures"`
	Kills      int    `json:"kills"`
	Deaths     int    `json:"deaths"`
	Victories  int    `json:"victories"`
	Alive      bool   `json:"alive"`
	RespawnIn  int    `json:"respawn_in"`
	LastUpdate string `json:"last_update,omitempty"`
}

type NPCView struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	X      int    `json:"x"`
	Y      int    `json:"y"`
	HP     int    `json:"hp"`
	MaxHP  int    `json:"max_hp"`
	Attack int    `json:"attack"`
	Alive  bool   `json:"alive"`
}

type TreasureView struct {
	ID    string `json:"id"`
	Kind  string `json:"kind"`
	X     int    `json:"x"`
	Y     int    `json:"y"`
	Value int    `json:"value"`
}

type MapBrief struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	NodeID     string `json:"node_id"`
	Players    int    `json:"players"`
	NPCs       int    `json:"npcs"`
	Treasures  int    `json:"treasures"`
	Version    int64  `json:"version"`
	Primary    bool   `json:"primary"`
	IsCurrent  bool   `json:"is_current"`
	Checkpoint int64  `json:"checkpoint"`
}

type NodeView struct {
	ID            string   `json:"id"`
	Addr          string   `json:"addr"`
	Healthy       bool     `json:"healthy"`
	PrimaryMaps   []string `json:"primary_maps"`
	ReplicaMaps   []string `json:"replica_maps"`
	LastHeartbeat string   `json:"last_heartbeat"`
}

// GatewayStatus 是 Gateway 对已提交拓扑和数据面连接的只读视图。Healthy 只表示
// Gateway 已建立对应 owner 的数据面客户端，实际节点健康由 coordinator 管理。
type GatewayStatus struct {
	Summary         string     `json:"summary"`
	RoutingReady    bool       `json:"routing_ready"`
	TopologyVersion uint64     `json:"topology_version"`
	Nodes           []NodeView `json:"nodes"`
}

type MapView struct {
	ID        string         `json:"id"`
	Name      string         `json:"name"`
	NodeID    string         `json:"node_id"`
	Width     int            `json:"width"`
	Height    int            `json:"height"`
	Terrain   []string       `json:"terrain"`
	Players   []PlayerView   `json:"players"`
	NPCs      []NPCView      `json:"npcs"`
	Treasures []TreasureView `json:"treasures"`
	Version   int64          `json:"version"`
}

type BossSite struct {
	MapID string `json:"map_id"`
	X     int    `json:"x"`
	Y     int    `json:"y"`
}

type BossState struct {
	Name      string     `json:"name"`
	Alive     bool       `json:"alive"`
	LastHit   string     `json:"last_hit"`
	RespawnAt time.Time  `json:"respawn_at"`
	AttackGap int        `json:"attack_gap"`
	Sites     []BossSite `json:"sites"`
	Version   int64      `json:"version"`
}
type BossView struct {
	Name      string     `json:"name"`
	HP        int        `json:"hp"`
	MaxHP     int        `json:"max_hp"`
	Alive     bool       `json:"alive"`
	LastHit   string     `json:"last_hit"`
	RespawnIn int        `json:"respawn_in"`
	AttackGap int        `json:"attack_gap"`
	Sites     []BossSite `json:"sites"`
	Version   int64      `json:"version"`
}

type WorldState struct {
	Self            PlayerView `json:"self"`
	Map             MapView    `json:"map"`
	Maps            []MapBrief `json:"maps"`
	Nodes           []NodeView `json:"nodes"`
	Boss            BossView   `json:"boss"`
	Events          []string   `json:"events"`
	SessionVersion  int64      `json:"session_version"`
	TopologyVersion uint64     `json:"topology_version"`
	MapEpoch        uint64     `json:"map_epoch"`
}

type UserProfile struct {
	Username     string `json:"username"`
	PasswordHash string `json:"password_hash"`
	LastMap      string `json:"last_map"`
	LastNode     string `json:"last_node"`
	X            int    `json:"x"`
	Y            int    `json:"y"`
	HP           int    `json:"hp"`
	MaxHP        int    `json:"max_hp"`
	Attack       int    `json:"attack"`
	Potions      int    `json:"potions"`
	Treasures    int    `json:"treasures"`
	Kills        int    `json:"kills"`
	Deaths       int    `json:"deaths"`
	Victories    int    `json:"victories"`
	Alive        bool   `json:"alive"`
}

type HotSession struct {
	Username       string    `json:"username"`
	MapID          string    `json:"map_id"`
	NodeID         string    `json:"node_id"`
	X              int       `json:"x"`
	Y              int       `json:"y"`
	HP             int       `json:"hp"`
	Treasures      int       `json:"treasures"`
	SessionVersion int64     `json:"session_version"`
	UpdatedAt      time.Time `json:"updated_at"`
}

type MapCheckpoint struct {
	MapID      string         `json:"map_id"`
	NodeID     string         `json:"node_id"`
	MapEpoch   uint64         `json:"map_epoch"`
	Version    int64          `json:"version"`
	Terrain    []string       `json:"terrain"`
	Players    []PlayerView   `json:"players"`
	NPCs       []NPCView      `json:"npcs"`
	Treasures  []TreasureView `json:"treasures"`
	Checkpoint time.Time      `json:"checkpoint"`
}

var worldStatePool = sync.Pool{
	New: func() interface{} {
		return &WorldState{}
	},
}

func AllocWorldState() *WorldState {
	return worldStatePool.Get().(*WorldState)
}

func FreeWorldState(ws *WorldState) {
	if ws == nil {
		return
	}
	// 帮助垃圾回收器清理切片内的对象引用（截断以复用底层数组）
	ws.Maps = ws.Maps[:0]
	ws.Nodes = ws.Nodes[:0]
	ws.Events = ws.Events[:0]
	ws.Map.Terrain = ws.Map.Terrain[:0]
	ws.Map.Players = ws.Map.Players[:0]
	ws.Map.NPCs = ws.Map.NPCs[:0]
	ws.Map.Treasures = ws.Map.Treasures[:0]
	ws.Boss.Sites = ws.Boss.Sites[:0]
	// 清零标量字段，避免对象复用时携带陈旧数据
	ws.Self = PlayerView{} // PlayerView 全标量，无切片，可整体覆盖
	ws.Map.ID, ws.Map.Name, ws.Map.NodeID = "", "", ""
	ws.Map.Width, ws.Map.Height = 0, 0
	ws.Map.Version = 0
	ws.Boss.Name, ws.Boss.LastHit = "", ""
	ws.Boss.HP, ws.Boss.MaxHP = 0, 0
	ws.Boss.Alive = false
	ws.Boss.RespawnIn, ws.Boss.AttackGap = 0, 0
	ws.Boss.Version = 0
	ws.SessionVersion = 0
	ws.TopologyVersion = 0
	ws.MapEpoch = 0
	worldStatePool.Put(ws)
}
