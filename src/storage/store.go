package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	json "github.com/goccy/go-json"

	"battleworld/config"
	"battleworld/protocol"

	"github.com/redis/go-redis/v9"
	"golang.org/x/crypto/bcrypt"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

var (
	ErrGlobalBossNotInitialized = errors.New("boss state not initialized")
	ErrDatabaseUnavailable      = errors.New("sql database is not configured")
)

type Store struct {
	db  *gorm.DB
	rdb *redis.Client
	ctx context.Context
}

// RedisClient 返回控制面选主使用的 Redis 客户端。业务代码应继续通过 Store 方法访问
// 数据；该访问器仅用于把同一 Redis 实例注入 token 租约 Elector。
func (s *Store) RedisClient() *redis.Client {
	if s == nil {
		return nil
	}
	return s.rdb
}

func (s *Store) database() (*gorm.DB, error) {
	if s == nil || s.db == nil {
		return nil, ErrDatabaseUnavailable
	}
	return s.db, nil
}

type GlobalSession struct {
	Username string `json:"username"`
	MapID    string `json:"map_id"`
	NodeID   string `json:"node_id"`
	Version  int64  `json:"version"`
}
type UserRecord struct {
	Username     string `gorm:"primaryKey"`
	PasswordHash string
	LastMap      string
	LastNode     string
	X            int
	Y            int
	HP           int
	MaxHP        int
	Attack       int
	Potions      int
	Treasures    int
	Kills        int
	Deaths       int
	Victories    int
	Alive        bool
}

func (UserRecord) TableName() string {
	return "users"
}

func toDBUser(p protocol.UserProfile) UserRecord {
	return UserRecord{
		Username:     p.Username,
		PasswordHash: p.PasswordHash,
		LastMap:      p.LastMap,
		LastNode:     p.LastNode,
		X:            p.X,
		Y:            p.Y,
		HP:           p.HP,
		MaxHP:        p.MaxHP,
		Attack:       p.Attack,
		Potions:      p.Potions,
		Treasures:    p.Treasures,
		Kills:        p.Kills,
		Deaths:       p.Deaths,
		Victories:    p.Victories,
		Alive:        p.Alive,
	}
}

func fromDBUser(u UserRecord) protocol.UserProfile {
	return protocol.UserProfile{
		Username:     u.Username,
		PasswordHash: u.PasswordHash,
		LastMap:      u.LastMap,
		LastNode:     u.LastNode,
		X:            u.X,
		Y:            u.Y,
		HP:           u.HP,
		MaxHP:        u.MaxHP,
		Attack:       u.Attack,
		Potions:      u.Potions,
		Treasures:    u.Treasures,
		Kills:        u.Kills,
		Deaths:       u.Deaths,
		Victories:    u.Victories,
		Alive:        u.Alive,
	}
}

// NewRedisStore creates a Store backed by an already-configured Redis client.
// It is suitable for Redis-only collaborators such as topology fencing; SQL-backed
// account methods require a Store returned by NewStore.
func NewRedisStore(client *redis.Client) (*Store, error) {
	if client == nil {
		return nil, errors.New("redis client 不能为空")
	}
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("ping redis: %w", err)
	}
	return &Store{rdb: client, ctx: ctx}, nil
}

func NewStore(baseDir string) (*Store, error) {
	// 数据库密码必须显式注入，避免把秘密写死在代码里。
	if config.DBPassword() == "" {
		return nil, errors.New("未设置 BW_DB_PASSWORD 或 BATTLEWORLD_PGPASSWORD 环境变量：请注入数据库密码后重试")
	}

	// 连接参数由环境变量注入，兼容 BW_* 与 BATTLEWORLD_* 命名。
	dsn := fmt.Sprintf("host=%s user=%s password='%s' dbname=%s port=%s sslmode=disable TimeZone=Asia/Shanghai",
		config.DBHost(), config.DBUser(), config.DBPassword(), config.DBName(), config.DBPort())
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		fmt.Printf("警告: 无法连接PostgreSQL (%v)!\n可能需要确保 PostgreSQL 在 %s:%s 运行\n", err, config.DBHost(), config.DBPort())
		return nil, err
	}

	// 自动迁移表结构
	if err := db.AutoMigrate(&UserRecord{}); err != nil {
		return nil, err
	}

	sqlDB, err := db.DB()
	if err == nil {
		// 设置空闲连接池中连接的最大数量
		sqlDB.SetMaxIdleConns(20)
		// 设置打开数据库连接的最大数量
		sqlDB.SetMaxOpenConns(100)
		// 设置了连接可复用的最大时间
		sqlDB.SetConnMaxLifetime(time.Hour)
	}

	// 连接 Redis (热数据)
	rdb := redis.NewClient(&redis.Options{
		Addr:     config.RedisAddr(),
		Password: config.RedisPassword(),
		DB:       config.RedisDB(),
	})

	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		fmt.Printf("警告: 无法连接Redis (%v)!\n可能需要确保 Redis 在 %s 运行\n", err, config.RedisAddr())
		return nil, err
	}

	return &Store{
		db:  db,
		rdb: rdb,
		ctx: ctx,
	}, nil
}

func (s *Store) Register(username, password string) error {
	if username == "" || password == "" {
		return errors.New("用户名和密码不能为空")
	}
	db, err := s.database()
	if err != nil {
		return err
	}

	// 在PG里检查是否已存在
	var count int64
	db.Model(&UserRecord{}).Where("username = ?", username).Count(&count)
	if count > 0 {
		return fmt.Errorf("用户 %q 已存在", username)
	}

	hashed, err := hashPassword(password)
	if err != nil {
		return err
	}

	p := protocol.UserProfile{
		Username:     username,
		PasswordHash: hashed,
		LastMap:      "green",
		X:            4,
		Y:            4,
		HP:           protocol.InitHP,
		MaxHP:        protocol.InitHP,
		Attack:       protocol.InitAttack,
		Potions:      protocol.MaxPotions,
		Alive:        true,
	}

	dbUser := toDBUser(p)
	if err := db.Create(&dbUser).Error; err != nil {
		return err
	}
	return nil
}

func (s *Store) Authenticate(username, password string) (*protocol.UserProfile, error) {
	if username == "" || password == "" {
		return nil, errors.New("用户名和密码不能为空")
	}
	db, err := s.database()
	if err != nil {
		return nil, err
	}

	var user UserRecord
	if err := db.Where("username = ?", username).First(&user).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, fmt.Errorf("用户 %q 不存在", username)
		}
		return nil, err
	}

	if !verifyPassword(user.PasswordHash, password) {
		return nil, errors.New("密码错误")
	}

	profile := fromDBUser(user)
	return &profile, nil
}

func (s *Store) LoadProfile(username string) (*protocol.UserProfile, error) {
	db, err := s.database()
	if err != nil {
		return nil, err
	}
	var user UserRecord
	if err := db.Where("username = ?", username).First(&user).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, fmt.Errorf("用户 %q 不存在", username)
		}
		return nil, err
	}

	profile := fromDBUser(user)
	return &profile, nil
}

func (s *Store) SaveProfile(profile protocol.UserProfile) error {
	db, err := s.database()
	if err != nil {
		return err
	}
	// 如果密码为空，从数据库中取回原有密码再更新
	dbUser := toDBUser(profile)

	// 构建复用的 db 链式调用
	tx := db

	// 如果密码为空，我们就告诉 GORM 在更新时忽略 PasswordHash 这个字段
	// 这样数据库里原有的密码就不会被覆盖了
	if profile.PasswordHash == "" {
		tx = tx.Omit("PasswordHash")
	}

	// Save() 会强行更新除 Omit 指定之外的所有字段（包括值为 0 的 HP、物品数等），安全且高效
	return tx.Save(&dbUser).Error
}

func (s *Store) SaveHotSession(session protocol.HotSession) error {
	data, err := json.Marshal(session)
	if err != nil {
		return err
	}
	// 利用 redis 保存热 session，并可配置 30 分钟无心跳过期，或者永久（按原逻辑）
	return s.rdb.HSet(s.ctx, "hot_sessions", session.Username, data).Err()
}

func (s *Store) DeleteHotSession(username string) error {
	return s.rdb.HDel(s.ctx, "hot_sessions", username).Err()
}

func (s *Store) SaveCheckpoint(cp protocol.MapCheckpoint) error {
	data, err := json.Marshal(cp)
	if err != nil {
		return err
	}
	return s.rdb.HSet(s.ctx, "checkpoints", cp.MapID, data).Err()
}

// SaveCheckpointIfOwner 仅在 Redis 中的地图 fence 与 nodeID/epoch 同时匹配时保存
// checkpoint。即使旧 owner 尚未刷新本地拓扑，也无法覆盖已切换到新 epoch 的快照。
func (s *Store) SaveCheckpointIfOwner(cp protocol.MapCheckpoint, nodeID string, epoch uint64) error {
	if cp.MapID == "" {
		return errors.New("checkpoint map id is required")
	}
	if nodeID == "" {
		return errors.New("checkpoint owner node id is required")
	}
	if epoch == 0 {
		return errors.New("checkpoint map epoch must be greater than zero")
	}
	if cp.NodeID != nodeID || cp.MapEpoch != epoch {
		return fmt.Errorf("checkpoint ownership mismatch: checkpoint=(%q,%d), request=(%q,%d)", cp.NodeID, cp.MapEpoch, nodeID, epoch)
	}
	data, err := json.Marshal(cp)
	if err != nil {
		return err
	}

	const saveCheckpointIfOwner = `
local fence = redis.call('GET', KEYS[1])
if not fence then
  return 0
end
local ok, decoded = pcall(cjson.decode, fence)
if not ok or type(decoded) ~= 'table' or decoded.owner == nil or decoded.epoch == nil then
  return -1
end
if decoded.owner ~= ARGV[1] or tostring(decoded.epoch) ~= ARGV[2] then
  return 0
end
redis.call('HSET', KEYS[2], ARGV[3], ARGV[4])
return 1
`
	result, err := s.rdb.Eval(s.ctx, saveCheckpointIfOwner, []string{MapFenceRedisKey(cp.MapID), "checkpoints"}, nodeID, strconv.FormatUint(epoch, 10), cp.MapID, data).Int()
	if err != nil {
		return fmt.Errorf("save fenced checkpoint: %w", err)
	}
	if result != 1 {
		return ErrMapFenceRejected
	}
	return nil
}

// RequireMapFence 即时读取 Redis fence，确认节点仍是 mapID 在 epoch 下的 owner。
// 节点在每个本地 world 写入前调用它，避免仅凭可能滞后的拓扑缓存继续接受旧主写入。
func (s *Store) RequireMapFence(mapID, nodeID string, epoch uint64) error {
	if mapID == "" || nodeID == "" || epoch == 0 {
		return ErrMapFenceRejected
	}
	data, err := s.rdb.Get(s.ctx, MapFenceRedisKey(mapID)).Bytes()
	if err != nil {
		return ErrMapFenceRejected
	}
	var fence struct {
		Owner string `json:"owner"`
		Epoch uint64 `json:"epoch"`
	}
	if err := json.Unmarshal(data, &fence); err != nil || fence.Owner != nodeID || fence.Epoch != epoch {
		return ErrMapFenceRejected
	}
	return nil
}

func (s *Store) LoadCheckpoint(mapID string) (*protocol.MapCheckpoint, bool) {
	data, err := s.rdb.HGet(s.ctx, "checkpoints", mapID).Bytes()
	if err != nil {
		return nil, false // 找不到或者出错，认为没有该快照
	}

	var cp protocol.MapCheckpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return nil, false
	}
	return &cp, true
}

// LoadTopology 读取完整路由拓扑。Key 不存在并非错误，调用方可通过 found=false
// 判断是否需要受控初始化；持久化数据损坏必须返回错误，绝不能视为一份空拓扑。
func (s *Store) LoadTopology() (*Topology, bool, error) {
	data, err := s.rdb.Get(s.ctx, TopologyRedisKey).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("load topology: %w", err)
	}

	var topology Topology
	if err := json.Unmarshal(data, &topology); err != nil {
		return nil, false, fmt.Errorf("%w: decode topology: %v", ErrTopologyCorrupt, err)
	}
	if err := topology.Validate(nil); err != nil {
		return nil, false, fmt.Errorf("%w: %v", ErrTopologyCorrupt, err)
	}
	topology.Normalize()
	return &topology, true, nil
}

// SaveTopology 通过版本化 CAS 写入完整拓扑，确保管理修复也会同步更新地图 fence。
// 它不再执行无条件 SET；正常控制面变更仍应直接调用 CompareAndSaveTopology。
func (s *Store) SaveTopology(topology Topology) error {
	current, found, err := s.LoadTopology()
	if err != nil {
		return err
	}
	if !found {
		return s.CompareAndSaveTopology(0, topology)
	}
	return s.CompareAndSaveTopology(current.Version, topology)
}

// CompareAndSaveTopology 仅在已存储拓扑的版本等于 expectedVersion 时原子写入 next，
// 并在同一个 Lua 事务中同步全部地图的 owner/epoch fence。expectedVersion=0 仅能在
// 拓扑尚不存在时创建版本 1。
func (s *Store) CompareAndSaveTopology(expectedVersion uint64, next Topology) error {
	return s.compareAndSaveTopology(expectedVersion, next, nil, 0)
}

// CompareAndSaveTopologyForLeader 仅允许 current leader term 对应的控制面提交拓扑。
// term 必须已写入 next；存储层还会拒绝将持久化 topology 的 term 倒退。
func (s *Store) CompareAndSaveTopologyForLeader(expectedVersion uint64, next Topology, term uint64) error {
	if term == 0 || next.LeaderTerm != term {
		return ErrLeaderTermInvariant
	}
	return s.compareAndSaveTopology(expectedVersion, next, nil, term)
}

// CompareAndSaveTopologyAndMigrateSessions 将被切换地图上仍指向 oldNodeID 的全局会话
// 原子迁移至 newNodeID。会话解码失败、拓扑版本冲突或 Lua 运行错误时，topology、fence
// 与会话均不写入，避免暴露半提交状态。
func (s *Store) CompareAndSaveTopologyAndMigrateSessions(expectedVersion uint64, next Topology, mapID, oldNodeID, newNodeID string) error {
	if mapID == "" || oldNodeID == "" || newNodeID == "" {
		return errors.New("session migration map id and node ids are required")
	}
	return s.compareAndSaveTopology(expectedVersion, next, &sessionMigration{MapID: mapID, OldNodeID: oldNodeID, NewNodeID: newNodeID}, 0)
}

// CompareAndSaveTopologyAndMigrateSessionsForLeader 是故障切换使用的 term-fenced CAS。
func (s *Store) CompareAndSaveTopologyAndMigrateSessionsForLeader(expectedVersion uint64, next Topology, term uint64, mapID, oldNodeID, newNodeID string) error {
	if term == 0 || next.LeaderTerm != term {
		return ErrLeaderTermInvariant
	}
	if mapID == "" || oldNodeID == "" || newNodeID == "" {
		return errors.New("session migration map id and node ids are required")
	}
	return s.compareAndSaveTopology(expectedVersion, next, &sessionMigration{MapID: mapID, OldNodeID: oldNodeID, NewNodeID: newNodeID}, term)
}

type sessionMigration struct {
	MapID     string `json:"map_id"`
	OldNodeID string `json:"old_node_id"`
	NewNodeID string `json:"new_node_id"`
}

func (s *Store) compareAndSaveTopology(expectedVersion uint64, next Topology, migration *sessionMigration, leaderTerm uint64) error {
	prepared, err := prepareTopology(next)
	if err != nil {
		return err
	}
	if expectedVersion == 0 {
		if prepared.Version != 1 {
			return fmt.Errorf("initial topology must use version 1, got %d", prepared.Version)
		}
	} else if prepared.Version != expectedVersion+1 {
		return fmt.Errorf("next topology version must be %d, got %d", expectedVersion+1, prepared.Version)
	}

	payload, err := json.Marshal(prepared)
	if err != nil {
		return fmt.Errorf("encode topology: %w", err)
	}

	mapIDs := make([]string, 0, len(prepared.Owners))
	for mapID := range prepared.Owners {
		mapIDs = append(mapIDs, mapID)
	}
	sort.Strings(mapIDs)
	keys := make([]string, 3, len(mapIDs)+3)
	keys[0] = TopologyRedisKey
	keys[1] = "global_sessions"
	keys[2] = CoordinatorLeaderTermRedisKey
	args := make([]interface{}, 0, 4+len(mapIDs)*3)
	args = append(args, expectedVersion, payload)
	if migration == nil {
		args = append(args, "")
	} else {
		migrationPayload, err := json.Marshal(migration)
		if err != nil {
			return fmt.Errorf("encode session migration: %w", err)
		}
		args = append(args, migrationPayload)
	}
	if leaderTerm == 0 {
		args = append(args, "")
	} else {
		args = append(args, strconv.FormatUint(leaderTerm, 10))
	}
	for _, mapID := range mapIDs {
		keys = append(keys, MapFenceRedisKey(mapID))
		args = append(args, mapID, prepared.Owners[mapID], strconv.FormatUint(prepared.MapEpochs[mapID], 10))
	}

	const compareAndSetTopology = `
local nextOK, nextTopology = pcall(cjson.decode, ARGV[2])
if not nextOK or type(nextTopology) ~= 'table' or type(nextTopology.owners) ~= 'table' or type(nextTopology.map_epochs) ~= 'table' then
  return -1
end
if ARGV[4] ~= '' then
  local durableTerm = tonumber(redis.call('GET', KEYS[3])) or 0
  local requestedTerm = tonumber(ARGV[4]) or 0
  local topologyTerm = tonumber(nextTopology.leader_term) or 0
  if durableTerm == 0 or durableTerm ~= requestedTerm or topologyTerm ~= durableTerm then
    return -4
  end
end
local current = redis.call('GET', KEYS[1])
if not current then
  if ARGV[1] ~= '0' then
    return 0
  end
else
  local ok, decoded = pcall(cjson.decode, current)
  if not ok or type(decoded) ~= 'table' or not decoded.version or tonumber(decoded.version) == nil or tonumber(decoded.version) < 1 then
    return -1
  end
  if tonumber(decoded.version) ~= tonumber(ARGV[1]) then
    return 0
  end
local nextOK, nextTopology = pcall(cjson.decode, ARGV[2])
if not nextOK or type(nextTopology) ~= 'table' or type(nextTopology.owners) ~= 'table' or type(nextTopology.map_epochs) ~= 'table' then
return -1
end
local currentTerm = tonumber(decoded.leader_term) or 0
local nextTerm = tonumber(nextTopology.leader_term) or 0
if nextTerm < currentTerm then
return -4
end
for mapID, currentEpoch in pairs(decoded.map_epochs or {}) do

    local nextEpoch = tonumber(nextTopology.map_epochs[mapID])
    if not nextEpoch then
      return -3
    end
    local currentOwner = (decoded.owners or {})[mapID] or ''
    local nextOwner = nextTopology.owners[mapID] or ''
    if currentOwner ~= nextOwner then
      if nextEpoch ~= tonumber(currentEpoch) + 1 then
        return -3
      end
    elseif nextEpoch ~= tonumber(currentEpoch) then
      return -3
    end
  end
  for mapID, nextEpoch in pairs(nextTopology.map_epochs) do
    if (decoded.map_epochs or {})[mapID] == nil and tonumber(nextEpoch) ~= 1 then
      return -3
    end
  end
end

local updates = {}
if ARGV[3] ~= '' then
  local migrationOK, migration = pcall(cjson.decode, ARGV[3])
  if not migrationOK or type(migration) ~= 'table' then
    return -2
  end
  local sessions = redis.call('HGETALL', KEYS[2])
  for index = 1, #sessions, 2 do
    local sessionOK, session = pcall(cjson.decode, sessions[index + 1])
    if not sessionOK or type(session) ~= 'table' then
      return -2
    end
    if session.map_id == migration.map_id and session.node_id == migration.old_node_id then
      session.node_id = migration.new_node_id
      session.version = (tonumber(session.version) or 0) + 1
      table.insert(updates, sessions[index])
      table.insert(updates, cjson.encode(session))
    end
  end
end

redis.call('SET', KEYS[1], ARGV[2])
local argIndex = 5
for keyIndex = 4, #KEYS do
  local fence = cjson.encode({map_id = ARGV[argIndex], owner = ARGV[argIndex + 1], epoch = tonumber(ARGV[argIndex + 2])})
  redis.call('SET', KEYS[keyIndex], fence)
  argIndex = argIndex + 3
end
if #updates > 0 then
  redis.call('HSET', KEYS[2], unpack(updates))
end
return 1
`

	result, err := s.rdb.Eval(s.ctx, compareAndSetTopology, keys, args...).Int()
	if err != nil {
		return fmt.Errorf("compare and save topology: %w", err)
	}
	switch result {
	case 1:
		return nil
	case -1:
		return ErrTopologyCorrupt
	case -2:
		return ErrGlobalSessionCorrupt
	case -3:
		return ErrMapEpochInvariant
	case -4:
		return ErrLeaderTermInvariant
	default:
		return ErrTopologyVersionConflict
	}
}

// PublishTopologyChanged 广播已提交的拓扑版本。消费者必须从 Redis 重新加载拓扑，
// 不能将该通知载荷视为拓扑数据本身。
func (s *Store) PublishTopologyChanged(version uint64) error {
	if version == 0 {
		return errors.New("topology event version must be greater than zero")
	}
	if err := s.rdb.Publish(s.ctx, TopologyEventChannel, strconv.FormatUint(version, 10)).Err(); err != nil {
		return fmt.Errorf("publish topology change: %w", err)
	}
	return nil
}

// hashPassword 生成 bcrypt 哈希；失败时返回 error。
func hashPassword(password string) (string, error) {
	b, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	return string(b), err
}

// verifyPassword 校验密码。兼容旧 SHA-256 哈希（以 "$2" 前缀区分 bcrypt）。
func verifyPassword(hash, password string) bool {
	if strings.HasPrefix(hash, "$2") {
		return bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)) == nil
	}
	// 旧版无盐 SHA-256 兼容路径
	sum := sha256.Sum256([]byte(password))
	return hash == hex.EncodeToString(sum[:])
}

// NodeRegistryInfo 表示节点在注册中心上报的可达地址和单一地图候选能力。
//
// MapID 只表示该节点参与哪张地图的 owner 选举，不代表节点已经获得路由主权。
// 同图的非 owner 健康候选节点由 coordinator 在故障转移时选作 standby；实际所有权
// 只能由已提交的 Topology 决定。
type NodeRegistryInfo struct {
	ID    string `json:"id"`
	Addr  string `json:"addr"`
	MapID string `json:"map_id"`
	// Draining 表示节点正在执行受控下线。它是候选与连接调度信号，不直接授予或
	// 收回主权；coordinator 必须通过带 epoch 的拓扑提交完成实际地图移交。
	Draining bool `json:"draining"`
}

// RegisterNode 以租约形式上报节点注册信息；该操作不会修改 Topology。
func (s *Store) RegisterNode(info NodeRegistryInfo, ttl time.Duration) error {
	data, err := json.Marshal(info)
	if err != nil {
		return err
	}
	return s.rdb.Set(s.ctx, "battle:node:registry:"+info.ID, data, ttl).Err()
}

func (s *Store) GetActiveNodes() ([]NodeRegistryInfo, error) {
	iter := s.rdb.Scan(s.ctx, 0, "battle:node:registry:*", 100).Iterator()
	var nodes []NodeRegistryInfo
	for iter.Next(s.ctx) {
		data, err := s.rdb.Get(s.ctx, iter.Val()).Bytes()
		if err != nil {
			continue // 键可能在 SCAN 与 GET 之间过期，跳过
		}
		var info NodeRegistryInfo
		if json.Unmarshal(data, &info) == nil {
			nodes = append(nodes, info)
		}
	}
	return nodes, iter.Err()
}

func (s *Store) SaveGlobalSession(session GlobalSession) error {
	data, err := json.Marshal(session)
	if err != nil {
		return err
	}
	// session:username
	return s.rdb.HSet(s.ctx, "global_sessions", session.Username, data).Err()
}

func (s *Store) LoadGlobalSession(username string) (*GlobalSession, bool) {
	data, err := s.rdb.HGet(s.ctx, "global_sessions", username).Bytes()
	if err != nil {
		return nil, false
	}
	var gs GlobalSession
	if err := json.Unmarshal(data, &gs); err != nil {
		return nil, false
	}
	return &gs, true
}

func (s *Store) DeleteGlobalSession(username string) error {
	return s.rdb.HDel(s.ctx, "global_sessions", username).Err()
}

func (s *Store) GetAllGlobalSessions() ([]GlobalSession, error) {
	result, err := s.rdb.HGetAll(s.ctx, "global_sessions").Result()
	if err != nil {
		return nil, err
	}
	var sessions []GlobalSession
	for _, raw := range result {
		var gs GlobalSession
		if json.Unmarshal([]byte(raw), &gs) == nil {
			sessions = append(sessions, gs)
		}
	}
	return sessions, nil
}

// PublishEvent sends a message to a specific pub/sub channel.
func (s *Store) PublishEvent(channel, event string) error {
	return s.rdb.Publish(s.ctx, channel, event).Err()
}

// SubscribeEvents subscribes to all relevant event channels and returns the message channel.
func (s *Store) SubscribeEvents() *redis.PubSub {
	pubsub := s.rdb.Subscribe(s.ctx, "events:global", TopologyEventChannel)
	pubsub.PSubscribe(s.ctx, "events:map:*", "events:user:*")
	return pubsub
}

func (s *Store) LoadGlobalBoss() (protocol.BossState, int32, error) {
	ctx := s.ctx
	hpStr, err := s.rdb.Get(ctx, "boss:global:hp").Result()
	if err != nil {
		if err == redis.Nil {
			return protocol.BossState{}, 0, ErrGlobalBossNotInitialized
		}
		return protocol.BossState{}, 0, err
	}
	hp, err := strconv.ParseInt(hpStr, 10, 32)
	if err != nil {
		return protocol.BossState{}, 0, err
	}

	stateJson, err := s.rdb.Get(ctx, "boss:global:state").Result()
	if err != nil {
		return protocol.BossState{}, 0, err
	}

	var state protocol.BossState
	err = json.Unmarshal([]byte(stateJson), &state)
	return state, int32(hp), err
}

func (s *Store) InitGlobalBoss(hp int32, state protocol.BossState) error {
	ctx := s.ctx
	stateJson, err := json.Marshal(state)
	if err != nil {
		return err
	}
	pipe := s.rdb.TxPipeline()
	pipe.Set(ctx, "boss:global:hp", hp, 0)
	pipe.Set(ctx, "boss:global:state", stateJson, 0)
	_, err = pipe.Exec(ctx)
	return err
}

func (s *Store) SaveGlobalBoss(state protocol.BossState) error {
	ctx := s.ctx
	stateJson, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return s.rdb.Set(ctx, "boss:global:state", stateJson, 0).Err()
}

// func (s *Store) DecrGlobalBossHP(username string, damage int) (int32, error) {
// 	ctx := s.ctx
// 	newHp, err := s.rdb.DecrBy(ctx, "boss:global:hp", int64(damage)).Result()
// 	return int32(newHp), err
// }

// AtomicAttackBoss 使用 Lua 脚本原子性地执行扣血与首领死亡判定
func (s *Store) AtomicAttackBoss(username string, damage int) (int32, bool, protocol.BossState, error) {
	ctx := s.ctx
	script := `
		local hpKey = KEYS[1]
		local stateKey = KEYS[2]
		local damage = tonumber(ARGV[1])
		local username = ARGV[2]
		
		local hp = tonumber(redis.call('GET', hpKey) or '0')
		if hp <= 0 then
			return {-1, 0, redis.call('GET', stateKey)}
		end
		
		local newHp = redis.call('DECRBY', hpKey, damage)
		local isKiller = 0
		
		-- 如果扣血跨过 0 线，则当前请求就是致命一击
		if newHp <= 0 and (newHp + damage) > 0 then
			isKiller = 1
			newHp = 0
			redis.call('SET', hpKey, 0) -- 修正负血量
			
			-- 同步更新 State
			local stateStr = redis.call('GET', stateKey)
			if stateStr then
				-- 由于 Lua 环境没有内建完善的 JSON 库处理全部结构，可通过外部传入或简单替换解决
				-- 这里依赖外部 Go 代码拿到 isKiller 后再安全保存。这种变通减少在 Lua 里做复杂的 Json 处理。
			end
		end
		
		return {newHp, isKiller}
	`

	res, err := s.rdb.Eval(ctx, script, []string{"boss:global:hp", "boss:global:state"}, int64(damage), username).Result()
	if err != nil {
		return 0, false, protocol.BossState{}, err
	}

	rawRes := res.([]interface{})
	if len(rawRes) == 3 { // HP 已经 <= 0
		stateJson := rawRes[2].(string)
		var state protocol.BossState
		json.Unmarshal([]byte(stateJson), &state)
		return -1, false, state, nil
	}

	newHp := int32(rawRes[0].(int64))
	isKiller := rawRes[1].(int64) == 1

	return newHp, isKiller, protocol.BossState{}, nil
}

// func (s *Store) TryLockBossKill() bool {
// 	ctx := s.ctx
// 	ok, err := s.rdb.SetNX(ctx, "boss:global:kill_lock", true, 15*time.Second).Result()
// 	return err == nil && ok
// }

func (s *Store) TryLockBossRespawn() bool {
	ctx := s.ctx
	ok, err := s.rdb.SetNX(ctx, "boss:global:respawn_lock", true, 10*time.Second).Result()
	return err == nil && ok
}
