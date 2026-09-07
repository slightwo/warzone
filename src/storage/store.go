package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
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

var ErrGlobalBossNotInitialized = errors.New("boss state not initialized")

type Store struct {
	db  *gorm.DB
	rdb *redis.Client
	ctx context.Context
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

	// 在PG里检查是否已存在
	var count int64
	s.db.Model(&UserRecord{}).Where("username = ?", username).Count(&count)
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
	if err := s.db.Create(&dbUser).Error; err != nil {
		return err
	}
	return nil
}

func (s *Store) Authenticate(username, password string) (*protocol.UserProfile, error) {
	if username == "" || password == "" {
		return nil, errors.New("用户名和密码不能为空")
	}

	var user UserRecord
	if err := s.db.Where("username = ?", username).First(&user).Error; err != nil {
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
	var user UserRecord
	if err := s.db.Where("username = ?", username).First(&user).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, fmt.Errorf("用户 %q 不存在", username)
		}
		return nil, err
	}

	profile := fromDBUser(user)
	return &profile, nil
}

func (s *Store) SaveProfile(profile protocol.UserProfile) error {
	// 如果密码为空，从数据库中取回原有密码再更新
	dbUser := toDBUser(profile)

	// 构建复用的 db 链式调用
	tx := s.db

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

// SaveTopology 写入完整的拓扑文档。该方法仅用于管理修复和受控初始化；正常的
// 控制面变更必须调用 CompareAndSaveTopology。
func (s *Store) SaveTopology(topology Topology) error {
	prepared, err := prepareTopology(topology)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(prepared)
	if err != nil {
		return fmt.Errorf("encode topology: %w", err)
	}
	if err := s.rdb.Set(s.ctx, TopologyRedisKey, payload, 0).Err(); err != nil {
		return fmt.Errorf("save topology: %w", err)
	}
	return nil
}

// CompareAndSaveTopology 仅在已存储拓扑的版本等于 expectedVersion 时原子写入 next。
// expectedVersion=0 仅能在拓扑尚不存在时创建版本 1。
func (s *Store) CompareAndSaveTopology(expectedVersion uint64, next Topology) error {
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

	const compareAndSetTopology = `
local current = redis.call('GET', KEYS[1])
if not current then
  if ARGV[1] ~= '0' then
    return 0
  end
  redis.call('SET', KEYS[1], ARGV[2])
  return 1
end

local ok, decoded = pcall(cjson.decode, current)
if not ok or type(decoded) ~= 'table' or not decoded.version or tonumber(decoded.version) == nil or tonumber(decoded.version) < 1 then
  return -1
end
if tonumber(decoded.version) ~= tonumber(ARGV[1]) then
  return 0
end
redis.call('SET', KEYS[1], ARGV[2])
return 1
`

	result, err := s.rdb.Eval(s.ctx, compareAndSetTopology, []string{TopologyRedisKey}, expectedVersion, payload).Int()
	if err != nil {
		return fmt.Errorf("compare and save topology: %w", err)
	}
	if result == -1 {
		return ErrTopologyCorrupt
	}
	if result != 1 {
		return ErrTopologyVersionConflict
	}
	return nil
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

// NodeRegistryInfo 表示节点在注册中心上报的可达地址和承载候选能力。
//
// Maps 与 Replicas 为兼容既有 JSON 协议保留，它们分别表示声明的主地图候选和
// 副本地图候选，不代表节点已获得地图路由主权。主从归属只能由已提交的 Topology 决定。
type NodeRegistryInfo struct {
	ID       string   `json:"id"`
	Addr     string   `json:"addr"`
	Maps     []string `json:"maps"`
	Replicas []string `json:"replicas"`
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
