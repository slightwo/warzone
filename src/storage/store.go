package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"battleworld/protocol"

	"github.com/redis/go-redis/v9"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type Store struct {
	db  *gorm.DB
	rdb *redis.Client
	ctx context.Context
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
	// 连接 PostgreSQL (冷数据)
	// 根据实际环境修改 DSN
	dsn := "host=localhost user=postgres password='Wu050601&&' dbname=battleworld port=5432 sslmode=disable TimeZone=Asia/Shanghai"
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		fmt.Printf("警告: 无法连接PostgreSQL (%v)!\n可能需要确保 PostgreSQL 在 localhost:5432 运行\n", err)
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
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		fmt.Printf("警告: 无法连接Redis (%v)!\n可能需要确保 Redis 在 localhost:6379 运行\n", err)
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

	p := protocol.UserProfile{
		Username:     username,
		PasswordHash: hashPassword(password),
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

	if user.PasswordHash != hashPassword(password) {
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

func hashPassword(password string) string {
	sum := sha256.Sum256([]byte(password))
	return hex.EncodeToString(sum[:])
}
