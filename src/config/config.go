package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

const (
	// 本地联调默认存储配置。环境变量仍可覆盖这些值，便于需要连接其它环境时使用。
	defaultDBHost     = "127.0.0.1"
	defaultDBPort     = "5432"
	defaultDBUser     = "wu_han_wen"
	defaultDBName     = "battleworld"
	defaultDBPassword = "Wu050601&&"
	defaultRedisPort  = "6379"
)

// EnvOr 返回环境变量 key 的非空值；为空时回退到 def。
func EnvOr(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

// EnvOrAny 按变量名称顺序返回第一个非空值，便于平滑兼容已有部署配置。
func EnvOrAny(def string, keys ...string) string {
	for _, key := range keys {
		if value := strings.TrimSpace(os.Getenv(key)); value != "" {
			return value
		}
	}
	return def
}

// 数据库连接配置。未设置环境变量时直接使用本地联调默认值。
func DBHost() string     { return EnvOrAny(defaultDBHost, "BATTLEWORLD_STORE_ADDR", "BW_DB_HOST") }
func DBUser() string     { return EnvOrAny(defaultDBUser, "BATTLEWORLD_PGUSER", "BW_DB_USER", "PGUSER") }
func DBPassword() string { return EnvOrAny(defaultDBPassword, "BATTLEWORLD_PGPASSWORD", "BW_DB_PASSWORD") }
func DBName() string     { return EnvOrAny(defaultDBName, "BATTLEWORLD_PGDB", "BW_DB_NAME") }
func DBPort() string     { return EnvOrAny(defaultDBPort, "BATTLEWORLD_PGPORT", "BW_DB_PORT") }

// Redis 连接配置。未设置环境变量时连接本机默认实例。
func RedisAddr() string {
	if addr := EnvOrAny("", "BW_REDIS_ADDR"); addr != "" {
		return addr
	}
	return fmt.Sprintf("%s:%s", defaultDBHost, EnvOrAny(defaultRedisPort, "BATTLEWORLD_REDIS_PORT"))
}

func RedisPassword() string { return EnvOrAny("", "BATTLEWORLD_REDIS_PASSWORD", "BW_REDIS_PASSWORD") }

func RedisDB() int {
	value := EnvOrAny("0", "BATTLEWORLD_REDIS_DB", "BW_REDIS_DB")
	db, err := strconv.Atoi(value)
	if err != nil || db < 0 {
		return 0
	}
	return db
}

// CoordinatorID 返回部署可配置的协调器身份。为空时由 coordinator 使用主机和进程号生成
// 本实例标识；Redis lease token 仍会额外包含不可复用的随机部分。
func CoordinatorID() string {
	return EnvOrAny("", "BATTLEWORLD_COORDINATOR_ID", "BW_COORDINATOR_ID")
}
