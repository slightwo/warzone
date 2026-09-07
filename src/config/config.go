package config

import (
	"fmt"
	"os"
	"os/user"
	"strconv"
	"strings"
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

// 数据库连接配置。优先采用 BATTLEWORLD_*，兼容先前的 BW_* 命名。
func DBHost() string { return EnvOrAny("127.0.0.1", "BATTLEWORLD_STORE_ADDR", "BW_DB_HOST") }

func DBUser() string {
	if configured := EnvOrAny("", "BATTLEWORLD_PGUSER", "BW_DB_USER", "PGUSER"); configured != "" {
		return configured
	}
	if current, err := user.Current(); err == nil && current.Username != "" {
		return current.Username
	}
	return "postgres"
}

func DBPassword() string { return EnvOrAny("", "BATTLEWORLD_PGPASSWORD", "BW_DB_PASSWORD") }
func DBName() string     { return EnvOrAny("battleworld", "BATTLEWORLD_PGDB", "BW_DB_NAME") }
func DBPort() string     { return EnvOrAny("5432", "BATTLEWORLD_PGPORT", "BW_DB_PORT") }

// Redis 连接配置。
func RedisAddr() string {
	if addr := EnvOrAny("", "BW_REDIS_ADDR"); addr != "" {
		return addr
	}
	return fmt.Sprintf("%s:%s", DBHost(), EnvOrAny("6379", "BATTLEWORLD_REDIS_PORT"))
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
