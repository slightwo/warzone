package elector

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"battleworld/config"
	"battleworld/storage"

	"github.com/redis/go-redis/v9"
)

const (
	DefaultLeaseKey = "battle:coordinator:leader:lease"
	DefaultTermKey  = storage.CoordinatorLeaderTermRedisKey
)

// RedisOptions 控制 Redis 租约的键名与时序。TTL 必须大于续租间隔，避免正常调度
// 抖动导致不必要的 leadership 丢失。
type RedisOptions struct {
	LeaseKey      string
	TermKey       string
	TTL           time.Duration
	RenewInterval time.Duration
	RetryInterval time.Duration
}

func (o RedisOptions) normalized() (RedisOptions, error) {
	if o.LeaseKey == "" {
		o.LeaseKey = DefaultLeaseKey
	}
	if o.TermKey == "" {
		o.TermKey = DefaultTermKey
	}
	defaults := config.DefaultRuntime().LeaderElection
	if o.TTL == 0 {
		o.TTL = defaults.LeaseTTL.Duration
	}
	if o.RenewInterval == 0 {
		o.RenewInterval = defaults.RenewInterval.Duration
	}
	if o.RetryInterval == 0 {
		o.RetryInterval = defaults.CampaignRetryInterval.Duration
	}
	if o.TTL <= 0 || o.RenewInterval <= 0 || o.RetryInterval <= 0 {
		return RedisOptions{}, errors.New("redis election durations must be positive")
	}
	if o.RenewInterval >= o.TTL {
		return RedisOptions{}, errors.New("redis election renew interval must be shorter than lease TTL")
	}
	return o, nil
}

// RedisElector 使用不可复用的实例 token 作为租约值。续租和释放均以 Lua 比较 token，
// 因而过期旧 leader 即使恢复网络，也无法覆盖或删除新 leader 的租约。
type RedisElector struct {
	client     redis.UniversalClient
	instanceID string
	token      string
	options    RedisOptions

	mu           sync.RWMutex
	leader       bool
	term         uint64
	expiresAt    time.Time
	leaderCtx    context.Context
	leaderCancel context.CancelFunc
	campaigning  bool
}

func NewRedis(client redis.UniversalClient, instanceID string, options RedisOptions) (*RedisElector, error) {
	if client == nil {
		return nil, errors.New("redis election client is nil")
	}
	instanceID = strings.TrimSpace(instanceID)
	if instanceID == "" {
		return nil, errors.New("coordinator instance id is required")
	}
	options, err := options.normalized()
	if err != nil {
		return nil, err
	}
	rawToken := make([]byte, 24)
	if _, err := rand.Read(rawToken); err != nil {
		return nil, fmt.Errorf("generate election token: %w", err)
	}
	return &RedisElector{
		client:     client,
		instanceID: instanceID,
		token:      instanceID + ":" + hex.EncodeToString(rawToken),
		options:    options,
	}, nil
}

func (e *RedisElector) Campaign(ctx context.Context) (context.Context, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	e.mu.Lock()
	if e.leader && e.leaderCtx != nil && e.leaderCtx.Err() == nil {
		leaderCtx := e.leaderCtx
		e.mu.Unlock()
		return leaderCtx, nil
	}
	if e.campaigning {
		e.mu.Unlock()
		return nil, ErrCampaignInProgress
	}
	e.campaigning = true
	e.mu.Unlock()
	defer func() {
		e.mu.Lock()
		e.campaigning = false
		e.mu.Unlock()
	}()

	for {
		term, acquired, err := e.acquire(ctx)
		if err != nil {
			e.loseLeadership("acquire Redis error")
			return nil, fmt.Errorf("acquire leader lease: %w", err)
		}
		if acquired {
			leaderCtx, cancel := context.WithCancel(ctx)
			e.mu.Lock()
			e.leader = true
			e.term = term
			e.expiresAt = time.Now().Add(e.options.TTL)
			e.leaderCtx = leaderCtx
			e.leaderCancel = cancel
			e.mu.Unlock()
			log.Printf("[elector] coordinator %s acquired Redis leader lease, term=%d, ttl=%s", e.instanceID, term, e.options.TTL)
			go e.renewLoop(ctx, leaderCtx)
			return leaderCtx, nil
		}

		timer := time.NewTimer(e.options.RetryInterval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

// acquire 使用同一段 Lua 同时创建 lease token 与递增持久化 LeaderTerm。由此保证任一
// 时刻只要其他实例能够取得新租约，存储层看到的 term 也已大于旧 leader 可携带的 term。
func (e *RedisElector) acquire(ctx context.Context) (uint64, bool, error) {
	const acquireLeaseAndTerm = `
if redis.call('SET', KEYS[1], ARGV[1], 'NX', 'PX', ARGV[2]) then
  return redis.call('INCR', KEYS[2])
end
return 0
`
	term, err := e.client.Eval(ctx, acquireLeaseAndTerm, []string{e.options.LeaseKey, e.options.TermKey}, e.token, e.options.TTL.Milliseconds()).Uint64()
	if err != nil {
		return 0, false, err
	}
	return term, term > 0, nil
}

func (e *RedisElector) renewLoop(campaignCtx, leaderCtx context.Context) {
	ticker := time.NewTicker(e.options.RenewInterval)
	defer ticker.Stop()
	for {
		select {
		case <-campaignCtx.Done():
			_ = e.releaseToken(context.Background())
			e.loseLeadership("campaign context cancelled")
			return
		case <-leaderCtx.Done():
			return
		case <-ticker.C:
			renewed, err := e.renew(campaignCtx)
			if err != nil {
				e.loseLeadership(fmt.Sprintf("renew Redis error: %v", err))
				return
			}
			if !renewed {
				e.loseLeadership("lease token no longer matches")
				return
			}
		}
	}
}

func (e *RedisElector) renew(ctx context.Context) (bool, error) {
	const renewIfTokenMatches = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return 1
end
return 0
`
	result, err := e.client.Eval(ctx, renewIfTokenMatches, []string{e.options.LeaseKey}, e.token, e.options.TTL.Milliseconds()).Int()
	if err != nil {
		return false, err
	}
	if result == 1 {
		e.mu.Lock()
		if e.leader {
			e.expiresAt = time.Now().Add(e.options.TTL)
		}
		e.mu.Unlock()
	}
	return result == 1, nil
}

func (e *RedisElector) IsLeader() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.leader && e.leaderCtx != nil && e.leaderCtx.Err() == nil
}

func (e *RedisElector) Term() uint64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if !e.leader || e.leaderCtx == nil || e.leaderCtx.Err() != nil {
		return 0
	}
	return e.term
}

func (e *RedisElector) Resign() error {
	e.loseLeadership("resigned")
	if err := e.releaseToken(context.Background()); err != nil {
		return fmt.Errorf("release leader lease: %w", err)
	}
	return nil
}

func (e *RedisElector) releaseToken(ctx context.Context) error {
	const deleteIfTokenMatches = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
`
	_, err := e.client.Eval(ctx, deleteIfTokenMatches, []string{e.options.LeaseKey}, e.token).Result()
	return err
}

func (e *RedisElector) loseLeadership(reason string) {
	e.mu.Lock()
	wasLeader := e.leader
	term := e.term
	cancel := e.leaderCancel
	e.leader = false
	e.term = 0
	e.expiresAt = time.Time{}
	e.leaderCancel = nil
	e.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if wasLeader {
		log.Printf("[elector] coordinator %s lost Redis leader lease, former_term=%d, reason=%s", e.instanceID, term, reason)
	}
}

func (e *RedisElector) Status() Status {
	e.mu.RLock()
	defer e.mu.RUnlock()
	status := Status{Leader: e.leader && e.leaderCtx != nil && e.leaderCtx.Err() == nil, LeaderID: e.instanceID}
	if status.Leader {
		status.Term = e.term
		status.LeaseRemaining = time.Until(e.expiresAt)
		if status.LeaseRemaining < 0 {
			status.LeaseRemaining = 0
		}
	}
	return status
}

var _ Elector = (*RedisElector)(nil)
var _ StatusReporter = (*RedisElector)(nil)
