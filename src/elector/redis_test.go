package elector

import (
	"context"
	"net"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestRedisElectorAllowsOnlyOneLeaderThenFailsOver(t *testing.T) {
	client := newElectorTestRedis(t)
	options := RedisOptions{
		LeaseKey:      "test:leader:lease",
		TermKey:       "test:leader:term",
		TTL:           500 * time.Millisecond,
		RenewInterval: 50 * time.Millisecond,
		RetryInterval: 10 * time.Millisecond,
	}
	first, err := NewRedis(client, "coordinator-a", options)
	if err != nil {
		t.Fatalf("创建第一个 elector: %v", err)
	}
	second, err := NewRedis(client, "coordinator-b", options)
	if err != nil {
		t.Fatalf("创建第二个 elector: %v", err)
	}
	t.Cleanup(func() {
		_ = first.Resign()
		_ = second.Resign()
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	firstLeaderCtx, err := first.Campaign(ctx)
	if err != nil {
		t.Fatalf("第一个实例竞选 leader: %v", err)
	}
	if !first.IsLeader() || first.Term() != 1 {
		t.Fatalf("第一个实例选主状态错误: leader=%t term=%d", first.IsLeader(), first.Term())
	}

	secondResult := make(chan struct {
		ctx context.Context
		err error
	}, 1)
	go func() {
		leaderCtx, campaignErr := second.Campaign(ctx)
		secondResult <- struct {
			ctx context.Context
			err error
		}{ctx: leaderCtx, err: campaignErr}
	}()

	select {
	case result := <-secondResult:
		t.Fatalf("第一个租约仍有效时第二个实例获得结果: ctx=%v err=%v", result.ctx, result.err)
	case <-time.After(100 * time.Millisecond):
	}

	if err := first.Resign(); err != nil {
		t.Fatalf("第一个实例释放 leader 租约: %v", err)
	}
	select {
	case <-firstLeaderCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("释放租约后第一个 leader context 未取消")
	}

	select {
	case result := <-secondResult:
		if result.err != nil {
			t.Fatalf("第二个实例接管 leader: %v", result.err)
		}
		if result.ctx == nil || !second.IsLeader() || second.Term() != 2 {
			t.Fatalf("第二个实例接管状态错误: ctx=%v leader=%t term=%d", result.ctx, second.IsLeader(), second.Term())
		}
	case <-time.After(time.Second):
		t.Fatal("第二个实例未在原 leader 释放后接管")
	}
}

func TestRedisElectorAcquiresLeaseAndTermAtomically(t *testing.T) {
	client := newElectorTestRedis(t)
	options := RedisOptions{
		LeaseKey:      "test:leader:lease",
		TermKey:       "test:leader:term",
		TTL:           500 * time.Millisecond,
		RenewInterval: 50 * time.Millisecond,
	}
	election, err := NewRedis(client, "coordinator-a", options)
	if err != nil {
		t.Fatalf("创建 elector: %v", err)
	}

	term, acquired, err := election.acquire(context.Background())
	if err != nil {
		t.Fatalf("原子获取租约: %v", err)
	}
	if !acquired || term != 1 {
		t.Fatalf("原子获取结果错误: acquired=%t term=%d", acquired, term)
	}
	persistedTerm, err := client.Get(context.Background(), options.TermKey).Uint64()
	if err != nil {
		t.Fatalf("读取持久化 term: %v", err)
	}
	if persistedTerm != term {
		t.Fatalf("租约与任期未同时写入: persisted=%d term=%d", persistedTerm, term)
	}
	other, err := NewRedis(client, "coordinator-b", options)
	if err != nil {
		t.Fatalf("创建第二个 elector: %v", err)
	}
	secondTerm, secondAcquired, err := other.acquire(context.Background())
	if err != nil {
		t.Fatalf("第二个实例尝试获取租约: %v", err)
	}
	if secondAcquired || secondTerm != 0 {
		t.Fatalf("已有租约时错误获取 leader: acquired=%t term=%d", secondAcquired, secondTerm)
	}
}

func TestRedisElectorCancelsLeaderContextWhenLeaseTokenChanges(t *testing.T) {
	client := newElectorTestRedis(t)
	options := RedisOptions{
		LeaseKey:      "test:leader:lease",
		TermKey:       "test:leader:term",
		TTL:           500 * time.Millisecond,
		RenewInterval: 30 * time.Millisecond,
		RetryInterval: 10 * time.Millisecond,
	}
	election, err := NewRedis(client, "coordinator-a", options)
	if err != nil {
		t.Fatalf("创建 elector: %v", err)
	}
	t.Cleanup(func() { _ = election.Resign() })

	leaderCtx, err := election.Campaign(context.Background())
	if err != nil {
		t.Fatalf("竞选 leader: %v", err)
	}
	if err := client.Set(context.Background(), options.LeaseKey, "replacement-token", options.TTL).Err(); err != nil {
		t.Fatalf("模拟外部接管租约: %v", err)
	}

	select {
	case <-leaderCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("租约 token 变化后 leader context 未取消")
	}
	if election.IsLeader() || election.Term() != 0 {
		t.Fatalf("租约 token 变化后仍保留 leader 状态: leader=%t term=%d", election.IsLeader(), election.Term())
	}
	owner, err := client.Get(context.Background(), options.LeaseKey).Result()
	if err != nil {
		t.Fatalf("读取新租约 token: %v", err)
	}
	if owner != "replacement-token" {
		t.Fatalf("旧实例错误删除新租约: owner=%q", owner)
	}
}

func newElectorTestRedis(t *testing.T) *redis.Client {
	t.Helper()
	redisServer, err := exec.LookPath("redis-server")
	if err != nil {
		t.Skip("redis-server is required for elector integration tests")
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("预留 Redis 端口: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatalf("释放 Redis 端口: %v", err)
	}

	process := exec.Command(redisServer,
		"--bind", "127.0.0.1",
		"--port", strconv.Itoa(port),
		"--save", "",
		"--appendonly", "no",
	)
	if err := process.Start(); err != nil {
		t.Fatalf("启动临时 Redis: %v", err)
	}
	t.Cleanup(func() {
		if process.Process != nil {
			_ = process.Process.Kill()
		}
		_ = process.Wait()
	})

	client := redis.NewClient(&redis.Options{Addr: net.JoinHostPort("127.0.0.1", strconv.Itoa(port))})
	t.Cleanup(func() { _ = client.Close() })
	deadline := time.Now().Add(3 * time.Second)
	for {
		if err := client.Ping(context.Background()).Err(); err == nil {
			return client
		} else if time.Now().After(deadline) {
			t.Fatalf("临时 Redis 未就绪: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
