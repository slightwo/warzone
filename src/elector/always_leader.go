package elector

import (
	"context"
	"sync"
)

// AlwaysLeader 用于单实例运行和不依赖 Redis 的单元测试。它固定使用 term=1；生产
// 多实例 coordinator 必须使用 RedisElector，由 Redis 持久化递增 term。
type AlwaysLeader struct {
	mu        sync.Mutex
	leader    bool
	leaderCtx context.Context
	cancel    context.CancelFunc
}

func NewAlwaysLeader() *AlwaysLeader {
	return &AlwaysLeader{leader: true}
}

func (e *AlwaysLeader) Campaign(ctx context.Context) (context.Context, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.leader {
		e.leader = true
	}
	if e.leaderCtx == nil || e.leaderCtx.Err() != nil {
		e.leaderCtx, e.cancel = context.WithCancel(ctx)
	}
	return e.leaderCtx, nil
}

func (e *AlwaysLeader) IsLeader() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.leader
}

func (e *AlwaysLeader) Term() uint64 {
	if !e.IsLeader() {
		return 0
	}
	return 1
}

func (e *AlwaysLeader) Resign() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.leader = false
	if e.cancel != nil {
		e.cancel()
	}
	return nil
}

func (e *AlwaysLeader) Status() Status {
	return Status{Leader: e.IsLeader(), Term: e.Term(), LeaderID: "single-instance"}
}

var _ Elector = (*AlwaysLeader)(nil)
var _ StatusReporter = (*AlwaysLeader)(nil)
