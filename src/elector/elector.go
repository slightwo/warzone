// Package elector 提供协调器选主的可替换抽象。
package elector

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrCampaignInProgress 表示同一实例已有一个正在等待选主结果的 Campaign 调用。
	ErrCampaignInProgress = errors.New("leader campaign already in progress")
	// ErrNotLeader 表示操作要求当前实例持有 leader 租约，但该租约已不存在。
	ErrNotLeader = errors.New("not leader")
)

// Elector 决定当前 coordinator 实例是否可执行控制面决策。Campaign 阻塞到获得
// leader 租约或调用方 context 结束；返回的 leaderCtx 在租约续约失败、被抢占或
// Resign 后立即取消。
type Elector interface {
	Campaign(ctx context.Context) (leaderCtx context.Context, err error)
	IsLeader() bool
	Term() uint64
	Resign() error
}

// Status 是选主状态的可观测快照。LeaseRemaining 仅表示当前实例已知的本地租约
// 剩余时间，不能作为继续执行控制面写入的授权依据。
type Status struct {
	Leader         bool
	LeaderID       string
	Term           uint64
	LeaseRemaining time.Duration
}

// StatusReporter 为日志、健康检查等观测场景提供附加信息，不增加 Elector 的最小契约。
type StatusReporter interface {
	Status() Status
}
