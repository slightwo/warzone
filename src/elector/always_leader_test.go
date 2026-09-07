package elector

import (
	"context"
	"testing"
)

func TestAlwaysLeaderCampaignAndResign(t *testing.T) {
	election := NewAlwaysLeader()
	campaignCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	leaderCtx, err := election.Campaign(campaignCtx)
	if err != nil {
		t.Fatalf("竞选 leader: %v", err)
	}
	if leaderCtx == nil || !election.IsLeader() || election.Term() != 1 {
		t.Fatalf("选主状态错误: ctx=%v leader=%t term=%d", leaderCtx, election.IsLeader(), election.Term())
	}
	if err := election.Resign(); err != nil {
		t.Fatalf("释放 leader: %v", err)
	}
	select {
	case <-leaderCtx.Done():
	default:
		t.Fatal("Resign 后 leader context 未取消")
	}
	if election.IsLeader() || election.Term() != 0 {
		t.Fatalf("Resign 后状态错误: leader=%t term=%d", election.IsLeader(), election.Term())
	}
}
