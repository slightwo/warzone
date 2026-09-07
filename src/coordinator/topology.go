package coordinator

import (
	"errors"
	"fmt"
	"log"
	"time"

	"battleworld/storage"
)

// bootstrapTopologyIfAbsent 仅在 Redis 尚无拓扑时，根据已验证健康的节点候选能力创建
// Version 1。CAS 冲突表示其他控制面已完成 bootstrap，不得重试覆盖。
func (c *Coordinator) bootstrapTopologyIfAbsent(nodes []storage.NodeRegistryInfo) error {
	term, leader := c.currentLeaderTerm()
	if !leader {
		return nil
	}
	_, found, err := c.store.LoadTopology()
	if err != nil {
		return fmt.Errorf("bootstrap 前读取拓扑失败: %w", err)
	}
	if found {
		return nil
	}

	initial, ready, err := storage.BuildInitialTopology(c.knownMapIDs(), nodes, time.Now().UTC())
	if err != nil {
		return fmt.Errorf("构造初始拓扑失败: %w", err)
	}
	if !ready {
		return nil
	}
	if !c.hasLeaderTerm(term) {
		return nil
	}
	initial.LeaderTerm = term
	if err := c.store.CompareAndSaveTopologyForLeader(0, initial, term); err != nil {
		if errors.Is(err, storage.ErrTopologyVersionConflict) {
			return nil
		}
		return fmt.Errorf("提交初始拓扑失败: %w", err)
	}
	if err := c.store.PublishTopologyChanged(initial.Version); err != nil {
		return fmt.Errorf("初始拓扑已提交，但发布刷新通知失败: %w", err)
	}
	log.Printf("[coordinator/topology] 已创建初始拓扑版本 %d，leader term=%d", initial.Version, term)
	return nil
}
