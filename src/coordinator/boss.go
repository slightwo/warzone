package coordinator

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"time"

	"battleworld/protocol"
	"battleworld/storage"
)

const defaultBossHP int32 = 1600

// ensureBoss 只负责首次初始化。之后的生命值和死亡状态由节点侧原子 Boss RPC 更新，
// coordinator 仅在满足复活时间后恢复全局状态。
func (c *Coordinator) ensureBoss() error {
	if _, leader := c.currentLeaderTerm(); !leader {
		return nil
	}
	_, _, err := c.store.LoadGlobalBoss()
	if err == nil {
		return nil
	}
	if !errors.Is(err, storage.ErrGlobalBossNotInitialized) {
		return fmt.Errorf("读取世界首领状态失败: %w", err)
	}

	state := protocol.BossState{
		Name:      "王子文",
		Alive:     true,
		Sites:     c.bossSites(),
		Version:   1,
		AttackGap: 2000,
	}
	if err := c.store.InitGlobalBoss(defaultBossHP, state); err != nil {
		return fmt.Errorf("初始化世界首领失败: %w", err)
	}
	log.Printf("[coordinator/boss] 已初始化世界首领 %q", state.Name)
	return nil
}

func (c *Coordinator) bossLoop(leaderCtx context.Context) {
	ticker := time.NewTicker(c.bossReconcileInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.restoreBossIfDue()
		case <-leaderCtx.Done():
			return
		}
	}
}

func (c *Coordinator) restoreBossIfDue() {
	if _, leader := c.currentLeaderTerm(); !leader {
		return
	}
	state, hp, err := c.store.LoadGlobalBoss()
	if err != nil {
		log.Printf("[coordinator/boss] 读取世界首领状态失败: %v", err)
		return
	}
	if state.Alive || state.RespawnAt.After(time.Now()) {
		return
	}
	if !c.store.TryLockBossRespawn() {
		return
	}

	// 获得锁后重新读取，避免在锁竞争窗口覆盖其它控制面已经完成的恢复。
	state, hp, err = c.store.LoadGlobalBoss()
	if err != nil || state.Alive || state.RespawnAt.After(time.Now()) {
		return
	}
	state.Alive = true
	state.LastHit = ""
	state.RespawnAt = time.Time{}
	state.Version++
	if hp <= 0 {
		hp = defaultBossHP
	}
	if err := c.store.InitGlobalBoss(hp, state); err != nil {
		log.Printf("[coordinator/boss] 复活世界首领失败: %v", err)
		return
	}
	if err := c.store.PublishEvent("events:global", fmt.Sprintf("世界首领【%s】重新降临，所有服务器均可参与讨伐", state.Name)); err != nil {
		log.Printf("[coordinator/boss] 发布首领复活事件失败: %v", err)
	}
}

func (c *Coordinator) bossSites() []protocol.BossSite {
	c.mu.RLock()
	mapIDs := make([]string, 0, len(c.configs))
	for mapID := range c.configs {
		mapIDs = append(mapIDs, mapID)
	}
	sort.Strings(mapIDs)
	sites := make([]protocol.BossSite, 0, len(mapIDs))
	for _, mapID := range mapIDs {
		mapConfig := c.configs[mapID]
		sites = append(sites, protocol.BossSite{MapID: mapID, X: mapConfig.BossX, Y: mapConfig.BossY})
	}
	c.mu.RUnlock()
	return sites
}
