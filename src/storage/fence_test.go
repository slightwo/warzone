package storage

import (
	"errors"
	"testing"
	"time"

	"battleworld/protocol"
)

func TestTopologyCASUpdatesFenceAndRejectsOldCheckpointWriter(t *testing.T) {
	store := newTopologyTestStore(t)
	initial := validTopology()
	if err := store.CompareAndSaveTopology(0, initial); err != nil {
		t.Fatalf("提交初始拓扑: %v", err)
	}

	oldCheckpoint := protocol.MapCheckpoint{MapID: "green", NodeID: "node-a", MapEpoch: 1, Version: 10, Checkpoint: time.Now().UTC()}
	if err := store.SaveCheckpointIfOwner(oldCheckpoint, "node-a", 1); err != nil {
		t.Fatalf("初始 owner 保存 checkpoint: %v", err)
	}

	next := initial.Clone()
	next.Version = 2
	next.Owners["green"] = "node-b"
	next.MapEpochs["green"] = 2
	next.UpdatedAt = time.Now().UTC()
	if err := store.CompareAndSaveTopology(1, next); err != nil {
		t.Fatalf("切换 topology/fence: %v", err)
	}
	if err := store.SaveCheckpointIfOwner(oldCheckpoint, "node-a", 1); !errors.Is(err, ErrMapFenceRejected) {
		t.Fatalf("旧 owner checkpoint 错误 = %v，want ErrMapFenceRejected", err)
	}

	newCheckpoint := protocol.MapCheckpoint{MapID: "green", NodeID: "node-b", MapEpoch: 2, Version: 11, Checkpoint: time.Now().UTC()}
	if err := store.SaveCheckpointIfOwner(newCheckpoint, "node-b", 2); err != nil {
		t.Fatalf("新 owner 保存 checkpoint: %v", err)
	}
	loaded, ok := store.LoadCheckpoint("green")
	if !ok || loaded.NodeID != "node-b" || loaded.MapEpoch != 2 || loaded.Version != 11 {
		t.Fatalf("fenced checkpoint = %+v, found=%t", loaded, ok)
	}
}

func TestTopologyCASAtomicallyMigratesAffectedSessions(t *testing.T) {
	store := newTopologyTestStore(t)
	initial := validTopology()
	if err := store.CompareAndSaveTopology(0, initial); err != nil {
		t.Fatalf("提交初始拓扑: %v", err)
	}
	if err := store.SaveGlobalSession(GlobalSession{Username: "green-player", MapID: "green", NodeID: "node-a", Version: 9}); err != nil {
		t.Fatalf("保存受影响会话: %v", err)
	}
	if err := store.SaveGlobalSession(GlobalSession{Username: "other-player", MapID: "cave", NodeID: "node-a", Version: 4}); err != nil {
		t.Fatalf("保存无关会话: %v", err)
	}

	next := initial.Clone()
	next.Version = 2
	next.Owners["green"] = "node-b"
	next.MapEpochs["green"] = 2
	next.UpdatedAt = time.Now().UTC()
	if err := store.CompareAndSaveTopologyAndMigrateSessions(1, next, "green", "node-a", "node-b"); err != nil {
		t.Fatalf("原子提交 topology/fence/session: %v", err)
	}

	affected, ok := store.LoadGlobalSession("green-player")
	if !ok || affected.NodeID != "node-b" || affected.Version != 10 {
		t.Fatalf("受影响会话未迁移: %+v, found=%t", affected, ok)
	}
	unaffected, ok := store.LoadGlobalSession("other-player")
	if !ok || unaffected.NodeID != "node-a" || unaffected.Version != 4 {
		t.Fatalf("无关会话被错误迁移: %+v, found=%t", unaffected, ok)
	}
	loaded, found, err := store.LoadTopology()
	if err != nil || !found || loaded.Version != 2 || loaded.Owners["green"] != "node-b" || loaded.MapEpochs["green"] != 2 {
		t.Fatalf("原子提交 topology 错误: topology=%+v found=%t err=%v", loaded, found, err)
	}
}

func TestTopologyCASRejectsInvalidOwnerEpochTransition(t *testing.T) {
	store := newTopologyTestStore(t)
	initial := validTopology()
	if err := store.CompareAndSaveTopology(0, initial); err != nil {
		t.Fatalf("提交初始拓扑: %v", err)
	}

	invalid := initial.Clone()
	invalid.Version = 2
	invalid.Owners["green"] = "node-b"
	invalid.UpdatedAt = time.Now().UTC()
	if err := store.CompareAndSaveTopology(1, invalid); !errors.Is(err, ErrMapEpochInvariant) {
		t.Fatalf("owner 变更不递增 epoch 错误 = %v，want ErrMapEpochInvariant", err)
	}
	loaded, found, err := store.LoadTopology()
	if err != nil || !found || loaded.Version != 1 || loaded.Owners["green"] != "node-a" || loaded.MapEpochs["green"] != 1 {
		t.Fatalf("无效 epoch 迁移仍改变 topology: topology=%+v found=%t err=%v", loaded, found, err)
	}
}
