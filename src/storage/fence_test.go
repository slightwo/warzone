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
		t.Fatalf("create topology: %v", err)
	}
	oldCheckpoint := protocol.MapCheckpoint{MapID: "green", NodeID: "node-a", MapEpoch: 1, Version: 10, Checkpoint: time.Now().UTC()}
	if err := store.SaveCheckpointIfOwner(oldCheckpoint, "node-a", 1); err != nil {
		t.Fatalf("save initial checkpoint: %v", err)
	}
	next := initial.Clone()
	next.Version = 2
	next.Owners["green"] = "node-b"
	next.Replicas["green"] = "node-a"
	next.MapEpochs["green"] = 2
	next.UpdatedAt = time.Now().UTC()
	if err := store.CompareAndSaveTopology(1, next); err != nil {
		t.Fatalf("switch topology: %v", err)
	}
	if err := store.SaveCheckpointIfOwner(oldCheckpoint, "node-a", 1); !errors.Is(err, ErrMapFenceRejected) {
		t.Fatalf("old writer error = %v, want ErrMapFenceRejected", err)
	}
	newCheckpoint := protocol.MapCheckpoint{MapID: "green", NodeID: "node-b", MapEpoch: 2, Version: 11, Checkpoint: time.Now().UTC()}
	if err := store.SaveCheckpointIfOwner(newCheckpoint, "node-b", 2); err != nil {
		t.Fatalf("save new checkpoint: %v", err)
	}
	loaded, ok := store.LoadCheckpoint("green")
	if !ok || loaded.NodeID != "node-b" || loaded.MapEpoch != 2 {
		t.Fatalf("unexpected checkpoint: %+v, found=%t", loaded, ok)
	}
}

func TestTopologyCASRejectsInvalidOwnerEpochTransition(t *testing.T) {
	store := newTopologyTestStore(t)
	initial := validTopology()
	if err := store.CompareAndSaveTopology(0, initial); err != nil {
		t.Fatalf("create topology: %v", err)
	}
	invalid := initial.Clone()
	invalid.Version = 2
	invalid.Owners["green"] = "node-b"
	invalid.Replicas["green"] = "node-a"
	invalid.UpdatedAt = time.Now().UTC()
	if err := store.CompareAndSaveTopology(1, invalid); !errors.Is(err, ErrMapEpochInvariant) {
		t.Fatalf("invalid owner epoch error = %v, want ErrMapEpochInvariant", err)
	}
}
