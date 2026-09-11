package node

import (
	"reflect"
	"testing"
	"time"

	"battleworld/pb"
	"battleworld/protocol"

	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestCheckpointRoundTripUsesTimestamp(t *testing.T) {
	capturedAt := time.Date(2026, time.September, 10, 12, 34, 56, 0, time.UTC)
	original := protocol.MapCheckpoint{
		MapID: "green", NodeID: "node-a", MapEpoch: 7, Version: 42,
		Terrain:    []string{"....", "####"},
		Players:    []protocol.PlayerView{{Username: "tester", MapID: "green", HP: 120, Alive: true}},
		NPCs:       []protocol.NPCView{{ID: "npc-1", Name: "守卫", HP: 40, Alive: true}},
		Treasures:  []protocol.TreasureView{{ID: "gold-1", Kind: "gold", Value: 3}},
		Checkpoint: capturedAt,
	}

	wire := ToCheckpoint(original)
	if wire.GetCapturedAt() == nil || !wire.GetCapturedAt().AsTime().Equal(capturedAt) {
		t.Fatalf("captured_at = %v, want %v", wire.GetCapturedAt(), capturedAt)
	}
	actual, err := FromCheckpoint(wire)
	if err != nil {
		t.Fatalf("checkpoint 从 wire 还原: %v", err)
	}
	if !reflect.DeepEqual(actual, original) {
		t.Fatalf("checkpoint 往返不一致:\n got:  %#v\n want: %#v", actual, original)
	}
}

func TestCheckpointRejectsInvalidTimestamp(t *testing.T) {
	_, err := FromCheckpoint(&pb.NodeCheckpoint{CapturedAt: &timestamppb.Timestamp{Seconds: 253402300800}})
	if err == nil {
		t.Fatal("非法 Timestamp 未被拒绝")
	}
}
