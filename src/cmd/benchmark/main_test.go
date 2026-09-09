package main

import (
	"testing"
	"time"

	"battleworld/pb"
)

func TestPendingCommandsResolveOnlyMatchingRequestID(t *testing.T) {
	pending := newPendingCommands()
	first := time.Unix(100, 0)
	second := time.Unix(200, 0)
	pending.start(11, first)
	pending.start(12, second)

	if _, ok := pending.resolve(99); ok {
		t.Fatal("未知 request_id 不应被视为已确认命令")
	}
	if startedAt, ok := pending.resolve(12); !ok || !startedAt.Equal(second) {
		t.Fatalf("request_id=12 的确认时间 = %v, %t", startedAt, ok)
	}
	if startedAt, ok := pending.resolve(11); !ok || !startedAt.Equal(first) {
		t.Fatalf("request_id=11 的确认时间 = %v, %t", startedAt, ok)
	}
	if _, ok := pending.resolve(11); ok {
		t.Fatal("确认后的 request_id 不应再次计入 RTT")
	}
}

func TestPendingCommandsForgetRemovesFailedSend(t *testing.T) {
	pending := newPendingCommands()
	pending.start(7, time.Now())
	pending.forget(7)
	if _, ok := pending.resolve(7); ok {
		t.Fatal("发送失败并清理后的 request_id 不应产生 RTT")
	}
}

func TestV1Direction(t *testing.T) {
	for _, test := range []struct {
		direction pb.Direction
		want      string
	}{
		{direction: pb.Direction_DIRECTION_UP, want: "up"},
		{direction: pb.Direction_DIRECTION_DOWN, want: "down"},
		{direction: pb.Direction_DIRECTION_LEFT, want: "left"},
		{direction: pb.Direction_DIRECTION_RIGHT, want: "right"},
	} {
		if got := v1Direction(test.direction); got != test.want {
			t.Fatalf("v1Direction(%s) = %q, want %q", test.direction, got, test.want)
		}
	}
}
