package pb

import (
	"reflect"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestClientEnvelopePayloadRoundTrip(t *testing.T) {
	tests := []struct {
		name    string
		payload isClientEnvelope_Payload
	}{
		{name: "login", payload: &ClientEnvelope_Login{Login: &LoginRequest{Username: "tester", Password: "secret"}}},
		{name: "register", payload: &ClientEnvelope_Register{Register: &RegisterRequest{Username: "tester", Password: "secret", ConfirmPassword: "secret"}}},
		{name: "quick_enter", payload: &ClientEnvelope_QuickEnter{QuickEnter: &QuickEnterRequest{Username: "tester", Password: "secret"}}},
		{name: "move", payload: &ClientEnvelope_Move{Move: &MoveCommand{Direction: Direction_DIRECTION_UP}}},
		{name: "attack", payload: &ClientEnvelope_Attack{Attack: &AttackCommand{}}},
		{name: "attack_boss", payload: &ClientEnvelope_AttackBoss{AttackBoss: &AttackBossCommand{}}},
		{name: "heal", payload: &ClientEnvelope_Heal{Heal: &HealCommand{}}},
		{name: "buy_item", payload: &ClientEnvelope_BuyItem{BuyItem: &BuyItemCommand{Item: "potion"}}},
		{name: "switch_map", payload: &ClientEnvelope_SwitchMap{SwitchMap: &SwitchMapCommand{MapId: "cave"}}},
		{name: "logout", payload: &ClientEnvelope_Logout{Logout: &LogoutCommand{}}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			original := &ClientEnvelope{RequestId: 42, Payload: test.payload}
			actual := roundTripProto(t, original, &ClientEnvelope{}).(*ClientEnvelope)

			if !proto.Equal(actual, original) {
				t.Fatalf("ClientEnvelope protobuf 往返后不一致:\n got:  %v\n want: %v", actual, original)
			}
			if actual.GetRequestId() != 42 {
				t.Fatalf("request_id = %d, want 42", actual.GetRequestId())
			}
			if got, want := reflect.TypeOf(actual.GetPayload()), reflect.TypeOf(test.payload); got != want {
				t.Fatalf("payload wrapper = %v, want %v", got, want)
			}
		})
	}
}

func TestServerEnvelopePayloadRoundTrip(t *testing.T) {
	state := &WorldState{SessionVersion: 11, TopologyVersion: 12, MapEpoch: 13}
	tests := []struct {
		name      string
		requestID uint64
		payload   isServerEnvelope_Payload
	}{
		{name: "authenticated", requestID: 42, payload: &ServerEnvelope_Authenticated{Authenticated: &Authenticated{State: state}}},
		{name: "command_result", requestID: 43, payload: &ServerEnvelope_CommandResult{CommandResult: &CommandResult{Message: "移动已接受"}}},
		{name: "state", requestID: 0, payload: &ServerEnvelope_State{State: state}},
		{name: "error", requestID: 44, payload: &ServerEnvelope_Error{Error: &ErrorResponse{Code: ErrorCode_ERROR_CODE_STALE_ROUTE, Message: "路由已过期", Retryable: true}}},
		{name: "notice", requestID: 45, payload: &ServerEnvelope_Notice{Notice: &ServerNotice{Message: "维护通知"}}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			original := &ServerEnvelope{RequestId: test.requestID, Payload: test.payload}
			actual := roundTripProto(t, original, &ServerEnvelope{}).(*ServerEnvelope)

			if !proto.Equal(actual, original) {
				t.Fatalf("ServerEnvelope protobuf 往返后不一致:\n got:  %v\n want: %v", actual, original)
			}
			if got, want := reflect.TypeOf(actual.GetPayload()), reflect.TypeOf(test.payload); got != want {
				t.Fatalf("payload wrapper = %v, want %v", got, want)
			}
		})
	}
}

func TestV2ContractShapeAndStableErrorCodes(t *testing.T) {
	file := File_pb_battle_proto
	gateway := findService(t, file, "GatewayService")
	legacy := findMethod(t, gateway, "GameStream")
	assertStreamingMethod(t, legacy, true, true, "Message", "Message")
	v2 := findMethod(t, gateway, "GameStreamV2")
	assertStreamingMethod(t, v2, true, true, "ClientEnvelope", "ServerEnvelope")

	admin := findService(t, file, "AdminService")
	status := findMethod(t, admin, "GetGatewayStatus")
	assertStreamingMethod(t, status, false, false, "GatewayStatusRequest", "GatewayStatusResponse")

	nodeV2 := findService(t, file, "NodeServiceV2")
	assertStreamingMethod(t, findMethod(t, nodeV2, "AddPlayer"), false, false, "NodeAddPlayerRequest", "NodeAddPlayerResponse")
	assertStreamingMethod(t, findMethod(t, nodeV2, "Promote"), false, false, "NodePromoteRequest", "NodePromoteResponse")

	authority := findMessage(t, file, "MapAuthority")
	for name, number := range map[protoreflect.Name]protoreflect.FieldNumber{
		"map_id": 1, "owner_node_id": 2, "map_epoch": 3,
	} {
		assertFieldNumber(t, authority, name, number)
	}
	playerState := findMessage(t, file, "PlayerState")
	assertMissingField(t, playerState, "password_hash")
	checkpoint := findMessage(t, file, "NodeCheckpoint")
	assertFieldNumber(t, checkpoint, "captured_at", 8)
	if field := checkpoint.Fields().ByName("captured_at"); field == nil || string(field.Message().FullName()) != "google.protobuf.Timestamp" {
		t.Fatalf("NodeCheckpoint.captured_at must be google.protobuf.Timestamp, got %v", field)
	}

	clientEnvelope := findMessage(t, file, "ClientEnvelope")
	assertFieldNumber(t, clientEnvelope, "request_id", 1)
	assertOneof(t, clientEnvelope, "payload")
	for name, number := range map[protoreflect.Name]protoreflect.FieldNumber{
		"login": 10, "register": 11, "quick_enter": 12,
		"move": 20, "attack": 21, "attack_boss": 22, "heal": 23,
		"buy_item": 24, "switch_map": 25, "logout": 26,
	} {
		assertFieldNumber(t, clientEnvelope, name, number)
	}
	assertMissingField(t, clientEnvelope, "type")
	assertMissingField(t, clientEnvelope, "action")

	serverEnvelope := findMessage(t, file, "ServerEnvelope")
	assertFieldNumber(t, serverEnvelope, "request_id", 1)
	assertOneof(t, serverEnvelope, "payload")
	for name, number := range map[protoreflect.Name]protoreflect.FieldNumber{
		"authenticated": 10, "command_result": 20, "state": 30, "error": 40, "notice": 50,
	} {
		assertFieldNumber(t, serverEnvelope, name, number)
	}
	assertMissingField(t, serverEnvelope, "type")
	assertMissingField(t, serverEnvelope, "action")

	login := findMessage(t, file, "LoginRequest")
	assertMissingField(t, login, "password_hash")
	register := findMessage(t, file, "RegisterRequest")
	assertMissingField(t, register, "password_hash")
	quickEnter := findMessage(t, file, "QuickEnterRequest")
	assertMissingField(t, quickEnter, "password_hash")

	statusResponse := findMessage(t, file, "GatewayStatusResponse")
	for name, number := range map[protoreflect.Name]protoreflect.FieldNumber{
		"routing_ready": 1, "topology_version": 2, "nodes": 3, "summary": 4,
	} {
		assertFieldNumber(t, statusResponse, name, number)
	}

	for name, number := range map[protoreflect.Name]protoreflect.EnumNumber{
		"ERROR_CODE_INVALID_REQUEST":  1,
		"ERROR_CODE_AUTH_FAILED":      2,
		"ERROR_CODE_ROUTE_NOT_READY":  3,
		"ERROR_CODE_STALE_ROUTE":      4,
		"ERROR_CODE_COMMAND_REJECTED": 5,
		"ERROR_CODE_INTERNAL":         6,
	} {
		value := file.Enums().ByName("ErrorCode").Values().ByName(name)
		if value == nil || value.Number() != number {
			t.Fatalf("ErrorCode.%s = %v, want %d", name, value, number)
		}
	}
}

func TestLegacyV1WireContractRemainsStable(t *testing.T) {
	file := File_pb_battle_proto
	message := findMessage(t, file, "Message")
	for name, number := range map[protoreflect.Name]protoreflect.FieldNumber{
		"type": 1, "action": 2, "username": 3, "password": 4, "dir": 5,
		"map_id": 6, "node_id": 7, "confirm": 8, "item": 9, "text": 10,
		"ok": 11, "error": 12, "state": 13,
	} {
		assertFieldNumber(t, message, name, number)
	}

	worldState := findMessage(t, file, "WorldState")
	for name, number := range map[protoreflect.Name]protoreflect.FieldNumber{
		"self": 1, "map": 2, "maps": 3, "nodes": 4, "boss": 5,
		"events": 6, "session_version": 7, "topology_version": 8, "map_epoch": 9,
	} {
		assertFieldNumber(t, worldState, name, number)
	}
}

func roundTripProto(t *testing.T, original, target proto.Message) proto.Message {
	t.Helper()
	encoded, err := proto.Marshal(original)
	if err != nil {
		t.Fatalf("marshal %T: %v", original, err)
	}
	if err := proto.Unmarshal(encoded, target); err != nil {
		t.Fatalf("unmarshal %T: %v", target, err)
	}
	return target
}

func findService(t *testing.T, file protoreflect.FileDescriptor, name protoreflect.Name) protoreflect.ServiceDescriptor {
	t.Helper()
	service := file.Services().ByName(name)
	if service == nil {
		t.Fatalf("未找到 service %q", name)
	}
	return service
}

func findMethod(t *testing.T, service protoreflect.ServiceDescriptor, name protoreflect.Name) protoreflect.MethodDescriptor {
	t.Helper()
	method := service.Methods().ByName(name)
	if method == nil {
		t.Fatalf("未找到 %s.%s", service.FullName(), name)
	}
	return method
}

func assertStreamingMethod(t *testing.T, method protoreflect.MethodDescriptor, clientStreams, serverStreams bool, input, output protoreflect.Name) {
	t.Helper()
	if method.IsStreamingClient() != clientStreams || method.IsStreamingServer() != serverStreams {
		t.Fatalf("%s streaming = client:%t server:%t, want client:%t server:%t", method.FullName(), method.IsStreamingClient(), method.IsStreamingServer(), clientStreams, serverStreams)
	}
	if method.Input().Name() != input || method.Output().Name() != output {
		t.Fatalf("%s types = %s -> %s, want %s -> %s", method.FullName(), method.Input().Name(), method.Output().Name(), input, output)
	}
}

func findMessage(t *testing.T, file protoreflect.FileDescriptor, name protoreflect.Name) protoreflect.MessageDescriptor {
	t.Helper()
	message := file.Messages().ByName(name)
	if message == nil {
		t.Fatalf("未找到 message %q", name)
	}
	return message
}

func assertFieldNumber(t *testing.T, message protoreflect.MessageDescriptor, name protoreflect.Name, number protoreflect.FieldNumber) {
	t.Helper()
	field := message.Fields().ByName(name)
	if field == nil || field.Number() != number {
		t.Fatalf("%s.%s tag = %v, want %d", message.FullName(), name, field, number)
	}
}

func assertOneof(t *testing.T, message protoreflect.MessageDescriptor, name protoreflect.Name) {
	t.Helper()
	if message.Oneofs().ByName(name) == nil {
		t.Fatalf("%s 缺少 oneof %q", message.FullName(), name)
	}
}

func assertMissingField(t *testing.T, message protoreflect.MessageDescriptor, name protoreflect.Name) {
	t.Helper()
	if message.Fields().ByName(name) != nil {
		t.Fatalf("%s 不应声明字段 %q", message.FullName(), name)
	}
}
