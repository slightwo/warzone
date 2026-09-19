package main

import (
	"errors"
	"strings"

	"battleworld/pb"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type errorKind uint8

const (
	authError errorKind = iota
	commandError
	stateError
)

func sendInitialError(stream pb.GatewayService_GameStreamServer, requestID uint64, code pb.ErrorCode, message string, retryable bool) error {
	err := stream.Send(errorEnvelope(requestID, code, message, retryable))
	if err != nil {
		return err
	}
	return errors.New(message)
}

func gatewayError(requestID uint64, err error, kind errorKind) *pb.ServerEnvelope {
	code, retryable := mapError(err, kind)
	return errorEnvelope(requestID, code, err.Error(), retryable)
}

func errorEnvelope(requestID uint64, code pb.ErrorCode, message string, retryable bool) *pb.ServerEnvelope {
	return &pb.ServerEnvelope{
		RequestId: requestID,
		Payload: &pb.ServerEnvelope_Error{Error: &pb.ErrorResponse{
			Code:      code,
			Message:   message,
			Retryable: retryable,
		}},
	}
}

func mapError(err error, kind errorKind) (pb.ErrorCode, bool) {
	if err == nil {
		return pb.ErrorCode_ERROR_CODE_INTERNAL, true
	}
	switch status.Code(err) {
	case codes.FailedPrecondition:
		return pb.ErrorCode_ERROR_CODE_STALE_ROUTE, true
	case codes.Unavailable, codes.DeadlineExceeded:
		return pb.ErrorCode_ERROR_CODE_ROUTE_NOT_READY, true
	case codes.InvalidArgument:
		return pb.ErrorCode_ERROR_CODE_INVALID_REQUEST, false
	}

	message := err.Error()
	switch {
	case containsAny(message, "code = FailedPrecondition", "code = FAILED_PRECONDITION"):
		return pb.ErrorCode_ERROR_CODE_STALE_ROUTE, true
	case containsAny(message, "code = Unavailable", "code = DeadlineExceeded"):
		return pb.ErrorCode_ERROR_CODE_ROUTE_NOT_READY, true
	case containsAny(message, "code = InvalidArgument"):
		return pb.ErrorCode_ERROR_CODE_INVALID_REQUEST, false
	}
	if kind == authError {
		switch {
		case containsAny(message, "不能为空", "不一致", "请求不能为空", "首条消息"):
			return pb.ErrorCode_ERROR_CODE_INVALID_REQUEST, false
		case containsAny(message, "不存在", "密码", "已经在线"):
			return pb.ErrorCode_ERROR_CODE_AUTH_FAILED, strings.Contains(message, "已经在线")
		}
	}
	if containsAny(message, "路由", "同步中", "承载节点", "topology authority unavailable") {
		return pb.ErrorCode_ERROR_CODE_ROUTE_NOT_READY, true
	}
	if containsAny(message, "authority denied", "stale topology", "当前 epoch", "epoch") {
		return pb.ErrorCode_ERROR_CODE_STALE_ROUTE, true
	}
	if kind == stateError {
		return pb.ErrorCode_ERROR_CODE_ROUTE_NOT_READY, true
	}
	if kind == commandError {
		if containsAny(message, "不能为空", "不允许", "缺少可识别", "无效") {
			return pb.ErrorCode_ERROR_CODE_INVALID_REQUEST, false
		}
		return pb.ErrorCode_ERROR_CODE_COMMAND_REJECTED, false
	}
	return pb.ErrorCode_ERROR_CODE_INTERNAL, true
}

func containsAny(message string, fragments ...string) bool {
	for _, fragment := range fragments {
		if strings.Contains(message, fragment) {
			return true
		}
	}
	return false
}
