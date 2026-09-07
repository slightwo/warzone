// Package lifecycle 提供进程级 health、ready 与 drain HTTP 端点的通用实现。
package lifecycle

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
)

// Status 是进程生命周期的可观测快照。
type Status struct {
	Draining          bool  `json:"draining"`
	ActiveConnections int64 `json:"active_connections"`
}

// Callbacks 将具体进程的生命周期状态和 drain 动作注入 HTTP handler。
type Callbacks struct {
	Status func() Status
	Ready  func() bool
	Drain  func(context.Context) error
}

// NewHandler 返回固定的健康检查路由：/healthz、/readyz 与 /drain。
//
// /healthz 只表示进程仍可响应；/readyz 在 draining 或依赖尚未就绪时返回 503；
// /drain 仅接受 POST，且要求回调实现幂等。
func NewHandler(callbacks Callbacks) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet {
			methodNotAllowed(writer)
			return
		}
		writeJSON(writer, http.StatusOK, currentStatus(callbacks))
	})
	mux.HandleFunc("/readyz", func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet {
			methodNotAllowed(writer)
			return
		}
		status := currentStatus(callbacks)
		if status.Draining || callbacks.Ready == nil || !callbacks.Ready() {
			writeJSON(writer, http.StatusServiceUnavailable, status)
			return
		}
		writeJSON(writer, http.StatusOK, status)
	})
	mux.HandleFunc("/drain", func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost {
			methodNotAllowed(writer)
			return
		}
		if callbacks.Drain == nil {
			writeError(writer, http.StatusNotImplemented, errors.New("drain is not supported"))
			return
		}
		if err := callbacks.Drain(request.Context()); err != nil {
			writeError(writer, http.StatusConflict, err)
			return
		}
		writeJSON(writer, http.StatusAccepted, currentStatus(callbacks))
	})
	return mux
}

func currentStatus(callbacks Callbacks) Status {
	if callbacks.Status == nil {
		return Status{}
	}
	return callbacks.Status()
}

func methodNotAllowed(writer http.ResponseWriter) {
	writer.Header().Set("Allow", http.MethodGet+", "+http.MethodPost)
	writeError(writer, http.StatusMethodNotAllowed, errors.New("method not allowed"))
}

func writeError(writer http.ResponseWriter, status int, err error) {
	writeJSON(writer, status, struct {
		Error string `json:"error"`
	}{Error: err.Error()})
}

func writeJSON(writer http.ResponseWriter, status int, payload any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(payload)
}
