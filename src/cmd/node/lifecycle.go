package main

import (
	"context"
	"net/http"
	"time"

	"battleworld/config"
	"battleworld/lifecycle"
	"battleworld/node"
)

// serveLifecycle 在给定地址暴露 /healthz、/readyz、/drain。它将进程
// 生命周期状态桥接到 NodeService：/healthz 恒为 200；/readyz 在 drain 期间返回 503，
// 避免调度器把新会话路由到正在下线的节点；/drain 调用 BeginDrain，超时由调用方控制。
func serveLifecycle(addr string, ns *node.NodeService, drainTimeout time.Duration, runtime config.RuntimeConfig) {
	mux := http.NewServeMux()
	mux.Handle("/", lifecycle.NewHandler(lifecycle.Callbacks{
		Status: func() lifecycle.Status {
			state := ns.DrainState()
			return lifecycle.Status{
				Draining:          state.Draining,
				ActiveConnections: state.ActiveConnections,
			}
		},
		Ready: func() bool {
			return !ns.IsDraining()
		},
		Drain: func(ctx context.Context) error {
			reqCtx, cancel := context.WithTimeout(context.Background(), drainTimeout)
			defer cancel()
			return ns.BeginDrain(reqCtx)
		},
	}))
	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: runtime.HTTP.LifecycleReadHeaderTimeout.Duration,
	}
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		panic(err)
	}
}
