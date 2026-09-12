package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"

	"battleworld/config"
	"battleworld/coordinator"
	"battleworld/lifecycle"
	"battleworld/storage"
)

func main() {
	lifecycleAddr := flag.String("lifecycle-addr", "", "生命周期 HTTP 监听地址；为空时禁用（如：127.0.0.1:9412）")
	flag.Parse()

	dataRoot := resolveDataRoot()
	store, err := storage.NewStore(dataRoot)
	if err != nil {
		fmt.Fprintf(os.Stderr, "初始化协调器存储失败：%v\n", err)
		os.Exit(1)
	}

	runtime, err := config.LoadRuntime()
	if err != nil {
		fmt.Fprintf(os.Stderr, "加载运行时配置失败：%v\n", err)
		os.Exit(1)
	}

	control, err := coordinator.NewWithRuntimeConfig(store, runtime)
	if err != nil {
		fmt.Fprintf(os.Stderr, "初始化 coordinator 失败：%v\n", err)
		os.Exit(1)
	}
	if err := control.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "启动 coordinator 失败：%v\n", err)
		os.Exit(1)
	}
	defer control.Close()

	// draining 标志为 1 时，/readyz 返回 503；/drain 主动让出 leader 租约。
	var draining atomic.Bool
	if *lifecycleAddr != "" {
		go serveCoordinatorLifecycle(*lifecycleAddr, control, &draining, runtime)
	}

	fmt.Println("coordinator 已启动：负责节点发现、健康检查、拓扑初始化和 Boss 生命周期")
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	fmt.Println("coordinator 正在关闭")
}

// serveCoordinatorLifecycle 暴露 /healthz、/readyz、/drain。coordinator 的 drain
// 表示主动让出 leader 租约并停止控制循环，供运维在滚动升级前按序下线。
func serveCoordinatorLifecycle(addr string, control *coordinator.Coordinator, draining *atomic.Bool, runtime config.RuntimeConfig) {
	handler := lifecycle.NewHandler(lifecycle.Callbacks{
		Status: func() lifecycle.Status {
			return lifecycle.Status{Draining: draining.Load()}
		},
		Ready: func() bool {
			return !draining.Load()
		},
		Drain: func(ctx context.Context) error {
			if !draining.CompareAndSwap(false, true) {
				return nil
			}
			control.Close()
			return nil
		},
	})
	srv := &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: runtime.HTTP.LifecycleReadHeaderTimeout.Duration,
	}
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		fmt.Fprintf(os.Stderr, "coordinator lifecycle 监听失败：%v\n", err)
	}
}

func resolveDataRoot() string {
	if root := strings.TrimSpace(os.Getenv("LAB3_DATA_ROOT")); root != "" {
		return root
	}
	return "."
}
