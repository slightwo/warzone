package main

import (
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"battleworld/coordinator"
	"battleworld/storage"
)

func main() {
	dataRoot := resolveDataRoot()
	store, err := storage.NewStore(dataRoot)
	if err != nil {
		fmt.Fprintf(os.Stderr, "初始化协调器存储失败：%v\n", err)
		os.Exit(1)
	}

	control, err := coordinator.New(store)
	if err != nil {
		fmt.Fprintf(os.Stderr, "初始化 coordinator 失败：%v\n", err)
		os.Exit(1)
	}
	if err := control.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "启动 coordinator 失败：%v\n", err)
		os.Exit(1)
	}
	defer control.Close()

	fmt.Println("coordinator 已启动：负责节点发现、健康检查、拓扑初始化和 Boss 生命周期")
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	fmt.Println("coordinator 正在关闭")
}

func resolveDataRoot() string {
	if root := strings.TrimSpace(os.Getenv("LAB3_DATA_ROOT")); root != "" {
		return root
	}
	return "."
}
