#!/bin/bash

# 获取传入的参数，如果不传则使用默认值 (默认 100并发，30秒)
USERS=${1:-100}
DURATION=${2:-30}

# 确保在项目的根目录下执行 (方便 go run 找到 go.mod)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"
cd "$SCRIPT_DIR/../.." || exit 1

# cleanup_ports() {
#     echo "清理相关端口 (9310-9313)..."
#     # 查找占用 9310 到 9313 端口的进程并杀掉
#     lsof -i :9310-9313 -t | xargs -r kill -9
# }

# # 压测前先清理一下可能残留的端口占用
# cleanup_ports

echo "==================================="
echo "编译游戏服务端..."
go build -o cmd/benchmark/server_bin cmd/server/main.go

echo "启动游戏服务端..."
./cmd/benchmark/server_bin > cmd/benchmark/server.log 2>&1 &
SERVER_PID=$!

echo "等待服务端初始化 (3秒)..."
sleep 3

echo "==================================="
echo "开始执行压测..."
echo "并发用户数: $USERS"
echo "测试时长: $DURATION 秒"
go run cmd/benchmark/main.go -u $USERS -t $DURATION
echo "==================================="

echo "压测完成，准备关闭服务端 (PID: $SERVER_PID)..."
# 使用 kill -2 (SIGINT) 原地优雅关闭服务端
kill -2 $SERVER_PID

# 等待服务端进程完全退出
wait $SERVER_PID
echo "服务端已关闭..."

# # 兜底清理残留端口
# cleanup_ports

echo "压测脚本执行完毕。"
