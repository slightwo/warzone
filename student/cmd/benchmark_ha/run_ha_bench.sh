#!/usr/bin/env bash
# 移除 set -euo pipefail 中对没有找到进程时的 kill -9 报错
echo "清理环境..."
lsof -ti :9310 -ti :9311 -ti :9312 -ti :9313 | xargs -r kill -9 || true

echo "启动服务端..."
mkdir -p /tmp/bench_ha_data
LAB3_DATA_ROOT=/tmp/bench_ha_data go run ../server > server_ha.log 2>&1 &
SERVER_PID=$!
sleep 3

echo "运行 RTO 混沌工程探针..."
go run main.go

echo "清理服务端..."
kill -9 $SERVER_PID || true
