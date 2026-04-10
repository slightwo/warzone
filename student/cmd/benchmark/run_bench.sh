#!/usr/bin/env bash
set -euo pipefail

# 默认参数
CLIENTS=500
DURATION=60
CHAOS="" # 可为空，如果需要传入就是 -chaos

while [[ $# -gt 0 ]]; do
  case $1 in
    -c|--clients)
      CLIENTS="$2"
      shift 2
      ;;
    -d|--duration)
      DURATION="$2"
      shift 2
      ;;
    --chaos)
      CHAOS="-chaos"
      shift
      ;;
    *)
      shift
      ;;
  esac
done
#  echo "🧹 清理旧环境环境..."
#  lsof -ti :9310 -ti :9311 -ti :9312 -ti :9313 | xargs -r kill -9

echo "🟢 启动服务端..."
mkdir -p /tmp/bench_data
LAB3_DATA_ROOT=/tmp/bench_data   go run ../server > server_bench.log 2>&1 &
SERVER_PID=$!
sleep 3

echo "🔵 启动压测挂载... 参数: -clients $CLIENTS -duration $DURATION $CHAOS"
go run main.go -clients=$CLIENTS -duration=$DURATION $CHAOS

echo "🔴 清理服务端..."
kill -2 $SERVER_PID 2>/dev/null || true

 echo "🧹 清理旧环境环境..."
 lsof -ti :9310 -ti :9311 -ti :9312 -ti :9313 | xargs -r kill -9
