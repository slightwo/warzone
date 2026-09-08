#!/bin/zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="$ROOT_DIR/log"
PID_DIR="$LOG_DIR/pids"
PORTS=(9310 9311 9312 9313 9411 9412 9413 9421 9422)

# stop.sh 用于本机联调环境的彻底清理。Node 收到 TERM 时会尝试 drain；当多个节点
# 同时停止而无可用副本时，它会按保护策略拒绝退出。因此本脚本在宽限期后强制回收
# 固定测试端口，避免 go run 派生的实际监听进程遗留。
stop_pid_file() {
  local name="$1"
  local pid_file="$PID_DIR/${name}.pid"
  if [[ ! -f "$pid_file" ]]; then
    return
  fi

  local pid
  pid="$(<"$pid_file")"
  if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
    echo "向 $name (PID $pid) 发送 TERM"
    kill -TERM "$pid" 2>/dev/null || true
  fi
  rm -f "$pid_file"
}

listener_pids() {
  local port
  for port in "${PORTS[@]}"; do
    lsof -tiTCP:"$port" -sTCP:LISTEN 2>/dev/null || true
  done | sort -u
}

terminate_listeners() {
  local signal="$1"
  local pid
  for pid in $(listener_pids); do
    echo "向监听进程 PID $pid 发送 $signal"
    kill -"$signal" "$pid" 2>/dev/null || true
  done
}

stop_pid_file gateway
stop_pid_file coordinator
stop_pid_file node-a
stop_pid_file node-b
stop_pid_file node-c

# PID 文件记录的是 go run 进程；实际监听者可能是它派生的子进程，因此再按端口发送 TERM。
terminate_listeners TERM

echo "等待最多 5 秒，让服务完成正常关闭..."
for _ in {1..10}; do
  if [[ -z "$(listener_pids)" ]]; then
    break
  fi
  sleep 0.5
done

# 本地测试需要保证下次 ./start.sh 不会被旧端口阻塞。若 graceful drain 因没有副本而
# 未完成，则只对本项目的固定端口监听者执行最终 KILL。
if [[ -n "$(listener_pids)" ]]; then
  echo "宽限期结束，强制回收遗留监听端口..."
  terminate_listeners KILL
  sleep 0.5
fi

echo "剩余监听："
lsof -nP -iTCP -sTCP:LISTEN | egrep ':(9310|9311|9312|9313|9411|9412|9413|9421|9422)\b' || true

echo "已完成本地服务停止与端口回收。"
