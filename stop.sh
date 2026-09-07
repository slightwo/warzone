#!/bin/zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="$ROOT_DIR/log"
PID_DIR="$LOG_DIR/pids"

stop_pid_file() {
  local name="$1"
  local pid_file="$PID_DIR/${name}.pid"
  if [[ -f "$pid_file" ]]; then
    local pid
    pid="$(cat "$pid_file")"
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      echo "停止 $name (PID $pid)"
      kill "$pid" 2>/dev/null || true
    fi
    rm -f "$pid_file"
  fi
}

stop_pid_file gateway
stop_pid_file coordinator
stop_pid_file node-a
stop_pid_file node-b
stop_pid_file node-c

sleep 1

# 兜底：按端口清理，避免 PID 文件过期。
for pid in $( (lsof -tiTCP:9310 -sTCP:LISTEN || true; \
               lsof -tiTCP:9311 -sTCP:LISTEN || true; \
               lsof -tiTCP:9312 -sTCP:LISTEN || true; \
               lsof -tiTCP:9313 -sTCP:LISTEN || true; \
               lsof -tiTCP:9411 -sTCP:LISTEN || true; \
               lsof -tiTCP:9412 -sTCP:LISTEN || true; \
               lsof -tiTCP:9413 -sTCP:LISTEN || true; \
               lsof -tiTCP:9421 -sTCP:LISTEN || true; \
               lsof -tiTCP:9422 -sTCP:LISTEN || true) | sort -u ); do
  echo "按端口停止 PID $pid"
  kill "$pid" 2>/dev/null || true
done

sleep 1

echo "剩余监听："
lsof -nP -iTCP -sTCP:LISTEN | egrep ':(9310|9311|9312|9313|9411|9412|9413|9421|9422)\\b' || true

echo "已执行停止。"
