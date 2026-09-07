#!/bin/zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
SRC_DIR="$ROOT_DIR/src"
LOG_DIR="$ROOT_DIR/log"
PID_DIR="$LOG_DIR/pids"

mkdir -p "$LOG_DIR" "$PID_DIR"

export PATH="/opt/homebrew/bin:$PATH"

# 本地 PostgreSQL 与 Redis 默认连接信息已在 src/config/config.go 中提供，
# 直接执行本脚本即可；若要连接其它环境，仍可通过 BW_* 环境变量覆盖。

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "缺少命令: $1"
    exit 1
  fi
}

require_cmd go
require_cmd lsof

stop_existing_listeners() {
  local pids
  pids=$( (lsof -tiTCP:9310 -sTCP:LISTEN || true; \
           lsof -tiTCP:9311 -sTCP:LISTEN || true; \
           lsof -tiTCP:9312 -sTCP:LISTEN || true; \
           lsof -tiTCP:9313 -sTCP:LISTEN || true; \
           lsof -tiTCP:9411 -sTCP:LISTEN || true; \
           lsof -tiTCP:9412 -sTCP:LISTEN || true; \
           lsof -tiTCP:9413 -sTCP:LISTEN || true; \
           lsof -tiTCP:9421 -sTCP:LISTEN || true; \
           lsof -tiTCP:9422 -sTCP:LISTEN || true) | sort -u )
  if [[ -n "$pids" ]]; then
    echo "发现旧进程，占用业务或 lifecycle 端口，正在停止:"
    echo "$pids"
    while IFS= read -r pid; do
      [[ -n "$pid" ]] && kill "$pid" 2>/dev/null || true
    done <<< "$pids"
    sleep 1
  fi
}

start_service() {
  local name="$1"
  shift
  local log_file="$LOG_DIR/${name}.log"
  local pid_file="$PID_DIR/${name}.pid"

  echo "启动 $name ..."
  nohup "$@" > "$log_file" 2>&1 &
  local pid=$!
  echo "$pid" > "$pid_file"
  sleep 1

  if kill -0 "$pid" 2>/dev/null; then
    echo "  $name 已启动 (PID $pid)"
  else
    echo "  $name 启动失败，请检查日志: $log_file"
    exit 1
  fi
}

stop_existing_listeners

cd "$SRC_DIR"

# 先启动节点使其完成 Redis 注册；Coordinator 随后依据这些候选能力创建拓扑；
# Gateway 最后启动并消费已提交的拓扑进行数据面路由。
start_service node-a go run ./cmd/node -id node-a -addr 127.0.0.1:9311 -maps green -replicas cave -lifecycle-addr 127.0.0.1:9411
start_service node-b go run ./cmd/node -id node-b -addr 127.0.0.1:9312 -maps cave,ruins -lifecycle-addr 127.0.0.1:9412
start_service node-c go run ./cmd/node -id node-c -addr 127.0.0.1:9313 -replicas green,ruins -lifecycle-addr 127.0.0.1:9413
start_service coordinator go run ./cmd/coordinator -lifecycle-addr 127.0.0.1:9421
start_service gateway go run ./cmd/server -lifecycle-addr 127.0.0.1:9422

echo
echo "全部启动完成。日志目录: $LOG_DIR"
echo "  gateway     -> $LOG_DIR/gateway.log"
echo "  coordinator -> $LOG_DIR/coordinator.log"
echo "  node-a      -> $LOG_DIR/node-a.log"
echo "  node-b      -> $LOG_DIR/node-b.log"
echo "  node-c      -> $LOG_DIR/node-c.log"
echo
echo "业务监听端口："
lsof -nP -iTCP -sTCP:LISTEN | egrep ':(9310|9311|9312|9313)\\b' || true
echo "lifecycle 端点："
echo "  node-a:      http://127.0.0.1:9411/{healthz,readyz,drain}"
echo "  node-b:      http://127.0.0.1:9412/{healthz,readyz,drain}"
echo "  node-c:      http://127.0.0.1:9413/{healthz,readyz,drain}"
echo "  coordinator: http://127.0.0.1:9421/{healthz,readyz,drain}"
echo "  gateway:     http://127.0.0.1:9422/{healthz,readyz,drain}"
