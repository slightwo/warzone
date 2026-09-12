#!/bin/zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
SRC_DIR="$ROOT_DIR/src"
LOG_DIR="$ROOT_DIR/log"
PID_DIR="$LOG_DIR/pids"

# 格式：节点名|业务监听端口|地图 ID|lifecycle 端口。
# 每张地图恰有两个候选节点：字典序较小者初始成为 owner，另一个作为 standby。
typeset -a NODE_SPECS=(
  'node-a|9311|green|9411'
  'node-b|9312|green|9412'
  'node-c|9313|cave|9413'
  'node-d|9314|cave|9414'
  'node-e|9315|ruins|9415'
  'node-f|9316|ruins|9416'
)
CONTROL_PORTS=(9421 9422)

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

node_field() {
  local spec="$1"
  local field="$2"
  local -a fields
  fields=("${(@s:|:)spec}")
  print -r -- "${fields[$field]}"
}

listener_pids() {
  local spec port lifecycle_port control_port
  for spec in "${NODE_SPECS[@]}"; do
    port="$(node_field "$spec" 2)"
    lifecycle_port="$(node_field "$spec" 4)"
    lsof -tiTCP:"$port" -sTCP:LISTEN 2>/dev/null || true
    lsof -tiTCP:"$lifecycle_port" -sTCP:LISTEN 2>/dev/null || true
  done
  for control_port in "${CONTROL_PORTS[@]}"; do
    lsof -tiTCP:"$control_port" -sTCP:LISTEN 2>/dev/null || true
  done
}

stop_existing_listeners() {
  local pids pid
  pids="$(listener_pids | sort -u)"
  if [[ -z "$pids" ]]; then
    return
  fi

  echo "发现旧进程，占用业务或 lifecycle 端口，正在停止:"
  echo "$pids"
  while IFS= read -r pid; do
    [[ -n "$pid" ]] && kill "$pid" 2>/dev/null || true
  done <<< "$pids"
  sleep 1
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

start_nodes() {
  local spec node_id port map_id lifecycle_port
  for spec in "${NODE_SPECS[@]}"; do
    node_id="$(node_field "$spec" 1)"
    port="$(node_field "$spec" 2)"
    map_id="$(node_field "$spec" 3)"
    lifecycle_port="$(node_field "$spec" 4)"
    start_service "$node_id" go run ./cmd/node \
      -id "$node_id" \
      -addr "127.0.0.1:$port" \
      -map "$map_id" \
      -lifecycle-addr "127.0.0.1:$lifecycle_port"
  done
}

stop_existing_listeners

cd "$SRC_DIR"

# 先启动 6 个节点，使三张地图各完成 2 个 Redis 注册；Coordinator 随后依据候选能力
# 创建拓扑，Gateway 最后消费已提交拓扑并建立数据面路由。
start_nodes
start_service coordinator go run ./cmd/coordinator -lifecycle-addr 127.0.0.1:9421
start_service gateway go run ./cmd/server -lifecycle-addr 127.0.0.1:9422

echo
echo "全部启动完成。日志目录: $LOG_DIR"
echo "  gateway     -> $LOG_DIR/gateway.log"
echo "  coordinator -> $LOG_DIR/coordinator.log"
echo "  node-a..node-f -> $LOG_DIR/node-*.log"
echo
echo "节点注册："
echo "  green: node-a (9311), node-b (9312)"
echo "  cave:  node-c (9313), node-d (9314)"
echo "  ruins: node-e (9315), node-f (9316)"
echo "业务监听端口："
lsof -nP -iTCP -sTCP:LISTEN | egrep ':(9311|9312|9313|9314|9315|9316)\\b' || true
echo "lifecycle 端点："
echo "  node-a..node-f: http://127.0.0.1:9411..9416/{healthz,readyz,drain}"
echo "  coordinator: http://127.0.0.1:9421/{healthz,readyz,drain}"
echo "  gateway:     http://127.0.0.1:9422/{healthz,readyz,drain}"
