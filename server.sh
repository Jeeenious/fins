#!/usr/bin/env bash
# ============================================================================
# server.sh — 启动 server：普通进程，向 client 发一份配置（HTTP POST 一次即退）。
#   不独占核心、不设 RT、无需 sudo —— 与 client.sh（独占核 + RT）分离。
#
# 用法：
#   ./server.sh                              # 发编译期写死的 DEFAULT_CFG
#   ./server.sh <cfg.json> [rpc_port=18080]  # 指定配置路径 / 端口
#   环境变量 SERVER_BIN 可覆盖 server 二进制路径（默认仓库根 server）
#
# 示例：
#   ./server.sh pipeline/cfg_usr_fork100.json 18080
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVER_BIN="${SERVER_BIN:-$SCRIPT_DIR/server}"

[ -x "$SERVER_BIN" ] || { echo "!! 未找到 $SERVER_BIN，先 cmake --build" >&2; exit 2; }
echo "== 启动 server（不独占核心）：$SERVER_BIN $*"
exec "$SERVER_BIN" "$@"
