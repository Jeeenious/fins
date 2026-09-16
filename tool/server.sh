#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(dirname "$SCRIPT_DIR")"
SERVER_BIN="${SERVER_BIN:-$ROOT/bin/server}"

# 直接透传参数执行 server
[ -x "$SERVER_BIN" ] || { echo "!! 找不到 $SERVER_BIN" >&2; exit 1; }
exec "$SERVER_BIN" "$@"