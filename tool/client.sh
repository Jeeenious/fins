#!/usr/bin/env bash
set -euo pipefail

CGBASE=/sys/fs/cgroup
CG=$CGBASE/fins_exclusive
RT_CONF=/etc/security/limits.d/50-fins-rt.conf
RT_PRIO=95
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(dirname "$SCRIPT_DIR")"
CLIENT_BIN="${CLIENT_BIN:-$ROOT/bin/client}"

create_partition() {
  local cpus="$1"
  if [ -d "$CG" ]; then
    # 若存在残留直接释放进程并复用
    cat "$CG/cgroup.procs" 2>/dev/null | xargs -r kill -9 2>/dev/null || true
    rmdir "$CG" 2>/dev/null || true
  fi
  if ! grep -qw cpuset "$CGBASE/cgroup.subtree_control" 2>/dev/null; then
    echo "+cpuset" > "$CGBASE/cgroup.subtree_control" || true
  fi
  mkdir -p "$CG"
  echo "$(cat "$CGBASE/cpuset.mems.effective" 2>/dev/null || echo 0)" > "$CG/cpuset.mems"
  echo "$cpus" > "$CG/cpuset.cpus"
  if [ -w "$CG/cpuset.cpus.partition" ]; then
    echo isolated > "$CG/cpuset.cpus.partition" 2>/dev/null || true
  else
    echo 1 > "$CG/cpuset.cpus.exclusive" 2>/dev/null || true
  fi
}

run_in_partition() {
  local cpus="$1"; shift
  create_partition "$cpus"
  echo $$ > "$CG/cgroup.procs"
  if [ -n "${SUDO_USER:-}" ] && command -v setpriv >/dev/null; then
    prlimit --pid=$$ --rtprio="$RT_PRIO" 2>/dev/null || true
    exec setpriv --reuid="$SUDO_USER" --regid="$SUDO_USER" --init-groups "$@"
  fi
  exec "$@"
}

case "${1:-}" in
  -r)
    [ -d "$CG" ] || exit 0
    cat "$CG/cgroup.procs" 2>/dev/null | xargs -r kill -9 2>/dev/null || true
    rmdir "$CG" 2>/dev/null || true
    exit 0
    ;;
  *)
    CPUS="$1"; shift
    WORKERS="$1"; shift
    PORT="${1:-18080}"
    PDIR="${2:-$ROOT/lib}"
    run_in_partition "$CPUS" "$CLIENT_BIN" "$PORT" "$PDIR" "$WORKERS"
    ;;
esac