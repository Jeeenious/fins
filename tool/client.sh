#!/usr/bin/env bash
set -euo pipefail

CGBASE=/sys/fs/cgroup
CG=$CGBASE/fins_exclusive
RT_CONF=/etc/security/limits.d/50-fins-rt.conf
RT_PRIO=95
# 非 worker 线程（主循环/计时/组件）的核。空 = 取 CPUS 的最后一核（约定：调用方多给一个核当控制核，
# 如 CPUS=1-3 + WORKERS=2 → worker 1,2、控制核 3）。控制核必须留在同一个 cpuset 里（否则
# bind_core 会 EINVAL 静默失败），但**不能**去借 core 0——把 core 0 并进 cpuset.cpus 会让
# cpuset.cpus.partition=isolated 失败，worker 核的隔离跟着一起丢。
CONTROL_CPU="${CONTROL_CPU:-}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(dirname "$SCRIPT_DIR")"
CLIENT_BIN="${CLIENT_BIN:-$ROOT/build/bin/client}"

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
    PDIR="${2:-$ROOT/build/lib}"
    if [ -z "$CONTROL_CPU" ]; then          # 取范围里的最后一个核（"1-3" → 3；"0,1-2" → 2）
      IFS=',' read -ra _parts <<< "$CPUS"
      for _p in "${_parts[@]}"; do
        case "$_p" in *-*) CONTROL_CPU="${_p#*-}" ;; *) CONTROL_CPU="$_p" ;; esac
      done
    fi
    # cpuset = 整个范围（worker 核 + 控制核）：worker 由 client 显式绑到 1..WORKERS，控制线程绑
    # $CONTROL_CPU。若调用方没多给核（范围里核数 == WORKERS），client 会告警并回落 core 0，
    # 而 0 不在本 cpuset 里 → bind 失败 → 控制线程落回 worker 核（即旧行为）。
    run_in_partition "$CPUS" "$CLIENT_BIN" "$PORT" "$PDIR" "$WORKERS" "$CONTROL_CPU"
    ;;
esac