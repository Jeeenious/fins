#!/usr/bin/env bash
# ============================================================================
# run.sh — 核心独占一键脚本：RT 授权 + cpuset v2 隔离分区 + 启动 client
#
# 用法：
#   sudo ./run.sh -g                          # 授 RT 优先级（一次性，写 limits.d）
#   sudo ./run.sh <核范围>                     # 独占核 + 启动 client（worker=2 默认）
#   sudo ./run.sh <核范围> <worker数>          # 同上，指定 worker 数
#   sudo ./run.sh <核范围> <命令...>           # 独占核 + 启动任意命令（透传）
#   sudo ./run.sh -a <核范围> <pid>            # 把已运行进程移入独占分区
#   sudo ./run.sh -r                          # 删除独占分区、放回核心
#
# 封装 client 模式选项：
#   -p <port>      RPC 端口（默认 18080）
#   -d <dir>       插件目录（默认 ./plugins）
#   环境变量 CLIENT_BIN 可覆盖 client 二进制路径（默认 sdk/fins/test/cmake-build-debug/client）
#
# 示例：
#   sudo ./run.sh 1-2                 # 独占核 1-2，起 client（2 worker）
#   sudo ./run.sh 1-4 4 -p 19090      # 独占核 1-4，4 worker，端口 19090
#   sudo ./run.sh 1-2 ./client 18080 ./plugins   # 任意命令透传
#
# 注意：
#   - 核范围须覆盖 client worker 要绑的核（start(n) → 核 1..n）。
#   - cpuset 只管用户态任务；内核线程/中断仍可能进入，彻底隔离需 isolcpus（重启）。
# ============================================================================
set -euo pipefail

CGBASE=/sys/fs/cgroup
CG=$CGBASE/fins_exclusive
RT_CONF=/etc/security/limits.d/50-fins-rt.conf
RT_PRIO=95
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLIENT_BIN="${CLIENT_BIN:-$SCRIPT_DIR/sdk/fins/test/cmake-build-debug/client}"

# -g：写 limits.d 授 RT（需 root，幂等覆盖）
grant_rt() {
  [ "$(id -u)" -eq 0 ] || { echo "!! 需要 root：sudo $0 -g" >&2; exit 2; }
  local user="${SUDO_USER:-$USER}"
  [ -n "$user" ] && [ "$user" != "root" ] || {
    echo "!! 无法确定目标用户（请用 sudo 调用）" >&2; exit 2; }
  printf '%s - rtprio %s\n' "$user" "$RT_PRIO" > "$RT_CONF"
  echo "== 已写入 $RT_CONF"
  cat "$RT_CONF"
  cat <<EOF

  验证：重新登录（PAM 会话）后运行 \`ulimit -r\`，应显示 $RT_PRIO。
  之后 \`pthread_setschedparam(SCHED_FIFO, prio≤$RT_PRIO)\` 不再报 Operation not permitted。
EOF
}

create_partition() {
  local cpus="$1"
  if [ -d "$CG" ]; then
    echo "== 清理上次残留分区"
    rmdir "$CG" 2>/dev/null || { echo "!! 分区内仍有进程，先 sudo $0 -r" >&2; exit 1; }
  fi
  mkdir -p "$CG"
  # v2 root 只暴露 cpuset.mems.effective（单 NUMA=0），失败兜底 0
  echo "$(cat "$CGBASE/cpuset.mems.effective" 2>/dev/null || echo 0)" > "$CG/cpuset.mems"
  echo "$cpus" > "$CG/cpuset.cpus"
  if [ -w "$CG/cpuset.cpus.partition" ]; then
    echo isolated > "$CG/cpuset.cpus.partition"
    [ "$(cat "$CG/cpuset.cpus.partition")" = "isolated" ] || {
      echo "!! 分区隔离失败（核与其他分区重叠？）" >&2; exit 1; }
  else
    echo 1 > "$CG/cpuset.cpus.exclusive"    # 内核 <6.7 兜底
  fi
  echo "== 独占分区已建：核心 $cpus（其他进程不可再用）"
}

# 建分区 + 移入本脚本（sudo 的 root）→ 降权回调用用户 exec 命令
run_in_partition() {
  local cpus="$1"; shift
  create_partition "$cpus"
  echo $$ > "$CG/cgroup.procs"
  if [ -n "${SUDO_USER:-}" ] && command -v setpriv >/dev/null; then
    # setpriv 降权不重放 PAM limits：rtprio 会继承 sudo root 的默认 0 → 先以 root 抬软硬限制，
    # 子进程继承、setuid 后保留，jenny 才能设 SCHED_FIFO prio≤RT_PRIO（否则 Operation not permitted）。
    prlimit --pid=$$ --rtprio="$RT_PRIO" 2>/dev/null \
      || { ulimit -Hr "$RT_PRIO" 2>/dev/null || true; ulimit -r "$RT_PRIO" 2>/dev/null || true; }
    exec setpriv --reuid="$SUDO_USER" --regid="$SUDO_USER" --init-groups "$@"
  fi
  exec "$@"
}

# 默认模式：argv[2] 为纯数字 → worker 数，封装启动 client
launch_client() {
  local cpus="$1"; shift
  local workers=2 port=18080 pdir=./plugins
  if [ $# -gt 0 ] && [[ "$1" =~ ^[0-9]+$ ]]; then workers="$1"; shift; fi
  while [ $# -gt 0 ]; do
    case "$1" in
      -p) [ $# -ge 2 ] || { echo "!! -p 缺端口" >&2; exit 2; }; port="$2"; shift 2 ;;
      -d) [ $# -ge 2 ] || { echo "!! -d 缺目录" >&2; exit 2; }; pdir="$2"; shift 2 ;;
      *) echo "!! 未知选项 $1" >&2; exit 2 ;;
    esac
  done
  run_in_partition "$cpus" "$CLIENT_BIN" "$port" "$pdir" "$workers"
}

usage() {
  awk '/^# 用法：/{f=1; next} /^# 注意：/{f=0} f{print}' "$0" | sed 's/^# \{0,1\}//'
}

case "${1:-}" in
  -h|--help) usage ;;
  -g|--grant) grant_rt ;;
  -r|--remove)
    [ -d "$CG" ] || { echo "分区不存在"; exit 0; }
    rmdir "$CG" 2>/dev/null || {
      echo "!! 分区内还有进程：$(cat "$CG/cgroup.procs" 2>/dev/null | tr '\n' ' ')" >&2
      echo "   sudo kill -9 <上述pid> 后重试 -r" >&2; exit 1; }
    echo "== 已删除独占分区，核心已放回"
    ;;
  -a|--attach)
    [ $# -ge 3 ] || { echo "用法: sudo $0 -a <核> <pid>" >&2; exit 2; }
    create_partition "$2"
    echo "$3" > "$CG/cgroup.procs"
    echo "== 已把进程 $3 移入独占分区（核心 $2）"
    ;;
  *)
    case "$#" in
      0) usage; exit 2 ;;
      1) launch_client "$1" ;;        # 只有核范围 → 默认 worker=2 启动 client
      *) if [[ "$2" =~ ^[0-9]+$ ]]; then
           launch_client "$@"         # 核范围 + worker 数 → 指定 worker 启动 client
         else
           run_in_partition "$@"      # 核范围 + 非数字 → 任意命令透传
         fi ;;
    esac
    ;;
esac
