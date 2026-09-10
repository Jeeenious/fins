#!/usr/bin/env bash
# ============================================================================
# server.sh — 与 client.sh 对称：RT 授权 + cpuset v2 隔离分区 + 启动 bin/server
#
# client.sh 起 worker（bin/client）常驻跑图；server.sh 把一份 pipeline JSON
# POST 给它的 /update。两者共用同一个独占分区（fins_exclusive），所以分区相关
# 的 -g / -r / -a 行为完全一致。
#
# 用法：
#   sudo tool/server.sh -g                           # 授 RT 优先级（同 client.sh，一次性）
#   sudo tool/server.sh <核范围> <cfg.json> [port]    # 在独占分区内发配置
#   tool/server.sh <cfg.json> [port]                 # 不建分区直接发（无需 root）
#   tool/server.sh -p <port> <cfg.json>              # 端口也可用 -p 给
#   sudo tool/server.sh -a <核范围> <pid>            # 把已运行进程移入独占分区
#   sudo tool/server.sh -r                           # 删除独占分区、放回核心
#
# 环境变量 SERVER_BIN 可覆盖 server 二进制路径（默认仓库根 bin/server）。
#
# 示例：
#   sudo tool/client.sh 1-4 4                        # 另开终端先起 worker（4 worker，核 1-4）
#   tool/server.sh pipeline/feedback_u70_m3_ms05_s20632672.json
#   sudo tool/server.sh 1-4 pipeline/cfg.json 18080  # 让 server 也在独占核上发
#
# 注意：
#   - 不传核范围时不碰 cgroup，普通用户即可运行；
#   - 传核范围需要 root；分区与 client.sh 共用，切勿在 client 运行期间执行 -r。
# ============================================================================
set -euo pipefail

CGBASE=/sys/fs/cgroup
CG=$CGBASE/fins_exclusive
RT_CONF=/etc/security/limits.d/50-fins-rt.conf
RT_PRIO=95
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"   # repo/tool
ROOT="$(dirname "$SCRIPT_DIR")"           # 仓库根
SERVER_BIN="${SERVER_BIN:-$ROOT/bin/server}"   # server 生成在 bin/

# 核范围形如 1 / 1-4 / 1,3-5
is_cpu_range() { [[ "$1" =~ ^[0-9]+([,-][0-9]+)*$ ]]; }

# -g：写 limits.d 授 RT（需 root，幂等覆盖）
grant_rt() {
  [ "$(id -u)" -eq 0 ] || { echo "!! 需要 root：sudo tool/server.sh -g" >&2; exit 2; }
  local user="${SUDO_USER:-$USER}"
  [ -n "$user" ] && [ "$user" != "root" ] || {
    echo "!! 无法确定目标用户（请用 sudo 调用）" >&2; exit 2; }
  printf '%s - rtprio %s\n' "$user" "$RT_PRIO" > "$RT_CONF"
  echo "== 已写入 $RT_CONF"
  cat "$RT_CONF"
  cat <<EOF

  验证：重新登录（PAM 会话）后运行 \`ulimit -r\`，应显示 $RT_PRIO。
EOF
}

create_partition() {
  local cpus="$1"
  if [ -d "$CG" ]; then
    echo "== 复用已有独占分区：核心 $(cat "$CG/cpuset.cpus" 2>/dev/null)"
    return 0
  fi
  # cgroup v2：子 cgroup 只会出现父级 subtree_control 已启用的控制器文件。
  # root 默认只挂了 memory pids，不先启用 cpuset 的话 fins_exclusive 里根本没有
  # cpuset.cpus / cpuset.mems，写入会报"权限不够"。已启用则跳过（幂等）。
  if ! grep -qw cpuset "$CGBASE/cgroup.subtree_control" 2>/dev/null; then
    echo "+cpuset" > "$CGBASE/cgroup.subtree_control" || {
      echo "!! 无法启用 cpuset 控制器：$CGBASE/cgroup.subtree_control" >&2; exit 1; }
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
  [ "$(id -u)" -eq 0 ] || { echo "!! 指定核范围需要 root：sudo tool/server.sh $cpus ..." >&2; exit 2; }
  create_partition "$cpus"
  echo $$ > "$CG/cgroup.procs"
  if [ -n "${SUDO_USER:-}" ] && command -v setpriv >/dev/null; then
    # setpriv 降权不重放 PAM limits：rtprio 会继承 sudo root 的默认 0 → 先以 root 抬软硬限制，
    # 子进程继承、setuid 后保留（与 client.sh 同一处理）。
    prlimit --pid=$$ --rtprio="$RT_PRIO" 2>/dev/null \
      || { ulimit -Hr "$RT_PRIO" 2>/dev/null || true; ulimit -r "$RT_PRIO" 2>/dev/null || true; }
    exec setpriv --reuid="$SUDO_USER" --regid="$SUDO_USER" --init-groups "$@"
  fi
  exec "$@"
}

usage() {
  awk '/^# 用法：/{f=1; next} /^# 注意：/{f=0} f{print}' "$0" | sed 's/^# \{0,1\}//'
}

case "${1:-}" in
  -h|--help) usage; exit 0 ;;
  -g|--grant) grant_rt; exit 0 ;;
  -r|--remove)
    [ -d "$CG" ] || { echo "分区不存在"; exit 0; }
    rmdir "$CG" 2>/dev/null || {
      echo "!! 分区内还有进程：$(cat "$CG/cgroup.procs" 2>/dev/null | tr '\n' ' ')" >&2
      echo "   sudo kill -9 <上述pid> 后重试 -r" >&2; exit 1; }
    echo "== 已删除独占分区，核心已放回"
    exit 0 ;;
  -a|--attach)
    [ $# -ge 3 ] || { echo "用法: sudo $0 -a <核> <pid>" >&2; exit 2; }
    create_partition "$2"
    echo "$3" > "$CG/cgroup.procs"
    echo "== 已把进程 $3 移入独占分区（核心 $2）"
    exit 0 ;;
esac

# 可选核范围 + 可选 -p 端口；其余参数原样传给 bin/server（cfg.json [port]）
CPUS=""
ARGS=()
while [ $# -gt 0 ]; do
  case "$1" in
    -p) [ $# -ge 2 ] || { echo "!! -p 缺端口" >&2; exit 2; }; ARGS+=("$2"); shift 2 ;;
    -*) echo "!! 未知选项 $1" >&2; exit 2 ;;
    *) if [ -z "$CPUS" ] && is_cpu_range "$1"; then CPUS="$1"; shift; else break; fi ;;
  esac
done
ARGS+=("$@")

if [ -n "$CPUS" ]; then
  run_in_partition "$CPUS" "$SERVER_BIN" ${ARGS[@]+"${ARGS[@]}"}
else
  [ -x "$SERVER_BIN" ] || { echo "!! 找不到 $SERVER_BIN（先编译，或设 SERVER_BIN）" >&2; exit 1; }
  exec "$SERVER_BIN" ${ARGS[@]+"${ARGS[@]}"}
fi
