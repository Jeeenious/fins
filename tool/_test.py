import os
import re
import glob
import signal
import subprocess
import time
import shutil
import getpass
from datetime import datetime

# ================= 配置区 =================
LAUNCH_DIR = "./result/CIE_FIFO_IPC_nuc12"  # JSON 配置文件目录
RESULT_BASE_DIR = "./result/CIE_FIFO_IPC_nuc12"  # 评测结果输出目录
LTTNG_WARMUP_TIME = 1.0 # 预热时间 (s)
FINS_WARMUP_TIME = 1.0  # 预热时间 (s)
RUN_TIME = 2.0  # 持续运行时间 (s)
PORT = 18080  # 通信端口

# ===== CPU 隔离与绑定配置 =====
PIN_EXPERIMENT_TO_CPUS = False
CPU_OFFSET = 1  # 起始核号：绕开 0，从 1 开始

# ===== 断点续跑 =====
# True = 已有"完整结果"的同名配置直接跳过（批实验中断后接着跑）；想强制全部重测置 False。
SKIP_DONE = True

# ===== 显式使能的自定义 UST tracepoint =====
CUSTOM_UST_EVENTS = [
    "fins:wake",
    "fins:release",
    "fins:finished",
    "fins:sleep",
    "algo:execute",
    "algo:complete",
    "algo:working"
]

# ===== 监控的 Linux 内核调度 tracepoint =====
KERNEL_EVENTS = [
    "sched_switch",
]

# ==========================================

def repo_root():
    """自动定位仓库根目录"""
    d = os.path.abspath(os.getcwd())
    while True:
        if os.path.isfile(os.path.join(d, "build", "bin", "client")) and os.path.isdir(os.path.join(d, "tool")):
            return d
        p = os.path.dirname(d)
        if p == d: return None
        d = p


def parse_m_from_filename(filename):
    """从 pipeline 文件名中解析出 m 值 (例如 feedback_u70_m3_... -> 3)"""
    m_match = re.search(r'_m(\d+)', filename)
    if m_match:
        return int(m_match.group(1))
    m_match = re.search(r'm(\d+)_', filename)
    if m_match:
        return int(m_match.group(1))
    return 2  # 默认兜底值


# ===== 跑前守卫：残留 client 检查 + 清理 =====
# 客户端主线程被 set_thread_name 改名 → 进程 comm 就是 CLIENT_COMM（不是 "client"）；
# 且 httplib 在 Linux 给监听 socket 设了 SO_REUSEPORT：第二个 client 照样 bind 成功、不报错，
# 推送被内核分流到其中一个 → 两个进程各跑一份图，trace 里 worker tid 数变成 2×m（实测整批中招）。
CLIENT_COMM = "fins_main"
_KERNEL_TRACER_WARNED = False   # 内核域告警只喊一次（批实验里每 run 都喊太吵）


def _fins_main_pids():
    """(活着的, 僵尸) 两批 pid：comm == CLIENT_COMM 的进程。

    僵尸（STAT 含 Z）是**已经退出**的 client：信号对它无效（`kill -9` 也一样）、不占端口、不在
    trace 里，只是父进程没 wait() 回收（实测父进程是 Jupyter kernel —— notebook 里起 client 后
    没调 .wait()/.poll() 就会挂一个僵尸）。必须把它和"活着的残留"分开，否则守卫会一直报
    "清不掉"而拒绝开跑。
    """
    out = subprocess.run(["ps", "-eo", "pid,stat,comm"], capture_output=True, text=True).stdout
    alive, zombie = [], []
    for line in out.splitlines()[1:]:
        parts = line.split(None, 2)
        if len(parts) != 3 or parts[2].strip() != CLIENT_COMM:
            continue
        (zombie if "Z" in parts[1] else alive).append(int(parts[0]))
    return alive, zombie


def _client_pids():
    """活着的残留 client pid（僵尸不算，见 _fins_main_pids）。"""
    return _fins_main_pids()[0]


def _port_pids(port):
    """监听 port 的 pid（解析 ss -ltnp 的 pid=…）；有人占着但拿不到 pid → -1。"""
    res = subprocess.run(["ss", "-ltnp", f"sport = :{port}"], capture_output=True, text=True)
    pids = [int(m) for m in re.findall(r"pid=(\d+)", res.stdout)]
    if not pids and re.search(rf":{port}\b", res.stdout):
        pids = [-1]
    return pids


def ensure_port_free(port, sudo_password=None, grace=3.0):
    """跑前守卫：清掉残留 client 并确认端口空出来；清不掉就抛错中止，别把脏数据跑出来。

    先借 client.sh -r 清 cgroup 成员（cgroup 里的残留连 killpg 都不一定够），再按 pid 兜底
    SIGTERM → SIGKILL，最后复查 ss。返回被清掉的 pid 列表。
    """
    root = repo_root()
    zombie = _fins_main_pids()[1]
    if zombie:
        print(f"  ℹ️ 另有 {len(zombie)} 个 fins_main 僵尸进程（pid={zombie}）：已退出、父进程未 wait() "
              f"回收（常见于 Jupyter kernel）→ 杀不掉也不占端口，不影响实验")
    found = sorted(set(_client_pids()) | set(_port_pids(port)))
    if not found:
        return []

    print(f"  🧹 [守卫] 端口 {port} 上发现残留 client（pid={found}）→ 清理")
    if sudo_password is not None:
        subprocess.run(["sudo", "-S", os.path.join(root, "tool", "client.sh"), "-r"],
                       input=(sudo_password + "\n").encode(),
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, cwd=root)

    for sig, wait in ((signal.SIGTERM, grace), (signal.SIGKILL, 0.5)):
        alive = sorted({p for p in _client_pids() + _port_pids(port) if p > 0})
        if not alive:
            break
        for pid in alive:
            try:
                os.kill(pid, sig)
            except ProcessLookupError:
                pass
        time.sleep(wait)

    left = sorted(set(_client_pids()) | set(_port_pids(port)))
    if left:
        raise RuntimeError(f"❌ [守卫] 清不掉残留 client（pid={left}），端口 {port} 仍被占用。"
                           f"继续跑会把两个进程的数据混进同一份 trace，已中止。")
    print("  ✅ [守卫] 端口已空出、无残留 client")
    return found


# ===== 断点续跑：已完成的配置不重测 =====
# 结果目录名 = <test_name>_<HHMMSS>（每次跑时间戳都不同），所以"跑过没有"不能靠单个路径判断，
# 得按前缀找历史目录并校验它真的跑完了。
def _is_done(result_dir):
    """这次 run 算不算"完整"：client 日志里配置应用成功、正常退出，且 trace 目录非空。

    只认这三个marker，不猜数据质量：
      · `pipeline applied` —— 配置真的建了图（被 parse/topology 拒的 run 会缺它 → 会重测）
      · `bye`              —— client 走完了 teardown（中途崩/kill -9 的 run 不许当完成品）
      · `trace/` 非空      —— LTTng 原始轨迹搬过来了
    """
    log = os.path.join(result_dir, "client.log")
    trace = os.path.join(result_dir, "trace")
    if not (os.path.isfile(log) and os.path.isdir(trace) and os.listdir(trace)):
        return False
    with open(log, errors="ignore") as f:
        text = f.read()
    return "pipeline applied" in text and "bye" in text


def finished_result(result_base_dir, test_name):
    """同名配置已完成的最近一次结果目录；None = 没跑过 / 跑过但没跑完（→ 该重测）。"""
    base = os.path.join(repo_root(), result_base_dir)
    for d in sorted(glob.glob(os.path.join(base, test_name + "_*")), reverse=True):
        if _is_done(d):
            return d
    return None


def check_kernel_tracer(verbose_once=True):
    """跑前检查 lttng **内核域**（`-k`，`sched_switch` → preempt.csv 的来源）是否可用。

    内核域要 lttng 内核模块（按内核版本编译）。内核升级后没重建（本机没装 `lttng-modules-dkms`）
    就会 `Kernel tracer not available`——而 **UST 域（`-u`）不需要内核模块、照旧正常**，所以
    `algo:*`/`fins:*` 事件都在，只有 preempt.csv 空掉；事后从 CSV 才发现很费劲，故跑前就喊。
    """
    global _KERNEL_TRACER_WARNED
    ok = subprocess.run(["lttng", "list", "--kernel"],
                        capture_output=True, text=True).returncode == 0
    if ok or (not verbose_once) or _KERNEL_TRACER_WARNED:
        return ok
    _KERNEL_TRACER_WARNED = True
    rel = os.uname().release
    print("  ⚠️ [内核事件] lttng 内核域不可用 —— 本 run 没有 sched_switch，preempt.csv（甘特图/overhead "
          "的输入）会是空的；UST 事件（algo:*/fins:*）不受影响")
    print(f"     → 常见原因：内核升级后 lttng 内核模块没为 {rel} 重建"
          f"（查：`modinfo lttng-probe-sched`、`ls /lib/modules/{rel}/updates`）")
    print("     → 修：`sudo apt install lttng-modules-dkms`（DKMS 会给当前及以后的内核编）后重跑；"
          "急用可先重启到有模块的那个内核")
    return ok


def _stop_client(cl_proc, grace=3.0):
    """终止并**回收** client 子进程：poll() → SIGTERM(进程组) → 宽限 → SIGKILL → wait()。

    ⚠ 必须走 `poll()/wait()` 回收，光 kill 不收 → 子进程变**僵尸**（任何信号都杀不掉，ps 里一直挂着）。
    裸跑模式下 client 是**当前进程的直接子进程**（从 notebook 里跑 `_test.py` 时父进程就是常驻的
    Jupyter kernel）→ 没人替我们收，僵尸会挂到 kernel 退出为止。这就是"fins_main 杀不掉"的根因。

    @retval True = 用过 SIGKILL（SIGTERM 宽限内没退）
    """
    if cl_proc.poll() is not None:   # 已经退出的：poll() 顺手把僵尸收掉（← 这条最关键）
        return False
    try:
        os.killpg(cl_proc.pid, signal.SIGTERM)
    except (ProcessLookupError, PermissionError):
        pass
    try:
        cl_proc.wait(timeout=grace)
        return False
    except subprocess.TimeoutExpired:
        pass
    try:
        os.killpg(cl_proc.pid, signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        pass
    cl_proc.wait()
    return True


def reap_fins_zombies():
    """回收**本进程名下**的 fins_main 僵尸（`ps` 里 STAT=Z 的那些），返回回收到的 pid。

    僵尸只能由它的父进程 `waitpid` 回收 → 从起 client 的那个 Jupyter kernel 里调有用；换过 kernel
    之后收不了旧 kernel 的子进程（ChildProcessError），那种只能关掉旧 kernel。信号一律无效。
    """
    reaped = []
    for pid in _fins_main_pids()[1]:
        try:
            os.waitpid(pid, os.WNOHANG)
            reaped.append(pid)
        except ChildProcessError:
            pass
    return reaped


def run_once(cfg_rel_path, cfg_dir, result_base_dir, sudo_password, lttng_warm_up = 1.0, fins_warmup_time=2.0, run_time=10.0,
             port=18080, use_cgroup=True, cpu_offset=1):
    """单次测试执行函数：自动根据文件名中的 m 确定 worker 数与独占核范围"""
    root = repo_root()
    if not root:
        raise RuntimeError("❌ 错误: 找不到仓库根目录 (需包含 build/bin/client 与 tool/)")

    cfg_name = os.path.basename(cfg_rel_path)
    test_name = os.path.splitext(cfg_name)[0]

    # 动态解析 m 值
    m_val = parse_m_from_filename(cfg_name)
    workers = m_val
    # 核范围多留一核给非 worker 线程（主循环/计时/组件）：worker 绑 1..m，控制线程绑 m+1。
    # 控制核必须与 worker 同 cpuset（否则 bind 失败），又不能借 core 0（会把 isolated 分区搞掉）。
    cores_range = f"{cpu_offset}-{cpu_offset + m_val}" if m_val >= 1 else str(cpu_offset)

    timestamp = datetime.now().strftime("%H%M%S")
    session_name = "fins_eval_" + test_name + "_" + timestamp

    test_result_dir = os.path.join(root, result_base_dir, test_name + "_" + timestamp)
    os.makedirs(test_result_dir, exist_ok=True)

    raw_trace_output = os.path.join(root, "tool", "temp", "trace_raw_" + timestamp)

    print(f"\n>>> [开始测试] {test_name} (自动解析: m={m_val} -> workers={workers}, cores={cores_range})")

    # 0. 🧹 跑前守卫：残留 client 检查 + 清理（必须在建 LTTng 会话之前——否则旧进程的 tracepoint
    #    也会被追进本次会话，两个进程的图混在同一份 CSV 里，事后无法分离）
    ensure_port_free(port, sudo_password)
    check_kernel_tracer()   # 内核域不可用就喊一声（否则只有事后看空 preempt.csv 才发现）

    # 1. 🛑 【新增/强化】彻底清理可能残留的所有 LTTng 会话，防止多会话并发导致事件双写
    subprocess.run(["lttng", "stop"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    subprocess.run(["lttng", "destroy", "--all"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    # 针对当前同名会话的额外清理（双保险）
    subprocess.run(["lttng", "stop", session_name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    subprocess.run(["lttng", "destroy", session_name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    if os.path.exists(raw_trace_output):
        shutil.rmtree(raw_trace_output)

    # 2. 启动原生 LTTng 追踪
    print(f"  [1/5] 创建并启动 LTTng 追踪会话 ({session_name})...")
    create_res = subprocess.run(["lttng", "create", session_name, "--output=" + raw_trace_output],
                                capture_output=True, text=True)
    if create_res.returncode != 0:
        print(f"  ❌ LTTng 会话创建失败:\n{create_res.stderr}")
        return False

    # 动态使能自定义的 UST 事件 (必须绑定到当前 session)
    for ev in CUSTOM_UST_EVENTS:
        subprocess.run(["lttng", "enable-event", "-u", "-s", session_name, ev],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    # 🔴 关键：为用户态事件添加 vtid 上下文（这样 babeltrace2 和 Python 脚本才能拿到 tid）
    subprocess.run(["lttng", "add-context", "-u", "-s", session_name, "-t", "vtid"],
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    # 动态使能内核事件 (绑定到当前 session)
    for kev in KERNEL_EVENTS:
        subprocess.run(["lttng", "enable-event", "-k", "-s", session_name, kev],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    subprocess.run(["lttng", "start"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    print(f"  [2/5] 等待 LTTng 预热 {lttng_warm_up}s 并推送配置...")
    time.sleep(lttng_warm_up)

    # 3. 启动 Client (使用 sudo -S 通过标准输入传入密码)
    cl_log = os.path.join(test_result_dir, "client.log")
    if use_cgroup:
        client_script = os.path.join(root, "tool", "client.sh")
        cmd = ["sudo", "-S", client_script, cores_range, str(workers)]
        print(f"  [2/5] 启动 Client 进程 (独占核 {cores_range}, workers={workers})...")
    else:
        client_bin = os.path.join(root, "build", "bin", "client")
        lib_dir = os.path.join(root, "build", "lib")
        cmd = [client_bin, str(port), lib_dir, str(workers)]
        print(f"  [2/5] 启动 Client 进程 (裸跑模式, workers={workers})...")

    # 以管道方式启动子进程
    cl_log_f = open(cl_log, "w")
    cl_proc = subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE,
        stdout=cl_log_f,
        stderr=subprocess.STDOUT,
        cwd=root,
        start_new_session=True
    )

    # ★ 从这里到"client 已回收"之间必须 try/finally：中途异常 / Ctrl-C（notebook 里最常见——run_time
    #   是几十秒的 sleep）都会跳过尾部的 wait()，client 就变成"还在跑的残留 + 之后没人收的僵尸"。
    try:
        # 如果使用了 cgroup (sudo)，向其写入密码
        if use_cgroup and cl_proc.stdin:
            try:
                cl_proc.stdin.write((sudo_password + "\n").encode())
                cl_proc.stdin.flush()
            except Exception:
                pass

        # 4. 稳健的固定预热并推送配置
        print(f"  [3/5] 等待客户端预热 {fins_warmup_time}s 并推送配置...")
        time.sleep(fins_warmup_time)

        cfg_full_path = os.path.join(root, cfg_dir, cfg_rel_path)
        server_bin = os.path.join(root, "build", "bin", "server")

        srv_res = subprocess.run([server_bin, cfg_full_path, str(port)],
                                 stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, cwd=root)
        if srv_res.returncode == 0:
            print(f"  [Server] 配置灌入成功，持续运行 {run_time}s...")
        else:
            print(f"  ⚠️ [Server] 灌入配置返回值非 0: {srv_res.stdout.strip()}")

        time.sleep(run_time)

        # 5. 停止 LTTng
        print(f"  [4/5] 停止并销毁 LTTng 追踪会话...")
        subprocess.run(["lttng", "stop", session_name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        subprocess.run(["lttng", "destroy", session_name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    finally:
        # 6. 清理 Client（正常 / 异常 / Ctrl-C 都走这里）：终止 + 回收，杜绝僵尸与残留
        print(f"  [5/5] 关闭清理 Client 进程 (pgid={cl_proc.pid})...")
        if _stop_client(cl_proc):
            print("  ⚠️ [收尾] SIGTERM 宽限内没退出，已 SIGKILL")
        cl_log_f.close()

        if use_cgroup:
            cleanup_cmd = ["sudo", "-S", os.path.join(root, "tool", "client.sh"), "-r"]
            subprocess.run(
                cleanup_cmd,
                input=(sudo_password + "\n").encode(),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                cwd=root
            )

        # 6.5 收尾复查：本次 client 必须真退出了。没退出也不致命（下个 run 的跑前守卫会清），
        #     但必须留痕——残留进程会让下一个 run 的 trace 混进两个进程的数据。
        left = sorted(set(_client_pids()) | set(_port_pids(port)))
        if left:
            print(f"  ⚠️ [收尾] client 未退出（pid={left}）——已交给下个 run 的跑前守卫处理")

    # 7. 搬运 Trace
    final_trace_path = os.path.join(test_result_dir, "trace")
    success = False
    if os.path.exists(raw_trace_output):
        if os.path.exists(final_trace_path):
            shutil.rmtree(final_trace_path)
        sub_dirs = os.listdir(raw_trace_output)
        if len(sub_dirs) == 1:
            shutil.move(os.path.join(raw_trace_output, sub_dirs[0]), final_trace_path)
        else:
            shutil.move(raw_trace_output, final_trace_path)
        if os.path.exists(raw_trace_output):
            shutil.rmtree(raw_trace_output, ignore_errors=True)

        print(f"  [成功] 轨迹数据已归档至: {final_trace_path}")

        # 8. 校验 (同时兼顾自定义事件和内核事件的前缀匹配)
        bt_cmd = f"babeltrace2 '{final_trace_path}'"
        res = subprocess.run(bt_cmd, shell=True, capture_output=True, text=True)
        matched_lines = [line for line in res.stdout.splitlines() if
                         any(ev.split(':')[0] in line for ev in CUSTOM_UST_EVENTS)]

        if len(matched_lines) > 0:
            print(f"  [验证] ✅ 成功捕捉到 {len(matched_lines)} 条目标跟踪事件！")
            success = True
        else:
            print(f"  [验证] ⚠️ 未检测到目标追踪事件。")
    else:
        print(f"  [错误] 未能成功生成原始 LTTng 轨迹。")

    return success


def run_all(target_cfg_dir, target_result_dir, run_time=10.0, cpu_offset=1, skip_done=SKIP_DONE):
    """自动按文件名解析 m 的批量调度函数

    @param skip_done True = 已有完整结果的同名配置跳过（断点续跑），只跑没跑过/没跑完的。
    """
    root = repo_root()
    full_cfg_dir = os.path.join(root, target_cfg_dir)

    if not os.path.exists(full_cfg_dir):
        print(f"❌ 错误: 指定的配置目录不存在: {full_cfg_dir}")
        return

    cfg_files = []
    for rt, _, fns in os.walk(full_cfg_dir):
        for fn in sorted(fns):
            if fn.endswith(".json"):
                cfg_files.append(os.path.relpath(os.path.join(rt, fn), full_cfg_dir))

    if not cfg_files:
        print(f"⚠️ 在指定目录 {full_cfg_dir} 下未找到任何 JSON 配置文件。")
        return

    print("=" * 60)
    print("🔒 该评测脚本需要 root 权限来配置 Cgroup 与绑定核心")
    print("=" * 60)
    sudo_password = getpass.getpass("请输入您的 sudo 密码: ")

    print("=" * 60)
    print("🧹 跑前守卫：检查残留 client / 端口占用 / lttng 内核域...")
    ensure_port_free(PORT, sudo_password)
    check_kernel_tracer()

    print("=" * 60)
    print(f"🚀 开始智能批量评测（自动从文件名匹配 m）")
    print(f"   📂 输入配置目录: {target_cfg_dir}")
    print(f"   📁 结果输出目录: {target_result_dir}")
    print(f"   📊 发现测试用例: {len(cfg_files)} 个")
    print(f"   ⏭️ 断点续跑: {'开（已有的跳过；强制全跑把 SKIP_DONE 置 False）' if skip_done else '关（全部重测）'}")
    print("=" * 60)

    skipped, ran = [], []
    for i, cfg_rel in enumerate(cfg_files):
        print(f"\n进度 [{i + 1}/{len(cfg_files)}]: {cfg_rel}")
        test_name = os.path.splitext(os.path.basename(cfg_rel))[0]
        if skip_done:
            done = finished_result(target_result_dir, test_name)
            if done:
                print(f"  ⏭️ 跳过（已有完整结果: {os.path.relpath(done, root)}）")
                skipped.append(cfg_rel)
                continue

        ran.append(cfg_rel)
        ok = run_once(
            cfg_rel_path=cfg_rel,
            cfg_dir=target_cfg_dir,
            result_base_dir=target_result_dir,
            sudo_password=sudo_password,
            lttng_warm_up=LTTNG_WARMUP_TIME,
            fins_warmup_time=FINS_WARMUP_TIME,
            run_time=run_time,
            use_cgroup=PIN_EXPERIMENT_TO_CPUS,
            cpu_offset=cpu_offset
        )
        if not ok:
            print(f"  ❌ 本次没成功（未捕捉到目标事件）: {cfg_rel} —— 没有完成标记，下次跑会重测")
        time.sleep(1.5)

    if skipped:
        print(f"\n⏭️ 跳过 {len(skipped)} 个（已有完整结果），本次实跑 {len(ran)} 个")

    print("\n" + "=" * 60)
    print("✅ 所有按 m 自动适配的批量实验已执行完毕！")
    print("=" * 60)


if __name__ == "__main__":
    run_all(LAUNCH_DIR, RESULT_BASE_DIR, run_time=RUN_TIME, cpu_offset=CPU_OFFSET)