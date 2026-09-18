import os
import re
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
    cores_range = f"{cpu_offset}-{cpu_offset + m_val - 1}" if m_val > 1 else str(cpu_offset)

    timestamp = datetime.now().strftime("%H%M%S")
    session_name = "fins_eval_" + test_name + "_" + timestamp

    test_result_dir = os.path.join(root, result_base_dir, test_name + "_" + timestamp)
    os.makedirs(test_result_dir, exist_ok=True)

    raw_trace_output = os.path.join(root, "tool", "temp", "trace_raw_" + timestamp)

    print(f"\n>>> [开始测试] {test_name} (自动解析: m={m_val} -> workers={workers}, cores={cores_range})")

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
    cl_proc = subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE,
        stdout=open(cl_log, "w"),
        stderr=subprocess.STDOUT,
        cwd=root,
        start_new_session=True
    )

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

    # 6. 清理 Client
    print(f"  [5/5] 关闭清理 Client 进程 (pgid={cl_proc.pid})...")
    try:
        os.killpg(cl_proc.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass

    try:
        cl_proc.wait(timeout=3)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(cl_proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        cl_proc.wait()

    if use_cgroup:
        cleanup_cmd = ["sudo", "-S", os.path.join(root, "tool", "client.sh"), "-r"]
        subprocess.run(
            cleanup_cmd,
            input=(sudo_password + "\n").encode(),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            cwd=root
        )

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


def run_all(target_cfg_dir, target_result_dir, run_time=10.0, cpu_offset=1):
    """自动按文件名解析 m 的批量调度函数"""
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
    print(f"🚀 开始智能批量评测（自动从文件名匹配 m）")
    print(f"   📂 输入配置目录: {target_cfg_dir}")
    print(f"   📁 结果输出目录: {target_result_dir}")
    print(f"   📊 发现测试用例: {len(cfg_files)} 个")
    print("=" * 60)

    for i, cfg_rel in enumerate(cfg_files):
        print(f"\n进度 [{i + 1}/{len(cfg_files)}]: {cfg_rel}")
        run_once(
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
        time.sleep(1.5)

    print("\n" + "=" * 60)
    print("✅ 所有按 m 自动适配的批量实验已执行完毕！")
    print("=" * 60)


if __name__ == "__main__":
    run_all(LAUNCH_DIR, RESULT_BASE_DIR, run_time=RUN_TIME, cpu_offset=CPU_OFFSET)