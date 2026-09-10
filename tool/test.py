#!/usr/bin/env python
# coding: utf-8

# # test — 自动化数据采集（pipeline 测试）
# 
# 配套 `tool/uload.ipynb` 生成的 `{kind}_u.._m.._ms<桶号>_s<seed>.json`：
# 对每份 cfg，以 workers=m 起 `bin/client` + `bin/server` 灌 cfg，跑 `dur_s` 秒 → 终止 →
# 把 client 导出的 trace（`tool/temp/tracing.csv`）**复制**到结果目录（原件保留在 tool/temp，不删）。**只搬运原始 trace，不算指标**；
# **默认每份测一轮(trials=1)**。
# 
# ## 结果存放（镜像输入目录）
# - 输入目录 = `cfg_dir`（默认 `tool/uload/`，可递归含子目录）；
# - 结果根 = `test_dir`（默认 `tool/test/`），镜像 `cfg_dir` 的子目录结构；
# - 每份 cfg → 同目录 `trace_<cfg名去cfg_前缀>.csv`；`trials>1` 加 `_r<rep>`。
# 
# ## tool/temp（run 导出中间目录）
# - client(run) 把 trace 写 `./tool/temp/tracing.csv`（宏 `FINS_EXPORT_TRACING_PATH`，相对启动 cwd=仓库根；
#   `dag.json` 同目录）；client 启动时会自建该目录；
# - notebook 用 `temp_dir` 指向它（默认 `tool/temp`，相对仓库根或绝对）；跑前记下基线，
#   跑后只把**本轮新写入**的 trace **复制**到结果目录归档——tool/temp 原件绝不删除。

# In[14]:


# ==== 库体：test() —— 跑 cfg → 把 tool/temp/tracing.csv 复制到 out（镜像子目录，与 cfg 同名）====
import os, re, time, shutil, signal, subprocess


CFG_RE = re.compile(r"(?P<kind>\w+)_u(?P<u>\d+)_m(?P<m>\d+)_ms(?P<ms>\d+)_s(?P<seed>\d+)\.json$")
GRACE = 20    # SIGTERM 后等 client 正常退出的秒数；超时才 SIGKILL(视为非正常退出)


def repo_root():
    d = os.path.abspath(os.getcwd())
    while True:
        if os.path.isfile(os.path.join(d, "bin", "client")) and os.path.isdir(os.path.join(d, "tool")):
            return d
        p = os.path.dirname(d)
        if p == d: return None
        d = p


def find_cfgs(directory):
    out = []
    for rt, _, fns in os.walk(directory):
        for fn in sorted(fns):
            mt = CFG_RE.match(fn)
            if mt:
                out.append((os.path.relpath(os.path.join(rt, fn), directory),
                            int(mt["u"]) / 100.0, int(mt["m"]),
                            mt["kind"], int(mt["seed"])))
    return out


def run_and_cp(cfg_path, workers, dur_s, warm_s, port, dest, root, cores=None, temp_dir="tool/temp", stem=""):
    """升级：client 日志落文件 + 推 cfg 带重试(等到连通/超时) + 失败打印 client 日志尾部。"""
    export_dir = temp_dir if os.path.isabs(temp_dir) else os.path.join(root, temp_dir)
    os.makedirs(export_dir, exist_ok=True)
    cl_log = os.path.join(export_dir, f"client_{stem or os.path.basename(cfg_path)}.log")
    cands = [os.path.join(export_dir, "tracing.csv"),
             os.path.join(export_dir, "ros_tracing.csv"),
             os.path.join(root, "tracing.csv"),
             os.path.join(root, "ros_tracing.csv")]         # client 宏现导出 ./tool/temp/tracing.csv
    def _sig(p):
        try: st = os.stat(p); return (st.st_size, st.st_mtime_ns)
        except FileNotFoundError: return None
    snap = {c: _sig(c) for c in cands}
    if os.path.exists(cl_log): os.remove(cl_log)
    if cores:
        cmd = ["sudo", os.path.join(root, "tool", "client.sh"), str(cores), str(workers)]
    else:
        cmd = [os.path.join(root, "bin", "client"), str(port), os.path.join(root, "lib"), str(workers)]
    cl = subprocess.Popen(cmd, stdout=open(cl_log, "w"), stderr=subprocess.STDOUT,
                          cwd=root, start_new_session=True)
    intr = None
    push_err = "no server run"
    try:
        # ① 等 client RPC 就绪 + 推 cfg：轮询重试（覆盖慢启动/端口短暂占用），上限 warm_s+8s
        deadline = time.time() + max(warm_s, 1.0) + 8.0
        srv_out = ""
        while time.time() < deadline:
            srv = subprocess.run([os.path.join(root, "bin", "server"), cfg_path, str(port)],
                                 stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, cwd=root)
            srv_out = srv.stdout or ""
            if srv.returncode == 0:
                time.sleep(dur_s)          # 推成功才计时跑
                break
            time.sleep(0.25)
        else:
            push_err = srv_out.strip()
            print(f"      [run] 推 cfg 持续失败: {push_err}")
            print("      [run] client 日志尾部 >>>")
            try:
                tail = open(cl_log).read().splitlines()
                print("\n".join(tail[-25:] if len(tail) > 25 else tail))
            except OSError as e:
                print(f"      (读不到 client 日志 {cl_log}: {e})")
    except BaseException as ex:
        intr = ex
    killed = False
    try: os.killpg(cl.pid, signal.SIGTERM)
    except ProcessLookupError: pass
    try: rc = cl.wait(timeout=GRACE)
    except subprocess.TimeoutExpired:
        killed = True
        try: os.killpg(cl.pid, signal.SIGKILL)
        except ProcessLookupError: pass
        rc = cl.wait()
    if cores:
        subprocess.run(["sudo", os.path.join(root, "tool", "client.sh"), "-r"],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, cwd=root)
    if intr is not None: raise intr
    if killed or rc != 0:
        print(f"      [run] client 非正常退出 rc={rc} killed={killed} → 日志尾部:")
        try: print("\n".join(open(cl_log).read().splitlines()[-25:]))
        except OSError: pass
        return False, rc, killed
    for _ in range(30):
        for src in cands:
            cur = _sig(src)
            if cur is not None and cur != snap.get(src):
                try:
                    n_data = sum(1 for ln in open(src) if ln.strip() and not ln.startswith("tid,"))
                except OSError:
                    time.sleep(0.1); continue
                if n_data < 1:
                    return False, rc, killed
                try:
                    shutil.copy2(src, dest); return True, rc, killed
                except (FileNotFoundError, PermissionError, OSError):
                    time.sleep(0.1)
        time.sleep(0.1)
    return False, rc, killed


def test(directory, out, temp_dir="tool/temp", dur_s=5.0, warm_s=1.0, trials=1, only=None, u=None, m=None, port=18080, cores=None):
    """主入口：对 directory 里每份 cfg 跑一轮(dur_s 可配)，把 temp_dir 里的 tracing.csv 复制到(原件保留)
    out/<镜像子目录>/*.csv（trials>1 加 _r<rep>）。cores=None 裸 client；
    cores="1-6" 走 sudo tool/run.sh 独占核。返回成功搬了几份。"""
    root = repo_root()
    if root is None: raise RuntimeError("找不到仓库根(含 client 与 tool/)")
    directory = directory if os.path.isabs(directory) else os.path.join(root, directory)
    out = out if os.path.isabs(out) else os.path.join(root, out)
    cfgs = find_cfgs(directory)
    if only: cfgs = [c for c in cfgs if c[3] in only]
    if u is not None: cfgs = [c for c in cfgs if abs(c[1] - u) < 1e-9]
    if m is not None: cfgs = [c for c in cfgs if c[2] == m]
    if not cfgs:
        print(f"[test] {directory} 下没找到 *.json（先到 tool/uload.ipynb 生成）")
        return 0
    moved = 0
    for rel, uu, mm, kind, seed in cfgs:
        base = os.path.splitext(os.path.basename(rel))[0]
        rdir = os.path.dirname(rel)
        d = out if rdir == "." else os.path.join(out, rdir)
        os.makedirs(d, exist_ok=True)
        stem = base[4:] if base.startswith("cfg_") else base   # 去掉 cfg_ 前缀
        for rep in range(1, trials + 1):
            suffix = "" if trials == 1 else f"_r{rep}"
            dest = os.path.join(d, stem + suffix + ".csv")   # xxx.csv
            # ★ 断点续跑(B)：目标已归档且有数据行 → skip(重跑自动跳过已完成；配合 Ctrl-C 即“可续跑”)
            try:
                _has = any(ln.strip() and not ln.startswith("tid,") for ln in open(dest))
            except OSError:
                _has = False
            if _has:
                print(f"[test] {kind:9s} u={uu} m={mm} seed={seed} r{rep} -> skip (已归档)")
                continue
            ok, rc, killed = run_and_cp(os.path.join(directory, rel), mm, dur_s, warm_s, port, dest, root,
                                        cores=cores, temp_dir=temp_dir)
            if ok:       why = f"正常退出 rc={rc}"
            elif killed: why = f"超时被SIGKILL rc={rc}"
            elif rc == 0: why = "正常退出但无 trace(见上方 [run] 提示 / cfg 未灌入?)"
            elif rc is not None: why = f"退出码≠0 rc={rc}"
            else:        why = "rc=?"
            print(f"[test] {kind:9s} u={uu} m={mm} seed={seed} r{rep} -> "
                  + ("moved " + os.path.relpath(dest) + " (" + why + ")" if ok
                     else "no_ (" + why + ")"))
            moved += int(ok)
    print(f"[test] 完成: 搬了 {moved}/{len(cfgs) * trials} 份 trace → {out}")
    return moved


# ### 用法
# `test(directory, out='tool/test', temp_dir='tool/temp', dur_s=5.0, warm_s=1.0, trials=1, only, u, m, port, cores=None)`：
# - 对每份 cfg：起 client → 预热 `warm_s` → `server` 灌 cfg → 跑 **`dur_s` 秒** → SIGTERM →
#   把 client 在 `temp_dir` 导出的 `ros_tracing.csv` **复制**到 `out` 镜像子目录为 `trace_<cfg名去cfg_前缀>.csv`（原件保留）；
# - **独占核(正式实验)：`cores="1-6"` → 走 `sudo tool/client.sh <cores> <workers>`**（需要 root；jupyter 里要
#   passwordless sudo，否则请在终端 `sudo` 跑驱动脚本）；`cores=None` → 裸 `bin/client`，仅开发自检；
# - `directory/out/temp_dir` 相对路径按仓库根解析；client 启动 cwd=仓库根 → 导出目录固定 `root/<temp_dir>`。


# ── 参数（可配置）────────────────────────────
cfg_dir   = "tool/pipeline/"   # 目标 json(cfg)存放目录
test_dir = "tool/fins/"          # 结果根(镜像 cfg_dir 子目录结构)
temp_dir = "tool/temp/"              # client(client)导出目录(./tool/temp/tracing.csv，相对仓库根)
dur_s     = 10.0                  # ★ 测试时长(秒)：每份 cfg 跑多久
warm_s    = 2                  # 启动 client 后/计时前预热
trials    = 1                    # 每份测几轮(默认 1)


# ── 跑并复制：起 client/server→跑 dur_s→终止→把 tool/temp/tracing.csv 复制到 test 镜像(原件保留)，
#    每个 cfg 生成与它同名的 *.csv（纯搬运，不算指标）────────
if __name__ == "__main__":
    test(cfg_dir, test_dir, temp_dir, dur_s=dur_s, warm_s=warm_s, trials=trials)

