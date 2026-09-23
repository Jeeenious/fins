#!/usr/bin/env python3
# coding: utf-8

"""
FINS 项目专用的 LTTng 导出模块：同时输出 timeline 与 preempt 两个 CSV，
两者共享同一个 t0（= 所有最终导出事件中最早的 ts），保证时间轴对齐。

支持 Notebook 调用：run_export(...)
"""

import argparse
import csv
import os
import re
import bt2

RESULTS_DIR = "./result/FINS_FIFO_IPC_nuc12"
AFTER_US = 5000.0 * 1000

# CPU 过滤：exclude 优先；include 为 None 表示不限制。
# 注：批量入口 run_export **不暴露** include——它按每个实验目录名里的 m 现算范围（见 parse_m/cpus）；
# 这两个常量只作 collect()/process_one() 这类底层入口的默认值（直接调它们时可显式指定固定范围）。
EXCLUDE_CPUS = []
INCLUDE_CPUS = None

# worker comm 正则（ThreadPool::working() 里 pthread_setname_np 设置）
# FILTER_WORKER_ONLY：CLI（--worker-only）默认关——preempt CSV 必须保留完整切换序列，
# 两个绘图模块的区间重建都依赖"相邻行 = 相邻时间区间"；只留 worker 相邻行会让归因整体错位。
FILTER_WORKER_ONLY = False
WORKER_PATTERNS = [r"^fins_worker"]
_WORKER_REGEX = [re.compile(p) for p in WORKER_PATTERNS]


def cpus(cpu_start=1, num_workers=3):
    """worker 绑核范围 → 导出用的 CPU 列表。

    ThreadPool 把 worker i 绑到 core (cpu_start + i)（跳过 core 0），故范围 = [cpu_start, cpu_start+n-1]。
    必须与 _test.py 的 cores_range 一致：那里 cores_range = f"{cpu_offset}-{cpu_offset + m - 1}"，
    m 由配置文件名解析（如 ..._m3_... → 3）。
    @param cpu_start    起始核号（默认 1，绕开 core 0）
    @param num_workers  worker 数（= m）
    @retval list[int]   [cpu_start, cpu_start+1, ..., cpu_start+num_workers-1]
    """
    return list(range(cpu_start, cpu_start + num_workers))


# 实验目录名里的 m（worker 数）：与 _test.py.parse_m_from_filename 同一规则
# （目录名形如 feedback_u70_m3_ms05_s20632672_231518 → m=3）
_M_RE = re.compile(r"_m(\d+)")


def parse_m(name):
    """从实验目录名解析 worker 数 m（解析不出返回 None）。
    @param name 实验目录名（如 feedback_u70_m3_ms05_s20632672_231518）
    @retval int|None m；无 `_m<数字>` 段返回 None
    """
    hit = _M_RE.search(name or "")
    return int(hit.group(1)) if hit else None


def is_worker(comm):
    if not comm:
        return False
    return any(rx.search(str(comm)) for rx in _WORKER_REGEX)


# ---------- 通用字段读取 ----------

def _val(field):
    try:
        return field.value
    except Exception:
        pass
    s = str(field)
    try:
        return int(s)
    except (TypeError, ValueError):
        return s


def _fields(struct):
    return {k: _val(struct.get(k)) for k in struct.keys()} if struct is not None else {}


def _int_or(value, default=0):
    try:
        return int(_val(value))
    except (TypeError, ValueError):
        return default


class EventView:
    """把 bt2 message 拉平成好用的字段。"""

    def __init__(self, ev, msg):
        self.name = ev.name
        self.ts = int(msg.default_clock_snapshot.value)

        ctx = ev.common_context_field
        self.vpid = _int_or(ctx.get("vpid") if ctx is not None else None, 0)
        self.vtid = _int_or(ctx.get("vtid") if ctx is not None else None, 0)

        try:
            pc = ev.packet.context_field
            self.cpu = _int_or(pc.get("cpu_id") if pc is not None else None, -1)
        except Exception:
            self.cpu = -1

        self.payload = _fields(ev.payload_field)


def _iter_events(trace_dir):
    for msg in bt2.TraceCollectionMessageIterator(trace_dir):
        ev = getattr(msg, "event", None)
        if ev is None:
            continue
        yield EventView(ev, msg)


# ---------- 事件分类 ----------

def _classify_timeline(e):
    """返回 (kind, tag) 或 None（非 timeline 事件）。"""
    n = e.name
    if n in ("algo:execute", "lttng_ust_algo:execute"):
        return "execute", e.payload.get("node_id", "")
    if n in ("algo:complete", "lttng_ust_algo:complete"):
        return "complete", e.payload.get("node_id", "")
    if n in ("algo:working", "lttng_ust_algo:working"):
        node_id = e.payload.get("node_id", "")
        core_id = e.payload.get("core_id", -1)
        seg_us = e.payload.get("seg_us", 0)
        return "working", f"{node_id} [CPU {core_id}, {seg_us} us]"
    if n in ("fins:finished", "lttng_ust_fins:finished"):
        return "finished", ""
    if n in ("fins:release", "lttng_ust_fins:release"):
        return "release", ""
    if n in ("fins:sleep", "lttng_ust_fins:sleep"):
        return "sleep", ""
    if n in ("fins:wake", "lttng_ust_fins:wake"):
        return "wake", ""
    return None


# ---------- 单次扫描，收集两类事件 ----------

def collect(trace_dir,
            exclude_cpus=EXCLUDE_CPUS,
            include_cpus=INCLUDE_CPUS,
            want_preempt=True,
            want_timeline=True,
            preempt_all=True):
    """
    返回 (timeline_events, switch_events)。
    每个元素都是 dict，含 ts 等字段；不在这里算 t0。
    """
    exclude_set = set(exclude_cpus) if exclude_cpus else set()
    include_set = set(include_cpus) if include_cpus is not None else None

    timeline_events = []
    switch_events = []

    for e in _iter_events(trace_dir):
        # CPU 过滤
        if e.cpu in exclude_set:
            continue
        if include_set is not None and e.cpu not in include_set:
            continue

        if want_timeline:
            cls = _classify_timeline(e)
            if cls is not None:
                kind, tag = cls
                timeline_events.append({
                    "ts": e.ts, "tid": e.vtid, "cpu": e.cpu,
                    "kind": kind, "tag": tag,
                })

        if want_preempt and e.name == "sched_switch":
            prev_comm = e.payload.get("prev_comm", "")
            next_comm = e.payload.get("next_comm", "")
            prev_worker = is_worker(prev_comm)
            next_worker = is_worker(next_comm)

            if not preempt_all and not (prev_worker or next_worker):
                continue

            switch_events.append({
                "ts": e.ts, "cpu": e.cpu, "event_tid": e.vtid,
                "prev_tid": e.payload.get("prev_tid", ""),
                "prev_comm": prev_comm,
                "prev_state": e.payload.get("prev_state", ""),
                "next_tid": e.payload.get("next_tid", ""),
                "next_comm": next_comm,
                "prev_worker": prev_worker,
                "next_worker": next_worker,
            })

    return timeline_events, switch_events


# ---------- 共享 t0 ----------

def shared_t0(*event_lists):
    """在多个事件列表里求最早的 ts；全部为空返回 None。"""
    ts_values = [ev["ts"] for lst in event_lists for ev in lst]
    return min(ts_values) if ts_values else None


# ---------- 导出 ----------

def write_timeline_csv(events, t0, output_path, after_us=0.0):
    if not events or t0 is None:
        return 0

    kept = [(ev, (ev["ts"] - t0) / 1000.0) for ev in events]
    kept = [(ev, t_us) for ev, t_us in kept if t_us >= after_us]
    kept.sort(key=lambda x: x[0]["ts"])

    with open(output_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["seq", "t_us", "cpu", "tid", "kind", "tag"])
        for seq, (ev, t_us) in enumerate(kept):
            w.writerow([seq, round(t_us, 3), ev["cpu"], ev["tid"], ev["kind"], ev["tag"]])
    return len(kept)


def write_preempt_csv(events, t0, output_path, after_us=0.0):
    if not events or t0 is None:
        return 0

    kept = [(ev, (ev["ts"] - t0) / 1000.0) for ev in events]
    kept = [(ev, t_us) for ev, t_us in kept if t_us >= after_us]
    kept.sort(key=lambda x: x[0]["ts"])

    with open(output_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["seq", "t_us", "cpu", "prev_tid", "prev_comm", "prev_state",
                    "next_tid", "next_comm", "prev_is_worker", "next_is_worker", "event_tid"])
        for seq, (ev, t_us) in enumerate(kept):
            w.writerow([
                seq, round(t_us, 3), ev["cpu"],
                ev["prev_tid"], ev["prev_comm"], ev["prev_state"],
                ev["next_tid"], ev["next_comm"],
                int(ev["prev_worker"]), int(ev["next_worker"]),
                ev["event_tid"],
            ])
    return len(kept)


# ---------- 单实验处理 ----------

def process_one(folder, outdir, after_us,
                exclude_cpus=EXCLUDE_CPUS,
                include_cpus=INCLUDE_CPUS,
                preempt_all=True,   # CSV 默认完整切换序列（仅 --worker-only 时过滤）
                want_preempt=True,
                want_timeline=True):
    trace_path = os.path.join(folder, "trace")
    if not os.path.isdir(trace_path):
        return {"name": os.path.basename(folder), "error": "没有 trace/ 子目录"}

    name = os.path.basename(folder)
    timeline_events, switch_events = collect(
        trace_path,
        exclude_cpus=exclude_cpus,
        include_cpus=include_cpus,
        want_preempt=want_preempt,
        want_timeline=want_timeline,
        preempt_all=preempt_all,
    )

    t0 = shared_t0(
        timeline_events if want_timeline else [],
        switch_events if want_preempt else [],
    )
    if t0 is None:
        return {"name": name, "error": "没有可用事件"}

    result = {"name": name, "t0": t0}

    if want_timeline:
        p = os.path.join(outdir, f"{name}_timeline.csv")
        result["timeline"] = (os.path.basename(p),
                              write_timeline_csv(timeline_events, t0, p, after_us))

    if want_preempt:
        p = os.path.join(outdir, f"{name}_preempt.csv")
        result["preempt"] = (os.path.basename(p),
                             write_preempt_csv(switch_events, t0, p, after_us))

    return result


# ---------- 批量入口 ----------

def run_export(results_dir=RESULTS_DIR,
               outdir=RESULTS_DIR,
               after_us=AFTER_US,
               exclude_cpus=EXCLUDE_CPUS,
               cpu_offset=1,
               preempt_all=True,   # CSV 默认完整切换序列（仅 --worker-only 时过滤）
               want_preempt=True,
               want_timeline=True):
    """批量导出 results_dir 下每个实验目录。

    CPU 范围**按每个实验目录名里的 m 现算**（worker i 绑 core cpu_start+i，见 cpus()）——批量语料
    m 是混的（实测 m1/m2/m3 同目录），固定一套范围必然漏核（m=3 的实验只导出 core N，另两个核的
    worker 片段全丢，实测行数差 3.4 倍）。目录名解析不出 m 时不过滤（导出全部 CPU，不丢数据）并打警告。
    """
    os.makedirs(outdir, exist_ok=True)
    folders = sorted(
        os.path.join(results_dir, d)
        for d in os.listdir(results_dir)
        if os.path.isdir(os.path.join(results_dir, d))
    )
    if not folders:
        print(f"在 {results_dir}/ 下没有找到实验目录。")
        return

    tags = []
    if want_timeline: tags.append("timeline")
    if want_preempt:  tags.append("preempt")
    print(f"开始处理 {len(folders)} 个实验 -> {outdir}/  输出: {', '.join(tags)}")

    ok = 0
    for folder in folders:
        # CPU 范围：按**本次实验**的 m 现算（worker i 绑 core cpu_start+i）
        m = parse_m(os.path.basename(folder))
        if m is None:
            inc, cpu_note = None, "  [警告] 目录名无 _m<数字>，不过滤 CPU（导出全部核）"
        else:
            inc = cpus(cpu_offset, m)
            cpu_note = f"  cpus={inc[0]}-{inc[-1]}(m={m})"
        r = process_one(folder, outdir, after_us,
                        exclude_cpus=exclude_cpus, include_cpus=inc,
                        preempt_all=preempt_all,
                        want_preempt=want_preempt, want_timeline=want_timeline)
        name = r["name"]
        if "error" in r:
            print(f"  [跳过] {name}: {r['error']}{cpu_note}")
            continue

        parts = []
        if "timeline" in r:
            fn, n = r["timeline"]; parts.append(f"{fn} ({n} 行)")
        if "preempt" in r:
            fn, n = r["preempt"];  parts.append(f"{fn} ({n} 行)")
        ok += 1
        print(f"  [成功] {name}  t0={r['t0']}{cpu_note}  ->  " + " | ".join(parts))

    print(f"完成。成功 {ok} / {len(folders)}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--results", default=RESULTS_DIR)
    ap.add_argument("--outdir", default=RESULTS_DIR)
    ap.add_argument("--after-us", type=float, default=AFTER_US)
    ap.add_argument("--no-timeline", action="store_true")
    ap.add_argument("--no-preempt", action="store_true")
    ap.add_argument("--worker-only", action="store_true",
                    help="preempt CSV 仅保留至少一端是 fins_worker 的 sched_switch（默认全保留）")
    ap.add_argument("--exclude-cpus", type=int, nargs="*", default=EXCLUDE_CPUS)
    ap.add_argument("--cpu-start", type=int, default=1,
                    help="起始核号：按每个实验目录名里的 m 现算 CPU 范围（worker i 绑 core cpu-start+i）")
    args = ap.parse_args()

    run_export(
        results_dir=args.results,
        outdir=args.outdir,
        after_us=args.after_us,
        exclude_cpus=args.exclude_cpus,
        cpu_offset=args.cpu_start,
        preempt_all=not args.worker_only,
        want_timeline=not args.no_timeline,
        want_preempt=not args.no_preempt,
    )