#!/usr/bin/env python3
# coding: utf-8

"""
FINS 项目专用的 LTTng sched_switch 提取与工作线程过滤模块 (支持 Notebook 调用)
"""

import argparse
import csv
import glob
import os
import re
import bt2

RESULTS_DIR = "./result/FINS_FIFO_IPC_nuc12"
AFTER_US = 5000.0 * 1000

# EXCLUDE_CPUS: 需要排除的 CPU Core ID 列表，例如 [0, 1]；若为空则不排除。
EXCLUDE_CPUS = []
# INCLUDE_CPUS: 仅包含的 CPU Core ID 列表，例如 [2, 3, 4]；若为 None 则包含所有 Core。
INCLUDE_CPUS = None

# 线程池 worker 的 comm 由 ThreadPool::working() 用 pthread_setname_np 设为
# "fins_worker-<idx>"（见 core/thread_pool.hpp）。不能用进程名匹配——默认 comm 继承进程名，
# 进程内 计时/RPC/watchdog/硬件监控/TBB 等线程全都叫 "client"，会把它们一并误收。
TGT_PATTERNS = [r"^fins_worker"]


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
    """安全转 int：字段为 None / 缺失 / 不可解析（_val 会回落成 'None' 字符串）时返回 default。
    内核事件没有 vpid/vtid 上下文，UST 也只加了 vtid 上下文（见 _test.py 的 add-context），
    裸 int() 会直接 ValueError 崩掉。"""
    try:
        return int(_val(value))
    except (TypeError, ValueError):
        return default


class EventView:
    def __init__(self, ev, msg):
        self.ev = ev
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


_WORKER_REGEX = [re.compile(p) for p in TGT_PATTERNS]


def is_worker(comm):
    if not comm:
        return False
    return any(regex.search(str(comm)) for regex in _WORKER_REGEX)


def extract_switches(trace_dir, filter_worker_only=True, exclude_cpus=EXCLUDE_CPUS, include_cpus=INCLUDE_CPUS):
    exclude_cpus_set = set(exclude_cpus) if exclude_cpus else set()
    include_cpus_set = set(include_cpus) if include_cpus is not None else None

    switches, t0 = [], None
    for e in _iter_events(trace_dir):
        if e.name != "sched_switch":
            continue

        # 核心过滤逻辑
        if e.cpu in exclude_cpus_set:
            continue
        if include_cpus_set is not None and e.cpu not in include_cpus_set:
            continue

        if t0 is None or e.ts < t0:
            t0 = e.ts

        prev_comm, next_comm = e.payload.get("prev_comm", ""), e.payload.get("next_comm", "")
        prev_worker, next_worker = is_worker(prev_comm), is_worker(next_comm)

        # if filter_worker_only and not (prev_worker or next_worker):
        #     continue

        switches.append({
            "ts": e.ts, "cpu": e.cpu, "event_tid": e.vtid,
            "prev_tid": e.payload.get("prev_tid", ""), "prev_comm": prev_comm, "prev_state": e.payload.get("prev_state", ""),
            "next_tid": e.payload.get("next_tid", ""), "next_comm": next_comm,
            "prev_worker": prev_worker, "next_worker": next_worker,
        })
    return switches, t0


def export_switch_csv(switches, t0, output_path, after_us=0.0):
    if not switches:
        return 0

    rows = []
    for event in switches:
        t_us = (event["ts"] - t0) / 1000.0
        if t_us < after_us:
            continue
        rows.append([event["ts"], event["cpu"], event["prev_tid"], event["prev_comm"], event["prev_state"],
                     event["next_tid"], event["next_comm"], int(event["prev_worker"]), int(event["next_worker"]), event["event_tid"]])

    rows.sort(key=lambda row: row[0])
    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["seq", "t_us", "cpu", "prev_tid", "prev_comm", "prev_state", "next_tid", "next_comm", "prev_is_worker", "next_is_worker", "event_tid"])
        for seq, row in enumerate(rows):
            ts, cpu, p_tid, p_comm, p_st, n_tid, n_comm, p_w, n_w, e_tid = row
            writer.writerow([seq, round(ts / 1000.0, 3), cpu, p_tid, p_comm, p_st, n_tid, n_comm, p_w, n_w, e_tid])

    return len(rows)


def process_one(folder, outdir, after_us, filter_worker_only, exclude_cpus=EXCLUDE_CPUS, include_cpus=INCLUDE_CPUS):
    trace_path = os.path.join(folder, "trace")
    if not os.path.isdir(trace_path):
        return None, "没有 trace/ 子目录"

    name = os.path.basename(folder)
    switches, t0 = extract_switches(trace_path, filter_worker_only, exclude_cpus, include_cpus)
    if t0 is None:
        return None, "没有找到 sched_switch"

    output_path = os.path.join(outdir, f"{name}_preempt.csv")
    count = export_switch_csv(switches, t0, output_path, after_us=after_us)
    return os.path.basename(output_path), count


def run_export(results_dir=RESULTS_DIR, outdir=RESULTS_DIR, after_us=AFTER_US, filter_worker_only=False, exclude_cpus=EXCLUDE_CPUS, include_cpus=INCLUDE_CPUS):
    """供 Jupyter Notebook 调用的批量处理入口函数"""
    os.makedirs(outdir, exist_ok=True)
    folders = sorted([os.path.join(results_dir, d) for d in os.listdir(results_dir) if os.path.isdir(os.path.join(results_dir, d))])

    if not folders:
        print(f"在 {results_dir}/ 下没有找到实验目录。")
        return

    print(f"开始处理抢占数据: {len(folders)} 个实验 -> {outdir}/")
    ok, total_rows = 0, 0
    for folder in folders:
        res = process_one(folder, outdir, after_us, filter_worker_only, exclude_cpus, include_cpus)
        if res is None:
            continue
        output_name, count = res
        ok += 1
        total_rows += count
        print(f"  [成功] {os.path.basename(folder)} -> {output_name} ({count} rows)")
    print(f"完成：成功 {ok}/{len(folders)} 个实验，总计写入 {total_rows} 行")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument('--results', default=RESULTS_DIR)
    ap.add_argument('--outdir', default=RESULTS_DIR)
    ap.add_argument('--after-us', type=float, default=AFTER_US)
    ap.add_argument('--all', action='store_true')
    args = ap.parse_args()
    run_export(args.results, args.outdir, args.after_us, not args.all)