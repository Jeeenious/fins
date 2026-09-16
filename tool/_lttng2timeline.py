#!/usr/bin/env python3
# coding: utf-8

"""
FINS 项目专用的 LTTng timeline 导出模块 (支持 Notebook 调用)
"""

import argparse
import csv
import glob
import os
import bt2

RESULTS_DIR = "./result/FINS_FIFO_IPC_nuc12"
AFTER_US = 5000 * 1000

# EXCLUDE_CPUS: 需要排除的 CPU Core ID 列表，例如 [0, 1]；若为空则不排除。
EXCLUDE_CPUS = []
# INCLUDE_CPUS: 仅包含的 CPU Core ID 列表，例如 [2, 3, 4]；若为 None 则包含所有 Core。
INCLUDE_CPUS = None

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


class EventView:
    def __init__(self, ev, msg):
        self.ev = ev
        self.name = ev.name
        self.ts = int(msg.default_clock_snapshot.value)
        ctx = ev.common_context_field

        vpid_val = _val(ctx.get('vpid')) if ctx is not None else 0
        vtid_val = _val(ctx.get('vtid')) if ctx is not None else 0

        try:
            self.vpid = int(vpid_val)
        except (TypeError, ValueError):
            self.vpid = 0

        try:
            self.vtid = int(vtid_val)
        except (TypeError, ValueError):
            self.vtid = 0

        try:
            pc = ev.packet.context_field
            self.cpu = int(_val(pc.get('cpu_id'))) if pc is not None else -1
        except Exception:
            self.cpu = -1
        self.payload = _fields(ev.payload_field)


def _iter_events(trace_dir):
    for msg in bt2.TraceCollectionMessageIterator(trace_dir):
        ev = getattr(msg, 'event', None)
        if ev is None:
            continue
        yield EventView(ev, msg)


def analyze(trace_dir, min_us=0.0, exclude_cpus=EXCLUDE_CPUS, include_cpus=INCLUDE_CPUS):
    exclude_cpus_set = set(exclude_cpus) if exclude_cpus else set()
    include_cpus_set = set(include_cpus) if include_cpus is not None else None

    rows = []
    for e in _iter_events(trace_dir):
        # 核心过滤逻辑
        if e.cpu in exclude_cpus_set:
            continue
        if include_cpus_set is not None and e.cpu not in include_cpus_set:
            continue

        n = e.name

        # 算法事件：provider = algo（trace/algo_tracepoints.hpp 定义，plugin.so 内注册）
        if n in ('algo:execute', 'lttng_ust_algo:execute'):
            kind, tag = 'execute', e.payload.get('node_id', '')
        elif n in ('algo:complete', 'lttng_ust_algo:complete'):
            kind, tag = 'complete', e.payload.get('node_id', '')
        elif n in ('algo:working', 'lttng_ust_algo:working'):
            kind = 'working'
            node_id, core_id, seg_us = e.payload.get('node_id', ''), e.payload.get('core_id', -1), e.payload.get('seg_us', 0)
            tag = f'{node_id} [CPU {core_id}, {seg_us} us]'
        elif n in ('fins:finished', 'lttng_ust_fins:finished'):
            kind, tag = 'finished', ''
        elif n in ('fins:release', 'lttng_ust_fins:release'):
            kind, tag = 'release', ''
        elif n in ('fins:sleep', 'lttng_ust_fins:sleep'):
            kind, tag = 'sleep', ''
        elif n in ('fins:wake', 'lttng_ust_fins:wake'):
            kind, tag = 'wake', ''
        else:
            continue

        rows.append((e.ts, e.vtid, e.cpu, kind, tag))

    if not rows:
        return None

    t0 = min(r[0] for r in rows)
    cand = [(ts, vtid, cpu, kind, tag) for ts, vtid, cpu, kind, tag in rows if (ts - t0) / 1000.0 >= min_us]
    flat = [[seq, round(ts / 1000.0, 3), cpu, vtid, kind, tag] for seq, (ts, vtid, cpu, kind, tag) in enumerate(cand)]
    return flat


def export_one(folder, outdir, after_us, exclude_cpus=EXCLUDE_CPUS, include_cpus=INCLUDE_CPUS):
    trace_path = os.path.join(folder, "trace")
    if not os.path.exists(trace_path):
        return None, "没有 trace/ 子目录"

    name = os.path.basename(folder)
    rows = analyze(trace_path, min_us=after_us, exclude_cpus=exclude_cpus, include_cpus=include_cpus)
    if rows is None:
        return None, "解析失败"

    out = os.path.join(outdir, f"{name}_timeline.csv")
    with open(out, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['seq', 't_us', 'cpu', 'tid', 'kind', 'tag'])
        w.writerows(rows)
    return os.path.basename(out), len(rows)


def run_export(results_dir=RESULTS_DIR, outdir=RESULTS_DIR, after_us=AFTER_US, exclude_cpus=EXCLUDE_CPUS, include_cpus=INCLUDE_CPUS):
    """供 Jupyter Notebook 调用的批量处理入口函数"""
    os.makedirs(outdir, exist_ok=True)
    folders = sorted(
        [os.path.join(results_dir, d) for d in os.listdir(results_dir) if os.path.isdir(os.path.join(results_dir, d))])

    if not folders:
        print(f"在 {results_dir}/ 下没有找到实验文件夹。")
        return

    print(f"开始批量导出 timeline: {len(folders)} 个实验 -> {outdir}/")
    ok = 0
    for folder in folders:
        out, info = export_one(folder, outdir, after_us, exclude_cpus, include_cpus)
        if out:
            ok += 1
            print(f"  [成功] {os.path.basename(folder)} -> {out} ({info} 行)")
        else:
            print(f"  [跳过] {os.path.basename(folder)}: {info}")
    print(f"完成。成功 {ok} / {len(folders)}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument('--results', default=RESULTS_DIR)
    ap.add_argument('--outdir', default=RESULTS_DIR)
    ap.add_argument('--after-us', type=float, default=AFTER_US)
    args = ap.parse_args()
    run_export(args.results, args.outdir, args.after_us)