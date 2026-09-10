# -*- coding: utf-8 -*-
"""plot.ipynb 的数据分析层：按新格式（含 working 行）还原 job 生命周期与分段执行。

job 的物理形状（同一 tid 上）：

    wake ─ release ─ execute ─ working ─ working ─ ... ─ complete ─ finished ─ sleep
    └────── 线程忙期 ──────┘ └────── job 生命周期（execute→complete）──────┘

`working` 行 `n7 [CPU 2, 22793 us]` 给出 execute→complete 内部**一段执行的显式区间**：
`[t-22793, t]` 落在 CPU 2 上；行自身的 `cpu` 列是这一段结束时的核。
相邻两段的核不同 = 该 job 发生了迁移。

只支持新格式：每个 job 的 execute→complete 内**必有** working 行；缺失视为格式异常并报数。

对外入口（与旧 cell 同名，下游 cell 不用改）：
    df_jobs = _parse_hierarchy_data(csv)      # 每 job 一行，列名与旧版一致
    df_seg  = _parse_hierarchy_segments(csv)  # 每段一行，供核心甘特 / 利用率使用
"""
import os
import re

import numpy as np
import pandas as pd

# --------------------------------------------------------------------------- 常量

EXCL_TAGS = {"timer", "main", "main::expand", "main::rollover"}

WORK_RE = re.compile(r"^(?P<node>n\d+)\s*\[CPU\s*(?P<cpu>\d+)\s*,\s*(?P<dur>\d+)\s*us\]$")
TAG_RE = re.compile(r"^(?P<node>n\d+)(?::\d+)?$")

# 解析结果缓存：_parse_hierarchy_data 与 _parse_hierarchy_segments 共用一次解析
_CACHE = {}


def parse_working(tag):
    """`n7 [CPU 2, 22793 us]` -> (node, 段所在核, 段时长 us)。"""
    m = WORK_RE.match(str(tag).strip())
    return (m["node"], int(m["cpu"]), int(m["dur"])) if m else None


def norm_tag(tag):
    """把 `n5:0` / `n5` 统一成 `n5`；空 / 非法返回 None。"""
    m = TAG_RE.match(str(tag).strip())
    return m["node"] if m else None


# --------------------------------------------------------------------------- 解析

def parse_trace(csv):
    """读 trace 并规范化；`seq` 是全局时间序。"""
    df = pd.read_csv(csv)
    for c in ["tid", "seq", "t_us", "cpu"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["kind"] = df["kind"].astype(str).str.strip().str.lower()
    df["tag"] = df["tag"].astype(str).replace({"nan": None})
    df = df.dropna(subset=["tid", "seq", "t_us", "cpu"])
    df = df[df["t_us"] >= 0]
    return df.sort_values(["tid", "seq"], kind="stable").reset_index(drop=True)


def build_lifecycles(df):
    """按 tid 还原 job 的锚点序列：wake / release / execute / complete / finished / sleep。

    与旧 `_job_segments` 同口径，但**按 tid 而不是按 cpu 归组**——线程会跨核，
    按 cpu 归组会把迁移线程的事件切成两半，锚点全部错位。

    积压（back-to-back）时多个 job 共享批首 wake：只有批首 job 的 wak_rel 非空。
    """
    jobs = []
    for tid, g in df.groupby("tid", sort=False):
        current, pending_wake = None, None
        for r in g.itertuples(index=False):
            k = r.kind
            if k == "wake":
                pending_wake = r.t_us

            elif k == "release":
                # 上一个 job 已 finished：说明是 back-to-back，pending_wake 是它的 sleep 点
                if current is not None and current["fin"] is not None:
                    current["slp"] = pending_wake
                    jobs.append(current)
                    wak = pending_wake
                else:
                    wak = pending_wake
                current = dict(tid=int(tid), wak_rel=wak, wak_T=wak,
                               rel=r.t_us, rel_cpu=int(r.cpu),
                               exc=None, exc_cpu=None, com=None, com_cpu=None,
                               fin=None, fin_cpu=None, slp=None, tag=None, segs=[])
                pending_wake = None

            elif k == "execute" and current is not None:
                current["exc"], current["exc_cpu"] = r.t_us, int(r.cpu)
                if norm_tag(r.tag):
                    current["tag"] = norm_tag(r.tag)

            elif k == "complete" and current is not None:
                current["com"], current["com_cpu"] = r.t_us, int(r.cpu)
                if norm_tag(r.tag):
                    current["tag"] = norm_tag(r.tag)

            elif k == "finished" and current is not None:
                current["fin"], current["fin_cpu"] = r.t_us, int(r.cpu)

            elif k == "sleep":
                # finished -> wake -> sleep：真实 sleep 点
                if current is not None and pending_wake is not None:
                    current["slp"] = r.t_us
                    jobs.append(current)
                    current = None
                pending_wake = None

        if current is not None and current["fin"] is not None:
            jobs.append(current)          # trace 截断：slp 未知，保留其余锚点

    for i, j in enumerate(jobs):
        j["jid"] = i
    return jobs


def attach_segments(jobs, df):
    """把 working 行按 [execute, complete] 窗口归到 job，还原每段显式区间（新格式必有）。

    每个 working 行 `n7 [CPU 2, 22793 us]` 给出一段：`[t-22793, t]` 在 CPU 2 上，
    行自身的 `cpu` 列是这一段结束时的核（与下一段的核比较即知是否迁移）。
    末尾再补一段 `[最后一个 working, complete]`，落在 complete 的核上。

    注意 `dur` 是该段**真实占用该核**的时间，不是墙上时间：execute→complete 里
    没有被任何段覆盖的部分是该 job 被抢占/离核的时间（df_jobs 里记在 `algo_gap_us`）。

    严格要求每个 job 至少有一条 working 行；缺失的记为 `no_working` 并在解析时报数
    （不做任何「整段算一个核」的兜底，避免把迁移线程的锚点悄悄算错）。
    """
    work = df[df["kind"] == "working"]
    by_tid = {}
    for r in work.itertuples(index=False):
        p = parse_working(r.tag)
        if p:
            by_tid.setdefault(int(r.tid), []).append((r.t_us, p[1], p[2], int(r.cpu)))

    no_working = 0
    for j in jobs:
        ws = [(ts, sc, d, nc) for ts, sc, d, nc in sorted(by_tid.get(j["tid"], []))
              if j["exc"] is not None and j["com"] is not None and j["exc"] <= ts <= j["com"]]
        if not ws:
            j["segs"], j["no_working"] = [], True
            no_working += 1
            continue
        segs = [dict(seg=k, cpu=seg_cpu, start=ts - dur, end=ts, dur=float(dur),
                     next_cpu=next_cpu, migrated=(seg_cpu != next_cpu))
                for k, (ts, seg_cpu, dur, next_cpu) in enumerate(ws)]
        if j["com"] is not None and j["com"] > ws[-1][0]:
            segs.append(dict(seg=len(segs), cpu=ws[-1][3], start=ws[-1][0], end=j["com"],
                             dur=j["com"] - ws[-1][0], next_cpu=j["com_cpu"], migrated=False))
        j["segs"], j["no_working"] = segs, False
    if no_working:
        print(f"⚠️ {no_working} 个 job 的 [execute, complete] 窗口内没有 working 行（格式异常）")
    return jobs


def build_job_table(jobs):
    """每 job 一行；列名与旧 `_parse_hierarchy_data` 完全一致（下游 cell 不用改），
    另加分段相关的新列。"""
    t0 = min((j["rel"] for j in jobs if j["rel"] is not None), default=0.0)
    rows = []
    for j in jobs:
        if j["exc"] is None or j["com"] is None or j["fin"] is None or j["slp"] is None:
            continue                                   # 锚点不全，跳过（保持旧口径）
        tag = j["tag"] or "unknown"
        segs = j["segs"]
        cores = [s["cpu"] for s in segs]
        migrated = len(set(cores)) > 1
        algo_span = j["com"] - j["exc"]
        wake_2_held = (j["rel"] - j["wak_rel"]) if j["wak_rel"] is not None else np.nan
        thread_pre = (j["exc"] - j["wak_T"]) if j["wak_T"] is not None else np.nan
        thread_ovh = ((j["slp"] - j["wak_T"]) - algo_span) if j["wak_T"] is not None else np.nan

        m = {
            "wake_2_held_lat": wake_2_held,
            "held_2_exec_lat": j["exc"] - j["rel"],
            "algo_exec_span": algo_span,
            "exec_2_route_lat": j["fin"] - j["com"],
            "route_2_sleep_lat": j["slp"] - j["fin"],
            "thread_pre_exec_lat": thread_pre,
            "thread_post_exec_lat": j["slp"] - j["com"],
            "thread_overhead": thread_ovh,
        }
        rows.append({
            "jid": j["jid"], "tid": j["tid"], "algo": tag,
            "core_id": str(j["exc_cpu"]),              # 执行起点核（兼容旧列）
            "rel_cpu": j["rel_cpu"], "exc_cpu": j["exc_cpu"], "com_cpu": j["com_cpu"],
            "fin_cpu": j["fin_cpu"],
            # 分段信息（新）
            "algo_seg_n": len(segs),
            "algo_migrated": migrated,
            "algo_cores": ",".join(str(c) for c in cores),
            "algo_migrated_us": float(sum(s["dur"] for i, s in enumerate(segs)
                                          if s["cpu"] != j["exc_cpu"])) if migrated else 0.0,
            # 真实占核时间 = 各段 dur 之和；algo_exec_span 是墙钟跨度，
            # 两者之差 algo_gap_us 是 job 内部被抢占/离核的时间（不占任何核）。
            "algo_cpu_us": float(sum(s["dur"] for s in segs)),
            "algo_gap_us": float(algo_span - sum(s["dur"] for s in segs)),
            "thread_start_ms": (j["wak_T"] - t0) / 1000.0 if j["wak_T"] is not None else np.nan,
            "thread_work_ms": (algo_span + thread_ovh) / 1000.0
            if not (pd.isna(algo_span) or pd.isna(thread_ovh)) else np.nan,
            "algo_exec_ms": algo_span / 1000.0,
            "t_min_global": t0,
            **m,
        })
    return pd.DataFrame(rows)


def build_segment_table(df_jobs, jobs):
    """每段一行：核心甘特与利用率图的输入。"""
    jobs_by_id = {j["jid"]: j for j in jobs}
    rows = []
    for r in df_jobs.itertuples(index=False):
        j = jobs_by_id[r.jid]
        for s in j["segs"]:
            rows.append(dict(
                jid=j["jid"], tid=j["tid"], algo=r.algo, core_id=str(s["cpu"]),
                next_core_id=str(s["next_cpu"]), seg=s["seg"], nseg=len(j["segs"]),
                start=s["start"], end=s["end"], dur_us=s["dur"], dur_ms=s["dur"] / 1000.0,
                start_ms=(s["start"] - r.t_min_global) / 1000.0,
                migrated=bool(s["migrated"]) or (len({x["cpu"] for x in j["segs"]}) > 1),
                job_migrated=bool(r.algo_migrated),
            ))
    return pd.DataFrame(rows)


def parse_all(csv):
    """一次解析出 (df_raw, df_jobs, df_seg)。"""
    if csv in _CACHE:
        return _CACHE[csv]
    if not os.path.exists(csv):
        print(f"❌ File not found: {csv}")
        empty = pd.DataFrame()
        _CACHE[csv] = (empty, empty, empty)
        return _CACHE[csv]
    df = parse_trace(csv)
    jobs = attach_segments(build_lifecycles(df), df)
    df_jobs = build_job_table(jobs)
    df_seg = build_segment_table(df_jobs, jobs)
    print(f"📊 {os.path.basename(csv)}: events={len(df)} jobs={len(df_jobs)} "
          f"segments={len(df_seg)} 迁移 job={int(df_jobs['algo_migrated'].sum()) if len(df_jobs) else 0}")
    _CACHE[csv] = (df, df_jobs, df_seg)
    return _CACHE[csv]


# --------------------------------------------------------------------------- 兼容旧入口

def _parse_hierarchy_data(csv):
    """保持旧签名：返回每 job 一行的 df_jobs（列名不变，另加分段列）。"""
    return parse_all(csv)[1]


def _parse_hierarchy_segments(csv):
    """每段一行，供核心甘特 / 核利用率使用。"""
    return parse_all(csv)[2]


def _parse_hierarchy_raw(csv):
    return parse_all(csv)[0]


# --------------------------------------------------------------------------- 绘图

import plotly.express as px
import plotly.graph_objects as go

DISCRETE_SCI_COLORS = [
    "#3B5998", "#5E8B61", "#A6192E", "#D6A531", "#709BFF",
    "#925E9F", "#0099B4", "#FDAF91", "#4D4D4D", "#ADB6B6",
]


def _algo_num(a):
    m = re.search(r"(\d+)", str(a))
    return int(m.group(1)) if m else 10 ** 9


def algo_order(algos):
    return sorted({a for a in algos if a is not None}, key=lambda x: (_algo_num(x), str(x)))


def algo_color_map(algos):
    return {a: DISCRETE_SCI_COLORS[i % len(DISCRETE_SCI_COLORS)]
            for i, a in enumerate(algo_order(algos))}


def _clip(df_seg, win):
    """win=(t0_ms, t1_ms)：只看这个时间窗内的段（与 start_ms 同一坐标系）。"""
    if win is None:
        return df_seg
    lo, hi = win
    return df_seg[(df_seg["end"] / 1000.0 >= lo) & (df_seg["start_ms"] <= hi)].copy()


def draw_core_gantt(df_seg, win=None, only_migrated=False, show_migration=True,
                    title=None):
    """每核心一条泳道：直接用 working 给出的显式区间画，不在事件时刻上做推导。

    win=(t0_ms, t1_ms) 只看窗口；only_migrated=True 只看跨核 job 的段。
    """
    if df_seg is None or df_seg.empty:
        print("无分段数据")
        return None
    sg = _clip(df_seg, win)
    if only_migrated:
        sg = sg[sg["job_migrated"]]
    if sg.empty:
        print("窗口内无段")
        return None

    fig = px.bar(sg, base="start_ms", x="dur_ms", y="core_id", color="algo",
                 orientation="h", opacity=0.85,
                 color_discrete_map=algo_color_map(sg["algo"]),
                 category_orders={"algo": algo_order(sg["algo"])},
                 title=title or ("Core Timeline — 每段显式区间"
                                 + ("（仅迁移 job）" if only_migrated else "")),
                 hover_data={"tid": True, "algo": True, "core_id": True, "nseg": True,
                             "seg": True, "job_migrated": True,
                             "start_ms": ":.3f", "dur_ms": ":.3f"},
                 labels={"start_ms": "Time (ms)", "dur_ms": "Duration (ms)",
                         "core_id": "Core", "algo": "Algo"})
    fig.update_layout(barmode="overlay", plot_bgcolor="white",
                      height=240 + 90 * sg["core_id"].nunique(),
                      yaxis_title="Core", xaxis_title="Time (ms)")

    if show_migration:
        mg = sg[sg["migrated"]]
        if not mg.empty:
            fig.add_trace(go.Scatter(
                x=mg["end"] / 1000.0, y=mg["core_id"], mode="markers", name="迁移",
                marker=dict(symbol="line-ns-open", size=15, color="black", line_width=2),
                customdata=mg[["algo", "tid", "dur_us"]].values,
                hovertemplate=("迁移点<br>algo=%{customdata[0]} tid=%{customdata[1]}<br>"
                               "本段 %{customdata[2]} us<extra></extra>")))
    if win is not None:
        fig.update_xaxes(range=[win[0], win[1]])
    return fig


def draw_core_utilization(df_raw, df_seg, df_jobs, title=None):
    """每核时间都花在哪：Active = 该核上算法段之和；Overhead = 线程前后开销。

    注意与旧版口径的差别：旧版按 job 的 `core_id` 把整段 algo 记到一个核上，
    线程跨核时会算错；这里 Active 直接来自每段的真实所在核。
    Overhead 按发生位置拆：pre(release 前转身) 记 rel_cpu，post(收尾到 sleep) 记 fin_cpu。
    Idle = 墙钟 − Active − Overhead。
    """
    if df_jobs is None or df_jobs.empty:
        print("无 job 数据")
        return None
    wall = float(df_raw["t_us"].max() - df_raw["t_us"].min())
    cores = sorted(set(df_seg["core_id"]) | set(df_jobs["rel_cpu"].astype(str))
                   | set(df_jobs["fin_cpu"].astype(str)), key=lambda x: _algo_num(x))

    active = df_seg.groupby("core_id")["dur_us"].sum().to_dict()
    pre = df_jobs.groupby(df_jobs["rel_cpu"].astype(str))["thread_pre_exec_lat"].sum().to_dict()
    post = df_jobs.groupby(df_jobs["fin_cpu"].astype(str))["thread_post_exec_lat"].sum().to_dict()

    rec = []
    for c in cores:
        a = float(active.get(c, 0.0))
        o = float(pre.get(c, 0.0) or 0.0) + float(post.get(c, 0.0) or 0.0)
        i = max(0.0, wall - a - o)
        rec += [dict(core_id=c, State="1. Active", pct=a / wall * 100),
                dict(core_id=c, State="2. Overhead", pct=o / wall * 100),
                dict(core_id=c, State="3. Idle", pct=i / wall * 100)]
    df_util = pd.DataFrame(rec)
    fig = px.bar(df_util, x="pct", y="core_id", color="State", orientation="h",
                 barmode="stack", category_orders={"core_id": cores},
                 color_discrete_map={"1. Active": "#2C5F7A", "2. Overhead": "#D4834A",
                                     "3. Idle": "#8CAA8C"},
                 title=title or "Core Utilization (Active 按段真实所在核)",
                 labels={"pct": "Percentage of wall clock (%)", "core_id": "Core"})
    fig.update_layout(plot_bgcolor="white", xaxis=dict(range=[0, 105], ticksuffix="%"),
                      height=200 + 40 * len(cores), legend_title="")
    return fig
