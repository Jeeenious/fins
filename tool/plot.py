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
            # overhead 收尾段的右端：complete→finished 之后遇到的第一个 sleep/wake
            if (k in ("sleep", "wake") and current is not None
                    and current["fin"] is not None and current["post_end"] is None):
                current["post_end"] = r.t_us
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
                               fin=None, fin_cpu=None, slp=None, post_end=None,
                               tag=None, segs=[])
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
            jobs.append(current)  # trace 截断：slp 未知，保留其余锚点

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
    另加分段相关的新列。

    线程额外开销（thread_overhead）口径：
        [wake → release → execute]  +  [complete → finished → 下一个 sleep/wake]
    即算法段两侧紧邻的线程占用；算法段本身（execute→complete）是 Active，
    收尾边界之后的时间（等下一个 job）算 Idle。找得到 sleep/wake 就用它，
    找不到（trace 截断 / 背靠背无新 wake）退化成 finished。
    """
    t0 = min((j["rel"] for j in jobs if j["rel"] is not None), default=0.0)
    rows = []
    for j in jobs:
        if j["exc"] is None or j["com"] is None or j["fin"] is None or j["slp"] is None:
            continue  # 锚点不全，跳过（保持旧口径）
        tag = j["tag"] or "unknown"
        segs = j["segs"]
        cores = [s["cpu"] for s in segs]
        migrated = len(set(cores)) > 1
        algo_span = j["com"] - j["exc"]
        wake_2_held = (j["rel"] - j["wak_rel"]) if j["wak_rel"] is not None else np.nan
        thread_pre = (j["exc"] - j["wak_T"]) if j["wak_T"] is not None else np.nan
        post_end = j["post_end"] if j["post_end"] is not None else j["fin"]
        thread_post = post_end - j["com"]
        # 没有 wake 的背靠背 job：pre 记 NaN（图中单列），但 overhead 里按 0 计
        thread_ovh = (0.0 if pd.isna(thread_pre) else thread_pre) + thread_post

        m = {
            "wake_2_held_lat": wake_2_held,
            "held_2_exec_lat": j["exc"] - j["rel"],
            "algo_exec_span": algo_span,
            "exec_2_route_lat": j["fin"] - j["com"],
            "route_2_sleep_lat": j["slp"] - j["fin"],
            "thread_pre_exec_lat": thread_pre,
            "thread_post_exec_lat": thread_post,
            "thread_overhead": thread_ovh,
        }
        rows.append({
            "jid": j["jid"], "tid": j["tid"], "algo": tag,
            "core_id": str(j["exc_cpu"]),  # 执行起点核（兼容旧列）
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
            # 6 阶段：7 个锚点（wake/release/execute/working/complete/finished/sleep）
            # 之间的 6 段墙钟时间，sum = slp − wak_T。working 段只取**第一段**的边界 w1，
            # 多段 job（algo_seg_n>1）的其余部分都含在 ph_w1_com 里。
            "ph_wake_rel": (j["rel"] - j["wak_T"]) if j["wak_T"] is not None else np.nan,
            "ph_rel_exec": j["exc"] - j["rel"],
            "ph_exec_w1": (segs[0]["end"] - j["exc"]) if segs else np.nan,
            "ph_w1_com": (j["com"] - segs[0]["end"]) if segs else np.nan,
            "ph_com_fin": j["fin"] - j["com"],
            "ph_fin_slp": j["slp"] - j["fin"],
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
                end_ms=(s["end"] - r.t_min_global) / 1000.0,
                # migrated: 这一段的**结束处**发生了迁移（本段核 != 下一段核），迁移点就在 end_ms
                # job_migrated: 整个 job 是否跨核（筛选用）
                migrated=bool(s["migrated"]),
                job_migrated=bool(r.algo_migrated),
            ))
    return pd.DataFrame(rows)


def build_working_segments(df):
    rows = []

    work = df[df["kind"] == "working"].copy()

    for r in work.itertuples(index=False):
        p = parse_working(r.tag)
        if p is None:
            continue

        node, working_cpu, dur_us = p

        end_us = float(r.t_us)
        start_us = end_us - float(dur_us)

        event_cpu = int(r.cpu)

        rows.append({
            "tid": int(r.tid),
            "seq": int(r.seq),
            "node": node,

            # tag 中明确声明的执行 CPU
            "cpu": int(working_cpu),

            # tracepoint event 实际发生的 CPU
            "event_cpu": event_cpu,

            "start_us": start_us,
            "end_us": end_us,
            "dur_us": float(dur_us),

            # 两者不一致则标记
            "migrated": int(working_cpu) != event_cpu,
        })

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
    """win=(t0_ms, t1_ms)：只看这个时间窗内的段（与 start_ms / end_ms 同一坐标系）。"""
    if win is None:
        return df_seg
    lo, hi = win
    return df_seg[(df_seg["end_ms"] >= lo) & (df_seg["start_ms"] <= hi)].copy()


def draw_core_gantt(
    df_seg,
    win=None,
    show_migration=True,
    title=None,
):
    if df_seg is None or df_seg.empty:
        print("无分段数据")
        return None

    sg = df_seg.copy()

    # ------------------------------------------------------------
    # 1. 时间窗口
    # ------------------------------------------------------------
    if win is not None:
        lo, hi = win

        sg = sg[
            (sg["end_ms"] >= lo) &
            (sg["start_ms"] <= hi)
        ].copy()

    if sg.empty:
        print("窗口内无段")
        return None

    # ------------------------------------------------------------
    # 2. 固定 CPU lane 顺序
    #
    # 注意：
    # core_order 从原始 df_seg 获取，而不是 sg。
    # 这样过滤某个 node 后，CPU lane 不会重新排列。
    # ------------------------------------------------------------
    core_order = sorted(
        df_seg["core_id"].astype(str).unique(),
        key=lambda x: int(x),
    )

    # ------------------------------------------------------------
    # 3. 固定算法/节点颜色
    #
    # 也从完整 df_seg 获取，避免过滤节点后颜色重新分配。
    # ------------------------------------------------------------
    all_algo_order = algo_order(df_seg["algo"])
    all_algo_colors = algo_color_map(df_seg["algo"])

    # ------------------------------------------------------------
    # 4. Gantt
    # ------------------------------------------------------------
    fig = px.bar(
        sg,
        base="start_ms",
        x="dur_ms",
        y="core_id",
        color="algo",
        orientation="h",
        opacity=0.85,

        color_discrete_map=all_algo_colors,

        category_orders={
            "core_id": core_order,
            "algo": all_algo_order,
        },

        hover_data=[
            "jid",
            "tid",
            "algo",
            "core_id",
            "next_core_id",
            "seg",
            "start_ms",
            "end_ms",
            "dur_us",
            "migrated",
        ],
    )

    fig.update_layout(
        barmode="overlay",
        plot_bgcolor="white",

        # 使用完整 CPU 数量，而不是 sg 中的 CPU 数量
        height=240 + 90 * len(core_order),

        yaxis_title="CPU",
        xaxis_title="Time (ms)",
        title=title,
    )

    # ------------------------------------------------------------
    # 5. 时间窗口
    # ------------------------------------------------------------
    if win is not None:
        fig.update_xaxes(range=[win[0], win[1]])

    # ------------------------------------------------------------
    # 6. migration marker
    # ------------------------------------------------------------
    if show_migration:
        mg = sg[sg["migrated"]]

        if not mg.empty:
            fig.add_trace(
                go.Scatter(
                    x=mg["end_ms"],
                    y=mg["next_core_id"],
                    mode="markers",
                    name="迁移",

                    marker=dict(
                        symbol="x",
                        size=10,
                    ),

                    customdata=mg[
                        [
                            "jid",
                            "tid",
                            "algo",
                            "core_id",
                            "next_core_id",
                            "end_ms",
                        ]
                    ],

                    hovertemplate=(
                        "jid=%{customdata[0]}<br>"
                        "tid=%{customdata[1]}<br>"
                        "algo=%{customdata[2]}<br>"
                        "CPU=%{customdata[3]} → "
                        "%{customdata[4]}<br>"
                        "time=%{customdata[5]:.3f} ms"
                        "<extra></extra>"
                    ),
                )
            )

    return fig


def draw_core_utilization(df_raw, df_seg, df_jobs, title=None):
    """每核时间都花在哪：Active = 该核上算法段之和；Overhead = 线程前后开销。

    口径：
        Active   = Σ 每段 dur（按段真实所在核归属）
        Overhead = [wake → release → execute] + [complete → finished → 下一个 sleep/wake]
                   pre 记 release 所在核（rel_cpu），post 记 finished 所在核（fin_cpu）
        Idle     = 墙钟 − Active − Overhead（不夹断：三态应严格等于墙钟）
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
        i = wall - a - o
        if i < 0:
            print(f"⚠️ CPU{c}: Active+Overhead 超过墙钟 {(-i) / 1000:.1f} ms，口径仍有重叠")
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


# --------------------------------------------------------------------------- 额外开销

def _short_wakeups(df, max_gap_us=1000.0):
    """短唤醒：同一 tid 上相邻事件恰为 sleep/wake 且间隔 ≤ max_gap_us（线程空转轮询对）。

    按**较早那个事件**所在核归属。返回 DataFrame(core_id, dur_us, first_kind)。
    """
    rows = []
    for _, g in df.groupby("tid", sort=False):
        ev = list(g.itertuples(index=False))
        for a, b in zip(ev, ev[1:]):
            if {a.kind, b.kind} == {"sleep", "wake"} and (b.t_us - a.t_us) <= max_gap_us:
                rows.append((str(int(a.cpu)), float(b.t_us - a.t_us), a.kind))
    return pd.DataFrame(rows, columns=["core_id", "dur_us", "first_kind"])


def cpu_overhead_breakdown(df_raw, df_jobs, df_seg, max_short_us=1000.0,
                           include_exec_gap=True, verbose=True):
    """各 CPU 的额外开销占比。

    计算开销 `compute` = 该核上算法段的真实占核时间（Σ seg.dur）——已经明确，不重复计入。

    额外开销 = 短唤醒(sleep↔wake，间隔 ≤ max_short_us)
             + wake→release
             + release→execute（抢占/排队）
             + execute→finished 的**非计算**部分（跨段间隙 + 发布收尾）
             + finished→(之后第一个 wake 或 sleep)

    归属核：前两段记 release 所在核、execute→finished 记 execute 所在核、
    收尾段记 finished 所在核、短唤醒记较早事件的核。
    `include_exec_gap=False` 时不把 execute→finished 的非计算部分计入。

    返回每核一行的 DataFrame（含各阶段与占比），verbose=True 时打印百分比表。
    """
    if df_jobs is None or df_jobs.empty:
        print("无 job 数据")
        return pd.DataFrame()

    wall = float(df_raw["t_us"].max() - df_raw["t_us"].min())
    short = _short_wakeups(df_raw, max_short_us)

    j = df_jobs.copy()
    j["exc_fin"] = j["exec_2_route_lat"] + j["algo_exec_span"]  # finished − execute
    j["fin_next"] = (j["thread_post_exec_lat"] - j["exec_2_route_lat"]).clip(lower=0)  # post_end − finished

    def s(col, key):
        return j.groupby(j[key].astype(str))[col].sum().to_dict()

    stages = {
        "wake_rel": s("wake_2_held_lat", "rel_cpu"),
        "rel_exec": s("held_2_exec_lat", "rel_cpu"),
        "exc_fin": s("exc_fin", "exc_cpu"),
        "fin_next": s("fin_next", "fin_cpu"),
        "short_wake": short.groupby("core_id")["dur_us"].sum().to_dict() if len(short) else {},
        "fin_next_wake": s("fin_next", "fin_cpu"),  # 收尾段（端点可能是 wake 或 sleep）
    }
    compute = df_seg.groupby("core_id")["dur_us"].sum().to_dict() if len(df_seg) else {}

    cores = sorted(set(stages["exc_fin"]) | set(stages["wake_rel"]) | set(stages["fin_next"])
                   | set(compute) | set(stages["short_wake"]), key=lambda x: _algo_num(x))

    rec = []
    for c in cores:
        src = {k: float(v.get(c, 0.0) or 0.0) for k, v in stages.items()}
        comp = float(compute.get(c, 0.0))
        gap = max(0.0, src["exc_fin"] - comp)  # job 内的非计算时间
        ovh = src["short_wake"] + src["wake_rel"] + src["rel_exec"] + src["fin_next"]
        if include_exec_gap:
            ovh += gap
        rec.append(dict(core_id=c, wall_us=wall, compute_us=comp, compute_pct=comp / wall * 100,
                        short_wake_us=src["short_wake"], wake_rel_us=src["wake_rel"],
                        rel_exec_us=src["rel_exec"], exc_fin_us=src["exc_fin"],
                        exec_gap_us=gap, fin_next_us=src["fin_next"],
                        overhead_us=ovh, overhead_pct=ovh / wall * 100,
                        other_pct=(wall - comp - ovh) / wall * 100))
    df = pd.DataFrame(rec).sort_values("overhead_pct", ascending=False).reset_index(drop=True)

    if verbose:
        t = df.copy()
        for c in [x for x in t.columns if x.endswith("_us")]:
            t[c[: -len("_us")]] = (t[c] / 1000).round(2)  # µs -> ms
        t = t.drop(columns=[x for x in t.columns if x.endswith("_us")])
        print(f"额外开销占比（分母 = 墙钟 {wall / 1000:.0f} ms；"
              f"短唤醒阈值 {max_short_us:.0f} µs，{'计入' if include_exec_gap else '不计入'} execute→finished 的非计算部分）")
        print(t.round(3).to_string(index=False))
        fin_next, ovh = df["fin_next_us"].sum(), df["overhead_us"].sum()
        if ovh > 0 and fin_next > 0.5 * ovh and fin_next > wall:
            print("⚠️ 收尾段 finished→wake/sleep 占了大头：这份 trace 的 sleep/wake 是线程级轮询，"
                  "该段量到的是线程空等而不是 job 开销。\n"
                  "   要么把这段从额外开销里剔除（只看短唤醒/wake→release/release→execute/exc→fin 间隙），"
                  "要么改用 FINS 标准化后的 trace。")
    return df


def draw_cpu_overhead(df_ovh, title=None):
    """各 CPU 额外开销占比堆叠图（占比 %，分母为墙钟）。"""
    if df_ovh is None or df_ovh.empty:
        print("无数据")
        return None
    stages = [("short_wake_us", "短唤醒 sleep↔wake"), ("wake_rel_us", "wake→release"),
              ("rel_exec_us", "release→execute(抢占)"), ("exec_gap_us", "execute→finished 非计算"),
              ("fin_next_us", "finished→wake/sleep")]
    long = df_ovh.melt(id_vars=["core_id", "wall_us"], value_vars=[k for k, _ in stages],
                       var_name="stage", value_name="us")
    long["pct"] = long["us"] / long["wall_us"] * 100
    cmap = dict(zip([k for k, _ in stages],
                    ["#D6A531", "#D4834A", "#A6192E", "#925E9F", "#8CAA8C"]))
    fig = px.bar(long, x="pct", y="core_id", color="stage", orientation="h", barmode="stack",
                 color_discrete_map=cmap, category_orders={"core_id": list(df_ovh["core_id"])},
                 title=title or "Per-core Overhead Breakdown (% of wall clock)",
                 labels={"pct": "% of wall clock", "core_id": "Core", "stage": ""})
    fig.update_layout(plot_bgcolor="white", height=180 + 60 * len(df_ovh),
                      legend=dict(orientation="h", y=-0.25))
    return fig
