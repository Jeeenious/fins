from pathlib import Path
from collections import defaultdict

import pandas as pd
import plotly.express as px

# 目标 worker 的 comm 前缀（按实验场景选一个或全开）
WORKER_PREFIXES = (
    "fins_worker",
    "cie_container",
    "mte_container",
)


def _is_worker(comm_str: str) -> bool:
    return comm_str.startswith(WORKER_PREFIXES)


def _subtract_intervals(block_start, block_end, occupied):
    """
    从 [block_start, block_end] 中减去 occupied 里的所有区间。
    occupied: list of (start, end)，需已排序、互不重叠。
    返回剩余子区间列表 [(s, e), ...]。
    """
    remaining = []
    cursor = block_start
    for s, e in occupied:
        if e <= cursor:
            continue
        if s >= block_end:
            break
        if s > cursor:
            remaining.append((cursor, min(s, block_end)))
        cursor = max(cursor, e)
        if cursor >= block_end:
            break
    if cursor < block_end:
        remaining.append((cursor, block_end))
    return remaining


def generate_execution_gantt(
        preempt_csv: str,
        timeline_csv: str,
        zoom_window_ms=None,
        output_html: str = "cpu_activate_timeline.html",
        show_overhead: bool = True,
        overhead_color: str = "#FFB347",
):
    """
    生成 CPU 执行甘特图：
      - 有色块 = 目标 worker 执行 job 的时间段
      - 橙色块 = 目标 worker 在 CPU 上但不属于任何 job 的时间（Overhead）
      - 非 worker 的时间不画（留白）
    """
    print("正在读取数据 (Gantt)...")
    df_preempt = pd.read_csv(preempt_csv)
    df_timeline = pd.read_csv(timeline_csv)

    # ---- 统一时间基准 ----
    # 两个 CSV 应共享同一 t0，且第一行 t_us 已归零。
    # 为稳妥，仍以两者各自最小值为基准做一次线性对齐。
    preempt_t0 = df_preempt["t_us"].min()
    timeline_t0 = df_timeline["t_us"].min()

    def preempt_ms(ts_us):
        return (ts_us - preempt_t0) / 1000.0

    def timeline_ms(ts_us):
        return (ts_us - timeline_t0) / 1000.0

    # ============================================================
    # 1. 从 preempt.csv 构造每个 CPU 上"目标 worker"的运行片段
    # ============================================================
    cpu_intervals = []   # 只保留目标 worker 的片段

    for cpu, group in df_preempt.groupby("cpu"):
        group = group.sort_values("t_us")
        times = group["t_us"].values
        comms = group["next_comm"].values
        tids = group["next_tid"].values

        for i in range(len(times) - 1):
            start_ms = preempt_ms(times[i])
            end_ms = preempt_ms(times[i + 1])
            dur_ms = end_ms - start_ms
            if dur_ms <= 0:
                continue

            comm_str = str(comms[i])
            if not _is_worker(comm_str):
                continue    # 非 worker 不画

            cpu_intervals.append({
                "cpu_id": int(cpu),
                "start_ms": start_ms,
                "end_ms": end_ms,
                "dur_ms": dur_ms,
                "tid": int(tids[i]),
                "comm": comm_str,
            })

    df_cpu_blocks = pd.DataFrame(cpu_intervals)
    if df_cpu_blocks.empty:
        print("⚠️ 没有匹配到任何目标 worker 的 sched_switch 片段，图将为空。")

    # ============================================================
    # 2. 从 timeline.csv 提取 job 生命周期 (execute → complete)
    # ============================================================
    def clean_tag(val):
        if pd.isna(val):
            return None
        return str(val).split()[0]

    df_timeline["clean_tag"] = df_timeline["tag"].apply(clean_tag)
    exec_df = df_timeline[df_timeline["kind"] == "execute"].sort_values("t_us")
    comp_df = df_timeline[df_timeline["kind"] == "complete"].sort_values("t_us")

    jobs = []
    for _, ex_row in exec_df.iterrows():
        tid = int(ex_row["tid"])
        t_start = ex_row["t_us"]
        algo = ex_row["clean_tag"]
        if not algo:
            continue

        matched_comp = comp_df[
            (comp_df["tid"] == tid) & (comp_df["t_us"] >= t_start)
        ]
        if matched_comp.empty:
            continue

        t_end = matched_comp.iloc[0]["t_us"]
        jobs.append({
            "tid": tid,
            "algo": algo,
            "job_start_ms": timeline_ms(t_start),
            "job_end_ms": timeline_ms(t_end),
        })

    df_jobs = pd.DataFrame(jobs)

    # ============================================================
    # 3. 任务块 = jobs ∩ cpu_intervals
    # ============================================================
    refined_segments = []
    if not df_jobs.empty and not df_cpu_blocks.empty:
        for _, job in df_jobs.iterrows():
            tid = job["tid"]
            algo = job["algo"]
            j_start = job["job_start_ms"]
            j_end = job["job_end_ms"]

            matched_blocks = df_cpu_blocks[
                (df_cpu_blocks["tid"] == tid)
                & (df_cpu_blocks["end_ms"] > j_start)
                & (df_cpu_blocks["start_ms"] < j_end)
            ]
            for _, block in matched_blocks.iterrows():
                seg_start = max(j_start, block["start_ms"])
                seg_end = min(j_end, block["end_ms"])
                if seg_end > seg_start:
                    refined_segments.append({
                        "core_id": f"CPU {int(block['cpu_id'])}",
                        "raw_core": int(block["cpu_id"]),
                        "start_ms": seg_start,
                        "end_ms": seg_end,
                        "dur_ms": seg_end - seg_start,
                        "algo": algo,
                        "tid": tid,
                    })

    df_final_jobs = pd.DataFrame(refined_segments)

    # ============================================================
    # 4. Overhead = worker 片段 - 任务块
    # ============================================================
    df_overhead = pd.DataFrame()
    if show_overhead and not df_cpu_blocks.empty:
        # 按 tid 收集并合并 job 交集片段
        jobs_by_tid = defaultdict(list)
        if not df_final_jobs.empty:
            for _, seg in df_final_jobs.iterrows():
                jobs_by_tid[int(seg["tid"])].append((seg["start_ms"], seg["end_ms"]))

        for tid in jobs_by_tid:
            jobs_by_tid[tid].sort()
            merged = []
            for s, e in jobs_by_tid[tid]:
                if merged and s <= merged[-1][1]:
                    merged[-1] = (merged[-1][0], max(merged[-1][1], e))
                else:
                    merged.append((s, e))
            jobs_by_tid[tid] = merged

        overhead_segments = []
        for _, block in df_cpu_blocks.iterrows():
            tid = int(block["tid"])
            occupied = jobs_by_tid.get(tid, [])
            for s, e in _subtract_intervals(block["start_ms"], block["end_ms"], occupied):
                if e > s:
                    overhead_segments.append({
                        "core_id": f"CPU {block['cpu_id']}",
                        "raw_core": int(block["cpu_id"]),
                        "start_ms": s,
                        "end_ms": e,
                        "dur_ms": e - s,
                        "algo": "Overhead",
                        "tid": tid,
                    })
        df_overhead = pd.DataFrame(overhead_segments)

    # ============================================================
    # 5. 组装绘图数据
    # ============================================================
    parts = []
    if not df_overhead.empty:
        parts.append(df_overhead)
    if not df_final_jobs.empty:
        parts.append(df_final_jobs)

    if not parts:
        print("⚠️ 没有可绘制的区间（任务块与 Overhead 均为空）。")
        return None

    df_plot_all = pd.concat(parts, ignore_index=True)

    # ============================================================
    # 6. 配色与图例顺序
    # ============================================================
    discrete_sci_colors = [
        "#3B5998", "#5E8B61", "#A6192E", "#D6A531", "#709BFF",
        "#925E9F", "#0099B4", "#FDAF91", "#4D4D4D", "#ADB6B6",
    ]

    # CPU 泳道顺序
    if not df_cpu_blocks.empty:
        core_order = sorted(df_cpu_blocks["cpu_id"].unique())
    else:
        core_order = sorted(df_plot_all["raw_core"].unique())
    core_lane_order = [f"CPU {c}" for c in core_order]

    # 算法颜色
    all_algo_order = sorted(df_final_jobs["algo"].unique()) if not df_final_jobs.empty else []
    all_algo_colors = {
        a: discrete_sci_colors[i % len(discrete_sci_colors)]
        for i, a in enumerate(all_algo_order)
    }
    all_algo_colors["Overhead"] = overhead_color

    # 图例顺序：Overhead 在最后（画在最上层，视觉上不会被任务块盖住也不影响）
    full_algo_order = all_algo_order + (["Overhead"] if show_overhead and not df_overhead.empty else [])

    # ============================================================
    # 7. 绘图
    # ============================================================
    fig = px.bar(
        df_plot_all,
        base="start_ms",
        x="dur_ms",
        y="core_id",
        color="algo",
        orientation="h",
        opacity=0.9,
        color_discrete_map=all_algo_colors,
        category_orders={"core_id": core_lane_order, "algo": full_algo_order},
        custom_data=df_plot_all[["algo", "start_ms", "dur_ms", "end_ms", "tid"]],
    )

    fig.update_traces(
        hovertemplate=(
            "algo: %{customdata[0]}<br>"
            "tid: %{customdata[4]}<br>"
            "start: %{customdata[1]:.3f} ms<br>"
            "dura: %{customdata[2]:.3f} ms<br>"
            "end: %{customdata[3]:.3f} ms<extra></extra>"
        )
    )

    layout_kwargs = dict(
        barmode="overlay",
        plot_bgcolor="white",
        paper_bgcolor="white",
        height=200 + 40 * len(core_lane_order),
        yaxis_title="<b>CPU Core</b>",
        xaxis_title="<b>Time (ms from Trace Start)</b>",
        title="<b>CPU Execution Timeline (Task + Overhead)</b>",
        template="plotly_white",
        legend=dict(
            orientation="v",
            yanchor="middle",
            y=0.5,
            xanchor="left",
            x=1.02,
            title=None,
        ),
    )

    if zoom_window_ms is not None:
        layout_kwargs["xaxis"] = dict(
            range=[zoom_window_ms[0], zoom_window_ms[1]],
            title="<b>Time (ms from Trace Start)</b>",
        )

    fig.update_layout(**layout_kwargs)
    fig.write_html(output_html)
    print(f"甘特图已保存至: {output_html}")

    # 顺便打印一下统计
    total_task_ms = df_final_jobs["dur_ms"].sum() if not df_final_jobs.empty else 0.0
    total_over_ms = df_overhead["dur_ms"].sum() if not df_overhead.empty else 0.0
    total_worker_ms = total_task_ms + total_over_ms
    if total_worker_ms > 0:
        print(f"  worker 总 CPU 时间 : {total_worker_ms:.3f} ms")
        print(f"    - 任务    : {total_task_ms:.3f} ms ({total_task_ms / total_worker_ms * 100:.1f}%)")
        print(f"    - Overhead: {total_over_ms:.3f} ms ({total_over_ms / total_worker_ms * 100:.1f}%)")

    return fig


if __name__ == "__main__":
    generate_execution_gantt(
        preempt_csv="feedback_u70_m3_ms05_s20632672_135004_preempt.csv",
        timeline_csv="feedback_u70_m3_ms05_s20632672_135004_timeline.csv",
        zoom_window_ms=None,
        output_html="cpu_activate_timeline.html",
        show_overhead=True,
    )