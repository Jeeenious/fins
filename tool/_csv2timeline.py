from pathlib import Path
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

WORKER = "fins_worker"
# WORKER = "cie_container"

def generate_execution_gantt(
        preempt_csv: str = "feedback_u70_m3_ms05_s20632672_135004_preempt.csv",
        timeline_csv: str = "feedback_u70_m3_ms05_s20632672_135004_timeline.csv",
        zoom_window_ms=None,
        output_html: str = "cpu_activate_timeline.html",
):
    """生成对齐内核调度、同时展示应用层任务物理执行与 CPU Idle 状态的甘特图"""
    print("正在读取数据 (Gantt)...")
    df_preempt = pd.read_csv(preempt_csv)
    df_timeline = pd.read_csv(timeline_csv)

    global_t0 = df_timeline["t_us"].min()

    def to_ms(ts_us):
        return (ts_us - global_t0) / 1000.0

    # 1. 建立内核 CPU 运行区间表 (包括有效任务和 Idle/swapper)
    cpu_intervals = []
    idle_intervals = []

    for cpu, group in df_preempt.groupby("cpu"):
        group = group.sort_values("t_us")
        times = group["t_us"].values
        comms = group["next_comm"].values
        tids = group["next_tid"].values

        for i in range(len(times) - 1):
            start_us = times[i]
            end_us = times[i + 1]
            comm = comms[i]
            tid = tids[i]

            start_ms = to_ms(start_us)
            end_ms = to_ms(end_us)
            dur_ms = end_ms - start_ms
            comm_str = str(comm)

            if dur_ms > 0:
                if comm_str.startswith("fins_worker") or comm_str.startswith("cie_container") or comm_str.startswith("mte_container"):
                    cpu_intervals.append({
                        "cpu_id": int(cpu),
                        "start_ms": start_ms,
                        "end_ms": end_ms,
                        "dur_ms": dur_ms,
                        "tid": int(tid),
                        "comm": comm,
                    })
                else:
                    # ★ 修改处：任何非 fins_worker 的线程（包括 swapper、kworker、系统杂务等）统一归入 Idle/系统底噪
                    idle_intervals.append({
                        "core_id": f"CPU {int(cpu)}",
                        "start_ms": start_ms,
                        "end_ms": end_ms,
                        "dur_ms": dur_ms,
                        "algo": "Idle",
                    })

    df_cpu_blocks = pd.DataFrame(cpu_intervals)
    df_idle = pd.DataFrame(idle_intervals)

    # 2. 提取应用层 Job 的执行生命周期
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

        matched_comp = comp_df[(comp_df["tid"] == tid) & (comp_df["t_us"] >= t_start)]
        if not matched_comp.empty:
            t_end = matched_comp.iloc[0]["t_us"]
            jobs.append({
                "tid": tid,
                "algo": algo,
                "job_start_ms": to_ms(t_start),
                "job_end_ms": to_ms(t_end),
            })

    df_jobs = pd.DataFrame(jobs)

    # 3. 物理交集裁剪
    refined_segments = []
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

        if not matched_blocks.empty:
            for _, block in matched_blocks.iterrows():
                seg_start = max(j_start, block["start_ms"])
                seg_end = min(j_end, block["end_ms"])
                if seg_end > seg_start:
                    refined_segments.append({
                        "core_id": f"CPU {block['cpu_id']}",
                        "raw_core": block["cpu_id"],
                        "start_ms": seg_start,
                        "end_ms": seg_end,
                        "dur_ms": seg_end - seg_start,
                        "algo": algo,
                        "tid": tid,
                    })

    df_final_jobs = pd.DataFrame(refined_segments)
    if not df_final_jobs.empty:
        df_final_jobs["dur_ms"] = (
                df_final_jobs["end_ms"] - df_final_jobs["start_ms"]
        )

    # 4. 配色与图表绘制
    discrete_sci_colors = [
        "#3B5998",
        "#5E8B61",
        "#A6192E",
        "#D6A531",
        "#709BFF",
        "#925E9F",
        "#0099B4",
        "#FDAF91",
        "#4D4D4D",
        "#ADB6B6",
    ]

    core_order = sorted(df_cpu_blocks["cpu_id"].unique())
    core_lane_order = [f"CPU {c}" for c in core_order]

    # 算法列表与颜色映射（不含 Idle）
    all_algo_order = sorted(list(df_final_jobs["algo"].unique()))
    all_algo_colors = {
        a: discrete_sci_colors[i % len(discrete_sci_colors)]
        for i, a in enumerate(all_algo_order)
    }

    # 将 Idle 加入颜色映射（统一设为浅灰色）
    all_algo_colors["Idle"] = "#E5E5E5"
    full_algo_order = all_algo_order + ["Idle"]

    # 合并 Idle 数据和有效任务数据以便统一画图，或者通过多次 add_trace/px 叠加
    # 这里我们先用 df_idle 作为底图绘制 Idle，再用 px 绘制任务，或者直接组合 DataFrame
    df_plot_all = pd.concat([df_idle, df_final_jobs], ignore_index=True)

    fig = px.bar(
        df_plot_all,
        base="start_ms",
        x="dur_ms",
        y="core_id",
        color="algo",
        orientation="h",
        opacity=0.85,
        color_discrete_map=all_algo_colors,
        category_orders={"core_id": core_lane_order, "algo": full_algo_order},
        custom_data=df_plot_all[
            ["algo", "start_ms", "dur_ms", "end_ms"]
        ],
    )

    # 优化 Hover 提示（对 Idle 和 Task 分别处理或兼顾）
    fig.update_traces(
        hovertemplate=(
            "status/algo: %{customdata[0]}<br>"
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
        title="<b>CPU Execution Timeline (with Idle)</b>",
        template="plotly_white",
        legend=dict(
            orientation="v",
            yanchor="middle",
            y=0.5,
            xanchor="left",
            x=1.02,
            title=None
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
    return fig