from pathlib import Path
import pandas as pd
import plotly.express as px

# ===== 目标 worker 的 comm 前缀 =====
WORKER_PREFIXES = (
    "fins_worker",
    "cie_container",
    "mte_container",
)


def _is_worker(comm_str: str) -> bool:
    return comm_str.startswith(WORKER_PREFIXES)


def _merge_intervals(intervals):
    """合并重叠/相邻区间，返回排序后的 [(s, e), ...]。"""
    if not intervals:
        return []
    intervals = sorted(intervals, key=lambda x: x[0])
    merged = [intervals[0]]
    for s, e in intervals[1:]:
        ps, pe = merged[-1]
        if s <= pe:
            merged[-1] = (ps, max(pe, e))
        else:
            merged.append((s, e))
    return merged


def _sum_merged(intervals):
    return sum(e - s for s, e in _merge_intervals(intervals))


def analyze_cpu_utilization(
    preempt_csv: str = "feedback_u70_m3_ms05_s20632672_135004_preempt.csv",
    timeline_csv: str = "feedback_u70_m3_ms05_s20632672_135004_timeline.csv",
    analysis_window_ms=None,
    output_html: str = "cpu_utilization_breakdown.html",
):
    """
    CPU 利用率细分图（逐核 + AVG 汇总）：

      Active   = worker 执行 job 的时间（jobs ∩ worker_time，按 CPU 消重）
      Overhead = worker 在跑但不在任何 job 里的时间（worker_time − Active）
      Idle     = 总时间 − worker_time（非 worker 的一切）

    每核柱内三类百分比相加 = 100%。
    AVG 行用全局加权：Σ时间 / Σ总时间，而不是百分比简单平均。
    """
    print("正在读取数据并计算利用率占比...")
    df_preempt = pd.read_csv(preempt_csv)
    df_timeline = pd.read_csv(timeline_csv)

    # ---- 统一时间基准 ----
    preempt_t0 = df_preempt["t_us"].min()
    timeline_t0 = df_timeline["t_us"].min()

    def preempt_ms(ts_us):
        return (ts_us - preempt_t0) / 1000.0

    def timeline_ms(ts_us):
        return (ts_us - timeline_t0) / 1000.0

    window_start_ms, window_end_ms = (
        analysis_window_ms if analysis_window_ms else (-float("inf"), float("inf"))
    )

    def clip(s_ms, e_ms):
        """按窗口裁剪，返回 (eff_s, eff_e)；不相交返回 None。"""
        if e_ms < window_start_ms or s_ms > window_end_ms:
            return None
        eff_s = max(s_ms, window_start_ms) if analysis_window_ms else s_ms
        eff_e = min(e_ms, window_end_ms) if analysis_window_ms else e_ms
        if eff_e <= eff_s:
            return None
        return eff_s, eff_e

    all_cpus = sorted(df_preempt["cpu"].unique())

    # ============================================================
    # 1. 一次扫描 preempt：按 CPU 切片，收集 worker 片段，
    #    统计 total / worker 时间
    # ============================================================
    cpu_worker_segments = {cpu: [] for cpu in all_cpus}
    cpu_total_ms = {cpu: 0.0 for cpu in all_cpus}
    cpu_worker_ms = {cpu: 0.0 for cpu in all_cpus}

    for cpu in all_cpus:
        group = df_preempt[df_preempt["cpu"] == cpu].sort_values("t_us")
        times = group["t_us"].values
        comms = group["next_comm"].values
        tids = group["next_tid"].values

        for i in range(len(times) - 1):
            s_ms = preempt_ms(times[i])
            e_ms = preempt_ms(times[i + 1])
            clipped = clip(s_ms, e_ms)
            if clipped is None:
                continue
            eff_s, eff_e = clipped
            dur_ms = eff_e - eff_s
            cpu_total_ms[cpu] += dur_ms

            if _is_worker(str(comms[i])):
                cpu_worker_ms[cpu] += dur_ms
                cpu_worker_segments[cpu].append({
                    "cpu_id": int(cpu),
                    "start_ms": eff_s,
                    "end_ms": eff_e,
                    "tid": int(tids[i]),
                })

    # ============================================================
    # 2. 从 timeline 提取 job (execute -> complete)
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
        matched_comp = comp_df[(comp_df["tid"] == tid) & (comp_df["t_us"] >= t_start)]
        if matched_comp.empty:
            continue

        j_start_ms = timeline_ms(t_start)
        j_end_ms = timeline_ms(matched_comp.iloc[0]["t_us"])
        clipped = clip(j_start_ms, j_end_ms)
        if clipped is None:
            continue
        jobs.append({
            "tid": tid,
            "job_start_ms": clipped[0],
            "job_end_ms": clipped[1],
        })

    df_jobs = pd.DataFrame(jobs)

    # ============================================================
    # 3. Active = jobs ∩ worker_segments，按 CPU 收集后消重
    # ============================================================
    cpu_active_segments = {cpu: [] for cpu in all_cpus}

    if not df_jobs.empty:
        for cpu in all_cpus:
            worker_segs = cpu_worker_segments[cpu]
            if not worker_segs:
                continue
            worker_tids = {seg["tid"] for seg in worker_segs}
            cpu_jobs = df_jobs[df_jobs["tid"].isin(worker_tids)]
            for _, job in cpu_jobs.iterrows():
                tid = job["tid"]
                j_start = job["job_start_ms"]
                j_end = job["job_end_ms"]
                for block in worker_segs:
                    if block["tid"] != tid:
                        continue
                    seg_start = max(j_start, block["start_ms"])
                    seg_end = min(j_end, block["end_ms"])
                    if seg_end > seg_start:
                        cpu_active_segments[cpu].append((seg_start, seg_end))

    cpu_active_ms = {cpu: _sum_merged(cpu_active_segments[cpu]) for cpu in all_cpus}

    # ============================================================
    # 4. 逐核计算三分类百分比
    #    Active   = cpu_active_ms
    #    Overhead = cpu_worker_ms − cpu_active_ms
    #    Idle     = cpu_total_ms  − cpu_worker_ms
    # ============================================================
    breakdown_data = []
    cpu_summary = {}   # cpu -> (total, active, overhead, idle)

    for cpu in all_cpus:
        tot = cpu_total_ms[cpu]
        if tot <= 0:
            continue
        worker_ms = cpu_worker_ms[cpu]
        active_ms = min(cpu_active_ms.get(cpu, 0.0), worker_ms)
        overhead_ms = max(0.0, worker_ms - active_ms)
        idle_ms = max(0.0, tot - worker_ms)

        cpu_summary[cpu] = (tot, active_ms, overhead_ms, idle_ms)

        breakdown_data.append({
            "CPU": f"CPU {cpu}",
            "Category": "Active",
            "Percentage": (active_ms / tot) * 100,
            "Time_ms": active_ms,
            "Total_ms": tot,
        })
        breakdown_data.append({
            "CPU": f"CPU {cpu}",
            "Category": "Overhead",
            "Percentage": (overhead_ms / tot) * 100,
            "Time_ms": overhead_ms,
            "Total_ms": tot,
        })
        breakdown_data.append({
            "CPU": f"CPU {cpu}",
            "Category": "Idle",
            "Percentage": (idle_ms / tot) * 100,
            "Time_ms": idle_ms,
            "Total_ms": tot,
        })

    # ============================================================
    # 5. AVG 行：全局加权（Σ时间 / Σ总时间）
    # ============================================================
    total_all = sum(v[0] for v in cpu_summary.values())
    active_all = sum(v[1] for v in cpu_summary.values())
    overhead_all = sum(v[2] for v in cpu_summary.values())
    idle_all = sum(v[3] for v in cpu_summary.values())

    if total_all > 0:
        breakdown_data.append({
            "CPU": "AVG",
            "Category": "Active",
            "Percentage": (active_all / total_all) * 100,
            "Time_ms": active_all,
            "Total_ms": total_all,
        })
        breakdown_data.append({
            "CPU": "AVG",
            "Category": "Overhead",
            "Percentage": (overhead_all / total_all) * 100,
            "Time_ms": overhead_all,
            "Total_ms": total_all,
        })
        breakdown_data.append({
            "CPU": "AVG",
            "Category": "Idle",
            "Percentage": (idle_all / total_all) * 100,
            "Time_ms": idle_all,
            "Total_ms": total_all,
        })

    df_breakdown = pd.DataFrame(breakdown_data)

    # ============================================================
    # 6. 排序：各核按 Active% 降序，AVG 固定最后
    # ============================================================
    active_order = (
        df_breakdown[df_breakdown["Category"] == "Active"]
        .sort_values("Percentage", ascending=False)["CPU"]
        .tolist()
    )
    active_order = [c for c in active_order if c != "AVG"]
    y_order = active_order + (["AVG"] if total_all > 0 else [])

    # ============================================================
    # 7. 绘图
    # ============================================================
    fig = px.bar(
        df_breakdown,
        x="Percentage",
        y="CPU",
        color="Category",
        orientation="h",
        category_orders={
            "Category": ["Active", "Overhead", "Idle"],
            "CPU": y_order,
        },
        color_discrete_map={
            "Active": "#2C5F7A",
            "Overhead": "#D4834A",
            "Idle": "#8CAA8C",
        },
        custom_data=["Category", "Time_ms", "Total_ms"],
        title="<b>CPU Core Utilization Breakdown (per-core + AVG)</b>",
    )

    fig.update_traces(
        hovertemplate=(
            "%{y} / %{customdata[0]}<br>"
            "share: %{x:.2f}%<br>"
            "time : %{customdata[1]:.1f} ms<br>"
            "total: %{customdata[2]:.1f} ms<extra></extra>"
        )
    )

    fig.update_layout(
        barmode="stack",
        plot_bgcolor="white",
        paper_bgcolor="white",
        xaxis=dict(title="<b>Percentage (%)</b>", range=[0, 105]),
        yaxis=dict(title="<b>CPU Core</b>"),
        template="plotly_white",
        height=220 + 40 * len(y_order),
        legend=dict(
            orientation="v",
            yanchor="middle",
            y=0.5,
            xanchor="left",
            x=1.02,
            title=None,
        ),
    )

    # AVG 行稍微加粗 / 换个底色，方便区分（可选）
    fig.update_yaxes(
        tickmode="array",
        tickvals=y_order,
        ticktext=[
            f"<b>AVG</b>" if c == "AVG" else c for c in y_order
        ],
    )

    fig.write_html(output_html)
    print(f"利用率占比图已保存至: {output_html}")

    # ============================================================
    # 8. 控制台打印：逐核 + 全局
    # ============================================================
    print("\n逐核利用率：")
    for cpu in active_order:
        tot, a, o, i = cpu_summary[int(cpu.split()[1])]
        print(f"  {cpu:>6}  Active {a/tot*100:6.2f}%  "
              f"Overhead {o/tot*100:6.2f}%  Idle {i/tot*100:6.2f}%   "
              f"(total {tot:.1f} ms)")

    if total_all > 0:
        print("\n全局加权：")
        print(f"  Active   {active_all/total_all*100:6.2f}%  "
              f"({active_all:.1f} ms / {total_all:.1f} ms)")
        print(f"  Overhead {overhead_all/total_all*100:6.2f}%  "
              f"({overhead_all:.1f} ms / {total_all:.1f} ms)")
        print(f"  Idle     {idle_all/total_all*100:6.2f}%  "
              f"({idle_all:.1f} ms / {total_all:.1f} ms)")

    return fig


if __name__ == "__main__":
    analyze_cpu_utilization(
        preempt_csv="feedback_u70_m3_ms05_s20632672_135004_preempt.csv",
        timeline_csv="feedback_u70_m3_ms05_s20632672_135004_timeline.csv",
        analysis_window_ms=None,
        output_html="cpu_utilization_breakdown.html",
    )