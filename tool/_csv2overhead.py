from pathlib import Path
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# WORKER = "fins_worker"
WORKER = "cie_container"
# WORKER = "mte_container"

def analyze_cpu_utilization(
    preempt_csv: str = "feedback_u70_m3_ms05_s20632672_135004_preempt.csv",
    timeline_csv: str = "feedback_u70_m3_ms05_s20632672_135004_timeline.csv",
    analysis_window_ms=None,
    output_html: str = "cpu_utilization_breakdown.html",
):
  """计算并绘制消重后的 CPU 利用率占比横向细分图（无柱内百分比文本，按 Active -> Overhead -> Idle 从左到右堆叠）"""
  print("正在读取数据并计算利用率占比...")
  df_preempt = pd.read_csv(preempt_csv)
  df_timeline = pd.read_csv(timeline_csv)

  global_t0 = df_timeline["t_us"].min()

  def to_ms(ts_us):
    return (ts_us - global_t0) / 1000.0

  window_start_ms, window_end_ms = (
      analysis_window_ms if analysis_window_ms else (-float("inf"), float("inf"))
  )

  # 1. 建立内核 CPU 区间
  cpu_intervals = []
  cpu_total_stats = {}
  all_cpus = sorted(df_preempt["cpu"].unique())

  for cpu in all_cpus:
    group = df_preempt[df_preempt["cpu"] == cpu].sort_values("t_us")
    times = group["t_us"].values
    comms = group["next_comm"].values
    tids = group["next_tid"].values

    tot_us = 0
    for i in range(len(times) - 1):
      s_ms, e_ms = to_ms(times[i]), to_ms(times[i + 1])
      if not (e_ms < window_start_ms or s_ms > window_end_ms):
        eff_s = (
            max(s_ms, window_start_ms) if analysis_window_ms else s_ms
        )
        eff_e = (
            min(e_ms, window_end_ms) if analysis_window_ms else e_ms
        )
        dur_ms = eff_e - eff_s
        if dur_ms > 0:
          tot_us += dur_ms * 1000.0
          if "swapper" not in str(comms[i]): # 感觉这里应该是所有的非 worker
            cpu_intervals.append({
                "cpu_id": int(cpu),
                "start_ms": eff_s,
                "end_ms": eff_e,
                "tid": int(tids[i]),
                "comm": comms[i],
            })
    cpu_total_stats[cpu] = tot_us

  df_cpu_blocks = pd.DataFrame(cpu_intervals)

  # 2. 提取应用层 Job
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
      j_start_ms, j_end_ms = to_ms(t_start), to_ms(matched_comp.iloc[0]["t_us"])
      if not (j_end_ms < window_start_ms or j_start_ms > window_end_ms):
        eff_j_start = (
            max(j_start_ms, window_start_ms) if analysis_window_ms else j_start_ms
        )
        eff_j_end = (
            min(j_end_ms, window_end_ms) if analysis_window_ms else e_ms
        )  # safe check
        if eff_j_end > eff_j_start:
          jobs.append(
              {"tid": tid, "job_start_ms": eff_j_start, "job_end_ms": eff_j_end}
          )
  df_jobs = pd.DataFrame(jobs)

  # 3. 收集并消重合并
  cpu_active_segments = {cpu: [] for cpu in all_cpus}
  if not df_jobs.empty and not df_cpu_blocks.empty:
    for _, job in df_jobs.iterrows():
      tid, j_start, j_end = job["tid"], job["job_start_ms"], job["job_end_ms"]
      matched_blocks = df_cpu_blocks[
          (df_cpu_blocks["tid"] == tid)
          & (df_cpu_blocks["end_ms"] > j_start)
          & (df_cpu_blocks["start_ms"] < j_end)
      ]
      for _, block in matched_blocks.iterrows():
        seg_start = max(j_start, block["start_ms"])
        seg_end = min(j_end, block["end_ms"])
        if seg_end > seg_start:
          cpu_active_segments[block["cpu_id"]].append((seg_start, seg_end))

  def merge_intervals(intervals):
    if not intervals:
      return []
    intervals.sort(key=lambda x: x[0])
    merged = [intervals[0]]
    for current in intervals[1:]:
      prev_start, prev_end = merged[-1]
      curr_start, curr_end = current
      if curr_start <= prev_end:
        merged[-1] = (prev_start, max(prev_end, curr_end))
      else:
        merged.append(current)
    return merged

  cpu_active_duration_ms = {}
  for cpu in all_cpus:
    merged_segs = merge_intervals(cpu_active_segments[cpu])
    cpu_active_duration_ms[cpu] = sum([e - s for s, e in merged_segs])

  # 4. 计算百分比
  breakdown_data = []

  for cpu in all_cpus:
    tot_ms = cpu_total_stats[cpu] / 1000.0
    if tot_ms <= 0:
      continue
    active_ms = cpu_active_duration_ms.get(cpu, 0.0)

    idle_ms = 0
    cpu_group = df_preempt[df_preempt["cpu"] == cpu].sort_values("t_us")
    times = cpu_group["t_us"].values
    comms = cpu_group["next_comm"].values
    for i in range(len(times) - 1):
      s_ms, e_ms = to_ms(times[i]), to_ms(times[i + 1])
      if not (e_ms < window_start_ms or s_ms > window_end_ms):
        eff_s = (
            max(s_ms, window_start_ms) if analysis_window_ms else s_ms
        )
        eff_e = (
            min(e_ms, window_end_ms) if analysis_window_ms else e_ms
        )

        if eff_e > eff_s:
          comm_name = str(comms[i])
          if not comm_name.startswith(WORKER):
            idle_ms += eff_e - eff_s

    overhead_ms = max(0.0, tot_ms - active_ms - idle_ms)

    breakdown_data.append({
        "CPU": f"CPU {cpu}",
        "Category": "Active",
        "Percentage": (active_ms / tot_ms) * 100,
    })
    breakdown_data.append({
        "CPU": f"CPU {cpu}",
        "Category": "Overhead",
        "Percentage": (overhead_ms / tot_ms) * 100,
    })
    breakdown_data.append({
        "CPU": f"CPU {cpu}",
        "Category": "Idle",
        "Percentage": (idle_ms / tot_ms) * 100,
    })

  df_breakdown = pd.DataFrame(breakdown_data)

  title_suffix = (
      f" (Window: {analysis_window_ms} ms)"
      if analysis_window_ms
      else " (Full Timeline)"
  )

  fig = px.bar(
      df_breakdown,
      x="Percentage",
      y="CPU",
      color="Category",
      orientation="h",
      category_orders={"Category": ["Active", "Overhead", "Idle"]},
      color_discrete_map={
          "Active": "#2C5F7A",
          "Overhead": "#D4834A",
          "Idle": "#8CAA8C",
      },
      title=(
          "<b>CPU Core Utilization Breakdown</b>"
      ),
  )

  fig.update_layout(
      barmode="stack",
      plot_bgcolor="white",
      paper_bgcolor="white",
      xaxis=dict(title="<b>Percentage (%)</b>", range=[0, 105]),
      yaxis=dict(title="<b>CPU Core</b>"),
      template="plotly_white",
      height=200 + 40 * len(all_cpus),
      legend=dict(
          orientation="v",
          yanchor="middle",
          y=0.5,
          xanchor="left",
          x=1.02,
          title=None,
      ),
  )

  fig.write_html(output_html)
  print(f"利用率占比图已保存至: {output_html}")
  return fig