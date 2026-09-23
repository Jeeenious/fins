"""Task load structural analysis (double donut): which tasks make up the workload and
how much load each one contributes.

Ring 1  Task Frequency Ratio      P_i = jobs of node i / total jobs
Ring 2  Effective Workload Ratio  C_i = load of node i / total load

Slices are always grouped by NODE ID, never by algorithm name: several nodes of the
same algorithm can differ by a large factor (measured 1.3 ms vs 9.7 ms of per-job load),
so merging them into one slice hides the structure. A pipeline cfg may still be passed,
but only to annotate the algorithm name next to each node id.

Load metric (exec_source):
  "cpu"  (default) = sum of algo:working.seg_us = THREAD CPU TIME, as reported by the
                     plugin after calibrating against CLOCK_THREAD_CPUTIME_ID.
                     Queueing (release -> execute) and preemption are both excluded:
                     neither consumes a core, so counting them inflates the "real load".
  "wall"           = t_complete - t_execute, a wall-clock span that DOES include
                     preemption. On an oversubscribed machine this can exceed core-time
                     (measured: 32 node threads on 2 cores -> 118% of core-time).

Category (2x2, threshold category_pct), computed PER NODE ID:
  critical  high freq + high load -- serial bottleneck (Amdahl)
  overhead  high freq + low load  -- overhead source (many small tasks)
  compute   low freq  + high load -- parallelization candidate
  minor     low freq  + low load
A group slice takes the majority category of its members and reports the k/n tally.
"""

import json
import os
import re
from collections import defaultdict

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# *_timeline.csv = seq,t_us,cpu,tid,kind,tag   (columns set by tool/_lttng2csv.py)
_TIMELINE_COLS = ("t_us", "tid", "kind", "tag")
_CATEGORY_COLORS = {
    "critical": "#3B7A9E",
    "overhead": "#E8A87C",
    "compute": "#C87D7D",
    "minor": "#8CAA8C",
}
_CATEGORY_NAMES = {
    "critical": "Critical Path (High Freq + High Load)",
    "overhead": "Overhead Source (High Freq + Low Load)",
    "compute": "Compute Intensive (Low Freq + High Load)",
    "minor": "Minor Tasks (Low Freq + Low Load)",
}
# algo:working tag looks like "n3 [CPU 1, 900 us]"; one per segment (more if it migrates)
_SEG_RE = re.compile(r"\[CPU\s*(-?\d+),\s*(\d+)\s*us\]")


def _require_columns(df, cols, path, which):
    missing = [c for c in cols if c not in df.columns]
    if not missing:
        return
    hint = (" -- these look like a preempt CSV; the two arguments are swapped?"
            if "next_comm" in df.columns else "")
    raise ValueError(
        f"{which} CSV '{path}' is missing required columns {missing}{hint}\n"
        f"  actual columns = {list(df.columns)}")


def node_algo_map(pipeline_json):
    """{node id: algorithm name} from a pipeline cfg (used only for annotation)."""
    with open(pipeline_json, encoding="utf-8") as f:
        doc = json.load(f)
    nodes = doc["nodes"] if isinstance(doc, dict) else doc
    return {n["id"]: n["name"] for n in nodes}


def _seg_us(raw_tag):
    """Parse the seg_us out of an algo:working tag 'n3 [CPU 1, 900 us]'."""
    m = _SEG_RE.search(str(raw_tag))
    return float(m.group(2)) if m else 0.0


def _pair_exec_complete(df):
    """Pair execute->complete per tid, accumulating algo:working segments in between.

    Returns [(node, t_exec, t_comp, cpu_us)] where
      t_comp - t_exec = wall-clock span (includes preemption)
      cpu_us          = sum of algo:working.seg_us = thread CPU time (preemption excluded)
                        -- the plugin reports this after calibrating against
                        CLOCK_THREAD_CPUTIME_ID, so it is the load metric that excludes
                        both queueing (release -> execute) and preemption.
    Partial pairs at either end of the export are dropped.
    """
    out = []
    ev = df[df["kind"].isin(("execute", "complete", "working"))].sort_values(["tid", "t_us"])
    for tid, g in ev.groupby("tid"):
        pend, acc = None, 0.0
        for kind, t_us, node, tag in zip(g["kind"], g["t_us"], g["clean_tag"], g["tag"]):
            t_us = float(t_us)
            if kind == "working":
                if pend is not None:
                    acc += _seg_us(tag)
            elif kind == "execute":
                pend, acc = (node, t_us), 0.0
            elif pend is not None:
                nd, te = pend
                pend = None
                if nd:
                    out.append((nd, te, t_us, acc))
    return out


def _categorize(p_i, c_i, thr):
    if p_i > thr and c_i > thr:
        return "critical"
    if p_i > thr:
        return "overhead"
    if c_i > thr:
        return "compute"
    return "minor"


def _nat_key(s):
    d = "".join(ch for ch in str(s) if ch.isdigit())
    return (0, int(d)) if d else (1, str(s))


def _donut(labels, values, colors, hovers, pull_mask, title):
    return go.Pie(
        labels=labels, values=values, hole=0.5,
        marker=dict(colors=colors, line=dict(color="white", width=2)),
        textinfo="label+percent", textposition="outside",
        textfont=dict(size=11, color="#2C3E50"),
        hoverinfo="text", hovertext=hovers, showlegend=False,
        pull=[0.03 if p else 0 for p in pull_mask], sort=False, name=title)


def analyze_task_composition(
        timeline_csv: str,
        pipeline_json: str = None,
        analysis_window_ms=None,
        exec_source: str = "cpu",
        category_pct: float = 5.0,
        top_n: int = None,
        rings=(True, True),
        output_html: str = "task_composition_donut.html",
        output_csv: str = None,
):
    """Task load structural analysis: two donut rings.

    @param timeline_csv  *_timeline.csv (needs algo:execute / algo:complete / algo:working)
    @param pipeline_json pipeline cfg, OPTIONAL. Slices are always grouped by node id;
                         this is only used to annotate the algorithm name of each node.
    @param analysis_window_ms (lo, hi) in ms relative to the trace start (None = all),
                         judged by the job's algo:execute timestamp.
    @param exec_source   "cpu" (default) = sum of algo:working.seg_us (thread CPU time,
                         queueing and preemption excluded); "wall" = t_complete - t_execute.
                         "wall" includes preemption, which is also waiting rather than work,
                         so "cpu" is the metric that matches the stated intent.
    @param category_pct  2x2 classification threshold in percent, for P and C separately.
    @param top_n         draw only the N largest slices, merge the rest into "others"
                         (None = draw all). Useful when grouping produces many slices.
    @param rings         on/off per ring, drawn side by side in ONE figure:
                         (frequency, workload). e.g. (True, False) keeps only the
                         frequency ring.
    @param output_html   output path of the figure
    @param output_csv    per-node stats table (None = skip). Writes <stem>_by_id.csv
                         plus, when the slices differ from the nodes, <stem>.
    @retval (plotly Figure, per-node DataFrame) -- a SINGLE figure, same shape as
            analyze_cpu_utilization / generate_execution_gantt / analyze_hop_latency
    """
    if top_n is not None and top_n < 1:
        raise ValueError(f"top_n must be >= 1 or None, got {top_n}")
    rings = tuple(bool(x) for x in rings)
    if len(rings) != 2:
        raise ValueError(f"rings must be two flags (frequency, workload), got {rings}")
    if exec_source not in ("cpu", "wall"):
        raise ValueError(f"exec_source must be 'cpu' or 'wall', got {exec_source!r}")

    window_us = None
    if analysis_window_ms is not None:
        lo_ms, hi_ms = analysis_window_ms
        if lo_ms >= hi_ms:
            raise ValueError(f"analysis_window_ms must be (lo, hi) with lo < hi, "
                             f"got {analysis_window_ms}")
        window_us = (lo_ms * 1000.0, hi_ms * 1000.0)

    print("Reading timeline and aggregating jobs...")
    df = pd.read_csv(timeline_csv)
    _require_columns(df, _TIMELINE_COLS, timeline_csv, "timeline")
    df["clean_tag"] = df["tag"].apply(lambda v: None if pd.isna(v) else str(v).split()[0])

    n2a = node_algo_map(pipeline_json) if pipeline_json else {}
    if pipeline_json:
        print(f"Pipeline cfg: {pipeline_json} (algorithm names used for annotation only)")

    jobs = _pair_exec_complete(df)
    if not jobs:
        raise ValueError(
            f"'{timeline_csv}' has no closed execute->complete pairs.\n"
            f"  check that algo:execute/complete are enabled "
            f"(_test.py CUSTOM_UST_EVENTS)")

    rows, n_window, n_bad = [], 0, 0
    for node, t_exec, t_comp, cpu_us in jobs:
        if window_us is not None and not (window_us[0] <= t_exec <= window_us[1]):
            n_window += 1
            continue
        lat = cpu_us if exec_source == "cpu" else t_comp - t_exec
        if lat < 0:
            n_bad += 1
            continue
        rows.append({"node": node, "algo_exec_lat": lat, "cpu_us": cpu_us})

    if not rows:
        lo = df["t_us"].min() / 1000.0
        hi = df["t_us"].max() / 1000.0
        raise ValueError(
            f"no usable jobs in the analysis window "
            f"(outside window: {n_window}, negative load: {n_bad}).\n"
            f"  trace time range {lo:.1f} ~ {hi:.1f} ms (ms from trace start)\n"
            f"  current window {analysis_window_ms} ms")

    df_jobs = pd.DataFrame(rows)
    total_jobs = len(df_jobs)
    total_lat = df_jobs["algo_exec_lat"].sum()
    active_us = float(df_jobs["cpu_us"].sum())

    # ---- classification: PER NODE ID, using that node's own share of the global totals ----
    id_stats = df_jobs.groupby("node").agg(count=("algo_exec_lat", "size"),
                                           total_lat=("algo_exec_lat", "sum"),
                                           mean_lat=("algo_exec_lat", "mean"),
                                           cpu_us=("cpu_us", "sum"))
    id_stats["P_i"] = id_stats["count"] / total_jobs * 100.0
    id_stats["C_i"] = id_stats["total_lat"] / total_lat * 100.0
    id_stats["category"] = [_categorize(p, c, category_pct)
                            for p, c in zip(id_stats["P_i"], id_stats["C_i"])]
    id_stats = id_stats.reset_index()
    id_stats["algo"] = [n2a.get(n, "") for n in id_stats["node"]]
    id_stats = id_stats.sort_values("node", key=lambda s: s.map(_nat_key)).reset_index(drop=True)
    node_cat = dict(zip(id_stats["node"], id_stats["category"]))

    # ---- slices == node ids (never merged by algorithm name) ----
    grp = id_stats[["node", "count", "total_lat", "mean_lat", "P_i", "C_i",
                    "category", "algo", "cpu_us"]].copy()
    grp["nodes"] = grp["node"].map(lambda n: [n])
    grp["cat_tally"] = "1/1"

    df_full = grp.copy()
    df_stats = grp
    if top_n is not None and len(grp) > top_n:
        srt = grp.sort_values("C_i", ascending=False)
        head, tail = srt.head(top_n), srt.iloc[top_n:]
        if len(tail):
            tnodes = list(tail["node"])
            tcs = [node_cat[n] for n in tnodes]
            cat_rank = {"critical": 0, "overhead": 1, "compute": 2, "minor": 3}
            best = min(set(tcs), key=lambda c: (-tcs.count(c), cat_rank[c]))
            head = pd.concat([head, pd.DataFrame([{
                "node": f"others ({len(tail)})", "count": int(tail["count"].sum()),
                "total_lat": tail["total_lat"].sum(), "mean_lat": float("nan"),
                "P_i": tail["P_i"].sum(), "C_i": tail["C_i"].sum(),
                "category": best, "cat_tally": f"{tcs.count(best)}/{len(tcs)}",
                "algo": "", "nodes": tnodes,
                "cpu_us": float(tail["cpu_us"].sum())}])], ignore_index=True)
        df_stats = head.reset_index(drop=True)

    # ============================================================
    # console summary
    # ============================================================
    print(f"\n{total_jobs:,} jobs, total load {total_lat:,.1f} us"
          + ("  (metric = sum of algo:working.seg_us = THREAD CPU TIME, "
             "queueing and preemption excluded)" if exec_source == "cpu" else
             "  (metric = t_complete - t_execute = WALL CLOCK, preemption included)"))
    if analysis_window_ms:
        print(f"analysis window {analysis_window_ms[0]} ~ {analysis_window_ms[1]} ms, "
              f"{n_window} jobs outside")
    if n_bad:
        print(f"WARNING: {n_bad} jobs dropped (negative load / mismatched pairing)")

    print(f"\nPer-node classification (basis of the category, threshold {category_pct}%):")
    print(f"  {'node':<7}{'algo':<14}{'count':>8}{'P_i %':>8}{'load us':>13}{'C_i %':>8}   category")
    for _, r in id_stats.iterrows():
        print(f"  {r['node']:<7}{r['algo']:<14}{int(r['count']):>8}{r['P_i']:>8.2f}"
              f"{r['total_lat']:>13.1f}{r['C_i']:>8.2f}   {r['category']}")

    if len(df_stats) != len(id_stats):
        print("\nSlices (tail merged; slice category = majority of its member nodes):")
        print(f"  {'slice':<16}{'count':>8}{'P_i %':>8}{'load us':>13}{'C_i %':>8}   category")
        for _, r in df_stats.iterrows():
            print(f"  {r['node']:<16}{int(r['count']):>8}{r['P_i']:>8.2f}"
                  f"{r['total_lat']:>13.1f}{r['C_i']:>8.2f}   {r['category']} ({r['cat_tally']})")

    print(f"\nBy category (counted per node id):")
    for cat in ("critical", "overhead", "compute", "minor"):
        sub = id_stats[id_stats["category"] == cat]
        if sub.empty:
            continue
        print(f"  {cat:<9}{len(sub):>3} nodes   P {sub['P_i'].sum():6.1f}%  "
              f"C {sub['C_i'].sum():6.1f}%   {', '.join(sub['node'])}")

    if output_csv:
        root, ext = os.path.splitext(output_csv)
        by_id = f"{root}_by_id{ext or '.csv'}"
        id_stats.to_csv(by_id, index=False)
        print(f"\nper-node stats (classification basis) -> {by_id}")
        if len(df_stats) != len(id_stats):
            df_full.drop(columns=["nodes"]).to_csv(output_csv, index=False)
            print(f"slice stats (plot data source) -> {output_csv}")

    # ============================================================
    # figures
    # ============================================================
    labels = list(df_stats["node"])
    colors = [_CATEGORY_COLORS[c] for c in df_stats["category"]]
    hovers_freq = [
        f"<b>{r['node']}</b>" + (f" ({r['algo']})" if r["algo"] else "")
        + f"<br>Frequency: {r['P_i']:.1f}%<br>Count: {int(r['count']):,}"
          f"<br>Category: {r['category'].upper()} ({r['cat_tally']} ids)"
        for _, r in df_stats.iterrows()]
    hovers_load = [
        f"<b>{r['node']}</b>" + (f" ({r['algo']})" if r["algo"] else "")
        + f"<br>P_i: {r['P_i']:.1f}%<br>Load: {r['total_lat']:,.1f} us"
          f"<br>C_i: {r['C_i']:.1f}%<br>Mean/job: {r['mean_lat']:,.1f} us"
          f"<br>Category: {r['category'].upper()} ({r['cat_tally']} ids)"
        for _, r in df_stats.iterrows()]

    # ---- which rings go into the single figure (on/off per ring) ----
    # rings = (frequency, workload). Either can be turned off; the remaining rings are laid
    # out side by side in ONE figure.
    figs, titles = [], []
    if rings[0]:
        figs.append(go.Figure(_donut(
            labels, df_stats["P_i"], colors, hovers_freq,
            [c in ("critical", "overhead") for c in df_stats["category"]],
            "Task Frequency Ratio (P_i)")))
        titles.append("<b>Task Frequency Ratio (P_i)</b>")
    if rings[1]:
        figs.append(go.Figure(_donut(
            labels, df_stats["C_i"], colors, hovers_load,
            [c in ("critical", "compute") for c in df_stats["category"]],
            "Effective Workload Ratio (C_i)")))
        titles.append("<b>Effective Workload Ratio (C_i)</b>")
    if not figs:
        raise ValueError(f"rings={rings} turns every ring off; enable at least one")

    # legend swatches: category colors actually present
    present = [c for c in ("critical", "overhead", "compute", "minor")
               if c in set(df_full["category"])]
    legend = [dict(name=_CATEGORY_NAMES[c], color=_CATEGORY_COLORS[c]) for c in present]

    for f, t in zip(figs, titles):
        f.update_layout(title=dict(text=t, x=0.5), margin=dict(l=30, r=30, t=60, b=30))

    # A single Figure is always returned -- same shape as analyze_cpu_utilization /
    # generate_execution_gantt / analyze_hop_latency.
    if len(figs) == 1:
        fig = figs[0]
    else:
        fig = make_subplots(rows=1, cols=len(figs),
                            specs=[[{"type": "domain"}] * len(figs)],
                            subplot_titles=titles)
        for i, f in enumerate(figs, start=1):
            for tr in f.data:
                if tr.type == "pie":
                    fig.add_trace(tr, row=1, col=i)
        fig.update_layout(height=620, width=460 * len(figs) + 240,
                          legend=dict(orientation="h", yanchor="bottom", y=-0.10,
                                      xanchor="center", x=0.5,
                                      bgcolor="rgba(255,255,255,0.95)",
                                      bordercolor="rgba(0,0,0,0.15)", borderwidth=1,
                                      font=dict(size=11, color="#2C3E50")),
                          font=dict(family="Arial, sans-serif", size=12),
                          margin=dict(l=40, r=40, t=80, b=90))
    for it in legend:
        fig.add_trace(go.Scatter(x=[None], y=[None], mode="markers",
                                 marker=dict(size=13, color=it["color"],
                                             line=dict(color="white", width=1)),
                                 showlegend=True, name=it["name"]))

    # no colored background (the default template tints the subplot area), no axes
    fig.update_layout(plot_bgcolor="white", paper_bgcolor="white")
    fig.update_xaxes(visible=False)
    fig.update_yaxes(visible=False)

    fig.write_html(output_html)
    print(f"\nFigure ({len(figs)} ring(s): {', '.join(t.split('</b>')[0].strip('<b>') for t in titles)})"
          f" -> {output_html}")

    return fig, df_full


if __name__ == "__main__":
    analyze_task_composition(
        timeline_csv="multihop_u90_m1_ms01_s20831829_150941_timeline.csv",
        pipeline_json="multihop_u90_m1_ms01_s20831829.json",
        output_html="task_composition_donut.html",
    )
