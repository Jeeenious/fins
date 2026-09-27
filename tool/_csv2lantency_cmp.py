"""FINS / CIE serial 对比：数据收发延迟**箱线图** vs mesg size / utilization。

纵轴 = **数据收发延迟** = t_exec(下游) − t_complete(上游)，即「上游把帧写进槽 → 下游取出开始执行」。
这正是 `_csv2lantency.py` 三段分解里的 `total`（① 写槽 + ② 调度 + ③ 打包），本脚本直接复用那边的
配对/成链逻辑（`_pair_tid_events` + `_collect_hop_rows`），保证与 `hop_latency_violin.html` 同一口径。
画成箱线而非均值曲线：均值会被少数慢样本主导（实测 CIE m2 的均值/中位差 10–70 倍），箱线把
中位/四分位/离群点一次摊开；箱内虚线 = 均值（`boxmean=True`）。

一张 figure 两个子图，x 为**分类轴**（箱线按档位并排，等距排布）：
  · 左：x = mesg size 档位，utilization 固定 = `u_fix`
  · 右：x = utilization 档位，mesg size 固定 = `msg_fix`
  每个档位下一组箱 = 一个 (框架, m)。

输入目录：`dirs=` 传若干目录（绝对路径原样、相对路径按 cwd 解析）；不传则自动扫
`<repo>/tool/*_serial*`。框架名取目录名前缀（如 `FINS_serial_7B` → FINS）。

⚠️ **链结构**：能按 run 名找回 `tool/pipeline_*/*.json` 就用 cfg 里的真实边；否则退化成
   「按 n<数字> 升序串成单链」。多 path 拓扑下 n7(sink)→n8(src) 编号相邻但**没有边**，会连出假边
   （假边上的时延是两条链的相位差，毫无意义）。控制台打印的 `边`/`节点` 列可用来核对。

用法：
    uv run --with plotly --with pandas python tool/_csv2lantency_cmp.py
    uv run --with plotly --with pandas python tool/_csv2lantency_cmp.py \\
        --dirs tool/FINS_serial tool/CIE_serial_1MB --u 50
"""

import argparse
import contextlib
import glob
import io
import os
import re
import sys
from collections import defaultdict

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _csv2lantency import _collect_hop_rows, _infer_paths, _pair_tid_events, paths_from_pipeline

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# run 名：<kind>_u<u>_m<m>_ms<..>[_msg<N>]_s<seed>_<时间戳>
_RUN_RE = re.compile(
    r"^(?P<kind>[a-z]+)_u(?P<u>\d+)_m(?P<m>\d+)_ms(?P<ms>\d+)"
    r"(?:_msg(?P<msg>\d+))?_s(?P<seed>\d+)_(?P<ts>\d+)$"
)
# 目录名里的 mesg 后缀 → 字节数（CIE 侧的 run 名不带 _msg）
_SIZE_SUFFIX = re.compile(r"^(?P<n>\d+)(?P<suf>B|KB|MB|GB)$")
_SIZE_MULT = {"B": 1, "KB": 1024, "MB": 1024**2, "GB": 1024**3}

# 框架配色（同一框架在两个子图里同色）
_COLOR = {"FINS": "#1f77b4", "CIE": "#d62728", "MTE": "#2ca02c"}


def _parse_mesg_suffix(text):
    """目录名尾巴（7B / 1MB）→ 字节数；不匹配返回 None。"""
    m = _SIZE_SUFFIX.match(text)
    return int(m["n"]) * _SIZE_MULT[m["suf"]] if m else None


def _human_size(n):
    """字节数 → 图上的档位标签。"""
    for unit, div in (("GB", 1024**3), ("MB", 1024**2), ("KB", 1024)):
        if n >= div:
            return f"{n / div:g}{unit}"
    return f"{n}B"


def _resolve_dirs(dirs):
    """输入目录列表 → 绝对路径（相对路径按 cwd 解析）；空/None → 自动扫 <repo>/tool/*_serial*。"""
    if not dirs:
        return sorted(p for p in glob.glob(os.path.join(_ROOT, "tool", "*_serial*"))
                      if os.path.isdir(p))
    out = []
    for d in ([dirs] if isinstance(dirs, str) else dirs):
        p = os.path.abspath(d)
        if os.path.isdir(p):
            out.append(p)
        else:
            print(f"⚠️ 目录不存在，已跳过: {d}")
    return out


def discover_runs(dirs=None):
    """扫数据目录，返回 [{framework, u, m, mesg, csv, pipeline_json}]（pipeline_json 可为 None）。

    @param dirs 若干数据目录；None = 自动扫 `<repo>/tool/*_serial*`。目录名形如 `<框架>_serial[_<size>]`，
                框架名取前缀；mesg 优先取 run 文件名里的 `_msg<N>_`，否则取目录名里的 `_7B/_1MB` 尾巴。
    """
    runs = []
    for path in _resolve_dirs(dirs):
        dirname = os.path.basename(path.rstrip("/"))
        framework = dirname.split("_")[0].upper()
        dir_mesg = _parse_mesg_suffix(dirname.split("_serial", 1)[-1].lstrip("_"))

        for timeline in sorted(glob.glob(os.path.join(path, "*_timeline.csv"))):
            stem = os.path.basename(timeline)[: -len("_timeline.csv")]
            m = _RUN_RE.match(stem)
            if not m:  # 名字不合规（旧数据）→ 跳过并提示
                print(f"⚠️ 跳过（run 名不合规）: {dirname}/{stem}")
                continue
            mesg = int(m["msg"]) if m["msg"] else dir_mesg
            if mesg is None:
                print(f"⚠️ 跳过（定不出 mesg）: {dirname}/{stem}")
                continue
            # cfg 名**不含** run 尾部的时间戳；找不到就退化成单链推断（见模块 docstring）
            cfg_stem = stem[: -(len(m["ts"]) + 1)]
            cfg = glob.glob(os.path.join(_ROOT, "tool", "pipeline_*", cfg_stem + ".json"))
            runs.append({
                "framework": framework,
                "u": int(m["u"]),
                "m": int(m["m"]),
                "mesg": mesg,
                "csv": timeline,
                "pipeline_json": cfg[0] if cfg else None,
            })
    return runs


def run_hop_latency(run):
    """算一个 run 的逐跳 samples（只取 total）。返回 dict（含全部样本 + 汇总统计）或 None。

    成链优先级与 `_csv2lantency` 一致：pipeline_json（真实边）> 单链推断（多 path 下是假边）。
    """
    df = pd.read_csv(run["csv"])
    # tag 形如 "n3" 或 "n3 [CPU 1, 900 us]"（working 事件）→ 取首段
    df["clean_tag"] = df["tag"].apply(lambda v: None if pd.isna(v) else str(v).split()[0])
    jobs = _pair_tid_events(df)
    if not jobs:
        return None

    nodes = sorted({j["node"] for j in jobs})
    if run["pipeline_json"]:
        paths = paths_from_pipeline(run["pipeline_json"])
    else:
        with contextlib.redirect_stdout(io.StringIO()):  # 提示每 run 都打太吵，汇总时统一说
            paths = _infer_paths(nodes)

    rows, *_ = _collect_hop_rows(jobs, paths, skip_cycles=0, max_cycles=None,
                                 window_us=None, drop_negative=False)
    samples = [r["us"] for r in rows if r["segment"] == "total"]
    if not samples:
        return None
    srt = sorted(samples)
    plist = paths if isinstance(paths[0], list) else [paths]
    return {
        "samples": samples,
        "mean": sum(samples) / len(samples),
        "median": srt[len(srt) // 2],
        "p99": srt[min(len(srt) - 1, int(len(srt) * 0.99))],
        "n": len(samples),
        "paths": len(plist),
        "edges": sum(len(p) - 1 for p in plist),
        "nnodes": len(nodes),
    }


def collect(runs):
    """逐 run 算指标（按 (框架,u,m,mesg) 去重，多份取第一份）；返回可直接用的行表。"""
    table, cache = [], {}
    for r in runs:
        key = (r["framework"], r["u"], r["m"], r["mesg"])
        if key not in cache:
            cache[key] = run_hop_latency(r)
        met = cache[key]
        if met is None:
            continue
        if any(row[:4] == [r["framework"], r["u"], r["m"], r["mesg"]] for row in table):
            continue
        table.append([r["framework"], r["u"], r["m"], r["mesg"],
                      r["pipeline_json"] is not None, met])
    return table


def plot(table, u_fix, msg_fix, out_html=None):
    """一张 figure 两个子图，每个档位一组箱线（每个 (框架, m) 一个箱）。

    x 用**分类轴**：箱线按档位等距并排，比 log 数值轴上的 dodge 稳；左图档位按 mesg 升序、
    右图按 u 升序。y 对数轴（1GB 档到 25ms，跨 3 个数量级）。
    """
    u_vals = sorted({r[1] for r in table})
    msg_vals = sorted({r[3] for r in table})
    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=(f"vs mesg size（utilization u={u_fix} 固定）",
                        f"vs utilization（mesg={_human_size(msg_fix)} 固定）"),
        horizontal_spacing=0.09,
    )

    # (子图列, 取哪些行, 行 → 档位标签, 档位顺序, x 标题)
    panels = (
        (1, lambda r: r[1] == u_fix, lambda r: _human_size(r[3]),
         [_human_size(v) for v in msg_vals], "载荷大小"),
        (2, lambda r: r[3] == msg_fix, lambda r: f"u={r[1]}%",
         [f"u={v}%" for v in u_vals], "利用率"),
    )
    seen = set()
    for col, keep, label_of, order, xtitle in panels:
        grouped = defaultdict(list)                      # (series, 档位) → 样本
        for row in table:
            if keep(row):
                grouped[(f"{row[0]} m={row[2]}", label_of(row))].extend(row[5]["samples"])

        for series in sorted({k[0] for k in grouped}):
            xs, ys = [], []
            for lbl in order:
                s = grouped.get((series, lbl))
                if s:
                    xs += [lbl] * len(s)
                    ys += s
            if not ys:
                continue
            color = _COLOR.get(series.split()[0], "#7f7f7f")
            fig.add_trace(go.Box(
                x=xs, y=ys, name=series, legendgroup=series,
                showlegend=series not in seen,
                boxmean=True,                            # 箱内虚线 = 均值
                boxpoints="suspectedoutliers",           # 只画离群点（几万样本也扛得住）
                marker=dict(color=color, size=3, opacity=0.45),
                line=dict(color=color, width=1.5),
                fillcolor=color,
                opacity=0.5,
                hovertemplate=(f"<b>{series}</b>  %{{x}}<br>中位 %{{median:.1f}} µs"
                               "<br>均值 %{mean:.1f} µs<br>上/下四分位 %{q3:.1f} / %{q1:.1f}"
                               "<br>最大/最小 %{max:.1f} / %{min:.1f}<extra></extra>"),
            ), row=1, col=col)
            seen.add(series)

        fig.update_xaxes(categoryorder="array", categoryarray=order,
                         title_text=xtitle, row=1, col=col)

    fig.update_yaxes(type="log", title_text="数据收发延迟 (µs)", row=1, col=1)
    fig.update_yaxes(type="log", row=1, col=2)
    fig.update_layout(
        title=f"{'/'.join(sorted({r[0] for r in table}))} serial 链：数据收发延迟分布"
              f"（t_exec(下游) − t_complete(上游)；箱内虚线 = 均值，点 = 离群样本）",
        height=600, width=1360,
        boxmode="group",                                 # 同档位下多个框架并排
        legend=dict(orientation="h", yanchor="bottom", y=1.05, x=0),
        template="plotly_white",
        margin=dict(t=130),
    )
    if out_html:            # notebook 里传 None = 只返回 Figure 不写盘
        fig.write_html(out_html)
    return fig


def _print_table(table):
    """打印逐 run 汇总表 + 前提提示（cfg 缺失 / 均值被离群样本主导）。"""
    print(f"\n{'框架':<6}{'u':>4}{'m':>3}{'mesg':>11}{'cfg':>5}{'均值µs':>11}{'中位µs':>10}"
          f"{'p99µs':>10}{'样本':>8}{'路径':>6}{'边':>5}{'节点':>6}")
    for fw, u, m, mesg, has_cfg, met in sorted(table):
        flag = " ⚠均值被离群样本主导" if met["mean"] > 3 * met["median"] else ""
        print(f"{fw:<6}{u:>4}{m:>3}{_human_size(mesg):>11}{'✓' if has_cfg else '—':>5}"
              f"{met['mean']:>11.1f}{met['median']:>10.1f}{met['p99']:>10.1f}"
              f"{met['n']:>8}{met['paths']:>6}{met['edges']:>5}{met['nnodes']:>6}{flag}")

    no_cfg = {fw for fw, *_ , cfg, _m in table if not cfg}
    if no_cfg:
        print(f"\n⚠️ {', '.join(sorted(no_cfg))} 找不到 pipeline cfg → 链结构按 n<数字> 升序推断（单链假定）。"
              f"\n   多 path 拓扑会连出假边，此时箱线无意义 —— 请把 cfg 目录补上或显式传 chain。")


def analyze_serial_cmp(dirs=None, u_fix=30, msg_fix=1048576, output_html=None, verbose=True):
    """扫数据目录 → 逐跳收发延迟 → 返回双子图**箱线** Figure（与其它 `_csv2*.py` 的 `analyze_*` 同风格）。

    给 notebook 用：默认只返回 Figure（不写盘），`fig.show()` 即渲染；`output_html=` 非 None 时
    顺带写 HTML（脚本入口就是这么调的）。

    @param dirs        若干数据目录（list/tuple；单个 str 也行）；None = 自动扫 `<repo>/tool/*_serial*`。
                       相对路径按 cwd 解析、绝对路径原样；目录名形如 `<框架>_serial[_<size>]`。
    @param u_fix       左图（vs mesg size）固定的利用率（默认 30）
    @param msg_fix     右图（vs utilization）固定的载荷字节数（默认 1048576 = 1MB，两边唯一共同档）
    @param output_html 非 None → 额外写 HTML
    @param verbose     True → 打印逐 run 汇总表 + 前提提示
    @retval plotly Figure
    """
    runs = discover_runs(dirs)
    if not runs:
        raise SystemExit("没扫到任何 *_timeline.csv —— 检查 dirs= 指向的数据目录")
    table = collect(runs)
    if not table:
        raise SystemExit("扫到了 run 但没有可用的 hop 样本（trace 缺 release/execute？）")
    if verbose:
        _print_table(table)
    return plot(table, u_fix, msg_fix, output_html)


def main():
    ap = argparse.ArgumentParser(description="FINS / CIE serial 收发延迟箱线对比图")
    ap.add_argument("--dirs", nargs="*", default=None,
                    help="数据目录（可多个）；不给则自动扫 <repo>/tool/*_serial*")
    ap.add_argument("--u", type=int, default=30, help="左图（vs mesg size）固定的利用率（默认 30）")
    ap.add_argument("--msg", type=int, default=1048576,
                    help="右图（vs utilization）固定的载荷字节数（默认 1048576 = 1MB）")
    ap.add_argument("--out", default=os.path.join(_ROOT, "tool", "serial_latency_cmp.html"))
    args = ap.parse_args()

    fig = analyze_serial_cmp(dirs=args.dirs, u_fix=args.u, msg_fix=args.msg,
                             output_html=args.out)
    print(f"\n✅ 已写出 {args.out}（{len(fig.data)} 条箱线）")


if __name__ == "__main__":
    main()
