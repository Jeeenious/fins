"""超周期起始节点「释放抖动」度量（violin）。

起始节点 = cfg 里**无输入**的节点（`usr_src`），由定时器在每超周期起点释放（tp: 顶点）。它的
释放时刻本该严格等间隔（= 标称周期 T），实际会有波动；这个脚本量化波动并画成小提琴图。

两种口径（同一纵轴，单位 µs）：

  周期抖动 period jitter  = (t_{k+1} − t_k) − T
      逐拍间隔相对标称的偏差。**不累积**——只反映单拍的调度抖动。

  绝对抖动 absolute jitter = t_k − (t_0 + k·T)
      相对理想时刻网格的偏差，**会累积**：每拍的小偏差叠加，长时间跑下来就是漂移
      （drift = 末拍绝对抖动）。它回答"跑了一万拍之后，释放点偏离理想有多远"。

频率口径在控制台报：std(1/Δ)（Hz），以及相对抖动 std(Δ)/T（ppm）。

数据来源：`fins:release` 无 node tag，故按 tid 顺序配 release→execute、由 execute 的 tag 认节点
（与 _csv2lantency 同一套配对逻辑）。
"""

import json
from collections import defaultdict

import numpy as np
import pandas as pd
import plotly.express as px

# *_timeline.csv = seq,t_us,cpu,tid,kind,tag（列名由 tool/_lttng2csv.py 的 run_export 决定）
_TIMELINE_COLS = ("t_us", "tid", "kind", "tag")

_JITTER_KINDS = ("period jitter", "absolute jitter")


def _require_columns(df, cols, path, which="timeline"):
    """校验必需列，缺失时抛带诊断提示的 ValueError（不返回）。"""
    missing = [c for c in cols if c not in df.columns]
    if not missing:
        return
    hint = " —— 实际列像 preempt CSV（内核 sched_switch），传反了？" if "next_comm" in df.columns else ""
    raise ValueError(
        f"{which} CSV '{path}' 缺少必需列 {missing}{hint}\n"
        f"  实际列 = {list(df.columns)}\n"
        f"  正确用法：timeline_csv='*_timeline.csv'（UST 事件）")


def start_nodes(pipeline_json):
    """cfg 里无输入的节点 = 定时器驱动的起始节点 → {节点 id: 标称周期(ms)}。

    周期取 `period` 字段（ms）；事件触发的节点没有该字段，值为 0.0（此时要靠调用方
    显式给周期，或退化为"用实测间隔中位数当标称"）。
    """
    with open(pipeline_json, encoding="utf-8") as f:
        doc = json.load(f)
    nodes = doc["nodes"] if isinstance(doc, dict) else doc
    return {n["id"]: float(n.get("period") or 0.0) for n in nodes if not n.get("inputs")}


def _non_start_nodes(pipeline_json):
    """cfg 里有输入的节点 id 集合（非起始节点）。"""
    with open(pipeline_json, encoding="utf-8") as f:
        doc = json.load(f)
    nodes = doc["nodes"] if isinstance(doc, dict) else doc
    return {n["id"] for n in nodes if n.get("inputs")}


def _pair_releases(df):
    """按 tid 顺序配 release→execute，返回 [(tid, t_rel, node), ...]。

    `fins:release` 没有 node tag（只有 tid），所以节点身份从紧随其后的 `algo:execute`
    的 tag 取。缺 release 的 job（导出裁剪）跳过——抖动只用间隔，不需要跨节点配对，
    少一拍不影响其余样本。
    """
    out = []
    ev = (df[df["kind"].isin(("release", "execute"))]
          .sort_values(["tid", "t_us"]))
    for tid, g in ev.groupby("tid"):
        t_rel = None
        for kind, t_us, node in zip(g["kind"], g["t_us"], g["clean_tag"]):
            t_us = float(t_us)
            if kind == "release":
                t_rel = t_us
            elif t_rel is not None:
                if node:
                    out.append((int(tid), t_rel, node))
                t_rel = None
    return out


def _clip_bounds(clip_pct):
    """把 clip_pct 解析成 (lo_pct, hi_pct) 并校验。

    **标量 p 表示对称双边裁剪 (100−p, p)** —— 抖动是以 0 为中心的双边量，两头都有离群，
    只裁上尾的话纵轴仍被下尾撑开（实测 FINS 跨度 3550 → 560µs，差 6.3 倍）。
    故 clip_pct=99 ≡ (1, 99)。显式 (lo, hi) 用于非对称场合。
    """
    if isinstance(clip_pct, (int, float)):
        if not 50.0 < clip_pct <= 100.0:
            raise ValueError(
                f"clip_pct={clip_pct} 非法：标量表示**对称**双边裁剪 (100−p, p)，须 50 < p ≤ 100"
                f"（如 99 ≡ 裁掉最低 1% 与最高 1%）；要单边或非对称请传 (lo, hi)")
        return 100.0 - float(clip_pct), float(clip_pct)
    lo_p, hi_p = clip_pct
    if not 0.0 <= lo_p < hi_p <= 100.0:
        raise ValueError(f"clip_pct 须为 (lo, hi) 且 0 ≤ lo < hi ≤ 100，收到 {clip_pct}")
    return float(lo_p), float(hi_p)


def _clip_by_pct(df, clip_pct, kinds):
    """绘图前按分位数裁剪（**逐 (node, kind) 分组**）。只影响图，不影响统计与落盘。"""
    if clip_pct is None:
        return df, 0
    lo_p, hi_p = _clip_bounds(clip_pct)
    kept, dropped = [], 0
    for _, g in df.groupby(["node", "kind"], sort=False):
        lo, hi = g["us"].quantile(lo_p / 100.0), g["us"].quantile(hi_p / 100.0)
        m = (g["us"] >= lo) & (g["us"] <= hi)
        kept.append(g[m])
        dropped += int((~m).sum())
    return (pd.concat(kept, ignore_index=True) if kept else df), dropped


def analyze_release_jitter(
        timeline_csv: str,
        pipeline_json: str = None,
        nodes=None,
        periods=None,
        analysis_window_ms=None,
        clip_pct=99,
        missed_ratio: float = 1.5,
        output_html: str = "release_jitter_violin.html",
        output_csv: str = None,
        points=False,
):
    """起始节点释放抖动的度量与小提琴图。

    @param timeline_csv  *_timeline.csv（须含 fins:release / algo:execute）
    @param pipeline_json 可选。给了就取其中的**起始节点**（无输入的节点）及其标称周期
    @param nodes         要测的节点 id **列表**（单个也写 ['n0']）；
                         None = cfg 的起始节点（无 cfg 时取全部有 release 的节点）
    @param periods       标称周期覆盖，**逐节点的 dict** {节点 id: ms}（如 {'n0': 100, 'n8': 50}）；
                         None = 用 cfg 的 period，再退回"实测间隔中位数"（会打提示）。
                         不收标量：多路径配置各源节点周期未必相同，标量会算错
    @param analysis_window_ms (lo, hi)（ms，相对 trace 起点；None = 全窗口），按该次 release 判定
    @param clip_pct      绘图裁剪。**标量 p = 对称双边裁 (100−p, p)**，默认 99 ≡ (1, 99)：
                         抖动以 0 为中心、两头都有离群，只裁上尾纵轴仍被下尾撑开
                         （实测 FINS 跨度 3550 → 560µs）。标量须 50 < p ≤ 100；
                         要非对称就传显式 (lo, hi)。None = 不裁。
                         **只影响图**，控制台统计与 output_csv 始终是原始值
    @param missed_ratio  间隔 > missed_ratio × T 视为"疑似漏释放/多等了一拍"，计数上报（不丢）
    @param output_html   violin 输出路径
    @param output_csv    逐拍明细 CSV（None = 不写；始终未裁剪）
    @param points        violin 上是否叠加散点
    @retval (plotly Figure, 逐拍明细 DataFrame)
    """
    if clip_pct is not None:
        _clip_bounds(clip_pct)      # 非法值在入口就报错，别等画图
    if missed_ratio <= 1.0:
        raise ValueError(f"missed_ratio 须 > 1，收到 {missed_ratio}")

    window_us = None
    if analysis_window_ms is not None:
        lo_ms, hi_ms = analysis_window_ms
        if lo_ms >= hi_ms:
            raise ValueError(f"analysis_window_ms 须为 (lo, hi) 且 lo < hi，收到 {analysis_window_ms}")
        window_us = (lo_ms * 1000.0, hi_ms * 1000.0)

    print("正在读取数据并计算释放抖动...")
    df = pd.read_csv(timeline_csv)
    _require_columns(df, _TIMELINE_COLS, timeline_csv)
    df["clean_tag"] = df["tag"].apply(lambda v: None if pd.isna(v) else str(v).split()[0])

    cfg_start = start_nodes(pipeline_json) if pipeline_json else {}
    if pipeline_json:
        print(f"起始节点（cfg 里无输入者）：{cfg_start or '（无）'}")

    rels = _pair_releases(df)
    if not rels:
        raise ValueError(
            f"'{timeline_csv}' 里没有可用的 fins:release → algo:execute 配对。\n"
            f"  检查 fins:release 是否启用（_test.py 的 CUSTOM_UST_EVENTS）。")

    by_node = defaultdict(list)
    for _, t_rel, nd in rels:
        by_node[nd].append(t_rel)
    have = sorted(by_node, key=lambda s: (0, int(s[1:])) if s[1:].isdigit() else (1, s))

    # nodes 严格按设计只收 list（单个节点也写 ['n0']）。裸字符串会被逐字符迭代成 'n','0'，
    # 那是个静默陷阱，故直接报错而不是猜。
    if nodes is not None and isinstance(nodes, str):
        raise ValueError(
            f"nodes 要传**列表**（单个节点也写 ['{nodes}']），收到字符串 {nodes!r}\n"
            f"  字符串会被逐字符迭代成 {' , '.join(repr(c) for c in nodes)}，不会有匹配。")

    if nodes is None:
        targets = [n for n in cfg_start if n in by_node] or have
        if not cfg_start:
            print("⚠️ 未给 pipeline_json：无法识别起始节点，改测**全部**有 release 的节点")
    else:
        want = list(nodes)
        targets = [n for n in want if n in by_node]
        missing = [n for n in want if n not in by_node]
        if missing:
            print(f"⚠️ 指定节点里无 release 事件：{missing}")
        if not targets:
            raise ValueError(
                f"nodes={want} 里没有一个节点有 release 事件。\n"
                f"  本 trace 实际出现过的节点 = {have}\n"
                f"  （注意节点 id 是字符串 'n0'，不是 0）")

    if not targets:
        raise ValueError(f"没有可测的节点（候选 {have}）。")

    # 非起始节点的"释放" = worker 抢到任务的时刻（含排队），不是定时器释放时刻。
    # 那种抖动里混着调度排队噪声，量级往往比定时器抖动大一个数量级，别当成定时器精度读。
    if pipeline_json:
        downstream = sorted(set(targets) & _non_start_nodes(pipeline_json),
                            key=lambda s: (0, int(s[1:])) if s[1:].isdigit() else (1, s))
        if downstream:
            print(f"\n⚠️ {downstream} 不是起始节点（cfg 里有输入）：它们的 release 是 **worker "
                  f"抢到任务的时刻**、含排队，抖动里混着调度噪声，不等于定时器释放精度。"
                  f"要测定时器本身请只选无输入的起始节点（默认行为）。")

    # periods 严格按设计只收 dict（逐节点给）。理由：多路径配置里 anchor 之外的 src 由
    # _assign_temporal 从 divisors 里另抽周期（实测 200 个种子中 173 个两 src 周期不同），
    # 标量套上去会把它们算成"相对错误网格的偏差"，整条谱失真 —— 故不收标量、直接报错。
    if periods is None:
        periods = {}
    elif not isinstance(periods, dict):
        raise ValueError(
            f"periods 要传**逐节点的 dict**（如 {{'n0': 100, 'n8': 50}}，单位 ms），"
            f"收到 {periods!r}\n"
            f"  多路径配置里各源节点周期未必相同，标量会算错，故不提供该写法。")
    else:
        periods = {str(k): float(v) for k, v in periods.items()}
        unknown = sorted(set(periods) - set(targets))
        if unknown:
            raise ValueError(
                f"periods 里给了不在 targets 里的节点 {unknown}（会静默不生效）。\n"
                f"  当前 targets = {targets}")

    rows, summary = [], []
    for nd in targets:
        t = np.array(sorted(by_node[nd]), dtype=float)
        if window_us is not None:
            t = t[(t >= window_us[0]) & (t <= window_us[1])]
        if len(t) < 3:
            print(f"⚠️ {nd}: 窗口内仅 {len(t)} 次释放（<3），跳过")
            continue

        T_ms = periods.get(nd, cfg_start.get(nd, 0.0))
        src_T = "cfg"
        if not T_ms:
            T_ms = float(np.median(np.diff(t))) / 1000.0
            src_T = "median"
        T = T_ms * 1000.0

        dt = np.diff(t)                                   # 逐拍间隔（µs）
        pj = dt - T                                       # 周期抖动
        aj = t - (t[0] + np.arange(len(t)) * T)           # 绝对抖动（相对理想网格）
        aj = aj[1:]                                       # 首拍按定义为 0，去掉这个结构性零点

        n_short = int((dt < T / missed_ratio).sum())
        n_miss = int((dt > T * missed_ratio).sum())

        for k in range(len(dt)):
            rows.append({"node": nd, "cycle": k + 1, "t_rel_us": t[k + 1],
                         "interval_us": dt[k], "period_jitter_us": pj[k],
                         "abs_jitter_us": aj[k] if k < len(aj) else np.nan,
                         "nominal_us": T})
        # 长尾诊断：释放时刻是"线程真拿到 CPU"时才打的，故偶尔会被调度延迟几百µs~几ms。
        # 那种尾巴会把 std 抬起来，而典型样本其实很紧 —— 故同时给分位数与"剔尾后的 std"，
        # 并在尾巴主导 std 时明确点名，避免拿 std 当唯一结论。
        a_dev = np.abs(pj)
        core = a_dev[a_dev <= 500.0]
        tail_n = int((a_dev > 500.0).sum())
        summary.append({
            "node": nd, "n": len(t), "T_nom_us": T, "T_src": src_T,
            "mean_interval_us": float(dt.mean()), "std_period_us": float(dt.std(ddof=0)),
            "p50_us": float(np.median(a_dev)),
            "p90_us": float(np.percentile(a_dev, 90)),
            "p99_us": float(np.percentile(a_dev, 99)),
            "max_us": float(a_dev.max()),
            "std_core_us": float(core.std(ddof=0)) if len(core) else float("nan"),
            "tail_n": tail_n, "tail_frac": tail_n / len(pj),
            "std_abs_us": float(aj.std(ddof=0)) if len(aj) else float("nan"),
            "p95_abs_us": float(np.percentile(np.abs(aj), 95)) if len(aj) else float("nan"),
            "max_abs_us": float(np.abs(aj).max()) if len(aj) else float("nan"),
            "drift_us": float(aj[-1]) if len(aj) else float("nan"),
            "std_freq_hz": float((1e6 / dt).std(ddof=0)),
            "rel_ppm": float(dt.std(ddof=0) / T * 1e6),
            "n_short": n_short, "n_miss": n_miss,
        })

    if not rows:
        raise ValueError("没有节点满足样本数要求（每次释放 <3）。")

    df_rows = pd.DataFrame(rows)

    # ============================================================
    # 控制台
    # ============================================================
    print(f"\n释放抖动汇总（period jitter |Δ−T| 的分位数与标准差，单位 µs；T = 标称周期）")
    print(f"  {'node':<7}{'n':>6}{'T_nom(ms)':>11}{'来源':>7}{'p50':>8}{'p90':>9}{'p99':>9}"
          f"{'max':>10}{'std':>9}{'std剔尾':>9}{'尾巴':>9}{'ppm':>9}")
    for s in summary:
        tail = f"{s['tail_n']} ({s['tail_frac']*100:.0f}%)" if s["tail_n"] else "—"
        print(f"  {s['node']:<7}{s['n']:>6}{s['T_nom_us']/1000:>11.3f}{s['T_src']:>7}"
              f"{s['p50_us']:>8.1f}{s['p90_us']:>9.1f}{s['p99_us']:>9.1f}{s['max_us']:>10.1f}"
              f"{s['std_period_us']:>9.1f}{s['std_core_us']:>9.1f}{tail:>9}{s['rel_ppm']:>9.0f}")

    # 尾巴主导 std 时点名 —— 此时"std 大"不等于"整体抖"，是少数大延迟
    for s in summary:
        if s["tail_n"] and s["std_core_us"] < 0.5 * s["std_period_us"]:
            print(f"\n⚠️ {s['node']}：std {s['std_period_us']:.1f}µs 里主要是长尾 —— "
                  f"剔掉 {s['tail_n']} 个 >500µs 的样本（占 {s['tail_frac']*100:.1f}%）后只剩 "
                  f"{s['std_core_us']:.1f}µs。典型释放很紧（p50 {s['p50_us']:.1f}µs），"
                  f"但偶尔被推迟到 {s['max_us']:.0f}µs —— 这是**调度延迟**，不是时钟/定时器精度，"
                  f"也不是漏拍（漏拍会让间隔 ≈2T，本表 max 远小于 2T）")
    print("\n  另：绝对抖动的均值与漂移见下方 CSV；std 不反映漂移，两者要看哪个取决于问题")

    bad = [s for s in summary if s["n_short"] or s["n_miss"]]
    if bad:
        print("\n⚠️ 疑似漏释放 / 多等一拍（间隔偏离标称 > 1.5×，计数但未丢弃）：")
        for s in bad:
            print(f"    {s['node']}: 过短 {s['n_short']} 次，过长 {s['n_miss']} 次")

    if output_csv:
        df_rows.to_csv(output_csv, index=False)
        print(f"\n逐拍明细已保存至: {output_csv}")

    # ============================================================
    # violin
    # ============================================================
    df_long = pd.concat([
        df_rows[["node", "period_jitter_us"]].rename(columns={"period_jitter_us": "us"})
            .assign(kind="period jitter"),
        df_rows[["node", "abs_jitter_us"]].rename(columns={"abs_jitter_us": "us"})
            .assign(kind="absolute jitter"),
    ], ignore_index=True).dropna(subset=["us"])

    df_plot, n_clip = _clip_by_pct(df_long, clip_pct, _JITTER_KINDS)
    if clip_pct is not None:
        side = "对称两端" if isinstance(clip_pct, (int, float)) else "两端"
        print(f"\n绘图裁剪 clip_pct={clip_pct}（逐 (node, kind) 分组裁掉{side}）："
              f"丢弃 {n_clip} / {len(df_long)} 行（{n_clip/max(len(df_long),1)*100:.1f}%）"
              f"；图上纵轴 "
              f"[{df_plot['us'].min():.1f}, {df_plot['us'].max():.1f}] µs（跨度 "
              f"{df_plot['us'].max()-df_plot['us'].min():.1f}）"
              f"—— 上表与落盘明细均为**未裁剪**值")

    fig = px.violin(
        df_plot, x="node", y="us", color="kind", box=True, points=points,
        category_orders={"node": targets, "kind": list(_JITTER_KINDS)},
        color_discrete_map={"period jitter": "#2C5F7A", "absolute jitter": "#D4834A"},
        labels={"us": "<b>jitter (µs)</b>", "node": "<b>node</b>", "kind": ""},
        title="<b>Release Jitter of Hyperperiod Start Nodes</b>",
    )
    fig.update_traces(meanline_visible=True, jitter=0.25, marker=dict(size=3, opacity=0.4))
    fig.update_layout(
        violinmode="group", plot_bgcolor="white", paper_bgcolor="white",
        template="plotly_white", height=560, width=max(700, 130 * len(targets) + 300),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, title=None),
    )
    fig.add_hline(y=0.0, line=dict(color="#999999", width=1, dash="dot"))
    fig.write_html(output_html)
    print(f"\n小提琴图已保存至: {output_html}")

    return fig, df_rows


if __name__ == "__main__":
    analyze_release_jitter(
        timeline_csv="multihop_u90_m1_ms01_s20831829_150941_timeline.csv",
        pipeline_json="multihop_u90_m1_ms01_s20831829.json",
        output_html="release_jitter_violin.html",
    )
