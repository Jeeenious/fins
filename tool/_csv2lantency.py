"""multihop 单链的「上下游传参时延」分析（violin）。

单链假设（纯 relay、全 event 触发、T = H）下三条性质让分析成立：
  ① 每条边 1 producer / 1 consumer（单写者约束 + 链结构）→ 天然单入单出；
  ② t_ready(v) ≡ t_complete(u)（v 唯一阻塞前序就是 u）→「等齐」分量恒为 0；
  ③ 每节点每超周期恰好 1 个实例 → trace tag 唯一，第 k 次出现 = 第 k 拍，配对零歧义。

于是每个 hop 可拆成四段（只用现成事件，零 C++ 改动）：

  t_comp(u) ─①─> t_fin(u) ─②─> t_rel(v) ─③─> t_exec(v)
    ① 完成→finished：route_outputs 写槽 + 完成记账        —— 框架固有
    ② finished→release：挂锁/notify/就绪后等空闲 worker 抢 —— 调度策略（自变量）
    ③ release→execute：pack_inputs 取帧 + 打包 + 闭包入口  —— 框架固有

跨策略/跨 m 比较时看 ②；报「传参本身花多久」就报 ①+③。
已知偏差：② 的起点是 finished 而非「v 就绪」，二者之间隔着挂锁 + notify，这段被算进 ②
（把框架锁开销算成了调度）；① 含 complete/finished 两个 tracepoint 自身开销 → 上界。

**段为负只可能是 ②**：①（t_comp→t_fin）与 ③（t_rel→t_exec）都是**同线程内**的程序序差值，
恒 ≥ 0；只有 ② 跨线程。② < 0 需要「u/v 在不同线程」且「v 的 release 早于 u 的 finished」：

  · FINS：结构上不可能 —— finished 在 trigger_workload_ready（推就绪堆 + notify，v 才有机会
    被抢）**之前**发出，故真实边上 ② 恒 ≥ 0（实测 14000 个样本零负值）。
  · CIE：节点各自一线程、触发次序不同，真实边上 ② 可以成负（实测 56.7%），
    此时 ① 虚高、② 等量变负、两者抵消，故 total 仍有效。
  · 假边（多路径配置按 id 串成一条链）会让 ② 与 total **同时**为负 → 样本被 total<0 守卫丢弃，
    不会出现在负值统计里（负值只统计幸存样本）。

负值样本里 u/v 同线程的那部分，一定是"这对 (u,v) 不是真正的边"（结构问题），
脚本会分别计数并提示。段为负的样本默认保留在图上。
"""

import json
from collections import defaultdict

import pandas as pd
import plotly.express as px

# ============================================================
# 输入列校验
# ============================================================
# *_timeline.csv = seq,t_us,cpu,tid,kind,tag（列名由 tool/_lttng2csv.py 的 run_export 决定）
_TIMELINE_COLS = ("t_us", "tid", "kind", "tag")

# 每段一个标签；顺序 = 图例顺序（total 在最前，其后按 hop 内时间顺序）
_SEGMENTS = ("total", "① write-slot", "② schedule", "③ pack-input")


def _require_columns(df, cols, path, which="timeline"):
    """校验必需列，缺失时抛带诊断提示的 ValueError（不返回）。"""
    missing = [c for c in cols if c not in df.columns]
    if not missing:
        return

    hint = ""
    if "next_comm" in df.columns:
        hint = " —— 实际列像 preempt CSV（内核 sched_switch），传反了？"

    raise ValueError(
        f"{which} CSV '{path}' 缺少必需列 {missing}{hint}\n"
        f"  实际列 = {list(df.columns)}\n"
        f"  正确用法：timeline_csv='*_timeline.csv'（UST 事件）"
    )


def _pair_tid_events(df):
    """按 tid 顺序扫事件流，闭合 (release, execute, complete, finished) 四元组。

    段 ② 的终点是 fins:release（worker 抢到任务的那一刻），故必须按 tid 序贯闭合——
    release/execute/complete/finished 在**同一个 worker 线程**上严格按此序出现，
    第 i 个 release ↔ 第 i 个 execute，不存在跨线程错配。

    release 被 after_us 裁掉的 job **保留**（t_rel=None）。它只会是该 tid 的**第一个** job，
    而丢掉它会让该节点的序列整体前移一位 —— 之后所有边上的 k↔k 配对全部错位（total 恒负，
    差值 ≈ 一个周期），损失远大于保留：②③ 用不了，但 total（t_exec(v) − t_comp(u)）有效。
    结尾未闭合的 job 仍然丢弃（缺 complete 才是真的没法用）。
    """
    out = []
    ev = (df[df["kind"].isin(("release", "execute", "complete", "finished"))]
          .sort_values(["tid", "t_us"]))

    for tid, g in ev.groupby("tid"):
        t_rel = None    # 本 tid 最近一次 release（被 execute 消费后置 None，防跨 job 复用）
        pend = None     # 已 execute、等 complete 的 job
        last = None     # 已 complete、等 finished 的 job
        for kind, t_us, node in zip(g["kind"], g["t_us"], g["clean_tag"]):
            t_us = float(t_us)
            if kind == "release":
                t_rel = t_us
            elif kind == "execute":
                pend = (node, t_rel, t_us)
                t_rel = None
            elif kind == "complete" and pend is not None:
                node, tr, te = pend
                pend = None
                if not node:
                    continue    # 空 tag（tp 等非 job 事件）→ 丢弃
                last = {"tid": int(tid), "node": node, "t_rel": tr,
                        "t_exec": te, "t_comp": t_us, "t_fin": None}
                out.append(last)
            elif kind == "finished" and last is not None:
                last["t_fin"] = t_us
                last = None
    return out


def paths_from_pipeline(pipeline_json):
    """从 pipeline cfg 的 inputs/outputs 重建数据流路径，返回 [[节点 id, ...], ...]（一条链一个列表）。

    比按节点 id 排序可靠：多路径配置（paths>1）里 n7(sink) 与 n8(src) 编号相邻但**没有边**，
    按 id 排序会硬连出一条假边（该边两侧是两条独立并行的链，配出来的时延毫无意义）。

    只走 inputs（= 绑定边），hist 端口（窗口读、运行时无边）被排除——否则会把
    「读某个更早字段的窗口」误当成数据流前驱。分支处（fork/join/mixed）无法表示成链，
    会告警并只取一条分支；本函数面向 multihop 这类线性链。

    @param pipeline_json  pipeline cfg 路径
    @retval list[list[str]] 每条路径的节点 id 序列
    """
    with open(pipeline_json, encoding="utf-8") as f:
        doc = json.load(f)
    nodes = doc["nodes"] if isinstance(doc, dict) else doc

    producer = {}                       # 输出字段 → 生产者节点 id
    for n in nodes:
        for p in n.get("outputs", []):
            producer[p] = n["id"]

    hist_ports = {pn for n in nodes for entry in n.get("hist", []) for pn in entry}

    succ, has_pred = defaultdict(list), set()
    for n in nodes:
        for p in n.get("inputs", []):
            if p in hist_ports or p not in producer:
                continue                # 窗口读 / 无生产者 → 不构成绑定边
            succ[producer[p]].append(n["id"])
            has_pred.add(n["id"])

    heads = [n["id"] for n in nodes if n["id"] not in has_pred]
    if not heads:
        raise ValueError(f"'{pipeline_json}' 里找不到无前驱的源节点（有环？）")

    paths = []
    for h in heads:
        path, cur, seen = [h], h, {h}
        while succ.get(cur):
            nxt = sorted(set(succ[cur]))
            if len(nxt) > 1:
                print(f"⚠️ {cur} 有 {len(nxt)} 个后继（分支），paths_from_pipeline 只取 {nxt[0]}"
                      f"；fork/mixed 之类拓扑不适合按路径分析")
            cur = nxt[0]
            if cur in seen:             # 真环（feedback kind）——按线性链无法表达
                print(f"⚠️ 检测到环经 {cur}，路径在此截断")
                break
            seen.add(cur)
            path.append(cur)
        paths.append(path)
    return paths


def _split_known(paths_like):
    """判断传入的 chain 是「单条链」还是「多条链」：元素为 str = 单链，元素为 list/tuple = 多链。"""
    seq = list(paths_like)
    if not seq:
        return []
    return [list(p) for p in seq] if not isinstance(seq[0], str) else [seq]


def _infer_paths(nodes):
    """未给 chain 时按节点 id 推断（**假定单链**）。

    节点 id 形如 n0/n1/... → 按数字后缀升序；否则按字典序。多路径配置下这个假定不成立，
    故同时给一句提示——真实连接请用 paths_from_pipeline 或显式传 chain=[[...],[...]]。
    """
    def num(nd):
        s = nd[1:] if nd[:1] == "n" else nd
        return (0, int(s)) if s.isdigit() else (1, 0)

    if all(num(nd)[0] == 0 for nd in nodes):
        seq = sorted(nodes, key=num)
    else:
        print("⚠️ 节点 id 非 n<数字> 形态，按字典序推断链序；如不对请显式传 chain=[...]")
        seq = sorted(nodes)
    print("⚠️ 未给 chain：按节点 id 顺序推断为**单条链**。多路径配置（paths>1）下节点编号相邻"
          "不等于有边（如 n7(sink)→n8(src) 是假边）——请传 pipeline_json= 或 chain=[[...],[...]]")
    return [seq]


def _collect_hop_rows(jobs, paths, skip_cycles, max_cycles, window_us=None,
                      drop_negative=False):
    """按 (边, 拍) 收集四段。返回 (明细行, 丢弃计数, 窗口外丢弃数, 负值计数)。

    配对规则：**同一条路径内**相邻的两节点 u→v，第 k 个 complete(u) ↔ 第 k 个 execute(v)。
    跨路径不配对——多路径配置里上一条链的 sink 和下一条链的 src 编号相邻但无连接。
    window_us 非 None 时按**该跳的取出时刻** t_exec(v) 判是否落在窗口内——不要求整段
    都在窗口内，也不按 send 时刻判，故窗口边界不会切出半截样本。

    负值分两类，处理方式**不同**：
      · total < 0 → 整条样本不可用（索引配对错位 / 假边）→ **恒丢**，不受 drop_negative 影响。
      · 段为负 → 只可能是 ②（①③ 同线程恒 ≥ 0）。u/v 同线程时为负说明配对的不是真正的边
        （结构问题）；跨线程时取决于该框架把"发布/触发"放在 finished 之前还是之后。
        total 仍有效 → 默认**保留在图上**（诊断信号），drop_negative=True 可切回丢弃。
      负值计数只统计**幸存**样本（放在 total<0 丢弃之后），否则打印出来的"保留在图上"会与
      实际不符 —— 假边上的 ② 与 total 同时为负，那些样本其实已被丢掉。
    """
    by_node = defaultdict(list)
    for j in jobs:
        by_node[j["node"]].append(j)
    for nd in by_node:
        by_node[nd].sort(key=lambda j: j["t_exec"])

    rows, dropped = [], defaultdict(int)
    n_window = 0
    n_no_rel = 0
    n_head = 0
    neg_same_tid = 0
    neg_cross_tid = 0
    neg = defaultdict(int)

    # 逐条路径各自相邻配对 —— 多路径配置下跨路径的 (sink, src) 不是边，必须靠 paths 隔开
    edges = [(u, v) for path in paths for u, v in zip(path[:-1], path[1:])]
    for u, v in edges:
        ju, jv = by_node.get(u, []), by_node.get(v, [])
        if not ju or not jv:
            print(f"⚠️ 边 {u}→{v}：一侧无 job 事件（u={len(ju)}, v={len(jv)}），跳过")
            continue

        # 头部因果对齐：导出在 after_us 处硬切，不同线程的首个 job 会落在切点两侧，于是两条
        # 序列起点错开一位 —— 之后 k↔k **全部**错位，total 恒负且差值 ≈ 一个周期（实测
        # n2[0] 比 n1[0] 早 96ms）。裁掉超前的那一侧头部即可复位，不影响其余样本。
        ju, jv = list(ju), list(jv)
        while ju and jv and jv[0]["t_exec"] < ju[0]["t_comp"]:
            jv.pop(0)
            n_head += 1
        # 反向：生产者多出的前导（其第 2 个完成仍早于消费者第 1 次取出 → 第 1 个不是触发者）
        while len(ju) > 1 and jv and ju[1]["t_comp"] <= jv[0]["t_exec"]:
            ju.pop(0)
            n_head += 1

        if not ju or not jv:
            print(f"⚠️ 边 {u}→{v}：头部对齐后一侧为空（u={len(ju)}, v={len(jv)}），跳过")
            continue

        n = min(len(ju), len(jv))
        if len(ju) != len(jv):
            dropped[f"{u}→{v} 拍数不等"] += abs(len(ju) - len(jv))

        for k in range(n):
            if k < skip_cycles:
                continue
            if max_cycles is not None and k >= max_cycles:
                break

            a, b = ju[k], jv[k]
            t_send = a["t_comp"]
            t_fin = a["t_fin"] if a["t_fin"] is not None else a["t_comp"]
            t_rel, t_recv = b["t_rel"], b["t_exec"]

            if window_us is not None and not (window_us[0] <= t_recv <= window_us[1]):
                n_window += 1
                continue

            # t_rel 缺失 = 下游该拍的 release 被 after_us 裁掉（只可能是它线程里的第一个 job）。
            # ②③ 算不了，但 total 有效 —— 保留 total 行，别为一个边角样本把序列对齐破坏掉。
            seg = {"① write-slot": t_fin - t_send}
            if t_rel is not None:
                seg["② schedule"] = t_rel - t_fin
                seg["③ pack-input"] = t_recv - t_rel
            else:
                n_no_rel += 1

            total = t_recv - t_send
            hop = f"{u}→{v}"

            # 总延迟必须为正 —— total 是主指标，为负说明索引配对整体错位
            # （某节点丢了一个 job，序列前移一位，差值 ≈ 一个周期），整条样本都不可用，**恒丢**。
            if total < 0:
                neg[f"{hop} total"] += 1
                dropped[f"{hop} total<0（序列错位）"] += 1
                continue

            # ★ 负值计数只统计**幸存**（即将进图）的样本。放在 total<0 丢弃之后 ——
            #   否则统计里会混进已被丢弃的样本，打印时就会出现"② 为负 → 样本保留在图上"
            #   这种与实际不符的话（假边上的 ② 与 total 同时为负，样本其实已被丢掉）。
            for name, val in seg.items():
                if val < 0:
                    neg[f"{hop} {name}"] += 1
                    if name == "② schedule":
                        # u/v 同线程时 ② 不可能为负（release(v) 在 finished(u) 之后是程序序保证），
                        # 出现即说明配对的不是真正的边 → 链路结构有误。跨线程才可能真的是次序问题。
                        if a["tid"] == b["tid"]:
                            neg_same_tid += 1
                        else:
                            neg_cross_tid += 1
                continue

            # 段为负（②）只是仪器语义问题（finished 不在因果路径上），total 仍有效 → 默认保留
            if drop_negative and any(v < 0 for v in seg.values()):
                dropped[f"{hop} 负值段"] += 1
                continue

            rows.append({"hop": hop, "cycle": k, "segment": "total", "us": total,
                         "tid": b["tid"]})
            for name, val in seg.items():
                rows.append({"hop": hop, "cycle": k, "segment": name, "us": val,
                             "tid": b["tid"]})
    return rows, dropped, n_window, n_no_rel, n_head, neg, (neg_same_tid, neg_cross_tid)


def _clip_by_pct(df, clip_pct, segments, hops):
    """绘图前按分位数裁剪：**逐 (hop, segment) 分组**各裁掉上尾（或两端）。

    只作用于画图用的副本——控制台统计与 output_csv 始终是未裁剪的原始样本，
    裁掉多少会单独打印，避免"图看着很干净、实际掉了一堆长尾"。

    @param clip_pct None = 不裁；标量 p = 裁掉 >p 分位（如 99 = 去掉最慢的 1%）；
                    (lo, hi) = 同时裁掉 <lo 与 >hi 分位
    @retval (裁剪后的 df, 丢弃行数)
    """
    if clip_pct is None:
        return df, 0

    lo_p, hi_p = (0.0, clip_pct) if isinstance(clip_pct, (int, float)) else clip_pct

    kept, dropped = [], 0
    for (hop, seg), g in df.groupby(["hop", "segment"], sort=False):
        lo, hi = g["us"].quantile(lo_p / 100.0), g["us"].quantile(hi_p / 100.0)
        m = (g["us"] >= lo) & (g["us"] <= hi)
        kept.append(g[m])
        dropped += int((~m).sum())

    return (pd.concat(kept, ignore_index=True) if kept else df), dropped


def analyze_hop_latency(
        timeline_csv: str,
        pipeline_json: str = None,
        chain=None,
        analysis_window_ms=None,
        clip_pct=None,
        output_html: str = "hop_latency_violin.html",
        output_csv: str = None,
        skip_cycles: int = 0,
        max_cycles: int = None,
        points=False,
        drop_negative=False,
):
    """
    multihop 链的逐跳传参时延分解（violin + 箱线）。

    **链路结构必须给对**：多路径配置（paths>1）里上一条链的 sink 与下一条链的 src 编号
    相邻但**没有边**，按 id 顺序推断会连出一条假边（配出来的时延无意义）。优先级：
      pipeline_json（从 cfg 的 inputs/outputs 重建，最可靠） > chain=[[路径0],[路径1]] > 自动推断

    @param timeline_csv  *_timeline.csv（须含 fins:release / algo:execute/complete）
    @param pipeline_json pipeline cfg 路径。给了就从中重建真实路径（推荐，尤其多路径/多 m 配置）
    @param chain         链路结构：["n0","n1",...] = 单链；[["n0",...,"n7"],["n8",...,"n15"]] = 多链；
                         None = 按 n<数字> 自动推断（**假定单链**，多路径下会给出假边并提示）
    @param analysis_window_ms 分析窗口 (lo, hi)（**ms，相对 trace 起点**；None = 全窗口）。
                         按该跳的**取出时刻** t_exec(v) 判定，不是 send 时刻；不要求整段
                         都在窗口内，故边界不切半截样本。
                         注意：这是**绝对时间**窗口，只能裁掉开头/结尾的整拍。链头逐拍
                         偏慢（每周期空闲后核心降频/C-state 恢复）是**位置**效应，时间窗口
                         裁不掉——那条 violin 本来就是独立的 hop 分组，忽略即可。
    @param clip_pct      绘图裁剪（None = 不裁）。标量 p = 逐 (hop, segment) 裁掉 >p 分位
                        （如 99 = 去掉最慢的 1%）；(lo, hi) = 两端都裁。
                        **只影响图**：控制台统计与 output_csv 始终是原始样本。
    @param output_html   violin 输出路径
    @param output_csv    逐跳逐拍明细 CSV（None = 不写；始终未裁剪）
    @param skip_cycles   跳过前 N 拍（与 analysis_window_ms 叠加生效；导出已 after_us 裁剪，通常留 0）
    @param max_cycles    最多分析多少拍（None = 全部）
    @param points        violin 上是否叠加散点（"all"/False；样本多时 False）
    @param drop_negative False（默认）= 保留负值样本并画在图上（诊断信号，见 _collect_hop_rows）；
                         True = 丢弃负值样本（旧行为）
    @retval plotly Figure
    """
    if clip_pct is not None:
        lo_p, hi_p = (0.0, clip_pct) if isinstance(clip_pct, (int, float)) else clip_pct
        if not 0.0 <= lo_p < hi_p <= 100.0:
            raise ValueError(
                f"clip_pct 须为 p（0<p≤100）或 (lo, hi)（0≤lo<hi≤100），收到 {clip_pct}")

    window_us = None
    if analysis_window_ms is not None:
        lo_ms, hi_ms = analysis_window_ms
        if lo_ms >= hi_ms:
            raise ValueError(f"analysis_window_ms 须为 (lo, hi) 且 lo < hi，收到 {analysis_window_ms}")
        window_us = (lo_ms * 1000.0, hi_ms * 1000.0)
    print("正在读取数据并计算逐跳传参时延...")
    df = pd.read_csv(timeline_csv)
    _require_columns(df, _TIMELINE_COLS, timeline_csv)

    # tag 形如 "n3" 或 "n3 [CPU 1, 900 us]"（working 事件）；取首段
    df["clean_tag"] = df["tag"].apply(
        lambda v: None if pd.isna(v) else str(v).split()[0])

    jobs = _pair_tid_events(df)
    if not jobs:
        raise ValueError(
            f"'{timeline_csv}' 里没有闭合的 (release, execute, complete, finished) 事件对。\n"
            f"  检查 fins:release 是否启用（_test.py 的 CUSTOM_UST_EVENTS），"
            f"以及 after_us 是否把开头裁得太狠。")

    nodes = sorted({j["node"] for j in jobs})
    if pipeline_json:
        paths = paths_from_pipeline(pipeline_json)
    elif chain:
        paths = _split_known(chain)
    else:
        paths = _infer_paths(nodes)

    flat = [nd for p in paths for nd in p]
    missing = [nd for nd in flat if nd not in nodes]
    if missing:
        print(f"⚠️ 链路里有节点无 job 事件（tp 顶点？）：{missing}")
    extra = [nd for nd in nodes if nd not in flat]
    if extra:
        print(f"⚠️ trace 里有节点不在链路中（已忽略）：{extra}")

    rows, dropped, n_window, n_no_rel, n_head, neg, neg_tid = _collect_hop_rows(
        jobs, paths, skip_cycles, max_cycles, window_us, drop_negative)
    if not rows:
        raise ValueError(
            f"没有可分析的 hop 样本（链序为空 / 全被守卫丢弃 / 全在分析窗口外）。\n"
            f"  窗口外丢弃 {n_window} 个样本；trace 时间范围 "
            f"{df['t_us'].min()/1000:.1f}~{df['t_us'].max()/1000:.1f} ms"
            + (f"；当前窗口 {analysis_window_ms} ms" if analysis_window_ms else ""))

    df_all = pd.DataFrame(rows)

    # 假边哨兵：多路径配置里若漏给 pipeline_json/chain，跨链的相邻编号（上条链的 sink 与
    # 下条链的 src）会被当成边。两条链独立并行，配出来的 total 中位数 = 两者的相位差，
    # 远大于真实 hop（实测 21.6ms vs 100µs）→ 用离群就能抓出来。
    _med = df_all[df_all["segment"] == "total"].groupby("hop")["us"].median()
    if len(_med) >= 3:
        _ref = float(_med.median())
        _odd = _med[(_med > 10 * _ref) & (_med > 1000.0)]
        if len(_odd):
            print("⚠️ 以下边的 total 中位数远高于其余边（>10×），疑似**假边**"
                  "（多路径配置里跨链的相邻编号）：")
            for _h, _v in _odd.items():
                print(f"    {_h}: p50 {_v:.1f} µs（其余边中位 {_ref:.1f} µs）")
            print("    → 请传 pipeline_json=<cfg.json> 或 chain=[[路径0],[路径1],...] 指定真实链路")

    # ============================================================
    # 控制台：逐跳逐段统计
    # ============================================================
    hops = [f"{u}→{v}" for p in paths for u, v in zip(p[:-1], p[1:])]
    hops = [h for h in hops if h in set(df_all["hop"])]

    chains_desc = "   |   ".join(" → ".join(p) for p in paths)
    print(f"\n链路结构（{len(paths)} 条链，共 {len(flat)} 节点）: {chains_desc}")
    print(f"trace 时间范围 {df['t_us'].min()/1000:.1f} ~ {df['t_us'].max()/1000:.1f} ms"
          + (f"；分析窗口 {analysis_window_ms[0]} ~ {analysis_window_ms[1]} ms，"
             f"窗口外丢弃 {n_window} 个样本" if analysis_window_ms
             else "；未设分析窗口"))
    print(f"每跳样本数 {int(len(df_all) / len(_SEGMENTS) / max(len(hops), 1))} 拍"
          f"（跳过前 {skip_cycles} 拍）")

    for h in hops:
        sub = df_all[df_all["hop"] == h]
        print(f"\n  {h}")
        for seg in _SEGMENTS:
            s = sub[sub["segment"] == seg]["us"]
            if s.empty:
                continue
            print(f"    {seg:<14} mean {s.mean():7.2f}  p50 {s.quantile(.50):7.2f}  "
                  f"p95 {s.quantile(.95):7.2f}  p99 {s.quantile(.99):7.2f}  "
                  f"max {s.max():8.2f}  µs")

    if dropped:
        print("\n⚠️ 丢弃计数（守卫命中）：")
        for k, c in sorted(dropped.items()):
            print(f"    {k}: {c}")

    if n_head:
        print(f"\n头部对齐：裁掉超前的前导样本 {n_head} 个"
              f"（导出在 after_us 硬切，不同线程的首个 job 落在切点两侧 → 序列起点错开一位，"
              f"不裁则整条边的 total 恒负）")
    if n_no_rel:
        print(f"\n无 release 的样本 {n_no_rel} 个（下游首个 job 的 release 被裁）"
              f"：只报了 total，②③ 不可算")

    # 负值样本：total<0 的已丢弃（见上面的丢弃计数）；段为负的默认保留在图上（诊断信号）
    if neg:
        print("\n⚠️ 负值样本，按 (边, 段) 计数：")
        for k, c in sorted(neg.items()):
            print(f"    {k}: {c}")
        if any(k.endswith("total") for k in neg):
            print("    • total<0 = 该边的 total 为负 → 整条样本不可用，已丢弃（见上方丢弃计数）。"
                  "常见成因：链路结构给错（多路径配置按节点 id 串成一条链，上条链的 sink 接到了"
                  "下条链的 src）或某节点丢了一个 job 使序列错位")
        seg_neg = [k for k in neg if not k.endswith("total")]
        if seg_neg:
            same, cross = neg_tid
            print(f"    • 段为负（只可能是 ②：① 与 ③ 是**同线程内**的程序序差值，恒 ≥ 0）："
                  f"{'已丢弃' if drop_negative else '**保留在图上**'}")
            print(f"        其中 u/v **同线程** {same} 个 —— 同线程内 ② 不可能为负，"
                  f"出现即说明这对 (u,v) 不是真正的边（链路结构有误）")
            print(f"        其中 u/v **跨线程** {cross} 个 —— 跨线程时 release(v) 可以早于 "
                  f"finished(u)，取决于该框架把'发布/触发'放在 finished 之前还是之后")
        print("    因果判据：t_exec(v) − t_comp(u) 恒为正；① ③ 同线程恒 ≥ 0，只有 ② 可能为负")

    # 框架固有 vs 策略的全局占比（只对 hop 内三段求和，不含 total）
    seg_only = df_all[df_all["segment"] != "total"]
    tot = seg_only["us"].sum()
    if tot > 0:
        print("\n三段占比（全局）：")
        for seg in _SEGMENTS[1:]:
            v = seg_only[seg_only["segment"] == seg]["us"].sum()
            print(f"    {seg:<14} {v / tot * 100:6.2f}%  ({v:.1f} µs / {tot:.1f} µs)")

    if output_csv:
        df_all.to_csv(output_csv, index=False)   # 明细始终是**未过滤**的原始样本
        print(f"\n明细已保存至: {output_csv}（未过滤，供自行复核）")

    # ============================================================
    # 绘图前按分位数裁剪（只影响图，不影响上面的统计与落盘明细）
    # ============================================================
    df_plot, n_clip = _clip_by_pct(df_all, clip_pct, _SEGMENTS, hops)
    if clip_pct is not None:
        print(f"\n绘图裁剪 clip_pct={clip_pct}：按 (hop, segment) 分组各裁掉上尾，"
              f"共丢弃 {n_clip} / {len(df_all)} 行（{n_clip/max(len(df_all),1)*100:.1f}%）"
              f"—— 上面的 mean/p99/max 与落盘明细都是**未裁剪**的原始值")

    # ============================================================
    # violin + 箱线
    # ============================================================
    fig = px.violin(
        df_plot,
        x="hop",
        y="us",
        color="segment",
        box=True,
        points=points,
        category_orders={"hop": hops, "segment": list(_SEGMENTS)},
        color_discrete_map={
            "total": "#4D4D4D",
            "① write-slot": "#2C5F7A",
            "② schedule": "#D4834A",
            "③ pack-input": "#8CAA8C",
        },
        labels={"us": "<b>time (µs)</b>", "hop": "<b>hop</b>", "segment": ""},
        title="<b>Multihop Chain: Per-Hop Handoff Latency</b>",
    )
    fig.update_traces(meanline_visible=True, jitter=0.25,
                      marker=dict(size=3, opacity=0.4))
    fig.update_layout(
        violinmode="group",
        plot_bgcolor="white",
        paper_bgcolor="white",
        template="plotly_white",
        height=560,
        # 不设 rangemode="tozero"：负值样本要画出来（② 为负 = finished 不在因果路径上，
        # total 为负 = 序列错位），钉住零点会把它们整个藏到轴外。
        yaxis=dict(title="<b>time (µs)</b>"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02,
                    xanchor="right", x=1, title=None),
    )
    fig.write_html(output_html)
    print(f"\n小提琴图已保存至: {output_html}")

    return fig


if __name__ == "__main__":
    analyze_hop_latency(
        timeline_csv="hop_chain/multihop_u50_m2_ms05_chain6_timeline.csv",
        output_html="hop_latency_violin.html",
    )
