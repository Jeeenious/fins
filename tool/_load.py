#!/usr/bin/env python
# coding: utf-8

# # 任务拓扑生成器说明
# 
# ## 1. 概述
# 
# 本项目提供五类任务拓扑生成器：
# 
# - `multihop`：多跳链
# - `fork`：扇出
# - `join`：扇入
# - `feedback`：反馈（由 hist 窗口构成的闭环）
# - `mixed`：混合拓扑
# 
# **链/分支位置都可能混入 `usr_acc`（hist 窗口读）节点**（参数 `phist` / `hist`）：
# multihop / fork / join / feedback 四种在配置里各自单列这两项；mixed 的片段节点走默认值。
#
# - 每个已产出字段里**与 feed 不同名者**独立以概率 `phist` 被选作 hist 端口
#   → 命中 k 个即 `usr_acc{k}`（k=0 则退回 `usr_relay`；k 上限 10 = 插件侧 usr_acc..usr_acc10）。
#   故 `phist` 同时决定"有没有 hist"与"有几个"，`hist` 只管每个窗口的**长度 N**（须 >2）。
# - 第一个输入恒为前级 feed（标量读最新一帧）；hist 端口读该字段最近 N 帧窗口。
# - 数据驱动(event)链仍走绑定边精确消费单帧；显式周期(timed)任务由 period 定时释放，
#   输入从字段历史缓存取样。

import os
import json
import math
import random
from collections import Counter, deque

# ============================================================
# 1. Algorithm maps
# ============================================================

FAN_OUT = {
    1: "usr_relay",
    2: "usr_fork",
    3: "usr_fork3",
    4: "usr_fork4",
    5: "usr_fork5",
    6: "usr_fork6",
    7: "usr_fork7",
    8: "usr_fork8",
    9: "usr_fork9",
    10: "usr_fork10",
}

FAN_IN = {
    1: "usr_relay",
    2: "usr_join",
    3: "usr_join3",
    4: "usr_join4",
    5: "usr_join5",
    6: "usr_join6",
    7: "usr_join7",
    8: "usr_join8",
    9: "usr_join9",
    10: "usr_join10",
}

ACCUMULATOR = {
    1: "usr_acc",
    2: "usr_acc2",
    3: "usr_acc3",
    4: "usr_acc4",
    5: "usr_acc5",
    6: "usr_acc6",
    7: "usr_acc7",
    8: "usr_acc8",
    9: "usr_acc9",
    10: "usr_acc10",
}

# ============================================================
# 2. Configuration
# ============================================================

# 拓扑生成的默认参数：CONFIG 与各 _sk_* 的默认值共用同一份（故须定义在 CONFIG 之前）
ACC_PROB = 0.25  # 链上节点成为 hist 窗口节点（usr_acc）的概率
HIST_N_RANGE = (3, 8)  # hist 窗口长度 N 的默认范围（须 > 2）

CONFIG = {

    "topology": {

        # phist / hist 对以下四种拓扑同名同义（都经 _chain_step / _acc_or_relay）：
        #   phist = 每个与 feed 不同名的已产出字段，独立以此概率被选作 hist 端口
        #           → 命中 k 个即 usr_acc{k}（k=0 退回 usr_relay，k 上限 10）
        #   hist  = 每个窗口的长度 N（帧数），int 或 (lo, hi)，须 > 2
        # 省略则取默认（ACC_PROB / HIST_N_RANGE）。mixed 不列这两项，片段内部走默认值。
        #
        # 形状参数：paths = 独立链条数；fan = 扇出/扇入路数；
        # depth = 链深度（multihop 每条链 / fork 每条支路 / join lane 与尾链共用 /
        #         feedback 主链）

        "multihop": {
            "paths": (1, 4),
            "depth": (2, 8),
            "phist": ACC_PROB,
            "hist": HIST_N_RANGE,
        },

        "fork": {
            "fan": (2, 8),
            "depth": (1, 3),
            "phist": ACC_PROB,
            "hist": HIST_N_RANGE,
        },

        "join": {
            "fan": (2, 8),

            # 合并前（fan 条 lane）与合并后（尾链）共用同一个深度值
            "depth": (1, 3),
            "phist": ACC_PROB,
            "hist": HIST_N_RANGE,
        },

        "feedback": {
            "depth": (3, 8),

            # 环头 acc 恒建（闭环必需），phist 只作用于环头之前的普通节点；
            # hist 对两者都生效
            "phist": ACC_PROB,
            "hist": HIST_N_RANGE,
        },

        "mixed": {
            "nseg": (3, 8),

            # 每段四选一（顺序 = multihop / fork / join / feedback），
            # 四者之和必须为 1 —— 判据是 rng.random() 的落点区间，
            # 和不为 1 时 else 分支会吃掉残差，pfeedback 实际占比 ≠ 这里写的值
            "pmultihop": 0.25,
            "pfork": 0.25,
            "pjoin": 0.25,
            "pfeedback": 0.25,

            # 无 phist / hist：片段内节点走默认值，不再单列（同一语义不出现两处）
        },
    },

    "temporal": {

        # Probability that a non-source node is timed.
        #
        # 对 hist 节点同样适用：未抽中 timed 的 hist 节点走 event（窗口读与事件触发
        # 共存，见 _assign_temporal）。设 1.0 可退回"hist 必为 timed"的旧行为。
        "ptimed": 0.35,

        # 抽稀倍数 N > 1 的抽样概率（仅对 hist 节点选为 event 时生效）：
        # 支配节点每产出 N 帧本节点才触发 1 次 → 虚拟周期 = N × min(触发源周期)。
        # N 取 H/T_min 的因子，使 N×T_min 仍整除标称 HP —— 运行时会自动拓宽不整除的配置，
        # 但拓宽会让所有节点实例数一起翻倍，故生成期就避免。设 0.0 则恒 N=1（不抽稀）。
        "pthin": 0.25,

        # T values are H / divisor.
        #
        # Example:
        #
        # H=100
        #
        # divisor 1 -> 100 ms
        # divisor 2 -> 50 ms
        # divisor 4 -> 25 ms
        #
        "period_divisors": [1, 2, 4, 5, 10, 20],
    },

    "workload": {

        "u": [
            0.1,
            0.3,
            0.7,
            0.9,
        ],

        "workers": [
            1,
            2,
            3,
        ],

        # Hyperperiod.
        "H_ms": 100,

        # ----------------------------------------------------
        # Makespan classification.
        #
        # Example H=100, width=5:
        #
        # bin 00: [0,5)
        # bin 01: [5,10)
        # ...
        # bin 18: [90,95)
        # bin 19: [95,100]
        #
        # The last bucket includes H.
        # ----------------------------------------------------

        "makespan_bin_width_ms": 10.0,

        # Anything above this goes into overflow.
        #
        # None:
        #     use H_ms.
        #
        "makespan_max_ms": 80,

        # Number of final samples per bucket.
        "n_per": 5,

        # ----------------------------------------------------
        # Generation policy.
        # ----------------------------------------------------

        # Initial candidate generation.
        "initial_attempts": 500,

        # Extra attempts for incomplete buckets.
        "refill_attempts": 500,

        # Maximum number of refill rounds.
        "max_refill_rounds": 1,

        # Keep at most:
        #
        #     n_per * candidate_factor
        #
        # candidates per bucket.
        #
        "candidate_factor": 1,

        # Random seed.
        "seed_base": 20260910,

        # Output directory（默认回退路径，可被显式传入参数覆盖）。
        "out_dir": "pipeline",
    },

    "solver": {

        # Utilization numerical tolerance.
        "u_tolerance": 1e-8,

        # Minimum WCET.
        #
        # Plugin cfg is in microseconds.
        #
        "min_wcet_us": 100,

        # C_i <= T_i * max_c_ratio
        "max_c_ratio": 1.0,

        # Number of attempts used to find a C allocation.
        "c_attempts": 300,
    },
}


# ============================================================
# 3. Basic helpers
# ============================================================

def save(path, nodes):
    with open(path, "w") as f:
        json.dump(nodes, f, indent=2)


def _randint(rng, spec):
    if isinstance(spec, int):
        return spec

    lo, hi = spec

    if lo > hi:
        raise ValueError(
            f"Invalid integer range: {spec}"
        )

    return rng.randint(lo, hi)


def _choice(rng, values):
    values = list(values)

    if not values:
        raise ValueError(
            "Cannot choose from an empty sequence."
        )

    return rng.choice(values)


def _validate_probability(name, value):
    if not 0.0 <= value <= 1.0:
        raise ValueError(
            f"{name} must be in [0,1], got {value}"
        )


def _add(sk, algo, fo, ins=None, hist=None):
    node = {
        "algo": algo,
        "fo": fo,
    }

    if ins:
        node["ins"] = ins

    if hist:
        node["hist"] = dict(hist)

    sk.append(node)

    return len(sk) - 1


# ============================================================
# 4. Topology generation
# ============================================================

THIN_MAX = 4  # event 抽稀倍数 N 的上限（候选 = H/T_min 在 [2, THIN_MAX] 内的因子）


def _field_ref(idx, port):
    return f"p{idx}_{port}"


def _field_of(ref):
    return _field_ref(*ref)


def _record(sk, produced, idx, fo):
    for p in range(fo):
        produced.append((idx, p))


def _acc_or_relay(sk, rng, produced, feed, phist=ACC_PROB, hist=HIST_N_RANGE):
    """按 phist **逐个候选字段独立**判定，生成 usr_acc{k} 或 usr_relay，返回节点 index。

    候选 = 已产出字段里与 feed 不同名者。每个候选独立以 phist 概率被选中作 hist 端口
    → 命中数 k 随机，节点形态 = ACCUMULATOR[k]（usr_acc / usr_acc2 / …）；k=0 退回 relay。
    故 phist 同时决定"有没有 hist"与"有几个"，hist 只管每个窗口的**长度 N**。

    k 上限 = len(ACCUMULATOR)（= 插件侧 usr_acc..usr_acc10），超出部分截断。
    注意：候选数随图规模增长，故越靠后的节点越容易命中（P(命中≥1) = 1-(1-phist)^|cand|）。

    @param feed 本节点的主输入字段 (idx, port)
    @retval int 新节点 index（未登记 produced，由调用方 _record）
    """
    cand = [o for o in produced if _field_of(o) != _field_of(feed)]
    srcs = [o for o in cand if rng.random() < phist][:max(ACCUMULATOR)]

    if srcs:
        return _add(
            sk,
            ACCUMULATOR[len(srcs)],
            1,
            [feed] + srcs,
            hist={
                _field_of(o): _randint(rng, hist) for o in srcs
            },
        )
    return _add(sk, "usr_relay", 1, [feed])


def _chain_step(sk, rng, produced, feed, phist=ACC_PROB, hist=HIST_N_RANGE):
    """沿链推进一步：普通 relay 或 hist 节点（见 _acc_or_relay），并登记其输出字段。

    phist=0 → 纯 relay 链（单入单出、唯一前序，传参时延分析的前提）。
    返回新节点 index（其输出字段 = p{index}_0）。
    """
    n = _acc_or_relay(sk, rng, produced, feed, phist, hist)
    _record(sk, produced, n, 1)
    return n


def _sk_multihop(rng, paths, depth, phist=ACC_PROB, hist=HIST_N_RANGE):
    sk = []
    produced = []

    for _ in range(paths):

        r = _add(sk, "usr_src", 1)
        _record(sk, produced, r, 1)

        feed = (r, 0)
        for _ in range(depth):
            feed = (_chain_step(sk, rng, produced, feed, phist, hist), 0)

        _add(sk, "usr_sink", 1, [feed])

    return sk, set()


def _sk_fork(rng, fan, depth, phist=ACC_PROB, hist=HIST_N_RANGE):
    """扇出：src → fork{fan}，每条支路接 depth 跳后各自 sink（死端）。

    depth 是**已抽好的整数**（由 generate_topology 现抽），fan 条支路共用同一个值。
    """
    sk = []
    produced = []

    r = _add(sk, "usr_src", 1)
    _record(sk, produced, r, 1)

    f = _add(sk, FAN_OUT[fan], fan, [(r, 0)])
    _record(sk, produced, f, fan)

    for _ in range(fan):

        # 多输出节点：后继随机连到 f 的任意输出端口（0..fan-1）。
        port = rng.randrange(fan)
        feed = (f, port)

        for _ in range(depth):
            feed = (_chain_step(sk, rng, produced, feed, phist, hist), 0)

        _add(sk, "usr_sink", 1, [feed])

    return sk, set()


def _sk_join(rng, fan, depth, phist=ACC_PROB, hist=HIST_N_RANGE):
    """扇入：fan 条独立 lane（各带自己的 src）各接 depth 跳 → join{fan} → 再接 depth 跳 → sink。

    depth 是**已抽好的整数**，合并前与合并后共用同一个值——同一块里不能有两个 depth 键，
    且这两段的性质本就不同（合并前 fan 路可并行，合并后单路串行），用同一个深度只是
    统一了命名，不是把它们并成一件事。
    """
    sk = []
    produced = []
    lanes = []

    for _ in range(fan):

        r = _add(sk, "usr_src", 1)
        _record(sk, produced, r, 1)

        feed = (r, 0)
        for _ in range(depth):
            feed = (_chain_step(sk, rng, produced, feed, phist, hist), 0)

        lanes.append(feed)

    j = _add(sk, FAN_IN[fan], 1, lanes)
    _record(sk, produced, j, 1)

    feed = (j, 0)
    for _ in range(depth):
        feed = (_chain_step(sk, rng, produced, feed, phist, hist), 0)

    _add(sk, "usr_sink", 1, [feed])

    return sk, set()


def _sk_feedback(rng, depth, phist=ACC_PROB, hist=HIST_N_RANGE):
    """feedback：链首(环头) acc hist 读链尾输出 → 由 hist 窗口构成的闭环。

    环头之前可混入任意 relay/acc；环头(acc, timed) 之后到链尾强制 event，
    让链头→…→链尾有真实绑定边(前序)路径；链头 hist 读链尾字段，跨周期回环。
    phist 只作用于**环头之前**的普通节点；环头自身的 acc 是闭环必需，恒建、
    恒 1 个窗口（输入 = feed + 链尾回环字段），不受 phist 影响。
    hist（窗口长度 N）对两者都生效——环头读链尾字段的窗口长度也由它定。
    """
    if depth < 2:
        raise RuntimeError(f"feedback depth 需 ≥2 以构成闭环，depth={depth}")

    sk = []
    produced = []
    must_event = set()

    r = _add(sk, "usr_src", 1)
    _record(sk, produced, r, 1)

    # 环头位置：链中节点编号 1..depth（链尾 = depth）
    head = rng.randint(1, depth - 1)

    feed = (r, 0)
    for i in range(1, depth + 1):

        if i == head:

            # usr_acc：feed(前级字段, 标量) + hist(链尾 p{depth}_0, N>2)
            histN = _randint(rng, hist)
            n = _add(
                sk,
                ACCUMULATOR[1],
                1,
                [feed, (depth, 0)],
                hist={
                    _field_ref(depth, 0): histN,
                },
            )
            _record(sk, produced, n, 1)

        else:

            if i > head:

                # 环头之后 → 强制 event（绑定边）铺出到链尾的前序路径
                n = _add(sk, "usr_relay", 1, [feed])
                must_event.add(n)

            else:

                # 环头之前：普通节点（可混入 acc，acyclic）
                n = _chain_step(sk, rng, produced, feed, phist, hist)

            _record(sk, produced, n, 1)

        feed = (n, 0)

    # 链尾 = 编号 depth 的节点，其输出字段 = p{depth}_0（环头 hist 读它）
    _add(sk, "usr_sink", 1, [feed])

    return sk, must_event


def _sk_mixed(rng, nseg, pmultihop=0.25, pfork=0.25, pjoin=0.25, pfeedback=0.25):
    """混合骨架：从单个 src 起接 nseg 个片段，每段按四个概率之一选一种片段类型
    （顺序 = multihop / fork / join / feedback）。

    四个片段与四种 kind 同形，区别只在"挂在当前的 r 上"：
      multihop：从 r 接 depth 跳链，r 前移到链尾
      fork    ：从 r 扇出 fan 条分支，各接若干跳后各自 usr_sink（死端），**r 不变**
      join    ：从 r 扇出 fan 条分支 → 各接若干跳 → join 回一路，r 前移到 join 节点
      feedback：从 r 接一个 hist 节点（窗口读更早的字段，acyclic），r 前移

    判据是 rng.random() 的落点区间，故四者之和须为 1——否则 else 分支会吃掉残差，
    feedback 实际占比 ≠ 传入值。

    片段内部的节点一律走 _chain_step / _acc_or_relay 的**默认** phist / hist
    （ACC_PROB / HIST_N_RANGE）——mixed 不单列这两个键，避免同一语义在配置里出现两处。
    """
    total = pmultihop + pfork + pjoin + pfeedback
    if abs(total - 1.0) > 1e-9:
        raise ValueError(
            f"mixed 四个片段概率之和须为 1（pmultihop + pfork + pjoin + pfeedback），收到 {total}"
        )

    sk = []
    produced = []

    r = _add(sk, "usr_src", 1)
    _record(sk, produced, r, 1)

    for _ in range(nseg):

        x = rng.random()

        if x < pmultihop:

            # multihop 片段：从 r 接一条短链
            depth = rng.randint(1, 3)
            feed = (r, 0)
            for _ in range(depth):
                feed = (_chain_step(sk, rng, produced, feed), 0)
            r = feed[0]

        elif x < pmultihop + pfork:

            # fork 片段：扇出后各自收尾为 sink，r 不动（后续片段仍从 r 续接）
            fan = rng.randint(2, 5)
            f = _add(sk, FAN_OUT[fan], fan, [(r, 0)])
            _record(sk, produced, f, fan)

            for bi in range(fan):
                feed = (f, rng.randrange(fan))
                for _ in range(rng.randint(1, 3)):
                    feed = (_chain_step(sk, rng, produced, feed), 0)
                _add(sk, "usr_sink", 1, [feed])

        elif x < pmultihop + pfork + pjoin:

            # join 片段：扇出 → 各支路 → 汇回一路，r 前移到 join 节点
            fan = rng.randint(2, 5)
            f = _add(sk, FAN_OUT[fan], fan, [(r, 0)])
            _record(sk, produced, f, fan)

            tails = []
            # 分支端口**无放回**取（随机置换）：fork 的 fan 个输出端口恰好各接一条分支。
            # 有放回时两条分支可能连同一个端口，join 的输入里就会出现同一字段两次
            # （算法签名要求 N 个输入，重复字段使 join 名不副实；且该字段会在 event
            # 声明里重复，被 C++ 解析器判非法）。
            ports = rng.sample(range(fan), fan)
            for bi in range(fan):
                feed = (f, ports[bi])
                if rng.random() < 0.5:
                    feed = (_chain_step(sk, rng, produced, feed), 0)
                tails.append(feed)

            r = _add(sk, FAN_IN[fan], 1, tails)
            _record(sk, produced, r, 1)

        else:

            # feedback 片段：hist 节点读更早的字段（acyclic 字段窗口；
            # 真正的环只在 feedback kind 构造）
            r = _acc_or_relay(sk, rng, produced, (r, 0))
            _record(sk, produced, r, 1)

    _add(sk, "usr_sink", 1, [(r, 0)])

    return sk, set()


# 各拓扑块允许的键（白名单，generate_topology 逐块校验；旧键一律硬报错）
_TOPO_KEYS = {
    "multihop": ("paths", "depth", "phist", "hist"),
    "fork": ("fan", "depth", "phist", "hist"),
    "join": ("fan", "depth", "phist", "hist"),
    "feedback": ("depth", "phist", "hist"),
    # mixed 不列 phist / hist：片段内节点走默认值
    "mixed": ("nseg", "pmultihop", "pfork", "pjoin", "pfeedback"),
}

# 已作废的旧键 → 现键（仅用于报错时给改名指引；这些键不再被读取）
_TOPO_RENAMED = {
    "acc_prob": "phist",
    "bdepth": "depth",
    "tail": "depth",
    "histN": "hist",
    "chain_prob": "pmultihop",
    "fork_join_prob": "pfork / pjoin",
    "feedback_prob": "pfeedback",
    "hist_k": "（窗口个数已不可配，phist 逐候选判定）",
}


def generate_topology(kind, rng, config=None):
    cfg = CONFIG if config is None else config

    tc = cfg["topology"]

    def _check_keys(block, kind):
        """该拓扑块的键必须落在白名单内。

        旧键一律**硬报错**而不是静默忽略——静默忽略正是"配置改了不生效"的成因
        （mixed 的三个概率键、feedback.histN、bdepth/tail 都曾如此）。
        """
        unknown = sorted(set(block) - set(_TOPO_KEYS[kind]))
        if not unknown:
            return
        hints = [f"{k} → {_TOPO_RENAMED[k]}" for k in unknown if k in _TOPO_RENAMED]
        raise ValueError(
            f"[{kind}] 未知参数 {unknown}；该块可用键 = {list(_TOPO_KEYS[kind])}"
            + (f"（旧键改名：{'；'.join(hints)}）" if hints else "")
        )

    def _depth_spec(block):
        """该块的链深度规格（int 或 (lo, hi)）。"""
        if "depth" not in block:
            raise ValueError('[拓扑块] 缺少 depth（链深度，int 或 (lo, hi)）')
        return block["depth"]

    def _hist_args(block):
        """该拓扑块的 (phist, hist)，带范围校验。

        phist = 每个与 feed 不同名的已产出字段，独立以此概率被选作 hist 端口
                （命中 k 个即 usr_acc{k}，k=0 退回 usr_relay）
        hist  = 每个窗口的**长度 N**（帧数），int 或 (lo, hi)，每步现抽，须 > 2
                （C++ check_topology ④ 拒 N≤2；配小了原本要到灌配置时才报错）
        不写这两项时取默认（ACC_PROB / HIST_N_RANGE）。
        """
        phist = block.get("phist", ACC_PROB)
        hist = block.get("hist", HIST_N_RANGE)
        lo, _ = (hist, hist) if isinstance(hist, int) else hist
        if lo <= 2:
            raise ValueError(
                f"hist 窗口长度须 > 2（g_state.hpp check_topology ④），收到 {hist}"
            )
        return phist, hist

    if kind not in _TOPO_KEYS:
        raise ValueError(f"Unknown topology kind: {kind}")

    block = tc[kind]
    _check_keys(block, kind)

    if kind == "multihop":
        return _sk_multihop(
            rng,
            _randint(rng, block["paths"]),
            _randint(rng, block["depth"]),
            *_hist_args(block),
        )

    if kind == "fork":
        return _sk_fork(
            rng,
            _randint(rng, block["fan"]),
            _randint(rng, _depth_spec(block)),
            *_hist_args(block),
        )

    if kind == "join":
        return _sk_join(
            rng,
            _randint(rng, block["fan"]),
            # 合并前/合并后共用同一个抽出来的深度（同块不能有两个 depth 键）
            _randint(rng, _depth_spec(block)),
            *_hist_args(block),
        )

    if kind == "feedback":
        return _sk_feedback(
            rng,
            _randint(rng, block["depth"]),
            *_hist_args(block),
        )

    # mixed：无 phist / hist（片段内节点走默认值）
    return _sk_mixed(
        rng,
        _randint(rng, block["nseg"]),
        block.get("pmultihop", 0.25),
        block.get("pfork", 0.25),
        block.get("pjoin", 0.25),
        block.get("pfeedback", 0.25),
    )


# ============================================================
# 5. DAG ordering
# ============================================================

def _node_order(sk):
    return list(range(len(sk)))


# ============================================================
# 6. Period generation
# ============================================================

def _valid_periods(H_ms, divisors):
    periods = []

    for d in divisors:

        if d <= 0:
            raise ValueError(
                f"Invalid period divisor: {d}"
            )

        period = H_ms / float(d)

        ratio = H_ms / period

        if abs(
                ratio - round(ratio)
        ) < 1e-12:
            periods.append(
                period
            )

    if not periods:
        raise ValueError(
            f"No valid periods for H_ms={H_ms}, "
            f"divisors={divisors}"
        )

    return sorted(
        set(periods),
        reverse=True,
    )


# ============================================================
# 7. Temporal assignment
# ============================================================

def _input_field(pi, port):
    """输入端口对应的字段名。

    与 _make_pipeline 的 output_names 命名同源（p{生产者下标}_{输出序号}）——
    _assign_temporal 拿不到 output_names 表，故按同一规则就地重建。
    """
    return f"p{pi}_{port}"


def _event_ports(nd):
    """声明 event 的端口（字段名）列表 = **非 hist** 的输入端口。

    hist 端口是窗口读、不能同时作触发源（C++ check_topology ④ 拒绝 hist∩event
    同端口），故触发源只能是其余输入。顺序 = inputs 顺序（确定性），**按首次出现去重**
    —— 同一端口在 event 里出现两次会被 C++ 解析器判"重复声明"拒绝（inputs 允许重复，
    event 不允许）。
    """
    hist = set(nd.get("hist", {}).keys())
    seen = set()
    out = []
    for pi, port in nd.get("ins", []):
        name = _input_field(pi, port)
        if name in hist or name in seen:
            continue
        seen.add(name)
        out.append(name)
    return out


def _event_thinning(sk, i, T):
    """hist 节点选为 event 时的抽稀倍数 N = T[i] / min(非 hist 输入的前级周期)。

    与 _assign_temporal 的取值同源；比例非整数（异常）时回落到 1（不抽稀）。
    """
    nd = sk[i]
    hist = set(nd.get("hist", {}).keys())
    feed = [
        T[pi]
        for pi, port in nd.get("ins", [])
        if _input_field(pi, port) not in hist
    ]
    if not feed or any(p is None for p in feed):
        return 1
    ratio = T[i] / min(feed)
    if abs(ratio - round(ratio)) > 1e-9 or ratio < 1:
        return 1
    return int(round(ratio))


def _sample_thinning(rng, H_ms, T_min, pthin):
    """抽样 event 端口的抽稀倍数 N（支配节点每产出 N 帧，本节点触发 1 次）。

    N 必须整除 d = H_ms / T_min。C++ 侧已不再拒绝不整除的配置，而是把超周期拓宽到能容纳
    N × T_min（build_dominance 第二趟），但**生成期仍主动避开**：拓宽会把 HP 拉长（如 100 → 200ms），
    所有节点的实例数一起翻倍、静态展开变大、单次实验的有效拍数变少 —— 对采样语料没有任何好处。
    故候选 = divisors(d) ∩ [2, THIN_MAX]；无可用候选（d 为质数等）或未抽中 → N = 1。
    """
    if pthin <= 0 or rng.random() >= pthin:
        return 1

    d = int(round(H_ms / T_min))

    cands = [
        k
        for k in range(2, THIN_MAX + 1)
        if d % k == 0
    ]

    if not cands:
        return 1

    return rng.choice(cands)


def _virtual_period(rng, H_ms, t_min, pthin):
    """event 节点的虚拟周期 = 抽稀倍数 N × min(触发源周期)。

    与 C++ build_dominance 的支配端口规则一致：各 event 端口写同一个 N，故
    min over 端口 (N × T_p) = N × min(T_p)。t_min 由调用方从**该节点已定的触发源周期**
    里取最小（hist 节点只看非 hist 输入；其余节点看全部输入）。
    """
    return _sample_thinning(rng, H_ms, t_min, pthin) * t_min


def _assign_temporal(
        sk,
        H_ms,
        seed,
        ptimed,
        period_divisors,
        must_event=None,
        pthin=0.0):
    rng = random.Random(seed)

    if must_event is None:
        must_event = set()

    _validate_probability(
        "ptimed",
        ptimed,
    )

    _validate_probability(
        "pthin",
        pthin,
    )

    periods = _valid_periods(
        H_ms,
        period_divisors,
    )

    order = list(range(len(sk)))

    n = len(sk)

    trig = [None] * n
    T = [None] * n

    anchor_needed = True

    for i in order:

        ins = sk[i].get("ins", [])
        hist = sk[i].get("hist", {})

        # ----------------------------------------------------
        # Source
        # ----------------------------------------------------

        if not ins:

            trig[i] = "timed"

            if anchor_needed:
                T[i] = float(H_ms)
                anchor_needed = False
            else:
                T[i] = _choice(
                    rng,
                    periods,
                )

            continue

        # ----------------------------------------------------
        # hist（窗口读）节点
        #
        # 窗口读与事件触发**可以共存**（2026-09-19 起）：hist 节点按 ptimed 概率
        #   ① timed —— 由显式周期释放，窗口量由周期表达；
        #   ② event —— 由**非 hist 输入**的 producer 完成事件释放，hist 端口仍读窗口
        #      （hist 是窗口读、不能同端口兼任触发源）。
        #
        # 前级周期未知（拓扑序未保证）或无非 hist 输入时一律回落 timed：宁可退回旧行为，
        # 也不写出与 C++ 支配规则不一致的 T（否则 makespan/利用率与实际运行不符）。
        # ----------------------------------------------------

        if hist:
            feed_periods = [
                T[pi]
                for pi, port in ins
                if _input_field(pi, port) not in hist
            ]

            if (
                    feed_periods
                    and all(p is not None for p in feed_periods)
                    and rng.random() >= ptimed
            ):
                trig[i] = "event"
                T[i] = _virtual_period(rng, H_ms, min(feed_periods), pthin)

            else:
                trig[i] = "timed"
                T[i] = _choice(
                    rng,
                    periods,
                )

            continue

        # ----------------------------------------------------
        # Ordinary non-source node
        #
        # period 与 event 是二选一的触发模式，且**一律显式**：timed 写 period、
        # event 写 event 声明（见 _make_pipeline）。不再产出"既不写 period 也不写
        # event"的隐式事件节点 —— 那种配置依赖运行时的隐式回退，触发方式在 JSON 里
        # 读不出来（viewer 只能显示"旧式"，语义也不清晰）。
        # ----------------------------------------------------

        if i in must_event:
            # feedback 环：链头→链尾强制 event（绑定边铺出前序路径，
            # 使链尾输出在因果上源于链头）
            pred_periods = [
                T[pi]
                for pi, _ in ins
                if T[pi] is not None
            ]
            if not pred_periods:
                raise RuntimeError(
                    f"Node {i} must_event 但无前级周期。"
                )
            trig[i] = "event"
            T[i] = _virtual_period(rng, H_ms, min(pred_periods), pthin)
            continue

        if rng.random() < ptimed:

            trig[i] = "timed"

            T[i] = _choice(
                rng,
                periods,
            )

        else:

            trig[i] = "event"

            pred_periods = [
                T[pi]
                for pi, _ in ins
                if T[pi] is not None
            ]

            if not pred_periods:
                raise RuntimeError(
                    f"Node {i} has no predecessor period."
                )

            T[i] = _virtual_period(rng, H_ms, min(pred_periods), pthin)

    if H_ms not in T:
        raise RuntimeError(
            "Temporal generation failed: "
            "no H-period timed source."
        )

    return trig, T


# ============================================================
# 8. Build periodic release schedule
# ============================================================

def _timed_instance_count(H_ms, T):
    if T <= 0:
        raise ValueError(
            f"Invalid T={T}"
        )

    n = int(
        round(
            H_ms / T
        )
    )

    if n <= 0:
        raise RuntimeError(
            f"Invalid instance count "
            f"H={H_ms}, T={T}"
        )

    return n


def _release_time(ready, T):
    if T <= 0:
        raise ValueError(
            f"Invalid period {T}"
        )

    k = math.ceil(
        ready / T - 1e-12
    )

    return k * T


def _execution_inputs(sk, node_index):
    """
    Return inputs that participate in execution-time
    dependency propagation.

    hist 窗口输入不展开/不重复——它由 hist 节点的周期释放表达。
    """
    nd = sk[node_index]

    hist_ports = set(
        nd.get("hist", {}).keys()
    )

    result = []

    for pi, port in nd.get("ins", []):

        port_name = f"p{pi}_{port}"

        if port_name in hist_ports:
            continue

        result.append(
            (pi, port)
        )

    return result


# ============================================================
# 9. Makespan semantics
# ============================================================
def _calculate_makespan(sk, trig, T, C_ms, H_ms):
    """
    Calculate makespan using final periodic-instance output
    propagation.

    Semantics
    ---------
    1. Find the minimum period among all timed nodes.

    2. The last instance of that minimum-period stream is the
       temporal starting point.

           last_instance_time =
               (H_ms / min_T - 1) * min_T

    3. The last instance executes the minimum-period timed node
       itself.

    4. After that execution finishes, propagation continues from
       the OUTPUT of the current node.

    5. Only EVENT nodes belong to this final propagation chain.

       A downstream timed node does NOT create another periodic
       instance for this makespan calculation.

    6. hist inputs are window reads over a history buffer and do not
       create precedence edges (feedback goes through the window buffer).

    7. hist window length does not multiply execution time.

    Therefore:

        makespan =
            final_instance_release_time
            +
            C[min_period_node]
            +
            longest_event_downstream_path
    """

    n = len(sk)

    # ========================================================
    # 1. Find timed nodes
    # ========================================================

    timed_nodes = [
        i
        for i in range(n)
        if trig[i] == "timed"
    ]

    if not timed_nodes:
        raise RuntimeError(
            "No timed node exists."
        )

    # ========================================================
    # 2. Find minimum-period timed node(s)
    # ========================================================

    min_T = min(
        T[i]
        for i in timed_nodes
    )

    min_timed_nodes = [
        i
        for i in timed_nodes
        if abs(T[i] - min_T) < 1e-12
    ]

    # ========================================================
    # 3. Find the final instance time
    #
    # Example:
    #
    # H = 100
    # T = 10
    #
    # instances:
    #
    # 0, 10, ..., 90
    #
    # final instance = 90
    # ========================================================

    instance_count = _timed_instance_count(
        H_ms,
        min_T,
    )

    last_instance_time = (
                                 instance_count - 1
                         ) * min_T

    # ========================================================
    # 4. Build output -> consumers mapping
    #
    # (producer_node, output_port)
    #         |
    #         +----> consumer node
    #
    # IMPORTANT:
    #
    # hist inputs are window reads, NOT consumers for execution
    # propagation.
    # ========================================================

    output_consumers = {}

    for i, nd in enumerate(sk):

        hist_ports = set(
            nd.get("hist", {}).keys()
        )

        for pi, port in nd.get(
                "ins",
                [],
        ):

            port_name = (
                f"p{pi}_{port}"
            )

            # hist 窗口输入（回看历史字段）：不参与最终执行传播。
            if port_name in hist_ports:
                continue

            key = (
                pi,
                port,
            )

            output_consumers.setdefault(
                key,
                [],
            ).append(i)

    # ========================================================
    # 5. Longest path AFTER the current node
    #
    # IMPORTANT:
    #
    # We start from the CURRENT NODE'S OUTPUT.
    #
    # The current node's C is handled by the caller.
    #
    # Therefore this function only calculates:
    #
    #     output -> event -> event -> ... -> terminal
    #
    # Timed downstream nodes stop this propagation.
    # ========================================================

    memo = {}
    visiting = set()

    def longest_event_from_output(
            node_index,
            output_port,
    ):

        key = (
            node_index,
            output_port,
        )

        if key in memo:
            return memo[key]

        consumers = output_consumers.get(
            key,
            [],
        )

        # ----------------------------------------------------
        # No consumer:
        #
        # This output is terminal.
        # ----------------------------------------------------

        if not consumers:
            memo[key] = 0.0
            return 0.0

        longest = 0.0

        for child in consumers:

            # ------------------------------------------------
            # Only EVENT nodes participate in the final
            # propagation.
            #
            # If the next node is timed, this final instance
            # does not wait for a new periodic release.
            #
            # Therefore this branch stops here.
            # ------------------------------------------------

            if trig[child] != "event":
                continue

            child_cost = C_ms[child]

            # ------------------------------------------------
            # Continue from EVERY output of the event node.
            #
            # If the node has multiple outputs, the final
            # propagation may continue through different
            # consumers.
            #
            # For makespan, take the longest resulting branch.
            # ------------------------------------------------

            child_longest = 0.0

            fo = sk[child].get(
                "fo",
                1,
            )

            for child_port in range(fo):
                downstream = (
                    longest_event_from_output(
                        child,
                        child_port,
                    )
                )

                child_longest = max(
                    child_longest,
                    downstream,
                )

            total = (
                    child_cost
                    +
                    child_longest
            )

            longest = max(
                longest,
                total,
            )

        return_value = longest

        memo[key] = return_value

        return return_value

    # ========================================================
    # 6. Calculate makespan for every minimum-T node
    #
    # Start from the NODE'S OUTPUT.
    # ========================================================

    longest_total_execution = 0.0

    for anchor in min_timed_nodes:

        # ----------------------------------------------------
        # The final instance first executes the anchor itself.
        # ----------------------------------------------------

        anchor_execution = C_ms[anchor]

        # ----------------------------------------------------
        # Then propagate from the anchor's outputs.
        # ----------------------------------------------------

        downstream = 0.0

        fo = sk[anchor].get(
            "fo",
            1,
        )

        for port in range(fo):
            path = (
                longest_event_from_output(
                    anchor,
                    port,
                )
            )

            downstream = max(
                downstream,
                path,
            )

        total = (
                anchor_execution
                +
                downstream
        )

        longest_total_execution = max(
            longest_total_execution,
            total,
        )

    # ========================================================
    # 7. Final makespan
    # ========================================================

    makespan = (
            last_instance_time
            +
            longest_total_execution
    )

    return makespan


# ============================================================
# 10. Utilization
# ============================================================

def _calc_utilization(C_ms, T, m):
    total = 0.0

    for c, t in zip(
            C_ms,
            T,
    ):

        if t <= 0:
            raise ValueError(
                f"Invalid period {t}"
            )

        total += (
                c / t
        )

    return (
            total /
            float(m)
    )


# ============================================================
# 11. Random positive vector
# ============================================================

def _random_positive_vector(rng, n):
    vals = [
        -math.log(
            max(
                rng.random(),
                1e-15,
            )
        )
        for _ in range(n)
    ]

    s = sum(vals)

    if s <= 0:
        raise RuntimeError(
            "Unable to generate "
            "positive random vector."
        )

    return [
        x / s
        for x in vals
    ]


# ============================================================
# 12. Generate utilization-exact C
# ============================================================

def _initial_C_from_u(T, u, m, rng, min_c_ms, max_c_ratio):
    n = len(T)

    target_total_u = (
            u * m
    )

    # Maximum possible utilization:
    #
    # C_i / T_i <= max_c_ratio
    #
    max_total = (
            n *
            max_c_ratio
    )

    if target_total_u > max_total + 1e-12:
        return None

    shares = _random_positive_vector(
        rng,
        n,
    )

    C = []

    for i in range(n):

        task_u = (
                target_total_u
                *
                shares[i]
        )

        c = (
                task_u
                *
                T[i]
        )

        if c < min_c_ms:
            return None

        if c > T[i] * max_c_ratio:
            return None

        C.append(c)

    realized = _calc_utilization(
        C,
        T,
        m,
    )

    if realized <= 0:
        return None

    scale = (
            u /
            realized
    )

    C = [
        c * scale
        for c in C
    ]

    for i, c in enumerate(C):

        if c < min_c_ms:
            return None

        if c > T[i] * max_c_ratio:
            return None

    return C


# ============================================================
# 13. Makespan-aware C search
# ============================================================

def _solve_C_for_candidate(sk, trig, T, u, m, H_ms, seed, solver_cfg, ):
    rng = random.Random(seed)

    n = len(sk)

    min_c_ms = (
            solver_cfg["min_wcet_us"]
            /
            1000.0
    )

    max_c_ratio = (
        solver_cfg["max_c_ratio"]
    )

    attempts = (
        solver_cfg["c_attempts"]
    )

    best_C = None
    best_ms = None

    for _ in range(attempts):

        C = _initial_C_from_u(
            T,
            u,
            m,
            rng,
            min_c_ms,
            max_c_ratio,
        )

        if C is None:
            continue

        makespan = _calculate_makespan(
            sk,
            trig,
            T,
            C,
            H_ms,
        )

        if best_ms is None or makespan < best_ms:
            best_ms = makespan
            best_C = C

    if best_C is None:
        raise RuntimeError(
            "Unable to generate a legal "
            "WCET allocation for "
            f"u={u}, m={m}"
        )

    realized_u = _calc_utilization(
        best_C,
        T,
        m,
    )

    if abs(
            realized_u - u
    ) > solver_cfg["u_tolerance"]:
        raise RuntimeError(
            "Generated utilization is invalid: "
            f"target={u}, "
            f"actual={realized_u}"
        )

    return best_C, best_ms


# ============================================================
# 14. Makespan buckets
# ============================================================

def _build_buckets(H_ms, width, max_ms):
    if width <= 0:
        raise ValueError(
            "makespan_bin_width_ms "
            "must be > 0"
        )

    if max_ms is None:
        max_ms = H_ms

    if max_ms <= 0:
        raise ValueError(
            "makespan_max_ms "
            "must be > 0"
        )

    n_regular = int(
        math.ceil(
            max_ms /
            width
        )
    )

    buckets = []

    for i in range(n_regular):
        lo = (
                i *
                width
        )

        hi = min(
            (i + 1) * width,
            max_ms,
        )

        buckets.append({
            "id": i,
            "lo": lo,
            "hi": hi,
            "last": (
                    i ==
                    n_regular - 1
            ),
        })

    return buckets


def _bucket_for_makespan(makespan, buckets):
    for b in buckets:

        if b["last"]:

            if (
                    b["lo"]
                    <= makespan
                    <= b["hi"]
            ):
                return b

        else:

            if (
                    b["lo"]
                    <= makespan
                    <
                    b["hi"]
            ):
                return b

    return None


# ============================================================
# 15. Filename formatting
# ============================================================

def _format_u(u):
    return (
        f"{int(round(u * 100)):02d}"
    )


def _format_ms_bucket(bucket_id):
    return (
        f"{bucket_id:02d}"
    )


def _filename(kind, u, m, bucket_id, seed):
    return (
        f"{kind}"
        f"_u{_format_u(u)}"
        f"_m{m}"
        f"_ms{_format_ms_bucket(bucket_id)}"
        f"_s{seed}.json"
    )


# ============================================================
# 16. Candidate serialization
# ============================================================

def _make_pipeline(sk, trig, T, C_ms):
    nodes = []

    # --------------------------------------------------------
    # First determine every node's output port names.
    # --------------------------------------------------------

    output_names = {}

    for i, nd in enumerate(sk):
        fo = nd.get(
            "fo",
            1,
        )

        output_names[i] = [
            f"p{i}_{j}"
            for j in range(fo)
        ]

    # --------------------------------------------------------
    # Build final JSON nodes.
    #
    # period 只在 timed 节点上输出；event(数据驱动)节点省略 → 运行时按
    # "无显式周期 = 继承前级 + 绑定边"处理。
    # --------------------------------------------------------

    for i, nd in enumerate(sk):

        node_id = f"n{i}"

        # 字段顺序（2026-09-19 定，与 g_state.hpp NodeInfo 头注释同源）：
        #   id / name / version                        — 标识
        #   configs / inputs / outputs                 — 配置 + 端口（三者连着）
        #   hist                                       — 窗口读声明
        #   event | period                             — 触发模式（二选一，判据就是这两个字段本身）
        #   wcet / deadline / cap                      — 可有可无的属性（本生成器只写 wcet；deadline/cap 留空）
        node = {
            "id": node_id,

            "name": nd["algo"],

            "version": "1.0.0",

            # configs 为**位置式取值表**（NodeInfo 只取 value，顺序 = AlgoFunc 配置段序号）。
            # 每项单键对象，键 = c{节点序号}_{配置序号}，与数据端口 p{i}_{j} 同一编号体系；
            # C++ 会校验键的"配置序号"后缀等于下标、且"节点序号"等于本节点下标（写错即拒 → 配置自校验）：
            #   c{i}_0 = 节点 id（string）——插件签名首个配置参数（usr_* 的 `const std::string& name`），
            #           被 spin_cost_us 当作 tracepoint 的 node_id，时间线上据此标出是哪个节点
            #   c{i}_1 = cfg（int）—— 每拍忙等时长 µs
            "configs": [
                {
                    f"c{i}_0": node_id
                },
                {
                    f"c{i}_1": int(
                        round(
                            C_ms[i] * 1000.0
                        )
                    )
                },
            ],
        }

        # ----------------------------------------------------
        # inputs
        #
        # Convert:
        #
        # [(1, 0), (2, 0)]
        #
        # into:
        #
        # ["p1_0", "p2_0"]
        # ----------------------------------------------------

        if nd.get("ins"):
            node["inputs"] = [
                output_names[
                    pi
                ][port]
                for pi, port
                in nd["ins"]
            ]

        # ----------------------------------------------------
        # outputs：同样只有端口名（无初值），顺序 = 算法输出参数顺序
        # ----------------------------------------------------

        node["outputs"] = output_names[i]

        # ----------------------------------------------------
        # hist（窗口读标记）
        #
        # 写出形如 [{"p0_0": 5}, {"p1_0": 3}]（每元素单键对象），
        # 与 event 字段同形（见 g_state.hpp check_topology ④⑥）。
        #
        # 对应字段必须已在本节点 inputs 中；
        # 窗口长度 N > 2；
        # hist 允许 timed（显式周期）与 event（事件触发，见下 event 段）两类节点。
        # ----------------------------------------------------

        if nd.get("hist"):

            if trig[i] not in ("timed", "event"):
                raise RuntimeError(
                    f"Node n{i}: hist 节点须为 timed 或 event"
                )

            inputs = node.get(
                "inputs",
                [],
            )

            hist_out = []

            for port, count in nd["hist"].items():

                port_name = str(port)

                if port_name not in inputs:
                    raise RuntimeError(
                        f"Node n{i}: "
                        f"hist port {port_name} "
                        f"must also exist in inputs"
                    )

                count = int(count)

                if count <= 2:
                    raise RuntimeError(
                        f"Node n{i}: "
                        f"hist count must be > 2, got {count}"
                    )

                hist_out.append({port_name: count})

            node["hist"] = hist_out

        # ----------------------------------------------------
        # 触发细节（置末）：period / event 二选一，本身就是触发模式的声明
        #
        # timer → period；event → event 声明。两者二选一、恰好其一，
        # 由 g_state.hpp check_topology ⑦ 校验（都写或都不写都拒配置）。
        #
        # event 的触发源 = 非 hist 输入端口（hist 是窗口读，不能同端口兼任触发源；对
        # 非 hist 节点即全部输入）。各端口写同一个抽稀倍数 N = T[i] / min(触发源周期)，
        # 与 _assign_temporal 的虚拟周期取值同源 → C++ build_dominance 算出的 T 与 T[i] 一致；
        # 多端口时虚拟周期最小者即支配端口，只有它计入就绪等待（其余为非阻塞边）。
        # ----------------------------------------------------

        if trig[i] == "timed":

            node["period"] = float(T[i])

        else:

            event_ports = _event_ports(nd)

            if not event_ports:
                raise RuntimeError(
                    f"Node n{i}: event 节点无输入端口可作触发源"
                )

            thin = _event_thinning(sk, i, T)

            node["event"] = [
                {port_name: thin}
                for port_name in event_ports
            ]

        # ----------------------------------------------------
        # 可有可无的属性（置最末）
        #
        # wcet（ms）——调度/优先级用；生成器的求解器已定值，故总是写出。
        # deadline / cap 本生成器不写（deadline 缺省 0 = 未声明；cap 为预留字段）。
        # ----------------------------------------------------

        node["wcet"] = float(
            C_ms[i]
        )

        nodes.append(node)

    return nodes


def _make_metadata(kind, u, m, H_ms, makespan, bucket, seed):
    return {
        "generator_version": VERSION,

        "kind": kind,

        "u_target": u,

        "m": m,

        "H_ms": H_ms,

        "makespan_ms": makespan,

        "makespan_bucket": {
            "id": bucket["id"],
            "lo_ms": bucket["lo"],
            "hi_ms": bucket["hi"],
        },

        "seed": seed,

        "makespan_semantics": (
            "T is a release constraint only; "
            "only C contributes execution time. "
            "Makespan is measured from the final "
            "instance of the minimum-period timed "
            "stream through its downstream execution."
        ),

        "utilization_semantics": (
            "u = sum(C_i / T_i) / m"
        ),
    }


def _make_document(kind, u, m, H_ms, makespan, bucket, seed, pipeline, ):
    return pipeline


# ============================================================
# 17. Generate one candidate
# ============================================================

def generate_candidate(kind, u, m, H_ms, seed, config=None):
    cfg = (
        CONFIG
        if config is None
        else config
    )

    rng = random.Random(seed)

    # --------------------------------------------------------
    # Topology.
    # --------------------------------------------------------

    sk, must_event = generate_topology(
        kind,
        rng,
        cfg,
    )

    # --------------------------------------------------------
    # Temporal.
    # --------------------------------------------------------

    temporal_cfg = cfg["temporal"]

    trig, T = _assign_temporal(
        sk,
        H_ms,
        seed + 1000003,
        temporal_cfg["ptimed"],
        temporal_cfg["period_divisors"],
        must_event,
        temporal_cfg.get("pthin", 0.0),
    )

    # --------------------------------------------------------
    # WCET.
    # --------------------------------------------------------

    C_ms, makespan = (
        _solve_C_for_candidate(
            sk,
            trig,
            T,
            u,
            m,
            H_ms,
            seed + 2000003,
            cfg["solver"],
        )
    )

    # --------------------------------------------------------
    # Check utilization.
    # --------------------------------------------------------

    realized_u = _calc_utilization(
        C_ms,
        T,
        m,
    )

    if abs(
            realized_u - u
    ) > cfg["solver"]["u_tolerance"]:
        raise RuntimeError(
            "Candidate utilization mismatch: "
            f"target={u}, "
            f"actual={realized_u}"
        )

    # --------------------------------------------------------
    # Bucket.
    # --------------------------------------------------------

    buckets = _build_buckets(
        H_ms,
        cfg["workload"][
            "makespan_bin_width_ms"
        ],
        cfg["workload"][
            "makespan_max_ms"
        ],
    )

    bucket = _bucket_for_makespan(
        makespan,
        buckets,
    )

    if bucket is None:
        raise RuntimeError(
            f"Makespan {makespan:.1f} ms "
            "does not belong to any configured bucket."
        )

    # --------------------------------------------------------
    # Pipeline.
    # --------------------------------------------------------

    pipeline = _make_pipeline(
        sk,
        trig,
        T,
        C_ms,
    )

    return {
        "document": pipeline,
        "meta": {
            "kind": kind,
            "u_target": u,
            "m": m,
            "H_ms": H_ms,
            "makespan_ms": makespan,
            "makespan_bucket": bucket,
            "seed": seed,
        },
        "bucket_id": bucket["id"],
        "makespan": makespan,
        "seed": seed,
    }


# ============================================================
# 18. Candidate collection
# ============================================================

def _bucket_key(kind, u, m, bucket_id):
    return (
        kind,
        float(u),
        int(m),
        int(bucket_id),
    )


def _generate_candidates(kind, u, m, config, attempts, seed_start, buckets, candidates, ):
    successes = 0
    # 失败原因 → 次数。逐个尝试失败是采样常态（桶不匹配 / 该 (u,m) 无解），
    # 但**全部**失败且原因只有一种时，几乎必然是配置写错——那种情况必须报出来，
    # 否则只显示 generated=0，看不出是配置问题还是运气差（曾把 hist 语义改动的
    # 配置遗留藏了整整一轮）。
    reasons = Counter()

    for offset in range(attempts):

        seed = (
                seed_start
                +
                offset
        )

        try:

            result = generate_candidate(
                kind,
                u,
                m,
                config["workload"]["H_ms"],
                seed,
                config,
            )

        except (
                ValueError,
                RuntimeError,
        ) as exc:

            reasons[str(exc).splitlines()[0][:140]] += 1
            continue

        bucket_id = result[
            "bucket_id"
        ]

        key = _bucket_key(
            kind,
            u,
            m,
            bucket_id,
        )

        limit = (
                config["workload"]["n_per"]
                *
                config["workload"][
                    "candidate_factor"
                ]
        )

        if len(
                candidates.setdefault(
                    key,
                    [],
                )
        ) >= limit:
            continue

        candidates[key].append(
            result
        )

        successes += 1

    if successes == 0 and reasons:
        print(f"    [全部失败] {attempts} 次尝试均未产出候选，原因分布：")
        for msg, c in reasons.most_common(3):
            print(f"      ×{c:<5} {msg}")
        if len(reasons) == 1:
            print("      ⚠ 只有一种原因 → 大概率是配置写错，而不是采样运气差")

    return successes


# ============================================================
# 19. Final random sampling
# ============================================================

def _sample_final(candidates, kind, u, m, buckets, n_per, rng, ):
    selected = []

    missing = []

    for bucket in buckets:

        key = _bucket_key(
            kind,
            u,
            m,
            bucket["id"],
        )

        pool = candidates.get(
            key,
            [],
        )

        if len(pool) < n_per:
            missing.append({
                "bucket_id": bucket["id"],
                "lo_ms": bucket["lo"],
                "hi_ms": bucket["hi"],
                "available": len(pool),
                "required": n_per,
            })

            continue

        chosen = rng.sample(
            pool,
            n_per,
        )

        selected.extend(
            chosen
        )

    return selected, missing


# ============================================================
# 20. Save selected candidates
# ============================================================

def _save_selected(selected, out_dir):
    os.makedirs(
        out_dir,
        exist_ok=True,
    )

    paths = []

    for item in selected:
        meta = item["meta"]

        filename = _filename(
            meta["kind"],
            meta["u_target"],
            meta["m"],
            meta["makespan_bucket"]["id"],
            meta["seed"],
        )

        path = os.path.join(
            out_dir,
            filename,
        )

        save(
            path,
            item["document"],
        )

        paths.append(
            path
        )

    return paths


# ============================================================
# 21. Generate one configuration
# ============================================================

def generate_configuration(kind, u, m, config=None, ):
    cfg = (
        CONFIG
        if config is None
        else config
    )

    workload = cfg[
        "workload"
    ]

    H_ms = workload[
        "H_ms"
    ]

    n_per = workload[
        "n_per"
    ]

    buckets = _build_buckets(
        H_ms,
        workload[
            "makespan_bin_width_ms"
        ],
        workload[
            "makespan_max_ms"
        ],
    )

    candidates = {}

    seed_base = (
            workload[
                "seed_base"
            ]
            +
            abs(
                hash(
                    (
                        kind,
                        u,
                        m,
                    )
                )
            )
            %
            1000000
    )

    # ========================================================
    # Phase 1
    #
    # Generate as many candidates as possible.
    # ========================================================

    print(
        f"[PHASE-1] "
        f"kind={kind} "
        f"u={u} "
        f"m={m} "
        f"attempts="
        f"{workload['initial_attempts']}"
    )

    generated = _generate_candidates(
        kind,
        u,
        m,
        cfg,
        workload[
            "initial_attempts"
        ],
        seed_base,
        buckets,
        candidates,
    )

    print(
        f"[PHASE-1] "
        f"generated={generated}"
    )

    # ========================================================
    # Phase 2 / 3 / 4
    #
    # Select.
    # If missing, refill.
    # ========================================================

    rng = random.Random(
        seed_base + 777777
    )

    for refill_round in range(
            workload[
                "max_refill_rounds"
            ] + 1
    ):

        selected, missing = (
            _sample_final(
                candidates,
                kind,
                u,
                m,
                buckets,
                n_per,
                rng,
            )
        )

        if not missing:
            print(
                f"[SUCCESS] "
                f"kind={kind} "
                f"u={u} "
                f"m={m} "
                f"total="
                f"{len(selected)}"
            )

            return selected

        if (
                refill_round
                >= workload[
            "max_refill_rounds"
        ]
        ):

            print(
                f"[INCOMPLETE] "
                f"kind={kind} "
                f"u={u} "
                f"m={m}"
            )

            for item in missing:
                print(
                    "  bucket="
                    f"{item['bucket_id']:02d} "
                    f"["
                    f"{item['lo_ms']},"
                    f"{item['hi_ms']}"
                    f"] "
                    f"available="
                    f"{item['available']} "
                    f"required="
                    f"{item['required']}"
                )

            return selected

        # ----------------------------------------------------
        # Refill only missing buckets.
        # ----------------------------------------------------

        print(
            f"[REFILL-{refill_round + 1}] "
            f"missing={len(missing)} kind(s)"
        )

        generated = _generate_candidates(
            kind,
            u,
            m,
            cfg,
            workload[
                "refill_attempts"
            ],
            seed_base
            +
            (
                    refill_round + 1
            )
            *
            10000000,
            buckets,
            candidates,
        )

        print(
            f"[REFILL-{refill_round + 1}] "
            f"generated={generated}"
        )


# ============================================================
# 22. Generate entire experiment
# ============================================================

def repo_root():
    """仓库根（含 build/bin/client 与 tool/）；找不到返回 None。"""
    d = os.path.abspath(os.getcwd())
    while True:
        if os.path.isfile(os.path.join(d, "build", "bin", "client")) and os.path.isdir(os.path.join(d, "tool")):
            return d
        p = os.path.dirname(d)
        if p == d:
            return None
        d = p


def generate_all(out_dir=None, config=None):
    """
    生成所有实验拓扑。
    out_dir: 输出目录。如果为 None，则从 config["workload"]["out_dir"] 读取。
             如果是相对路径，会基于仓库根目录解析。
    """
    cfg = (
        CONFIG
        if config is None
        else config
    )

    workload = cfg["workload"]

    # 1. 确定输出目录：参数优先，其次取 config 里的配置
    if out_dir is None:
        out_dir = workload.get("out_dir", "pipeline")

    # 2. 如果是相对路径，统一挂载在仓库根目录下（避免 cwd 在 tool/ 或其他子目录下时路径错乱）
    if not os.path.isabs(out_dir):
        root = repo_root()
        if root is None:
            raise RuntimeError("找不到仓库根（需含 build/bin/client 与 tool/）")
        out_dir = os.path.join(root, out_dir)

    os.makedirs(
        out_dir,
        exist_ok=True,
    )

    all_selected = []

    # kinds 可从 config 覆盖（只生成指定拓扑；缺省 = 全部）——CONFIG 里只留一个拓扑块时，
    # 其余 kind 会在 generate_topology 里 KeyError，故必须能收窄
    kinds = cfg.get("kinds") or [
        "multihop",
        "fork",
        "join",
        "feedback",
        "mixed",
    ]

    for kind in kinds:

        for u in workload["u"]:

            for m in workload["workers"]:
                print()
                print(
                    "=" * 70
                )

                print(
                    f"[CONFIG] "
                    f"{kind} "
                    f"u={u} "
                    f"m={m}"
                )

                selected = (
                    generate_configuration(
                        kind,
                        u,
                        m,
                        cfg,
                    )
                )

                paths = _save_selected(
                    selected,
                    out_dir,
                )

                all_selected.extend(
                    paths
                )

                print(
                    f"[SAVED] "
                    f"{len(paths)} "
                    f"files"
                )

    print()
    print(
        "=" * 70
    )

    print(
        f"[DONE] "
        f"files={len(all_selected)}"
    )

    print(
        f"[OUTPUT] "
        f"{out_dir}"
    )

    return all_selected


# ============================================================
# 23. Optional inspection helper
# ============================================================

def inspect_file(path):
    """打印一份生成的 pipeline JSON 的可读摘要。

    schema = **裸节点数组**（_make_pipeline 产物），逐节点只含：
      id / name / version / outputs / wcet / parameters
      + 可选 period（时间触发）、inputs、hist（窗口读）、event（事件触发 + 抽稀倍数）。

    最终执行周期 T 与 trig 分类**不落 JSON**：运行时由 C++ 按支配规则现算（显式 period /
    N×支配源周期 / 继承前级），故此处只打印 JSON 里真实存在的键。
    """
    with open(path, "r") as f:
        nodes = json.load(f)

    print(f"file  = {path}")
    print(f"nodes = {len(nodes)}")
    print()

    for node in nodes:

        parts = [
            f"[{node['id']:>3}] "
            f"{node['name']:<12}"
        ]

        if "period" in node:
            parts.append(f"period={node['period']:g}ms")

        if node.get("inputs"):
            parts.append(f"inputs={','.join(node['inputs'])}")

        for entry in node.get("hist", []):
            for port, n in entry.items():
                parts.append(f"hist:{port}={n}")

        for entry in node.get("event", []):
            for port, n in entry.items():
                parts.append(f"event:{port}x{n}")

        # configs = [{c{i}_j: 值}, ...]，取各项的值按序展示（键只是自校验用的端口名）
        cfgs = [next(iter(c.values())) for c in node.get("configs", [])]

        if len(cfgs) > 1:
            parts.append(f"cfg={cfgs[1]}us")

        print("  ".join(parts))


if __name__ == "__main__":
    # 默认调用方式（会走配置或默认的 pipeline 目录）
    generate_all()