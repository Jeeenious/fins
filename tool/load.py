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
# **五类都可以混入 `usr_acc`（hist 窗口读）节点：**
# 
# - 普通链/分支位置按概率 `ACC_PROB` 被替换为 `usr_acc`：第一个输入 = 前级 feed
#   （标量读最新一帧），第二个输入 = `hist` 随机读一个**与 feed 不同名**的历史已产出
#   字段（最近 N>2 帧窗口）。
# - 数据驱动(event)链仍走绑定边精确消费单帧；显式周期(timed)任务由 period 定时释放，
#   输入从字段历史缓存取样。


import os
import json
import math
import random
from collections import deque

VERSION = "3.1.0"

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

CONFIG = {

    "topology": {

        "multihop": {
            "paths": (1, 4),
            "depth": (2, 8),
        },

        "fork": {
            "fan": (2, 8),
            "bdepth": (1, 3),
        },

        "join": {
            "fan": (2, 8),
            "bdepth": (1, 3),
            "tail": (1, 3),
        },

        "feedback": {
            "depth": (3, 8),
            "histN": (3, 8),
        },

        "mixed": {
            "nseg": (3, 8),

            "chain_prob": 0.45,
            "fork_join_prob": 0.30,
            "feedback_prob": 0.25,
        },
    },

    "temporal": {

        # Probability that a non-source node is timed.
        "ptimed": 0.35,

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

        # Output directory.
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

ACC_PROB = 0.25  # 普通链节点被替换为 acc（hist 窗口）的概率
HIST_N_RANGE = (3, 8)  # 额外 acc 的 hist 窗口长度（>2）


def _field_ref(idx, port):
    return f"p{idx}_{port}"


def _field_of(ref):
    return _field_ref(*ref)


def _record(sk, produced, idx, fo):
    for p in range(fo):
        produced.append((idx, p))


def _chain_step(sk, rng, produced, feed):
    """沿链推进一步：普通 relay，或按概率替换成 usr_acc（读一个与 feed
    不同名的历史已产出字段作 hist 窗口；无候选时退回 relay）。
    返回新节点 index（其输出字段 = p{index}_0）。"""
    cand = [o for o in produced if _field_of(o) != _field_of(feed)]
    if cand and rng.random() < ACC_PROB:
        hist_src = rng.choice(cand)
        n = _add(
            sk,
            ACCUMULATOR[1],
            1,
            [feed, hist_src],
            hist={
                _field_of(hist_src): rng.randint(*HIST_N_RANGE),
            },
        )
    else:
        n = _add(sk, "usr_relay", 1, [feed])
    _record(sk, produced, n, 1)
    return n


def _sk_multihop(rng, paths, depth):
    sk = []
    produced = []

    for _ in range(paths):

        r = _add(sk, "usr_src", 1)
        _record(sk, produced, r, 1)

        feed = (r, 0)
        for _ in range(depth):
            feed = (_chain_step(sk, rng, produced, feed), 0)

        _add(sk, "usr_sink", 1, [feed])

    return sk, set()


def _sk_fork(rng, fan, bdepth):
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

        for _ in range(bdepth):
            feed = (_chain_step(sk, rng, produced, feed), 0)

        _add(sk, "usr_sink", 1, [feed])

    return sk, set()


def _sk_join(rng, fan, bdepth, tail):
    sk = []
    produced = []
    lanes = []

    for _ in range(fan):

        r = _add(sk, "usr_src", 1)
        _record(sk, produced, r, 1)

        feed = (r, 0)
        for _ in range(bdepth):
            feed = (_chain_step(sk, rng, produced, feed), 0)

        lanes.append(feed)

    j = _add(sk, FAN_IN[fan], 1, lanes)
    _record(sk, produced, j, 1)

    feed = (j, 0)
    for _ in range(tail):
        feed = (_chain_step(sk, rng, produced, feed), 0)

    _add(sk, "usr_sink", 1, [feed])

    return sk, set()


def _sk_feedback(rng, depth, histN_range=HIST_N_RANGE):
    """feedback：链首(环头) acc hist 读链尾输出 → 由 hist 窗口构成的闭环。

    环头之前可混入任意 relay/acc；环头(acc, timed) 之后到链尾强制 event，
    让链头→…→链尾有真实绑定边(前序)路径；链头 hist 读链尾字段，跨周期回环。
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
            histN = rng.randint(*histN_range)
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
                n = _chain_step(sk, rng, produced, feed)

            _record(sk, produced, n, 1)

        feed = (n, 0)

    # 链尾 = 编号 depth 的节点，其输出字段 = p{depth}_0（环头 hist 读它）
    _add(sk, "usr_sink", 1, [feed])

    return sk, must_event


def _sk_mixed(rng, nseg):
    sk = []
    produced = []

    r = _add(sk, "usr_src", 1)
    _record(sk, produced, r, 1)

    for _ in range(nseg):

        x = rng.random()

        if x < 0.45:

            # Chain
            depth = rng.randint(1, 3)
            feed = (r, 0)
            for _ in range(depth):
                feed = (_chain_step(sk, rng, produced, feed), 0)
            r = feed[0]

        elif x < 0.75:

            # Fork -> branches -> join
            fan = rng.randint(2, 5)
            f = _add(sk, FAN_OUT[fan], fan, [(r, 0)])
            _record(sk, produced, f, fan)

            tails = []
            for _ in range(fan):
                # 多输出：分支随机连 fork 任意端口
                port = rng.randrange(fan)
                feed = (f, port)

                if rng.random() < 0.5:
                    feed = (_chain_step(sk, rng, produced, feed), 0)

                tails.append(feed)

            r = _add(sk, FAN_IN[fan], 1, tails)
            _record(sk, produced, r, 1)

        else:

            # Feedback 片段（acyclic 字段窗口；真正的环只在 feedback kind 构造）
            feed = (r, 0)
            cand = [o for o in produced if _field_of(o) != _field_of(feed)]

            if cand:
                hist_src = rng.choice(cand)
                histN = rng.randint(*HIST_N_RANGE)
                r = _add(
                    sk,
                    ACCUMULATOR[1],
                    1,
                    [feed, hist_src],
                    hist={
                        _field_of(hist_src): histN,
                    },
                )
            else:
                r = _add(sk, "usr_relay", 1, [feed])

            _record(sk, produced, r, 1)

    _add(sk, "usr_sink", 1, [(r, 0)])

    return sk, set()


def generate_topology(kind, rng, config=None):
    cfg = CONFIG if config is None else config

    tc = cfg["topology"]

    if kind == "multihop":
        return _sk_multihop(
            rng,
            _randint(rng, tc["multihop"]["paths"]),
            _randint(rng, tc["multihop"]["depth"]),
        )

    if kind == "fork":
        return _sk_fork(
            rng,
            _randint(rng, tc["fork"]["fan"]),
            _randint(rng, tc["fork"]["bdepth"]),
        )

    if kind == "join":
        return _sk_join(
            rng,
            _randint(rng, tc["join"]["fan"]),
            _randint(rng, tc["join"]["bdepth"]),
            _randint(rng, tc["join"]["tail"]),
        )

    if kind == "feedback":
        depth = _randint(rng, tc["feedback"]["depth"])
        return _sk_feedback(rng, depth, tuple(tc["feedback"]["histN"]))

    if kind == "mixed":
        return _sk_mixed(
            rng,
            _randint(rng, tc["mixed"]["nseg"]),
        )

    raise ValueError(f"Unknown topology kind: {kind}")


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

def _assign_temporal(
        sk,
        H_ms,
        seed,
        ptimed,
        period_divisors,
        must_event=None):
    rng = random.Random(seed)

    if must_event is None:
        must_event = set()

    _validate_probability(
        "ptimed",
        ptimed,
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
        # hist 声明在显式周期节点上：发起节点必须是 timed，
        # 不允许通过 predecessor 推导周期。
        # ----------------------------------------------------

        if hist:
            trig[i] = "timed"
            T[i] = _choice(
                rng,
                periods,
            )

            continue

        # ----------------------------------------------------
        # Ordinary non-source node
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
            T[i] = min(pred_periods)
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

            T[i] = min(pred_periods)

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

        node = {
            "id": f"n{i}",

            "name": nd["algo"],

            "version": "1.0.0",

            "outputs": output_names[i],

            "wcet": float(
                C_ms[i]
            ),

            "parameters": [
                {
                    "value": int(
                        round(
                            C_ms[i] * 1000.0
                        )
                    )
                }
            ],
        }

        if trig[i] == "timed":
            node["period"] = float(T[i])

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
        # hist（窗口读标记）
        #
        # 对应字段必须已在本节点 inputs 中；
        # hist 仅允许 timed（显式周期）节点；窗口长度 N > 2。
        # ----------------------------------------------------

        if nd.get("hist"):

            if trig[i] != "timed":
                raise RuntimeError(
                    f"Node n{i}: hist 仅允许 timed(显式周期) 节点声明"
                )

            inputs = node.get(
                "inputs",
                [],
            )

            hist_out = {}

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

                hist_out[port_name] = count

            node["hist"] = hist_out

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
    # return {
    #     "meta": _make_metadata(
    #         kind,
    #         u,
    #         m,
    #         H_ms,
    #         makespan,
    #         bucket,
    #         seed,
    #     ),
    #
    #     "pipeline": pipeline,
    # }
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
        ):

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

def _save_selected(selected, out_dir, ):
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

def generate_all(config=None):
    cfg = (
        CONFIG
        if config is None
        else config
    )

    workload = cfg[
        "workload"
    ]

    out_dir = workload[
        "out_dir"
    ]

    os.makedirs(
        out_dir,
        exist_ok=True,
    )

    all_selected = []

    kinds = [
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
    with open(path, "r") as f:
        data = json.load(f)

    meta = data["meta"]

    print(
        f"kind       = {meta['kind']}"
    )

    print(
        f"u          = {meta['u_target']}"
    )

    print(
        f"m          = {meta['m']}"
    )

    print(
        f"H          = {meta['H_ms']} ms"
    )

    print(
        f"makespan   = "
        f"{meta['makespan_ms']:.6f} ms"
    )

    print(
        f"bucket     = "
        f"{meta['makespan_bucket']['id']:02d}"
    )

    print(
        f"seed       = {meta['seed']}"
    )

    print()

    for node in data["pipeline"]:
        print(
            f"[{node['id']:02d}] "
            f"{node['algo']:12s} "
            f"trig={node['trig']:5s} "
            f"T={node['T_ms']:8.3f} ms "
            f"C={node['cfg']:6d} us"
        )


# In[4]:


if __name__ == "__main__":
    generate_all()
