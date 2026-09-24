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

import copy
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

        # 载荷字节数：**全局统一**，不随机 —— 写进每个节点的 configs c{i}_2
        # （插件侧 usr_* 的第三个配置参数；输出 resize 到该大小）。
        # 与 solver.min_wcet_us 的关系：载荷产出本身要花时间，若它吃掉全部执行用时预算，
        # 插件会如实上报实际消耗（不补自旋），该拍实际用时将超过预算 → 生成期会据此告警。
        #
        # **可给列表** → 每个 (kind,utilization,m) 逐值生成一份，文件名带 `_msg<字节>` 区分。
        # 同一载荷列表内共用同一 seed（seed 只 hash (kind,utilization,m)），故各档的**拓扑完全一致**，
        # 只有载荷大小不同 —— 这才是受控对比（改一个变量）。
        #
        # 档位选取依据（稳态成本 = 一遍 memset，实测见 _mesg_cost_us）：
        #   7 B      基线，与已有全部数据可比
        #   1 KB     L1 常驻，产出成本 ~0.03µs（可忽略）
        #   64 KB    出 L1 → 0.90µs，成本开始可见（32KB 处仅 0.13µs，L1 分界）
        #   1 MB     14µs/拍（~75GB/s），且冷启动首 1~2 拍付 mmap 缺页 ~250µs
        # 要夹逼 L1 分界就插 32KB；>128KB 的档位差异只在冷启动那 1~2 拍上体现。
        "mesg_size": [7, 1024, 65536, 1048576],

        # 目标利用率 utilization = Σ(C_i / T_i) / m（逐值一组实验；求解器按 utilization_tolerance 校验落点）
        "utilization": [
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

        # 接受域（四种写法，与 workload.utilization 同一套思路）：
        #   80          → [0, 80]（旧行为）
        #   None        → [0, H_ms]
        #   "any"       → [0, ∞)：上不封顶，makespan > H 的过载配置也收
        #   (lo, hi)    → 只收 [lo, hi]（hi 给 None 表示上不封顶）
        # 落在接受域外的候选由 _bucket_for_makespan 返回 None → 丢弃（桶号是标签，夹了就是假标签）。
        # 注意单链上 makespan ≡ ΣC ≡ u·m·H，与 utilization 是同一约束的两种说法，别无谓地卡两遍。
        "makespan": 80,

        # Output directory（默认回退路径，可被显式传入参数覆盖）。
        "out_dir": "pipeline",
    },

    # 生成期的采样/搜索预算（与「实验自变量」无关，故不放在 workload 下）。
    "search": {

        # Number of final samples per bucket.
        "n_per": 5,

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
    },

    "solver": {

        # Utilization numerical tolerance.
        "utilization_tolerance": 1e-8,

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
# 11.5 Utilization spec → concrete u
# ============================================================

def _u_window(T, m, min_c_ms, max_c_ratio):
    """可行利用率区间 [u_min, u_max]（与 utilization 同量纲：Σ(C_i/T_i)/m）。

    u_min = Σ(min_c_ms/T_i)/m
        —— 每个节点的 wcet 有地板（min_wcet_us），故 u 不可能比这更低；
    u_max = min(n·max_c_ratio/m, 1/m)
        —— 前者 = 单节点 C_i ≤ max_c_ratio·T_i；后者 = 单链串行 ΣC = u·m·H 必须 < H（不积压）。

    @retval (u_min, u_max) 闭区间；u_min ≥ u_max 表示该 (拓扑, m, min_wcet) 根本无解
    """
    u_min = sum(min_c_ms / t for t in T) / float(m)
    u_max = min(len(T) * max_c_ratio / float(m), 1.0 / float(m))
    return u_min, u_max


# 已提示过的 (spec, m, 可行域) 组合 —— _resolve_utilization 每候选调一次，同样的提示只打第一遍
_SEEN_U_SPEC = set()


def _resolve_utilization(spec, T, m, solver_cfg, seed):
    """把 `workload.utilization` 的一个元素解析成**具体 u**（必定落在可行域内）。

    三种写法（可混用在同一列表里）：
      · 数字（0.5）            → 精确目标（旧行为）；
      · 二元组（(0.7, 0.85)）  → 在该区间内均匀取样；
      · None / "any" / "free"  → **任意 u**：取满整个可行域后均匀取样。
    请求区间与可行域相交时取交集；不相交时**夹到最近边界并警告**，不抛 ——
    "不可行的是那一段，不是整个配置"。取样用 `Random(seed)` 固定，故同一
    (kind, spec, m) 反复生成得到同一个 u（各载荷档共用同一份求解结果，拓扑仍逐节点一致）。

    @retval float 具体目标 u（≥ u_min 且 ≤ u_max，可直接喂给求解器）
    """
    u_min, u_max = _u_window(
        T, m,
        solver_cfg["min_wcet_us"] / 1000.0,
        solver_cfg["max_c_ratio"],
    )
    if u_min >= u_max:
        raise RuntimeError(
            f"可行利用率区间为空：u_min={u_min:.4f} ≥ u_max={u_max:.4f}"
            f"（节点数={len(T)}, m={m}, min_wcet_us={solver_cfg['min_wcet_us']}, "
            f"max_c_ratio={solver_cfg['max_c_ratio']}）—— 请减小 min_wcet_us 或增大 m"
        )

    if isinstance(spec, (tuple, list)):
        if len(spec) != 2:
            raise ValueError(f"utilization 区间须为 (lo, hi)，收到 {spec!r}")
        lo, hi = float(spec[0]), float(spec[1])
        if lo > hi:
            raise ValueError(f"utilization 区间 lo > hi：{spec!r}")
    elif spec is None or (isinstance(spec, str) and spec.lower() in ("any", "free")):
        lo, hi = u_min, u_max
    elif isinstance(spec, str):
        raise ValueError(f"utilization 元素只能是数字、(lo, hi) 二元组或 'any'，收到 {spec!r}")
    else:
        lo = hi = float(spec)

    a, b = max(lo, u_min), min(hi, u_max)
    # 本函数每个候选尝试都会被调一次（同一 (spec, m) 最多上千次）→ 同样的提示只打第一遍，别刷屏
    tag = (repr(spec), m, round(u_min, 6), round(u_max, 6))

    if a > b:                                   # 请求区间与可行域不相交 → 夹到最近边界
        clamped = u_min if hi < u_min else u_max
        if tag not in _SEEN_U_SPEC:
            print(f"⚠️ utilization={spec!r} 与可行域 [{u_min:.4f}, {u_max:.4f}] 不相交（节点 {len(T)} 个, m={m}）"
                  f" → 夹到 {clamped:.4f}")
        _SEEN_U_SPEC.add(tag)
        return clamped

    if (a, b) != (lo, hi) and tag not in _SEEN_U_SPEC:
        print(f"⚠️ utilization={spec!r} 与可行域 [{u_min:.4f}, {u_max:.4f}] 相交后被夹为 [{a:.4f}, {b:.4f}]"
              f"（节点 {len(T)} 个, m={m}）")

    if not isinstance(spec, (int, float)) and tag not in _SEEN_U_SPEC:
        print(f"[RESOLVE] utilization={spec!r} → "
              + (f"u={a:.4f}" if b - a < 1e-12 else f"取样于 [{a:.4f}, {b:.4f}]"))

    _SEEN_U_SPEC.add(tag)

    return a if b - a < 1e-12 else random.Random(seed).uniform(a, b)


# ============================================================
# 12. Generate utilization-exact C
# ============================================================

def _initial_C_from_utilization(T, utilization, m, rng, min_c_ms, max_c_ratio):
    n = len(T)

    target_total_u = (
            utilization * m
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

    C = [
        target_total_u * shares[i] * T[i]
        for i in range(n)
    ]

    # 目标 u 贴近 wcet 地板时（Σmin_c/T_i ≈ u·m —— min_wcet 被载荷成本抬高、或 u 取到
    # `_u_window` 的 u_min），"均匀随机占比"要让**每个**节点都 ≥ 地板几乎不可能命中 →
    # 改用带下限的占比：share_i ≥ min_c/(u·m·T_i)，剩余份额再随机分。
    # 只在随机占比已经踩地板时才走这条分支 → 中高 u 的既有路径逐位不变（既有语料不受影响）。
    if any(c < min_c_ms for c in C):
        floors = [
            min_c_ms / (target_total_u * T[i])
            for i in range(n)
        ]

        rest = 1.0 - sum(floors)

        if rest < 0:
            return None

        w = _random_positive_vector(rng, n)

        C = [
            target_total_u * (floors[i] + rest * w[i]) * T[i]
            for i in range(n)
        ]

    for i, c in enumerate(C):

        if c < min_c_ms:
            return None

        if c > T[i] * max_c_ratio:
            return None

    realized = _calc_utilization(
        C,
        T,
        m,
    )

    if realized <= 0:
        return None

    scale = (
            utilization /
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

def _solve_C_for_candidate(sk, trig, T, utilization, m, H_ms, seed, solver_cfg, ):
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

        C = _initial_C_from_utilization(
            T,
            utilization,
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
            f"utilization={utilization}, m={m}"
        )

    realized_utilization = _calc_utilization(
        best_C,
        T,
        m,
    )

    if abs(
            realized_utilization - utilization
    ) > solver_cfg["utilization_tolerance"]:
        raise RuntimeError(
            "Generated utilization is invalid: "
            f"target={utilization}, "
            f"actual={realized_utilization}"
        )

    return best_C, best_ms


# ============================================================
# 14. Makespan buckets
# ============================================================

def _ms_json(v):
    """makespan 边界写进 metadata 时把上不封顶（inf）写成 JSON null —— json 的 Infinity
    不是合法 JSON，会让严格解析器报错。"""
    return v if isinstance(v, (int, float)) and math.isfinite(v) else None


def _build_buckets(H_ms, width, spec):
    """把 makespan 接受域切成 width 宽的桶。`spec`（= 配置里的 `makespan`）四种写法：

      · 数字 N     → 接受 [0, N]（旧行为）
      · None       → 接受 [0, H_ms]（旧行为）
      · "any"      → 接受 [0, ∞)：**上不封顶**，makespan > H 的过载候选也收
      · (lo, hi)   → 只接受 [lo, hi]（hi 给 None 表示上不封顶）

    桶 id 一律是**全局编号**（id = lo/width 取整），所以区间写法下文件名里的 `msNN` 仍是
    全局桶号，历史文件名语义不变。落在接受域外的候选由 `_bucket_for_makespan` 返回 None →
    调用方丢弃（makespan 无法"夹"，桶号是标签，夹了就是假标签）。
    """
    # 容错：makespan 只吃**一个** spec，(lo, hi) 写成 [(lo, hi)] 也认（单元素列表 → 拆开）
    while isinstance(spec, (list, tuple)) and len(spec) == 1 and isinstance(spec[0], (list, tuple)):
        spec = spec[0]

    lo, hi_spec = 0.0, spec

    if isinstance(hi_spec, (tuple, list)):
        if len(hi_spec) != 2:
            raise ValueError(
                f"makespan 区间须为 (lo, hi)，收到 {spec!r}"
            )

        lo, hi_spec = hi_spec

    if isinstance(hi_spec, str):
        if hi_spec.lower() not in ("any", "inf", "unbounded"):
            raise ValueError(
                f"makespan 的字符串写法只认 'any'，收到 {spec!r}"
            )

        hi_spec = math.inf

    if hi_spec is None:
        hi_spec = H_ms

    if lo is None:
        lo = 0.0

    if hi_spec <= 0:
        raise ValueError(
            "makespan "
            "must be > 0"
        )

    if lo < 0:
        raise ValueError(
            f"makespan 接受域下界须 ≥ 0，收到 {spec!r}"
        )

    if lo >= hi_spec:
        raise ValueError(
            f"makespan 接受域为空：{spec!r}（下界 {lo} ≥ 上界 {hi_spec}）"
        )

    # width 缺省（配置里没写 makespan_bin_width_ms）→ 整个接受域**一个桶**（不多分桶）
    if width is None:
        width = (hi_spec - lo) if math.isfinite(hi_spec) else max(H_ms - lo, 1.0)

    if width <= 0:
        raise ValueError(
            "makespan_bin_width_ms "
            "must be > 0"
        )

    i0 = int(math.floor(lo / width))

    # 上不封顶：先用 [lo, H_ms] 做有限分桶（桶号仍是全局编号），再把最后一桶的上界放开到 ∞
    unbounded = math.isinf(hi_spec)

    stop = (
        max(H_ms, lo + width)
        if unbounded
        else hi_spec
    )

    buckets = []
    i = i0

    while True:

        lo_i = lo if i == i0 else i * width

        hi_i = min(
            (i + 1) * width,
            stop,
        )

        last = hi_i >= stop

        buckets.append({
            "id": i,
            "lo": lo_i,
            "hi": hi_i,
            "last": last,
        })

        if last:

            if unbounded:
                buckets[-1]["hi"] = math.inf   # 上不封顶：>= H 的过载候选也收进这一桶

            break

        i += 1

        if i - i0 > 100000:
            raise ValueError(
                f"makespan 桶数过多（width={width} 太小？），spec={spec!r}"
            )

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

def _format_u(utilization):
    return (
        f"{int(round(utilization * 100)):02d}"
    )


def _format_ms_bucket(bucket_id):
    return (
        f"{bucket_id:02d}"
    )


def _check_workload_keys(cfg):
    """旧配置的 `"u"` 键已改名 `"utilization"` —— 明说，别让它落成看不懂的 KeyError。"""
    workload = cfg["workload"]

    if "u" in workload and "utilization" not in workload:
        raise KeyError(
            "workload 的键 'u' 已改名为 'utilization'（配置需同步更新）"
        )

    moved = [
        k for k in ("n_per", "initial_attempts", "refill_attempts",
                    "max_refill_rounds", "candidate_factor", "seed_base")
        if k in workload
    ]
    if moved:
        raise KeyError(
            f"workload 的键 {moved} 已移到顶层 cfg['search']（生成期采样/搜索预算，与实验自变量无关）"
            " —— 配置需同步更新"
        )

    if "makespan_max_ms" in workload and "makespan" not in workload:
        raise KeyError(
            "workload 的键 'makespan_max_ms' 已改名为 'makespan'（现支持 数字 / None / 'any' / (lo, hi)，"
            "配置需同步更新）"
        )


def _mesg_values(cfg):
    """载荷档位列表。标量 → 单元素列表（保持向后兼容，且此时文件名不带 `_msg` 后缀）。

    缺省 7（旧编译期常量的值）→ 旧的 workload 块不加此项也能跑；档位会在 `[MESG]` 行里打出。
    """
    mesg = cfg["workload"].get("mesg_size", 7)

    if isinstance(mesg, (list, tuple)):
        values = [int(v) for v in mesg]

        if not values:
            raise ValueError("workload.mesg_size 列表为空")

        return values

    return [int(mesg)]


def _mesg_cost_us(mesg_size):
    """产出 mesg_size 字节载荷的线程 CPU 耗时估计（µs）—— **稳态**，即每拍一份全新 output 串。

    本机实测（`(新 std::string).resize(n)`，单线程、预热后，CLOCK_THREAD_CPUTIME_ID）：
    resize 会把新字节值初始化为 0 → 成本就是一遍 memset，故 ~L1 内 250GB/s、出 L1 后 ~75GB/s。
    ≥64KB 段约 0.0137 µs/KB，与 mmap 阈值无关（稳态下阈值已自适应，131072/131073 处无跳变）。

    ⚠ **冷启动另算**：进程内前 1~2 次 1MB resize 走 mmap + 缺页，实测 247µs / 219µs
    （第 3 次起 14µs）—— 这是"首拍尾延迟"的来源，不在本函数口径内。
    ⚠ 重争抢会放大：32 线程跑在 20 核上 1MB 中位 66µs／最差 122µs（超售越重越大）。

    仅用于生成期告警，不参与调度计算。
    """
    # (字节, 实测 µs)：分界在 L1（32KB→64KB 跳变 0.13 → 0.90）
    points = [
        (7, 0.01),
        (1024, 0.03),
        (4096, 0.06),
        (32768, 0.13),
        (65536, 0.90),
        (262144, 3.46),
        (1048576, 14.04),
    ]

    if mesg_size <= points[0][0]:
        return points[0][1]

    if mesg_size >= points[-1][0]:
        return points[-1][1] * mesg_size / points[-1][0]

    # 对数线性插值（成本近似正比于数据量）
    for (b0, c0), (b1, c1) in zip(points, points[1:]):
        if mesg_size <= b1:
            w = math.log(mesg_size / b0) / math.log(b1 / b0)
            return c0 * (c1 / c0) ** w

    return points[-1][1]


def _warn_mesg_budget(cfg, mesg_values):
    """载荷产出耗时 vs 执行用时预算（solver.min_wcet_us）的静态核对。

    插件侧先做载荷产出、再只补差额自旋：若载荷本身就超过预算，`working` 会上报真实消耗
    （> cfg），该拍实际用时将超出预算 —— 时序分析里表现为"节点执行时间不受控"，故提前告警。

    两个口径分开看：**稳态**（每拍，决定 C_i 是否还有意义）与**冷启动**（进程内前 1~2 拍付 mmap
    缺页，只在 >128KB 时出现，通常已被 analysis_window_ms 滤掉，但会污染首拍统计）。
    """
    floor_us = cfg["solver"]["min_wcet_us"]
    worst = max(mesg_values)
    cost_us = _mesg_cost_us(worst)

    tiers = "  ".join(
        f"{m}B→{_mesg_cost_us(m):.2f}µs" for m in mesg_values
    )

    print(f"[MESG] 稳态产出耗时（本机实测估计）: {tiers}")

    notes = []

    if cost_us > 0.5 * floor_us:
        notes.append(
            f"稳态产出 {cost_us:.1f}µs 已占 min_wcet_us={floor_us}µs 的 "
            f"{cost_us / floor_us * 100:.0f}%（>50% → 执行用时脱离预算）"
        )

    # >128KB 走 mmap：进程内前 1~2 次还要付 mmap + 缺页，实测 1MB ≈ 247/219µs
    if worst > 128 * 1024:
        cold_us = 250.0 * worst / 1048576.0

        if cold_us > floor_us:
            notes.append(
                f"冷启动首 1~2 拍另需 ~{cold_us:.0f}µs（mmap+缺页）> 预算，该拍必然超时"
            )

    if notes:
        print(f"[WARN] mesg={worst}B: " + "；".join(notes))


def _filename(kind, utilization, m, bucket_id, seed, mesg=None):
    # 单档时不加 _msg：保持既有文件名不变（main.ipynb / 文档 / 已存语料都引用旧名）
    suffix = "" if mesg is None else f"_msg{int(mesg)}"

    return (
        f"{kind}"
        f"_u{_format_u(utilization)}"
        f"_m{m}"
        f"_ms{_format_ms_bucket(bucket_id)}"
        f"{suffix}"
        f"_s{seed}.json"
    )


# ============================================================
# 16. Candidate serialization
# ============================================================

def _make_pipeline(sk, trig, T, C_ms, mesg_size):
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
        #   cap                                        — 预留属性（本生成器不写；wcet/deadline 已不进 JSON）
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
            #   c{i}_2 = 载荷字节数（int）—— 输出 resize 到该大小；**全局统一**（workload.mesg_size）
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
                {
                    f"c{i}_2": int(mesg_size)
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
        # wcet 不写：执行时长由运行时维护（插件按 c{i}_1 忙等实测 + wcet_updater 自整定），
        # 求解器算出的 C_ms 只进 c{i}_1（µs）。deadline / cap 同样不写（预留字段）。
        # ----------------------------------------------------

        nodes.append(node)

    return nodes


def _make_metadata(kind, utilization, m, H_ms, makespan, bucket, seed):
    return {
        "generator_version": VERSION,

        "kind": kind,

        "utilization_target": utilization,

        "m": m,

        "H_ms": H_ms,

        "makespan_ms": makespan,

        "makespan_bucket": {
            "id": bucket["id"],
            "lo_ms": _ms_json(bucket["lo"]),
            "hi_ms": _ms_json(bucket["hi"]),
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
            "utilization = sum(C_i / T_i) / m"
        ),
    }


def _make_document(kind, utilization, m, H_ms, makespan, bucket, seed, pipeline, ):
    return pipeline


# ============================================================
# 17. Generate one candidate
# ============================================================

def generate_candidate(kind, utilization, m, H_ms, seed, config=None, mesg_size=None):
    cfg = (
        CONFIG
        if config is None
        else config
    )

    # None → 取第一档（本函数只负责求解，档位切换由 generate_all 复用同一份结果完成）
    if mesg_size is None:
        mesg_size = _mesg_values(cfg)[0]

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

    # ★ utilization 支持 数字 / (lo,hi) / None(=任意)：解析成**具体 u**（夹进可行域）。
    #   就地覆盖形参 → 后面的求解、tolerance 校验、metadata（进而文件名里的 uXX）全部用实得值。
    utilization = _resolve_utilization(
        utilization,
        T,
        m,
        cfg["solver"],
        seed + 7000001,
    )

    C_ms, makespan = (
        _solve_C_for_candidate(
            sk,
            trig,
            T,
            utilization,
            m,
            H_ms,
            seed + 2000003,
            cfg["solver"],
        )
    )

    # --------------------------------------------------------
    # Check utilization.
    # --------------------------------------------------------

    realized_utilization = _calc_utilization(
        C_ms,
        T,
        m,
    )

    if abs(
            realized_utilization - utilization
    ) > cfg["solver"]["utilization_tolerance"]:
        raise RuntimeError(
            "Candidate utilization mismatch: "
            f"target={utilization}, "
            f"actual={realized_utilization}"
        )

    # --------------------------------------------------------
    # Bucket.
    # --------------------------------------------------------

    buckets = _build_buckets(
        H_ms,
        cfg["workload"].get(
            "makespan_bin_width_ms"
        ),
        cfg["workload"][
            "makespan"
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
        mesg_size,
    )

    return {
        "document": pipeline,
        "meta": {
            "kind": kind,
            "utilization_target": utilization,
            "m": m,
            "H_ms": H_ms,
            "makespan_ms": makespan,
            "makespan_bucket": {
                **bucket,
                "lo": _ms_json(bucket["lo"]),
                "hi": _ms_json(bucket["hi"]),
            },
            "seed": seed,
            "mesg_size": mesg_size,
        },
        "bucket_id": bucket["id"],
        "makespan": makespan,
        "seed": seed,
    }


# ============================================================
# 18. Candidate collection
# ============================================================

def _bucket_key(kind, utilization, m, bucket_id):
    """候选去重键。utilization 可能是**具体数字**，也可能是区间/None（任意 u，由
    `_resolve_utilization` 在候选内解析）—— 后者不是 float，用 repr 做键。"""
    u_key = (
        repr(utilization)
        if utilization is None or isinstance(utilization, (tuple, list, str))
        else float(utilization)
    )
    return (
        kind,
        u_key,
        int(m),
        int(bucket_id),
    )


def _generate_candidates(kind, utilization, m, config, attempts, seed_start, buckets, candidates, mesg_size=None):
    successes = 0
    # 失败原因 → 次数。逐个尝试失败是采样常态（桶不匹配 / 该 (utilization,m) 无解），
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
                utilization,
                m,
                config["workload"]["H_ms"],
                seed,
                config,
                mesg_size,
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
            utilization,
            m,
            bucket_id,
        )

        limit = (
                config["search"]["n_per"]
                *
                config["search"][
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

def _sample_final(candidates, kind, utilization, m, buckets, n_per, rng, ):
    selected = []

    missing = []

    for bucket in buckets:

        key = _bucket_key(
            kind,
            utilization,
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
                "lo_ms": _ms_json(bucket["lo"]),
                "hi_ms": _ms_json(bucket["hi"]),
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

def _save_selected(selected, out_dir, mesg=None):
    os.makedirs(
        out_dir,
        exist_ok=True,
    )

    paths = []

    for item in selected:
        meta = item["meta"]

        filename = _filename(
            meta["kind"],
            meta["utilization_target"],
            meta["m"],
            meta["makespan_bucket"]["id"],
            meta["seed"],
            mesg,
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

def generate_configuration(kind, utilization, m, config=None, mesg_size=None):
    cfg = (
        CONFIG
        if config is None
        else config
    )

    _check_workload_keys(cfg)

    # mesg_size=None → 取 cfg 的第一档；cfg 是多档列表时必须显式传入（否则会把多档静默压成一档）
    if mesg_size is None:
        values = _mesg_values(cfg)

        if len(values) > 1:
            raise ValueError(
                f"workload.mesg_size 有 {len(values)} 档 {values}，"
                "需逐档调用：generate_configuration(..., mesg_size=<int>)"
            )

        mesg_size = values[0]

    mesg_size = int(mesg_size)

    workload = cfg[
        "workload"
    ]

    search = cfg[
        "search"
    ]

    H_ms = workload[
        "H_ms"
    ]

    n_per = search[
        "n_per"
    ]

    buckets = _build_buckets(
        H_ms,
        workload.get(
            "makespan_bin_width_ms"
        ),
        workload[
            "makespan"
        ],
    )

    candidates = {}

    seed_base = (
            search[
                "seed_base"
            ]
            +
            abs(
                hash(
                    (
                        kind,
                        utilization,
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
        f"utilization={utilization} "
        f"m={m} "
        f"attempts="
        f"{search['initial_attempts']}"
    )

    generated = _generate_candidates(
        kind,
        utilization,
        m,
        cfg,
        search[
            "initial_attempts"
        ],
        seed_base,
        buckets,
        candidates,
        mesg_size,
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
            search[
                "max_refill_rounds"
            ] + 1
    ):

        selected, missing = (
            _sample_final(
                candidates,
                kind,
                utilization,
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
                f"utilization={utilization} "
                f"m={m} "
                f"total="
                f"{len(selected)}"
            )

            return selected

        if (
                refill_round
                >= search[
            "max_refill_rounds"
        ]
        ):

            print(
                f"[INCOMPLETE] "
                f"kind={kind} "
                f"utilization={utilization} "
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
            utilization,
            m,
            cfg,
            search[
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
            mesg_size,
        )

        print(
            f"[REFILL-{refill_round + 1}] "
            f"generated={generated}"
        )


# ============================================================
# 22. Generate entire experiment
# ============================================================

def _retarget_mesg(item, mesg_size):
    """把候选的载荷档改为 mesg_size（深拷贝，只动 configs 的 c{i}_2）。

    同一 (kind,utilization,m) 的各载荷档**复用同一次求解结果**：拓扑、每节点 C_i、makespan、seed
    全部不动，唯一变量是载荷大小 —— 载荷扫描必须是受控对比，不能顺带换图。
    """
    if item["meta"].get("mesg_size") == mesg_size:
        return item

    document = copy.deepcopy(
        item["document"]
    )

    for i, node in enumerate(document):
        for entry in node.get("configs", []):
            key = f"c{i}_2"

            if key in entry:
                entry[key] = int(mesg_size)

    meta = dict(item["meta"])
    meta["mesg_size"] = int(mesg_size)

    return {
        "document": document,
        "meta": meta,
        "bucket_id": item["bucket_id"],
        "makespan": item["makespan"],
        "seed": item["seed"],
    }


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

    _check_workload_keys(cfg)

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

    # 载荷档位（列表；标量视为单档）—— 每个 (kind,utilization,m) 逐档出一份配置
    mesg_values = _mesg_values(cfg)

    _warn_mesg_budget(cfg, mesg_values)

    print()
    print(
        f"[PLAN] kinds={len(kinds)} "
        f"utilization={len(workload['utilization'])} "
        f"m={len(workload['workers'])} "
        f"mesg={len(mesg_values)}{mesg_values} "
        f"→ 求解 {len(kinds) * len(workload['utilization']) * len(workload['workers'])} 次，"
        f"落盘文件数 = 求解次数 × 每档 n_per 文件数 × {len(mesg_values)} 档"
    )

    for kind in kinds:

        for utilization in workload["utilization"]:

            for m in workload["workers"]:
                print()
                print(
                    "=" * 70
                )

                print(
                    f"[CONFIG] "
                    f"{kind} "
                    f"utilization={utilization} "
                    f"m={m}"
                )

                # 求解一次（按第一档载荷），随后各档只改 c{i}_2 → 各档拓扑完全一致
                selected = (
                    generate_configuration(
                        kind,
                        utilization,
                        m,
                        cfg,
                        mesg_size=mesg_values[0],
                    )
                )

                for mesg in mesg_values:
                    variants = [
                        _retarget_mesg(item, mesg)
                        for item in selected
                    ]

                    paths = _save_selected(
                        variants,
                        out_dir,
                        # 单档不写 _msg 后缀，保持既有文件名不变
                        mesg if len(mesg_values) > 1 else None,
                    )

                    all_selected.extend(
                        paths
                    )

                    tag = (
                        ""
                        if len(mesg_values) == 1
                        else f" mesg={mesg}B"
                    )

                    print(
                        f"[SAVED] "
                        f"{len(paths)} "
                        f"files"
                        f"{tag}"
                    )

    print()
    print(
        "=" * 70
    )

    print(
        f"[DONE] "
        f"files={len(all_selected)} "
        f"mesg={mesg_values}"
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
      id / name / version / configs / outputs
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

        if len(cfgs) > 2:
            parts.append(f"mesg={cfgs[2]}B")

        print("  ".join(parts))


if __name__ == "__main__":
    # 默认调用方式（会走配置或默认的 pipeline 目录）
    generate_all()