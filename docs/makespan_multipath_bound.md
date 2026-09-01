# Makespan 估计:Multi-Path Bound

> 状态:已落地。`client` 在每次超周期回绕(rollover)时用 Multi-Path Bound 估计当前 DAG 的
> makespan(响应时间上界),若超过超周期 `hyper_period` 则告警过载。算法取自 He et al. 2023。

## 1. 背景与目标

对 precedence graph(单 DAG)在 `m` 个 worker 上执行,需要一个 **makespan 上界**来做超周期
过载检查 / 可调度性判断。经典做法是 **Graham bound**:

```
R ≤ len(G) + (vol(G) − len(G)) / m
```

其中 `len(G)` = 关键路径长、`vol(G)` = 总负载。它隐含假设"不在关键路径上的顶点不能与关键
路径并行",在多核上**很悲观**——实际许多顶点会与关键路径并行执行。

**Multi-Path Bound** 利用多条路径的并行信息收紧这个界。它取 `j=0` 时恰好就是 Graham's bound,
`j≥1` 多覆盖路径后更紧(见 §3)。

本实现把**两个 bound 都实现了**——`graham_makespan`(1979)与 `mpb_makespan`(2023),
见 §4。

## 2. 算法来源

**Qingqiang He, Nan Guan, Shuai Zhao, Mingsong Lv, 《Multi-Path Bound for DAG Tasks》,
arXiv:2310.15471 (2023-10), IEEE TCAD 2024 (doi 10.1109/tcad.2024.3507563)。**

核心思想:选一组**互不相交**的路径(论文称 generalized path list),这些路径在多核上并行执行,
从而覆盖更多负载、收紧响应时间上界。

## 3. 核心公式(论文 Theorem 1 / 公式 9)

给定 generalized path list `(λ_i)_{i=0..k}`(k ≤ m−1,各路径互不相交),响应时间上界:

```
R ≤ min_{ j ∈ [0,k] } [ len(G) + ( vol(G) − Σ_{i=0..j} len(λ_i) ) / (m − j) ]
```

- `len(G)` — DAG 关键路径长
- `vol(G)` — 总负载(全部顶点 wcet 之和)
- `len(λ_i)` — 第 i 条路径的长度
- `m` — 核心 / worker 数
- 对每个 `j`:前 `j+1` 条路径已被"覆盖",剩余负载 `vol − Σlen(λ_i)` 由剩余 `m−j` 个核并行消化

**Algorithm 1(论文最优计算):**
```
1  w ← DAG 的 width(最大反链 = 最小路径覆盖)
2  n ← min{w, m}
3  foreach j = 0..n−1:
4      W ← 基数 j+1 的最大体积互不相交路径表   ← 归约为最小费用流
5      R_j ← len(G) + (vol(G) − W) / (m − j)
6  return min_j R_j
```

## 4. 本实现(`schedule/makespan_updater.hpp` 的 `fins::sched::graham_makespan` / `mpb_makespan`)

独立头文件 `schedule/makespan_updater.hpp` 提供**两个自包含函数**:

- `graham_makespan(dag, m)`:Graham's bound,`len(G) + (vol(G) − len(G)) / m`
- `mpb_makespan(dag, m)`:Multi-Path Bound,用**贪心取最长路径**构造 generalized path list
  (合法、安全上界;论文 §V 的最优路径表需最小费用流,见 §6),然后套公式 (9)。
  `j=0` 项即 Graham's bound,`j≥1` 更紧 → 支配前者。

```
mpb_makespan(dag, m):
  ① job 顶点集(跳过 "tp:" 时间点) + 拓扑序(Kahn, 仅 job 间边)
  ② len(G) 关键路径 + vol(G) 总负载(拓扑序 DP, wcet 为顶点权)
  ③ 贪心路径表: 反复在剩余顶点诱导子图上求最长路径, 移除其顶点, 直至取满 min(m, n) 条
  ④ 公式(9): 逐 j 算 R_j = len(G) + (vol − Σ_{i≤j} lenλ_i)/(m − j), 取 min
```

### 为何跳过 `tp:` 顶点

时间点顶点的 `wcet` 是相邻释放间隔(如 100ms),**不是执行负载**;若计入会淹没真正的
执行 makespan,故只统计 job 顶点(边也只在 job 顶点间计数)。

### 接线与开关

```cpp
// client.cpp main:
makespan_updater = [num_workers](fins::util::DirectedAcyclicGraph<Workload, Message> &dag) {
  return fins::sched::mpb_makespan(dag, num_workers);
};
```

- 开关:`#define FINS_CAL_MAKESPAN 1`(client.cpp 顶部;0 关闭)
- 调用点:`g_state.hpp` `rollover_hp()` 中 `#if FINS_CAL_MAKESPAN` 分支——每次超周期回绕
  算一次,`makespan > hyper_period` 打 `超周期过载` 告警日志
- `m` = 线程池 worker 数(函数槽签名不带 m,故闭包捕获 `num_workers`)

## 5. 验证结果(m=2,Debug 构建)

| 配置 | len(G) | vol(G) | 路径表 | 上界 | 核对 |
|---|---|---|---|---|---|
| join2 | 3ms | 7ms(b period=50 → 2 实例) | 3 + 3 | **4.0ms** | j=1: 3+(7−3−3)/1=4 |
| fork100 | 3ms | 102ms | 3 + 1×… | **52.5ms** | j=0: 3+(102−3)/2=52.5 |
| join100 | 3ms | 102ms | 3 + 1×… | **52.5ms** | 同 fork100 结构 |

`fork100` 的 100 个 sink 相互独立、2 worker 并行,实际执行 ~50ms + 3ms 关键路径,上界 52.5ms
贴合实际。全部配置 0 异常。

## 6. 与论文最优版的差距(可选升级)

贪心路径表每次取"当前剩余子图最长路径",不保证对公式 (9) 最优。论文用**最小费用流**
求"基数 j+1 的最大体积互不相交路径表":

- 网络构造:ancestor 连边(传递闭包)→ 顶点拆 `v_in/v_out`(边权 `−c(v)`)→ 源/汇,容量 1
- 流量 n 的最小费用流 ↔ 基数 n 的最大体积 generalized path list
- 复杂度 `O(w(|E| + |V| log|V|))`,w = width ≤ |V|

对 10-103 顶点的小图,贪心与最优差别通常很小;如需要可把 §4 ③ 替换为最小费用流。

## 7. 相关代码

- `schedule/makespan_updater.hpp`:`fins::sched::graham_makespan()` + `mpb_makespan()`(独立头文件)
- `example/client.cpp`:`#include "schedule/makespan_updater.hpp"` + `makespan_updater` 装配(默认用 mpb)
- `include/g_state.hpp`:`makespan_updater` 槽、`rollover_hp()` 的 `FINS_CAL_MAKESPAN` 分支
