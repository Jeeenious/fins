# Overhead 分解：逐核 Active / Overhead / Idle 占比

> 状态：已落地。`tool/_csv2overhead.py::analyze_cpu_utilization` 把每个核对 worker 的
> 占用切成三类并画堆叠柱（HTML）+ 打印控制台表。用于回答「设计利用率 `u` ↔ 实测利用率」。

## 1. 输入

与甘特图同源：`_lttng2csv.py` 导出的 `*_timeline.csv`（UST 事件）与 `*_preempt.csv`
（`sched_switch`），共享同一 t0。详见 `docs/timeline_gantt.md` §1。

## 2. 三分类定义

对每个 CPU，`worker 片段` = 该核上 `next_comm` 为 `fins_worker`（或 `cie_container` /
`mte_container`）的连续运行区间，由 preempt CSV 的相邻行重建（同甘特图 §2.1）。

| 类别 | 定义 | 含义 |
|---|---|---|
| **Active** | `∪(job ∩ 该核 worker 片段)`，按核合并消重 | worker 真正在执行任务 |
| **Overhead** | `该核 worker 片段总时长 − Active` | worker 在 CPU 上但不属于任何 job：取锁 / grab / 回锁 / notify / 释放路径 / 超时轮询等框架开销 |
| **Idle** | `该核总时长 − worker 片段总时长` | 非 worker 的一切：idle 任务、其他进程、中断、**控制线程**（见 §7） |

三者相加 = 该核总时长 → 柱内百分比和恒为 100%。

- `job` = 同一 tid 的 `execute → complete`（**按事件流顺序配对**，见 §4）。
- **Active 必须按核消重**：同一核任一时刻只有一个 worker 在跑，但一个 job 可能跨多个 worker
  片段（被抢占过），直接累加会重复计数 —— 故先 `_merge_intervals` 再求和。
- Active 只取「与 worker 片段相交」的部分：job 若落在非 worker 时间段上（例如核被别的进程
  占用），那部分算 Idle 而不是 Active。

```
            ┌──────────── 该核总时长 ────────────┐
 worker片段 │ Active │ Overhead │      Idle       │
            └────────┴──────────┴─────────────────┘
```

## 3. 分母与 AVG 行

- 逐核分母 = 该核 `(最后一次切换 − 第一次切换)`，即 preempt CSV 里该核的时间跨度。
- `analysis_window_ms=[a,b]` 可把统计裁剪到指定窗口（分母同步裁剪）。
- **AVG 行用全局加权** `Σ时间 / Σ总时间`，不是各核百分比的算术平均 —— 后者在核间窗口不等长时
  会失真。
- 排序：各核按 Active% 降序，AVG 固定放最后。

> 注：逐核分母取「该核自己的跨度」，各核起止略有差异（导出裁剪 + 首末切换时刻不同），
> 故逐核百分比严格来说不完全可比。若需要严格可比，应统一成全局窗口（或用
> `analysis_window_ms`）—— 当前沿用前者，改动会影响历史数字，未做。

## 4. 两个必须遵守的口径（都实测踩过）

**① 两文件必须用同一个 t0**：各取各自 `t_us.min()` 会让 job 区间整体平移（实测差 2612 µs），
job ∩ 片段错位 → **Active 低估、Overhead 虚高**（实测最多 7 倍：join 1.86% → 0.25%）。

**② execute/complete 必须按事件流顺序配对**：「取时间上第一个 complete」在开头缺 execute 时
会把 execute 配到下一个任务的 complete → 时长虚高。

两者现均已在 `_csv2timeline.py` / `_csv2overhead.py` 中修正。

## 5. 结果解读

实测（feedback / 1 MB 载荷 / 3 worker）：

```
CPU 3  Active  73.03%  Overhead   0.25%  Idle  26.72%
CPU 2  Active  70.24%  Overhead   0.25%  Idle  29.50%
CPU 1  Active  66.16%  Overhead   0.25%  Idle  33.59%
全局加权  Active 69.81%  Overhead 0.25%  Idle 29.94%
```

- **Active** ↔ 设计利用率 `u`：这是「实测 u」的来源。
- **Overhead** 是框架侧真实开销，典型 **0.1~0.4%**；异常偏高先查 §4。
- **Idle** 高不等于有问题：取决于 `u` 与 worker 数（`u=70%`、3 worker 时理论 Idle ≈ 30%）。

## 6. 控制核：非 worker 线程不进 worker 核

核的划分（2026-09-24 起）：

- **worker 核 = `1..m`**：`ThreadPool::working` 显式 `bind_core(i+1)`；`_lttng2csv` 的
  `cpus(cpu_start=1, m)` 导出的也正是这几个核 → 逐核统计只覆盖 worker 核。
- **控制核 = `m+1`**（裸跑默认 `0`）：主循环 / 计时线程 / 组件线程（RPC、插件 watchdog、
  硬件监控、httplib 连接线程）都绑这一核。实现见 `example/client.cpp` 开头的 `bind_core`
  + `FINS_CONTROL_CORE` / argv[4]：**新线程继承创建者的 CPU 掩码**，所以 main 一开始绑一次，
  之后建的线程全部跟着走；worker 由线程池显式绑到 1..m，不受影响。
- `tool/client.sh` 的 cpuset 必须**含控制核**（约定：调用方多给一核，如 `client.sh 1-3 2` →
  worker 1,2 + 控制核 3）。若 cpuset 不含控制核，`bind_core` 会 EINVAL **静默失败** → 控制
  线程落回 worker 核抢 CPU（实测：主循环/计时/组件与 worker 同核，就是那 0.7% 的 overhead 来源）。
  反例：为了腾出 core 0 而把 `0` 并进 `cpuset.cpus` 会让 `cpuset.cpus.partition=isolated`
  失败，连 worker 核的隔离一起丢——所以控制核取范围里的**最后一核**，不动 core 0。

对本文口径的影响：Overhead 只统计 `fins_worker*` 片段，控制线程的占用落在 **Idle** 里；控制核
（m+1）不在 `cpus()` 导出范围内，因此不参与逐核/AVG 统计（否则会拉低 AVG）。

## 7. 相关

- 导出：`tool/_lttng2csv.py`（`run_export` / `cpus(cpu_start, num_workers)`）
- 时序图：`tool/_csv2timeline.py`（见 `docs/timeline_gantt.md`）
- 实验驱动：`tool/_test.py`（`cores_range = <offset>-<offset+m>`：worker 核 1..m + 控制核 m+1，
  与 `cpus(cpu_start=1, m)` 的 worker 范围一致；跑前守卫 `ensure_port_free` 清残留 client）
