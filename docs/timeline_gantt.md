# Timeline 甘特图分析：任务 / Overhead 逐核时序

> 状态：已落地。`tool/_csv2timeline.py::generate_execution_gantt` 读入
> `_lttng2csv.py` 导出的两个 CSV，画出「每个 CPU 上 worker 的 job 执行段 + 框架开销段」
> 的时序图（HTML）。

## 1. 输入

`tool/_lttng2csv.run_export()` 一次扫描 LTTng trace，输出两个共享 t0 的 CSV：

| 文件 | 来源 | 关键列 |
|---|---|---|
| `*_timeline.csv` | UST 事件 | `t_us, cpu, tid, kind, tag` |
| `*_preempt.csv` | 内核 `sched_switch` | `t_us, cpu, prev_tid, prev_comm, prev_state, next_tid, next_comm` |

- `kind ∈ {execute, complete, working, release, finished, wake, sleep}`；`tag` = 节点号。
- 两文件 `t_us` 都是**同一时钟的绝对值**，且相对**同一个 t0** 归一化。

```
trace/ ──_lttng2csv.py──▶ *_timeline.csv ┐
                                          ├──_csv2timeline.py──▶ *.html
        ────────────────▶ *_preempt.csv  ┘
```

## 2. 原理

### 2.1 CPU 运行片段重建（逐核）

对每个 CPU，把 preempt CSV 按 `t_us` 升序，**相邻两行**之间 `[t_i, t_{i+1}]` 归给第 i 行的
`next_comm` / `next_tid` —— 即「这一行切进去的任务，一直跑到下一次切换」。

> **前提：CSV 必须是完整的切换序列**（导出时 `preempt_all=True`）。若只保留 worker 相邻行，
> 相邻两行不再时间相邻，区间归因整体错位。

只保留 `next_comm` 以 `fins_worker`（或 `cie_container` / `mte_container`）开头的片段；
非 worker 时间**不画**（留白）。

### 2.2 任务块 = job ∩ worker 片段

- `job` = 同一 tid 上一次 `execute` 到其后的 `complete`（**按事件流顺序闭合**，见 §3）。
- 与**同一 tid** 的 worker 片段求交 → `[max(job_start, blk_start), min(job_end, blk_end)]`。
- 画在有该片段出现的 CPU 泳道上，颜色按节点号分配。

### 2.3 Overhead = worker 片段 − 任务块

同一 tid 的任务块先排序合并，再用区间减法从该 tid 的每个 worker 片段里扣掉，剩余段即
**worker 在 CPU 上但不属于任何 job 的时间**（取锁 / grab / 回锁 / notify / 释放路径 / 空转
轮询等框架侧开销），画成橙色。

### 2.4 绘图

```python
px.bar(df, base="start_ms", x="dur_ms", y="core_id", color="algo",
       orientation="h", barmode="overlay")   # overlay：任务块盖在 Overhead 上层
```

`df = concat([overhead, jobs])` —— 顺序决定叠放层次（后画的在上）。
`zoom_window_ms=[a, b]` 设 x 轴范围（典型 `[0, 1000]` ms 看细节）。

## 3. 两个必须遵守的口径（都实测踩过）

**① 两文件必须用同一个 t0**
各取各自 `t_us.min()` 归一化，会让 job 区间整体平移（实测两文件 min 差 **2612 µs**），
job ∩ 片段求交随之错位：短任务受害最大 —— 平移量（2.6 ms）远大于任务本身（如 `usr_acc`
只有 193 µs），交集不再对应它真正执行的那一段；平移后若落到片段之外就被整个丢掉。
现取 `t0 = min(df_preempt.t_us.min(), df_timeline.t_us.min())`。

**② execute/complete 必须按事件流顺序配对**
「取时间上第一个 ≥ execute 的 complete」在**开头缺 execute**（导出裁剪 `after_us` 正好落在
一对中间）时会把每个 execute 配到**下一个任务**的 complete → 时长全部拉长、同一 complete 被
复用。现在按 tid 顺序闭合：开头孤立的 complete 丢弃、结尾未闭合的 execute 丢弃。

## 4. 输出与解读

```
worker 总 CPU 时间 : 21077.574 ms
  - 任务    : 21002.417 ms (99.6%)
  - Overhead:    75.158 ms (0.4%)
```

- **任务占比** = 真实执行利用率，与设计目标 `u` 对照。
- **Overhead 占比** = 框架开销，实测 1 MB 载荷 / 3 worker 下为 **0.1~0.4%**；若显著偏高，
  先查是否触发了 §3 的两个坑。
- 图上大片空白 = 该核上没有 worker 在跑（idle 或其他进程），不是数据缺失。

## 5. 相关

- 导出：`tool/_lttng2csv.py`（`run_export` / `cpus(cpu_start, num_workers)`）
- 三分类统计：`tool/_csv2overhead.py`（见 `docs/overhead_breakdown.md`）
- 实验驱动：`tool/_test.py`（`cores_range = f"{cpu_offset}-{cpu_offset+m-1}"`，须与
  `cpus()` 的核范围一致）
