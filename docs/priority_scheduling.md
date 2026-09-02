# 优先级调度：静态 + 动态多策略

> 状态:已落地。`client` 装配 `priority_updater` 槽,默认 **EDF 动态优先级**
> (`FINS_DYNAMIC_PRIORITY=1`);策略一行切换(`FINS_PRIORITY_POLICY`)。算法在
> `schedule/priority_updater.hpp`,自包含、无复杂公开数据结构。

## 1. 背景与机制

g_state 的就绪集是**懒最大堆**(`LazyMaxHeap<ReadyItem>`),排序键 `ReadyItem.prio`:
**值越大越优先**,相等时按入队序号 `seq` 小者先出(= FIFO 精确兜底)。优先级唯一来源 =
装配点注入的 `priority_updater` 槽:

```
int priority_updater(DAG&, const Workload&)   // 顶点 → 调度优先级
```

- `FINS_DYNAMIC_PRIORITY=1`:grab 决策点现算——`grab_ready_workload` 每拉一个 job 前,对
  全部就绪顶点重算 prio 再 rebuild(动态策略吃最新 ddl/时间)。
- `FINS_STATIC_PRIORITY=1`:rollover 每超周期赋值一次(静态策略固定不变,只需低频刷新)。
- 两开关**互斥**,勿同开(动态现算会覆盖静态赋值)。

## 2. 数值约定

- 返回 int,**越大越优先**。
- 时间量统一 µs 精度:`ms×1000` 取整。
- 绝对时间戳(steady_clock `now_ms()` 是巨量级)直接进 int32 会**溢出** → 动态策略一律用
  **相对量**(`ddl − now`)再缩放,单调变换保序。

## 3. 策略清单

### 3.1 静态 · 顶点字段(仅需 `period/deadline/wcet`)

| 函数 | 名称 | 规则 | 适用 |
|---|---|---|---|
| `prio_fifo` | FIFO | 恒 0(= 不注入时的纯 FIFO) | 显式兜底/对照 |
| `prio_rm` | RM(1973) | 周期越短越高 | 周期任务经典静态最优 |
| `prio_dm` | DM(1982) | 相对截止期越短越高 | RM 推广;任意截止期下静态最优 |
| `prio_sjf` | SJF | wcet 越小越高 | 最小化平均响应 |
| `prio_ljf` | LJF | wcet 越大越高 | 先做重活、尾部并行收尾 |
| `prio_density` | HDF | wcet/截止期 越大越高 | 负载/截止期最紧者先做 |

### 3.2 静态 · 图结构(DAG 感知;结构缓存按 `graph_version` 失效,同 makespan)

| 函数 | 规则 | 语义 |
|---|---|---|
| `prio_depth(dag, version, w)` | 拓扑深度(源=0)越大越高 | 越深 → 越早释放后继,缩短整图关键路径 |
| `prio_height(dag, version, w)` | 到汇距离越大越高 | 越接近 sink → 收尾阶段尽快完成 |

拓扑深度/高度是**未加权**量,只依赖边结构 → 纯结构可缓存(详见 §4)。

### 3.3 动态 · grab 现算(吃最新 ddl;内部读 `now_ms`)

| 函数 | 规则 | 说明 |
|---|---|---|
| `prio_edf` | `−(ddl−now)` | EDF(1973):最早绝对截止期 |
| `prio_llf` | `−(ddl−now−wcet)` | LLF/LST(1974):松弛度最小者最紧急;剩余执行未知 → 用 wcet 保守近似 |

动态策略要求 `ddl` 新鲜:`grab_ready_workload` 在 `FINS_DYNAMIC_PRIORITY` 分支开头已调
`update_abs_deadline()` 滚动校正,EDF/LLF 拿到的即当前周期截止期。

## 4. 结构缓存(深度/高度)

同 makespan 的模式:`detail::PriorityStructure` 缓存稠密下标、拓扑序、前驱/后继表、
`depth`/`height` 数组。这些是**纯结构量**,与 wcet 无关、跨 rollover 不变,只在
`expand_hp` 重建(`graph_version++`)后重建一次;每轮 grab 命中缓存 O(1) 读数组。
⚠ 若要做**加权**深度/高度(按 wcet),则依赖每轮自整定的 wcet、不可整缓存——本实现
取未加权版本以保持结构缓存有效。

## 5. 装配与开关

```cpp
// client.cpp main（一行选策略，version_of 供 DEPTH/HEIGHT 现读结构版本号）：
priority_updater = fins::sched::make_priority(FINS_PRIORITY_POLICY, [] { return graph_g.graph_version; });
```

```cpp
// client.cpp 顶部宏：
#define FINS_DYNAMIC_PRIORITY 1            // 动态（默认开）；与静态互斥
#define FINS_STATIC_PRIORITY  0
#define FINS_PRIORITY_POLICY fins::sched::Policy::EDF   // 换策略只改这行
```

- 策略选择器 `fins::sched::Policy`:FIFO/RM/DM/SJF/LJF/DENSITY/DEPTH/HEIGHT/EDF/LLF
- `make_priority(Policy, version_of)` 返回与槽签名一致的 `std::function`;DEPTH/HEIGHT 经
  `version_of` 每 grab 现读 `graph_version`(其余策略不调用,开销为零)
- 直接调单个函数亦可:`priority_updater = [](Dag &d, const Workload &w) { return fins::sched::prio_edf(w); };`

## 6. 注意事项

- **宏互斥**:`FINS_DYNAMIC_PRIORITY` 与 `FINS_STATIC_PRIORITY` 只开一个。
- **槽必注入**:任一宏开 1 而 `priority_updater` 为 nullptr → grab 现算调空函数崩(历史
  `bad_function_call` 根因)。client 已装配,勿删。
- 动态模式每次 grab 代价 = `update_abs_deadline()` 全图 O(V) + 就绪集现算 O(ready) + rebuild
  O(ready)。就绪集大时 EDF/LLF 的每次 `now_ms()`(~20ns vDSO)×ready 可感知,但相对就绪堆
  O(ready log n) 小一个量级,可接受。
- 动态策略与 `FINS_CAL_WCET` 联动:wcet 每轮自整定 → SJF/LJF/density/LLF 每轮取新值,属预期。

## 7. 相关代码

- `schedule/priority_updater.hpp`:`fins::sched::prio_*` + `Policy` + `make_priority`
- `example/client.cpp`:`#include` + `FINS_PRIORITY_POLICY` + `priority_updater` 装配 + 宏
- `include/g_state.hpp`:`priority_updater` 槽(签名未改,`int(DAG&, const Workload&)`)、
  `ReadyItem.prio` 最大堆语义、`grab_ready_workload`/`rollover_hp` 的宏分支、
  `graph_version`(DEPTH/HEIGHT 结构缓存失效信号)
