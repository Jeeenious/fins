# 优先级调度：静态多策略

> 状态:已落地。`client` 装配 `priority_updater` 槽,默认 **FIFO**(`FINS_PRIORITY_POLICY`);
> 策略一行切换。算法在 `schedule/priority_updater.hpp`,自包含、无复杂公开数据结构。
> ⚠ 吃截止期的动态策略(`prio_dm`/`prio_density`/`prio_edf`/`prio_llf`)已随 JSON 的
> `deadline` 输入一并删除,现行策略集**全是静态量**(顶点字段 / 图结构)。

## 1. 背景与机制

g_state 的就绪集是**懒最大堆**(`LazyMaxHeap<ReadyItem>`),排序键 `ReadyItem.prio`:
**值越大越优先**,相等时按入队序号 `seq` 小者先出(= FIFO 精确兜底)。优先级唯一来源 =
装配点注入的 `priority_updater` 槽:

```
int priority_updater(DAG&, const Workload&)   // 顶点 → 调度优先级
```

- `FINS_DYNAMIC_PRIORITY=1`:grab 决策点现算——`grab_ready_workload` 每拉一个 job 前,对
  全部就绪顶点重算 prio 再 rebuild(留给"吃时间量"的策略;**现行策略集无需**它)。
- `FINS_STATIC_PRIORITY=1`:rollover 每超周期赋值一次(静态策略固定不变,只需低频刷新)。
- 两开关**互斥**,勿同开(动态现算会覆盖静态赋值)。

## 2. 数值约定

- 返回 int,**越大越优先**。
- 时间量统一 µs 精度:`ms×1000` 取整。
- 绝对时间戳(steady_clock `now_ms()` 是巨量级)直接进 int32 会**溢出** → 要用时间量的策略
  一律取**相对量**(截止期 − `now_ms()`)再缩放,单调变换保序(见 §3.3 的 EDF)。

## 3. 策略清单

### 3.1 静态 · 顶点字段(仅需 `period/wcet`,两者都框架内维护)

| 函数             | 名称       | 规则                 | 适用               |
| -------------- | -------- | ------------------ | ---------------- |
| `prio_fifo`    | FIFO     | 恒 0(= 不注入时的纯 FIFO) | 显式兜底/对照(默认)     |
| `prio_rm`      | RM(1973) | 周期越短越高             | 周期任务经典静态最优       |
| `prio_sjf`     | SJF      | wcet 越小越高          | 最小化平均响应          |
| `prio_ljf`     | LJF      | wcet 越大越高          | 先做重活、尾部并行收尾      |

### 3.2 静态 · 图结构(DAG 感知;结构缓存按 `graph_version` 失效,同 makespan)

| 函数                             | 规则            | 语义                   |
| ------------------------------ | ------------- | -------------------- |
| `prio_depth(dag, version, w)`  | 拓扑深度(源=0)越大越高 | 越深 → 越早释放后继,缩短整图关键路径 |
| `prio_height(dag, version, w)` | 到汇距离越大越高      | 越接近 sink → 收尾阶段尽快完成  |

拓扑深度/高度是**未加权**量,只依赖边结构 → 纯结构可缓存(详见 §4)。

### 3.3 已删除的动态策略(EDF / LLF / DM / HDF)

| 函数             | 曾有规则              | 删除原因                                       |
| -------------- | ----------------- | ------------------------------------------ |
| `prio_edf`     | `−(ddl−now)`      | 依赖 JSON `deadline` 输入,已随该字段一起删            |
| `prio_llf`     | `−(ddl−now−wcet)` | 同上(松弛度用 wcet 保守近似)                        |
| `prio_dm`      | 相对截止期越短越高         | 同上                                          |
| `prio_density` | `wcet/截止期` 越大越高   | 同上(`deadline=0` 时分母退到 `1e-9` → 优先级爆表,本就是坑) |

**框架侧一并删掉了 `Workload::ddl` 与 `update_abs_deadline`**:截止期的来源是调度决策
(框架没有足够信息替用户定,见 `docs/pipeline_json_schema.md` §5)。要恢复 EDF/LLF,先得自备
截止期——由调度算法从 `configs`/另一份配置 JSON 注入并按拍自行刷新,再照上表一行写回策略。

## 4. 结构缓存(深度/高度)

同 makespan 的模式:`detail::PriorityStructure` 缓存稠密下标、拓扑序、前驱/后继表、
`depth`/`height` 数组。这些是**纯结构量**,与 wcet 无关、跨 rollover 不变,只在
`expand_hp` 重建(`graph_version++`)后重建一次;每轮 grab 命中缓存 O(1) 读数组。
⚠ 若要做**加权**深度/高度(按 wcet),则依赖每轮自整定的 wcet、不可整缓存——本实现
取未加权版本以保持结构缓存有效。

## 5. 装配与开关

```cpp
// client.cpp main（一行选策略，version_of 供 DEPTH/HEIGHT 现读结构版本号）：
priority_updater = fins::sched::make_priority(FINS_PRIORITY_POLICY,
    [] { return graph_g.graph_version; }, [] { return num_workers; });
```

```cpp
// client.cpp 顶部宏：
#define FINS_DYNAMIC_PRIORITY 0                            // 与静态互斥；现行策略集无需动态
#define FINS_STATIC_PRIORITY  0
#define FINS_PRIORITY_POLICY fins::sched::Policy::FIFO     // 换策略只改这行
```

- 策略选择器 `fins::sched::Policy`:FIFO/RM/SJF/LJF/DEPTH/HEIGHT
- `make_priority(Policy, version_of)` 返回与槽签名一致的 `std::function`;DEPTH/HEIGHT 经
  `version_of` 每 grab 现读 `graph_version`(其余策略不调用,开销为零)
- 直接调单个函数亦可:`priority_updater = [](Dag &d, const Workload &w) { return fins::sched::prio_rm(w); };`

## 6. 注意事项

- **宏互斥**:`FINS_DYNAMIC_PRIORITY` 与 `FINS_STATIC_PRIORITY` 只开一个。
- **槽必注入**:任一宏开 1 而 `priority_updater` 为 nullptr → grab 现算调空函数崩(历史
  `bad_function_call` 根因)。client 已装配,勿删。
- 动态模式每次 grab 代价 = 就绪集现算 O(ready) + rebuild O(ready)。就绪集大时每次 `now_ms()`
  (~20ns vDSO)×ready 可感知,但相对就绪堆 O(ready log n) 小一个量级,可接受。
- 与 `FINS_CAL_WCET` 联动:wcet 每轮自整定 → SJF/LJF 每轮取新值,属预期。

## 7. 相关代码

- `schedule/priority_updater.hpp`:`fins::sched::prio_*` + `Policy` + `make_priority`
- `example/client.cpp`:`#include` + `FINS_PRIORITY_POLICY` + `priority_updater` 装配 + 宏
- `include/g_state.hpp`:`priority_updater` 槽(签名未改,`int(DAG&, const Workload&)`)、
  `ReadyItem.prio` 最大堆语义、`grab_ready_workload`/`rollover_hp` 的宏分支、
  `graph_version`(DEPTH/HEIGHT 结构缓存失效信号)
