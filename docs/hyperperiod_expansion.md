# 超周期展开：算法逻辑

> 状态：已落地。`core/g_state.hpp::expand_hp` 把配置里**按节点声明**的 pipeline 静态展开成
> 一张"每超周期每个实例一个顶点"的可调度 DAG。本文按步骤记录算法逻辑与不变量。
> 配套的运行时另一半是 `rollover_hp`（回绕），见 §6。

## 1. 目标与输入输出

|      |                                                                                                  |
| ---- | ------------------------------------------------------------------------------------------------ |
| 输入   | `pipeline.nodes`（NodeInfo 声明表：period/event/inputs/outputs/hist…）+ `library.so_ctx`（算法 so 表） |
| 输出   | 顶点 `{id}:{k}` 的 DAG（顶点数 = Σ 每节点实例数 + 同步时间点数）+ 绑定边 + 时间点顶点 `tp:s`                                 |
| 语义   | **每个顶点每超周期恰好执行一次**（静态展开的根基，见 §5）                                                                 |
| 调用约束 | **无锁原语，调用方须持 `graph_g.mtx`**；由主线程调度循环在图静止（`is_hp_done`）时调用                                       |

`expand_hp` 开头做三件事（`g_state.hpp:1290`）：

```cpp
hyper_start_ms = fins::util::now_ms();   // 新图起点 = 当前真实时钟（勿残留上次回绕后的起点）
mesg_hist_cap.clear();                   // 只清容量表；message_hist_ 历史槽跨重建保留
pred_left_/in_degree_/nonblocking_in_/blocking_succ_/done_/ready_/tp_order_ .clear();
dag.clear();
```

空配置（`nodes.empty()`）→ `++graph_version` 后直接返回（空图；结构已清空，同样发失效信号）。

`graph_version++` 必须放在**全部建图步骤之后**——它是 makespan 结构缓存的失效信号。

## 2. 伪代码

```python
# ══════════ 入口：把声明式 pipeline 展开成静态图 ══════════
EXPAND(pipeline, library):                       # 无锁原语，调用方持 mtx
    hyper_start_ms ← now_ms()
    mesg_hist_cap.clear()                        # message_hist_ 历史槽跨重建保留，不清
    pred_left_, in_degree_, nonblocking_in_, blocking_succ_ ← {}
    done_, ready_, tp_order_ ← {}
    dag.clear()
    if pipeline.nodes is empty: graph_version++; return        # 空配置 → 空图

    idx          ← BUILD_PORT_INDEX(nodes)                    # ① producers/consumers + 一跳邻居
    H            ← BUILD_HYPER_PERIOD(nodes)                  # ② 标称超周期
    topo         ← BUILD_TOPO_ORDER(nodes, idx)               # ③ BFS 从源展开
    by_id        ← BUILD_INSTANCES(nodes, library.so_ctx)     # ④ 每节点一个算法实例
    (T, N, domi) ← BUILD_DOMINANCE(topo, nodes, idx, H)       # ⑤ 最终周期 / 实例数 / 支配端口
    BUILD_VERTEX(dag, nodes, T, N)                            # ⑥ 顶点 {id}:{k}
    BUILD_EDGE(nodes, N, domi)                                # ⑦ 绑定边 + 阻塞性
    BIND_SYNC(nodes, N)                                       # ⑦.5 tp 顶点 + 挂靠边
    BUILD_PRED()                                              # ⑨a 入度基准
    BIND_JOB(nodes, by_id, N)                                 # ⑧ job 闭包（pack/execute/route）
    SEED_READY()                                              # ⑨b 兜底就绪
    graph_version++                                           # 须在全部建图步骤之后

# ══════════ ② 标称超周期：整数毫秒 lcm ══════════
BUILD_HYPER_PERIOD(nodes):
    hp ← 1; any ← false
    for info in nodes where info.period > 0:                  # 无周期节点（纯事件）不参与
        any ← true
        Ti ← round(info.period)                               # 就近取整毫秒
        if |info.period − Ti| > 1e-6:
            WARN("period 非整数毫秒 → 就近取整")               # ⚠ 实际周期被改了，只警告
        hp ← lcm(hp, Ti)
    return any ? hp : 0                                       # 全程无周期 → 0（不参与回绕）

# ══════════ ⑤ 支配周期 + 实例数（三趟）══════════
BUILD_DOMINANCE(topo, nodes, idx, H):
    # 第一趟：按拓扑序定最终周期（保证前级已定）
    for id in topo:
        info ← nodes[id]
        if info.period > 0:
            T[id] ← info.period                               # 时间触发：显式周期
        elif info.event ≠ ∅:
            best ← +∞
            for (port, thin) in info.event:                    # 逐个 event 端口
                p ← idx.producers[port].only()                 # 单写者约束 → 唯一 producer
                if p ∉ T: continue                             # 前级尚未定出周期 → 跳过
                cand ← thin × T[p]                             # 虚拟周期 = 抽稀倍数 × 前级周期
                if cand < best: best, domi[id] ← cand, port
            if best = +∞: THROW("event 端口无可用周期的前级")
            T[id] ← best                                       # 支配端口 = 虚拟周期最小者
        else:
            THROW("既无 period 也无 event")                     # check_topology ⑦ 应已拒
    # 第二趟：拓宽 H 到能整除全部最终周期（乘性 → 单趟收敛）
    nominal ← H
    for id, Ti in T:
        H ← WIDEN_TO_MULTIPLE(H, Ti)
    if H > nominal × 100: THROW("拓宽超限")                    # HP_WIDEN_MAX_FACTOR
    if H > nominal: WARN("超周期由标称 {} 拓宽至 {}")
    # 第三趟：实例数
    for id, Ti in T:
        N[id] ← round(H / Ti)
    return (T, N, domi)

WIDEN_TO_MULTIPLE(hp, T):
    for m in 1..1024:                                          # 试到上限仍不可通约 → 原样返回
        if hp·m ≥ T and (hp·m / T) is near-integer:
            return hp·m                                         # 最小倍数；不可通则返回 hp
    return hp                                                   # 交由调用方的 100× 上限拒

# ══════════ ⑥ 建顶点 ══════════
BUILD_VERTEX(dag, nodes, T, N):
    for info in nodes:
        for k in 0..N[info.id]−1:
            dag.add_node(info.id + ":" + k,
                         Workload{ k, name, period: T[info.id] })   # wcet 不在此赋值；截止期不属框架

# ══════════ ⑦ 绑定边 ══════════
BUILD_EDGE(nodes, N, domi):
    producer_of ← first-wins map: 输出端口 → 节点 id              # 单写者保证唯一
    for info in nodes:
        Nc ← N[info.id]
        for port in info.input_ports:
            if not HAS_EDGE(info, port): continue               # period>0 → 恒无边；
                                                                # 事件节点非 event 端口 → 无边
            p ← producer_of[port]; if absent: continue          # 孤立输入 → 无边
            Np ← N[p]
            blocking ← (port == domi[info.id])                  # 支配端口阻塞；其余 event 端口非阻塞
            for k in 0..Nc−1:
                pk ← ((k+1)·Np − 1) / Nc                        # 整数式：时段内最新已完成帧
                ADD_EDGE(p + ":" + pk → info.id + ":" + k, tag=port, blocking)

ADD_EDGE(from, to, tag, blocking):                              # 唯一建边入口（含记账）
    dag.add_edge(from, to, tag, Message{})                      # 有边必有顶点，否则 THROW
    if blocking: blocking_succ_[from].append(to)                # 完成时递减用
    else:        nonblocking_in_[to].insert(tag)                # build_pred 扣减 / export 标 false

# ══════════ ⑦.5 时间链 ══════════
BIND_SYNC(nodes, N):
    sync ← ∅
    for info in nodes where info.period > 0:
        for k in 0..N[info.id]−1:
            sync.add(k × info.period)                           # 释放时刻并集（按绝对时刻聚合）
    tp_id ← { sorted(sync)[s] : "tp:" + s }                     # 序号化：避免浮点偏移做 key
    tp_order_ ← offsets sorted ascending                        # 释放顺序（grab 按序取）
    prev ← 0
    for (off, id) in sorted(tp_id):
        v ← Workload{ wcet: off − prev }                        # wcet 复用为"相对前一同步点的间隔"
        v.job ← λ: while not stopped and now < hyper_start_ms + off:
                       sleep_until(min(target, now + 10ms))     # 绝对时刻；10ms 仅为停止响应
        dag.add_node(id, v); prev ← off                         # 实时读 hyper_start_ms → rollover 重锚
    for info in nodes where info.period > 0:
        for k in 0..N[info.id]−1:
            ADD_EDGE("tp:" + index(k × info.period) → info.id + ":" + k, tag="time", blocking=true)

# ══════════ ⑨a / ⑨b 就绪基准 ══════════
BUILD_PRED():
    for id in dag.vertices():                                   # 两趟：先收集 id 再遍历
        in_degree_[id] = pred_left_[id] = in_edges(id) − |nonblocking_in_[id]|
        #                                          ↑ 非阻塞边不参与等齐

SEED_READY():
    for id in dag.vertices():
        if not id.startswith("tp:") and pred_left_[id] == 0:
            ready_.push({id, ...})                              # 有效配置下为空：源节点必有 tp 门

# ══════════ 运行时另一半：回绕（不重建图）══════════
ROLLOVER():                                                     # 主循环在图静止时调用
    done_.clear(); ready_.clear(); tp_released_ ← 0
    for id: pred_left_[id] ← in_degree_[id]                     # 不 clear dag，顶点对象存活
    k ← max(1, ceil((now_ms() − hyper_start_ms) / H))            # 推进到绝对网格下一未来边界
    if k ≥ 2: WARN("超周期过载：跳过 k−1 个释放拍")
    hyper_start_ms ← hyper_start_ms + k × H                      # ★ 不拨回完工时刻 → 不累积漂移
```

## 3. 九步流水线

```
① build_port_index → ② build_hyper_period → ③ build_topo_order → ④ build_instances
  → ⑤ build_dominance → ⑥ build_vertex → ⑦ build_edge → ⑦.5 bind_sync
  → ⑨a build_pred → ⑧ bind_job → ⑨b seed_ready
```

步骤号沿用代码注释；⑨a/⑧/⑨b 的**调用顺序**与编号不同（`g_state.hpp:1348-1355`）：
入度基准 `build_pred` 必须在 `bind_job` **之前**（`bind_job` 只填闭包、不动入度），
而初始就绪 `seed_ready` 必须在 `bind_job` 之后。

### ① 端口索引（`build_port_index`, :665）

建立 `producers`（输出端口名 → producer 节点）、`consumers`、以及一跳邻居 `in_producers` /
`out_consumers`。**预建全部节点条目**（含空集），后续步骤可直接 `.at()` 不去查存在性。

### ② 超周期（`build_hyper_period`, :702）

**当前实现 = 整数毫秒 lcm**：

```cpp
for (info : nodes) {
  if (T <= 0) continue;                       // 无周期节点不参与（event 节点的虚拟周期由 ⑤ 定）
  const long long Ti = std::llround(T);       // 就近取整毫秒
  if (|T - Ti| > 1e-6) FINS_LOG_WARN(...);    // 非整数毫秒：WARN 但继续
  hp = std::lcm(hp, Ti);                      // 整数 lcm（gcd 恒整数，无浮点病态）
}
return any_periodic ? hp : 0.0;               // 无周期节点 → 0（不参与回绕）
```

> ⚠ **本函数的 docstring 已过时**：它描述的是一套浮点方案（从 `Tmax` 起步找能整除全部周期的
> `k·Tmax`，非整数毫秒不取整）。代码已改为上面的整数 `lcm`。以代码为准。

### ③ 拓扑序（`build_topo_order`, :726）

Kahn BFS：入度为 0 者入队 → 出队即入 topo → 递减后继入度。**未被覆盖的节点补到末尾**
（环：`feedback` 拓扑靠 hist 字段窗口构成非真环，但防御性补入）。

### ④ 实例化（`build_instances`, :762）

每节点从 so 表构造**一个**算法实例（`by_id`），`configure` 按 `config_cache` 顺序位置式注入。
实例由全部 job 实例共享——串行性由释放时间点（时间触发）或数据前序（事件触发）保证，
**没有同节点 job 之间的 seq 边**（⑦ 已删）。

### ⑤ 支配周期 + 实例数（`build_dominance`, :843）

三趟：

**第一趟：定最终周期 `period_final`**（按拓扑序，保证前级已定）

- 显式 `period > 0` → 直接用
- `event` 非空 → **虚拟周期 = min over event 端口 (抽稀倍数 N × 该端口 producer 的最终周期)**，
  取最小者为**支配端口**（写入 `domi_port`，⑦ 据此定阻塞边）。并列时按端口名字典序（确定性）
- 都无 → 抛（`check_topology ⑦` 应已拒，此处防御）

**第二趟：拓宽超周期**

标称 HP（②）只由**显式周期**决定，而 event 的抽稀倍数会引入落在其整除格之外的周期
（如 N=4、支配源 50ms → 200ms，而标称 HP=100ms）。此时**不拒绝**，而是把 HP 延到能整除
全部最终周期的最小倍数（`widen_to_multiple`）。乘性拓宽单调 → **单趟收敛**。
拓宽超过标称 100 倍（`HP_WIDEN_MAX_FACTOR`）→ 抛（防静态展开爆内存）。

**第三趟：实例数** `node_count[id] = round(HP / T)`

### ⑥ 建顶点（`build_vertex`, :926）

每节点展开 `node_count` 个顶点 `{id}:{k}`，载荷 `k` / `name` / `period_final`（`wcet` 不在建图期
排期，由 `update_wcet_estimation` 自整定）。**截止期不属框架**——框架没有足够信息维护它
（取决于调度算法怎么排），需要时由调度算法自行注入/另读一份配置，见 `docs/pipeline_json_schema.md` §5。

### ⑦ 建边（`build_edge`, :978）

建边范围 = `port_has_edge(pn)`（`= period<=0 && event.count(pn) > 0`）——**时间触发节点恒无边**；
事件触发节点**只在 event 端口建边**。未建边的输入由消费者执行时从 producer 的输出字段历史槽
取样读（hist 窗口 / 最新一帧）。

**producer 绑定式**（单写者约束 → 输出端口唯一 producer）：

```
pk = ((k+1)·Np − 1) / Nc        // 整数式，Np/Nc = producer/consumer 实例数
producer:{pk} → consumer:{k}
```

语义：consumer 的第 k 拍取"该时段内 producer 最新已完成帧"。同速率一一对应；快→慢绑末帧；
慢→快共享帧；恒有边。

**阻塞性**（2026-09-19 加入）：

- event 节点的**支配端口** → **阻塞边**（唯一释放条件，计入 `pred_left`）
- 同节点其余 event 端口 → **非阻塞边**（帧可见但不参与等齐）

唯一建边入口 `add_edge_`（:969）同时维护记账：阻塞边入 `blocking_succ_[from]`（完成时递减用），
非阻塞边入 `nonblocking_in_[to]`（`build_pred` 从入度里扣、`export_dag` 标 `blocking=false`）。

> ⚠ **DAG 本身不认识"阻塞"**：`form.hpp` 是通用容器，阻塞语义只存在于 `PrecedenceGraph`
> 的这两个记账表里（用户明确要求通用容器不掺调度语义）。

### ⑦.5 时间链（`bind_sync`, :1020）

**同步点集合** = 全部显式周期节点的释放时刻并集：

```cpp
sync_points.insert(k * info.period);      // k = 0..N-1
```

按偏移升序去重 → 每个偏移一个顶点 `tp:{seq}`（序号化，避免浮点偏移做 key 的精度碰撞），
升序写入 `tp_order_`（`grab_delay_workload` 按序取）。

- `Workload.wcet` 复用为**相对前一个同步点的间隔**（理论延迟基准）
- `tp.job` = 睡到绝对时刻 `hyper_start_ms + off`，**job 内实时读 `hyper_start_ms`**
  → `rollover_hp` 重锚后自动对齐新起点
- 挂靠边 `tp:s → {id}:{k}`，tag = `"time"`（阻塞边，释放约束）

`tp.job` 的睡眠实现有讲究（:1050）：

```cpp
while (!stopped.load()) {
  target = now + (hyper_start_ms + off - now_ms()) * 1000µs;
  if (target <= now) break;                      // 已过 → 立即返回
  sleep_until(min(target, now + 10ms));          // ★ 绝对时刻，仅以 10ms 为上限分批
}
```

**用绝对时刻睡眠**（内核 hrtimer 精确唤醒），10ms 上限只为保留停止响应（`stopped` 置位后
最多 10ms 退出）。注释里记着旧实现的问题：原 `while(now<until) sleep_for(1ms)` 每拍醒约
100 次，**且醒来必晚 0~1ms（实测释放偏差中位 505µs / 最大 1023µs）**——释放时刻被量化掉了。

### ⑨a 入度基准（`build_pred`, :1083）

```cpp
阻塞入度 = 全部入边数 − 非阻塞入边数
in_degree_[id] = pred_left_[id] = 阻塞入度;
```

两趟实现（先收集 id 再遍历），规避 `for_each_vertex` 遍历期间嵌套 `in_nodes` 的 accessor。

### ⑧ job 闭包（`bind_job`, :1110）

**段 1**：登记 `mesg_hist_cap[输出端口]` = 该字段全部历史槽读者的保留长度 max（hist N 或 1）。
只对**被周期读**的字段建历史槽（无绑定边的输入）。

**段 2**：每实例填 `Workload.job` 闭包，捕获稳定解析态（`shared_ptr<const NodeInfo>` 按 k 共享）

+ 算法实例 + hist 窗口表。闭包内四件事：`pack_inputs` → `execute_and_time`（含 `record_exec`）
  → `route_outputs`（写全部下游绑定边 + `record_mesg`）。

### ⑨b 初始就绪（`seed_ready`, :1268）

`pred_left == 0` 且**不是 tp 门顶点**（`id` 不以 `tp:` 开头）者入就绪集。
有效配置下源节点必显式 period → 必有 tp 门 → `pred_left ≥ 1` → **初始就绪为空**，
图启动由 timer 释放 `tp:0`（offset 0 ≈ `hyper_start_ms`，立即触发）。此趟为无 tp 门顶点兜底。

## 4. 顶点数

```
顶点数 = Σ_id round(HP / T_id)  +  |sync_points|
sync_points = ∪_{period>0} {k·period : k = 0..N-1}
```

时间点按**绝对释放时刻聚合**：两个 50ms 任务与一个 100ms 任务在同时刻释放时**共用一个 tp**。
故 `|sync_points| = HP / gcd(全部显式周期)`，极端情形（周期互质）下会接近 HP——这是实例数
之外的第二处膨胀源。

## 5. 不变量

| 不变量                     | 由谁保证                                                 |
| ----------------------- | ---------------------------------------------------- |
| 每顶点每超周期恰执行一次            | `node_count = round(HP/T)` + 时间点覆盖全部释放拍              |
| 每条 `(to, tag)` 至多一条入边   | 单写者约束（`check_topology`）                              |
| 有边必有顶点                  | `form.hpp::add_edge` 建图自洽校验（缺失抛 `out_of_range`，精确到边） |
| 非阻塞边不计入释放等待             | `build_pred` 扣减 + `blocking_succ_` 只记阻塞边             |
| 环不存在（除 hist 字段窗口构成的非真环） | `check_topology ⑤` 环检测                               |

## 6. 运行时另一半：回绕（`rollover_hp`）

静态展开出的图跑完一个超周期后不重建，而是**回绕**：起点推进到下一拍 →
清调度增量状态（`done_`/`ready_` 清空、`tp_released_` 游标归零、`pred_left_` 重置回 `in_degree_`
基准），**不 clear dag**——顶点对象存活。

★ **严格网格对齐**：释放网格 = `{hp_grid_origin_ms + j·HP}`，原点在 `expand_hp` 锚定一次
（= 当时真实时钟），此后**永不改**。回绕把起点推进到网格上**严格 ≥ now 的最小拍**，由"原点 + 整拍号"
直接算出（**不在 `hyper_start_ms` 上累加**）→ 相位不漂移、浮点误差不随拍数累积：

```
j = max(1, ceil((now − 原点) / H − 1e-9))     // 1e-9 拍容差：恰在边界上完工不误判为越界
hyper_start_ms = 原点 + j * H
```

- **早完工**（`now` 落在本拍内）→ 下一拍 = 本拍边界，tp 睡到边界才放 → 周期任务**不提前释放**
- **过载**（排空晚于边界）→ 错过的整拍被跳过、对齐下一未来边界，**不累积漂移**
  （WARN：跳过 N 个释放拍）

非周期（事件）任务不受影响：绑定前序完成即就绪即跑。

## 7. 边界与失败模式

| 情形                   | 行为                                                                                    |
| -------------------- | ------------------------------------------------------------------------------------- |
| 空配置                  | 空图，`graph_version++`，不抛                                                               |
| `period` 非整数毫秒       | **WARN + 就近取整**（注意：实际周期被改了，与配置不再一致）                                                   |
| event 端口前级定不出周期      | 抛 `invalid_argument`（前级须是 timer，或可递归定周期的 event）                                       |
| 节点既无 period 又无 event | 抛（`check_topology ⑦` 应已拒，此处防御）                                                        |
| HP 拓宽 > 100× 标称      | 抛（抽稀倍数与标称 HP 不可通约，防静态展开爆内存）                                                           |
| 建边引用不存在的顶点           | `form.hpp` 抛 `out_of_range`，消息含 `from -> to`                                          |
| `[name:version]` 未注册 | `build_instances` 抛 `runtime_error`                                                   |
| 顶点数爆炸                | `node_count = HP/T`：小周期 + 大 HP 会放大。`HP_WIDEN_MAX_FACTOR` 只挡拓宽，**不挡** HP 本身由小周期 lcm 变大 |

## 8. 与配置校验的分工

`expand_hp` 假设配置**已通过** `Pipeline::check_topology()`。校验与展开的分工：

| 校验项                                  | 位置                               |
| ------------------------------------ | -------------------------------- |
| 触发模式二选一（period ⟺ event）              | `check_topology ⑦`               |
| hist 端口 ∈ inputs / N>2 / 不与 event 重叠 | `check_topology ④`               |
| event 端口 ∈ inputs / N≥1              | `check_topology ⑥`               |
| 环检测                                  | `check_topology ⑤`               |
| `configs` 键的节点序号                     | `check_topology ⑧`               |
| 有边必有顶点                               | `form.hpp::add_edge`             |
| 前级可定周期 / HP 拓宽上限                     | **`build_dominance`（展开期，唯一在此处）** |

后两行是"只能在展开期发现"的：② 的 HP 与 ⑤ 的最终周期是两个阶段，前者算完才知道后者
是否需要拓宽。
