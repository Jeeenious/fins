# Pipeline JSON Schema：可解析字段全表

> 状态：已落地。对应 `core/g_state.hpp` 的 `Pipeline::parse_pipeline`（第一级：逐节点格式校验）
> 与 `Pipeline::check_topology`（第二级：跨节点图结构校验）。本文是**唯一字段清单**，
> 生成器 `tool/_load2json.py`、可视化 `tool/viewer.html` 均按此产出/读取。

## 1. 顶层形态

`parse_pipeline` 接受三种（`core/g_state.hpp:333-352`）：

| 形态 | 说明 |
|---|---|
| `[ {节点}, {节点}, ... ]` | **标准形式**（生成器产出这种） |
| `{ "nodes": [ {节点}, ... ] }` | 带包装的等价形式 |
| `{节点}` | 单节点对象（须含 `name`） |
| `null` | 空配置 → 空图（幂等） |

**未列出的键一律静默忽略**——解析器按名取键，不校验"未知字段"（拼错字段名不会报错，
只会当没写）。新增字段前先查本文档。

## 2. 节点字段

字段顺序约定（生成器按此写出，**解析不依赖顺序**）：

```
id / name / version          标识
configs / inputs / outputs   配置 + 端口（三者连着）
hist                         窗口读声明
event | period               触发模式（二选一）
cap                          预留属性（可有可无，置最末）
```

| 字段 | 类型 | 必填 | 默认 | 语义 |
|---|---|---|---|---|
| `id` | string | ✅ | — | 节点唯一标识；顶点名 = `{id}:{k}`。生成器用 `n{i}`（i = 节点下标，`configs` 的键依赖它） |
| `name` | string | ✅ | — | 算法名；与 `version` 组成定位键 `name:version`，在插件库 `library_g.so_ctx` 里查 |
| `version` | string | ✅ | — | 算法版本（定位键的另一半） |
| `configs` | array | — | `[]` | 位置式配置值表：`[{c{i}_0: 值}, {c{i}_1: 值}, ...]`。注入顺序 = 数组顺序 = 算法配置段序号 |
| `inputs` | string[] | — | `[]` | 输入端口名（= 上游输出端口名，同名直连）；顺序 = 算法输入参数顺序 |
| `outputs` | string[] | — | `[]` | 输出端口名（生产者自己命名，全局唯一）；顺序 = 算法输出参数顺序 |
| `hist` | array | — | `[]` | 窗口读声明：`[{端口名: 窗口长度 N}, ...]`，N > 2 |
| `event` | array | — | `[]` | 事件触发声明：`[{端口名: 抽稀倍数 N}, ...]`，N ≥ 1（声明即事件触发） |
| `period` | number | — | `0` | 执行周期（ms，> 0）（声明即时间触发） |
| `cap` | number | — | — | **预留字段**（见 §7）：execute 耗时样本保留条数，正整数 |

> `wcet` / `deadline` **不在本 schema**：`wcet` 由框架内维护（`wcet_updater` 按执行历史自整定，
> 缺省 1ms）；**截止期不由框架维护**（框架没有足够信息——它取决于调度的人怎么排，见 §5）。
> 写进 JSON 会被静默忽略（解析器按名取键，不认识的键不报错），生成器也不再写出。

### 2.1 `configs` 的键：`c{i}_{j}`

与数据端口 `p{i}_{j}`（生产者下标\_输出序号）**同一编号体系**：

```json
"configs": [ {"c2_0": "n2"}, {"c2_1": 3619} ]
```

- `i` = **本节点下标**（在 `nodes` 数组中的位置），`j` = 配置项序号
- 只取**值**（顺序即语义，名字仅用于自校验），键写错直接拒配置（§6 ⑧）
- 例：`usr_relay(const std::string& name, int cfg, ...)` → `c{i}_0` = 节点 id、`c{i}_1` = 忙等时长 µs

## 3. 触发模式：`period` / `event` 二选一

**没有独立的 `type` 字段**——触发模式的判据就是 `period` / `event` 本身（`check_topology` ⑦）：

| 模式 | 判据 | 释放方式 | 输入读法 |
|---|---|---|---|
| 时间触发 | `period` > 0 | tp 时间点顶点（超周期内 `k·period` 各一个） | `hist` 端口窗口读；其余读历史槽**最新一帧** |
| 事件触发 | `event` 非空 | 由 `event` 端口的 producer 完成事件释放 | `event` 端口读绑定边帧；`hist` 端口窗口读；其余读**最新一帧** |

**二选一且必须恰好其一，不支持隐式事件节点**：
- 两者都写 → 拒（`period` 会静默压过 `event`：`port_has_edge` 对 `period>0` 恒返回 false）
- 都不写 → 拒（既无 tp 释放点也无绑定边，节点会静默不跑）

### 3.1 event 的虚拟周期与支配端口

- 虚拟周期 `T = min over event 端口 (抽稀倍数 N × 该端口 producer 的周期)`
- 取到最小值的端口 = **支配端口**，其绑定边是**阻塞边**（唯一释放条件）；同节点其余
  `event` 端口建**非阻塞边**（帧照样送到下游共享槽，但不参与"等齐"）
- 抽稀倍数 `N=2` 表示：支配节点每产出 2 帧，本节点触发 1 次
- 超周期 `HP = lcm(全部最终周期)`（`calculate_hp`）——取公倍数，**没有拓宽/拒绝路径**；
  整除判定用 `HP_DIV_EPS`（1e-5），不整除只 WARN（`count_instance` 点名），`HP > HP_MAX_WARN_MS`
  （1000ms）同样只 WARN 不拒绝

## 4. 端口与连线

- **同名直连**：消费者输入端口名 = 生产者输出端口名 → 自动建立绑定边，无需显式 edges 列表
- **单写者约束**：同名输出端口至多一个生产者（多写者拒，否则绑定边读哪条不确定）
- **孤立输入**：每个输入端口必须有生产者（含 `hist` 指向的字段；悬空窗口拒）
- 顶点实例绑定式：`pk = ((k+1)·Np − 1) / Nc`（快→慢绑末帧、慢→快共享帧）
- `hist` 端口是**窗口读**（无绑定边），不能同时声明 `event`（同一端口语义互斥）

## 5. 截止期：不由框架维护

**框架不持有、不算、也不校验截止期**（`Workload` 只有 `k`/`name`/`period`/`wcet`/`job`）：
截止期是**调度决策**的一部分，信息不在图里——同一张图换个调度算法/换份目标就完全不同，
框架替用户臆断一个（如"相对截止期 = 周期"）只会把错误假设固化进顶点属性。

需要截止期的策略/算法自己决定来源，两种常见做法：

- **算法侧注入**：把截止期当配置值传进算法（`configs` 的 `c{i}_j` 槽位），或由算法自己维护
  一张 `{顶点 id → 截止期}` 表；调度侧要读时从算法/外部表取。
- **另一份配置 JSON 读入**：单独一份 `deadline` 脚本产出（按节点 id / 实例 k 索引），
  由装配点或调度算法在 `expand_hp` 后读入并自行保存、自行刷新——**不落进 `Workload`**。

框架侧现存的与时间有关的量只有：`period`（框架算的最终执行周期）、`hp_start_ms`/`hp_origin_ms`
（释放网格，严格整拍推进）、`wcet`（自整定）。原先的 `prio_dm`/`prio_density`/`prio_edf`/`prio_llf`
（以及 `Workload::ddl` + `update_abs_deadline`）已随之删除，`Policy` = FIFO/RM/SJF/LJF/DEPTH/HEIGHT。

## 6. 校验规则清单

**第一级（`NodeInfo` 构造器，逐节点格式）**

| # | 规则 |
|---|---|
| 1 | `id` / `name` / `version` 必填 string |
| 2 | `configs` 每项须为单键对象，键 = `c{i}_{j}`（后缀必须等于下标） |
| 3 | `inputs` / `outputs` 为 string 数组 |
| 4 | `period` / `cap` 为 number；`cap` 为正整数 |
| 5 | `hist` / `event` 为 `[{端口名: 正整数}, ...]`，同端口不得重复声明 |
| 6 | `parameters` → **拒绝**并提示更名为 `configs` |

**第二级（`check_topology`，跨节点）**

| # | 规则 |
|---|---|
| ① | 单写者：同名输出端口至多一个生产者 |
| ② | 源节点（无输入）须时间触发：`period > 0` |
| ③ | 孤立输入：每个输入端口须有生产者 |
| ④ | `hist`：键 ∈ `inputs`、N > 2、节点须声明 `period` 或 `event`、不得与 `event` 键重叠 |
| ⑤ | 事件前序边无环（`event` 端口构成的 producer→consumer 子图） |
| ⑥ | `event`：键 ∈ `inputs`、N ≥ 1 |
| ⑦ | 触发模式二选一：`period>0` ⟺ 时间触发、`event` 非空 ⟺ 事件触发，不得都有/都无 |
| ⑧ | `configs[j]` 键须为 `c{本节点下标}_{j}` |

## 7. 预留字段：`cap`

- **已解析、已校验**（正整数）→ `NodeInfo::exec_cap`，但**当前未接线**（除了本字段，
  全链路无其他写入者）
- 语义：该节点算法保留多少次 `execute` 耗时样本 —— 对应图侧 `exec_hist_cap`，
  它喂 `record_exec`（job 执行结束时记录）的环形队列上限，最终供 `wcet_updater`
  自整定 WCET（`docs/wcet_estimation.md`）
- **注意键不同**：`exec_hist_cap` 的键是**算法名**（同算法的多个节点共享一份耗时历史），
  而 `cap` 写在节点上 → 接线时需定"同算法多节点各自声明 `cap` 时如何合并"（取 max？）
- 未接线时的实际行为：容量固定 `100`（`record_exec` 内的缺省）
- 接线方式（两处）：`expand_hp` 开头 `exec_hist_cap.clear()`，建图后按算法名填入

### 7.1 已移除 / 已拒绝的字段

| 字段 | 状态 |
|---|---|
| `parameters` | 已更名 `configs` 且改形为 `[{c{i}_j: 值}]`；出现即拒并提示 |
| `hist` 的旧 map 形态 `{"p0_0": 5}` | 只接受数组形态 `[{"p0_0": 5}]`，map 形态报"须为数组" |
| `type` | 曾短暂引入（timer/event 字符串），已删——触发模式由 `period`/`event` 字段本身判定 |
| `wcet` / `deadline` | 不进 JSON：`wcet` 由框架内维护（`wcet_updater` 自整定）；**截止期不由框架维护**（调度算法自己注入/另读一份配置，见 §5）。写了也静默忽略，生成器已不写出 |
| `trig` / `T_ms` | 从不属于本 schema；运行时由 `build_dominance` 现算（`_load2json.inspect_file` 曾有坏引用，已修） |

## 8. 完整示例

```json
[
  {"id": "n0", "name": "usr_src", "version": "1.0.0",
   "configs": [{"c0_0": "n0"}, {"c0_1": 1005}, {"c0_2": 1024}],
   "outputs": ["p0_0"], "period": 100.0},

  {"id": "n1", "name": "usr_acc", "version": "1.0.0",
   "configs": [{"c1_0": "n1"}, {"c1_1": 3979}, {"c1_2": 1024}],
   "inputs": ["p0_0", "p3_0"], "outputs": ["p1_0"],
   "hist": [{"p3_0": 3}], "event": [{"p0_0": 1}]}
]
```

`n0`：时间触发，周期 100ms，超周期 = 100ms。
`n1`：事件触发，支配端口 `p0_0`（抽稀 1 → 虚拟周期 = n0 的 100ms），`p3_0` 读最近 3 帧窗口。

## 9. 相关

- 解析/校验实现：`core/g_state.hpp`（`NodeInfo` 构造器、`check_topology`）
- 生成器：`tool/_load2json.py`（`_make_pipeline` 产出、`inspect_file` 打印、`generate_all` 批量）
- 可视化：`tool/viewer.html`（`inputKind` / `triggerDesc` 按 `period`/`event` 分派）
- 调度语义：`docs/precedence_graph_design.md`；WCET 自整定：`docs/wcet_estimation.md`
