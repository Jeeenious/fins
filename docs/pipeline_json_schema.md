# Pipeline JSON Schema：可解析字段全表

> 状态：已落地。对应 `core/g_state.hpp` 的 `Pipeline::parse_pipeline`（第一级：逐节点格式校验）
> 与 `Pipeline::check_topology`（第二级：跨节点图结构校验）。本文是**唯一字段清单**，
> 生成器 `tool/_load.py`、可视化 `tool/viewer.html` 均按此产出/读取。

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
`id / name / version / type / configs / wcet / inputs / outputs / hist / event|period`，
触发细节（`event` 或 `period`）一律置末。

| 字段 | 类型 | 必填 | 默认 | 语义 |
|---|---|---|---|---|
| `id` | string | ✅ | — | 节点唯一标识；顶点名 = `{id}:{k}`。生成器用 `n{i}`（i = 节点下标，`configs` 的键依赖它） |
| `name` | string | ✅ | — | 算法名；与 `version` 组成定位键 `name:version`，在插件库 `library_g.so_ctx` 里查 |
| `version` | string | ✅ | — | 算法版本（定位键的另一半） |
| `type` | string | ✅ | — | 触发模式：`"timer"`（时间触发）/ `"event"`（事件触发）。**唯一触发模式声明** |
| `configs` | array | — | `[]` | 位置式配置值表：`[{c{i}_0: 值}, {c{i}_1: 值}, ...]`。注入顺序 = 数组顺序 = 算法配置段序号 |
| `wcet` | number | — | `1` | 最坏执行时间（ms）；调度/优先级用 |
| `deadline` | number | — | `0` | 相对截止期（ms）；**0 = 未声明**，排序中视为最紧急（见 §5） |
| `inputs` | string[] | — | `[]` | 输入端口名（= 上游输出端口名，同名直连）；顺序 = 算法输入参数顺序 |
| `outputs` | string[] | — | `[]` | 输出端口名（生产者自己命名，全局唯一）；顺序 = 算法输出参数顺序 |
| `hist` | array | — | `[]` | 窗口读声明：`[{端口名: 窗口长度 N}, ...]`，N > 2 |
| `event` | array | — | `[]` | 事件触发声明：`[{端口名: 抽稀倍数 N}, ...]`，N ≥ 1（仅 `type="event"`） |
| `period` | number | — | `0` | 执行周期（ms，> 0）（仅 `type="timer"`） |
| `cap` | number | — | — | **预留字段**（见 §7）：execute 耗时样本保留条数，正整数 |

### 2.1 `configs` 的键：`c{i}_{j}`

与数据端口 `p{i}_{j}`（生产者下标\_输出序号）**同一编号体系**：

```json
"configs": [ {"c2_0": "n2"}, {"c2_1": 3619} ]
```

- `i` = **本节点下标**（在 `nodes` 数组中的位置），`j` = 配置项序号
- 只取**值**（顺序即语义，名字仅用于自校验），键写错直接拒配置（§6 ⑧）
- 例：`usr_relay(const std::string& name, int cfg, ...)` → `c{i}_0` = 节点 id、`c{i}_1` = 忙等时长 µs

## 3. 触发模式：`type` 与 `period`/`event` 必须一致

`type` 是唯一声明，承载周期的字段必须与之匹配（`check_topology` ⑦）：

| `type` | 必须 | 必须没有 | 释放方式 | 输入读法 |
|---|---|---|---|---|
| `"timer"` | `period` > 0 | `event` | tp 时间点顶点（超周期内 `k·period` 各一个） | `hist` 端口窗口读；其余读历史槽**最新一帧** |
| `"event"` | `event` 非空 | `period` | 由 `event` 端口的 producer 完成事件释放 | `event` 端口读绑定边帧；`hist` 端口窗口读；其余读**最新一帧** |

**不支持隐式事件节点**：既不写 `period` 也不写 `event` 的节点一律拒（`type` 与实际字段
不一致时，运行时按哪种模式走全凭猜，例如 `type="timer"` 却没 `period` 会既无 tp 释放点
也无绑定边 → 节点静默不跑）。

### 3.1 event 的虚拟周期与支配端口

- 虚拟周期 `T = min over event 端口 (抽稀倍数 N × 该端口 producer 的周期)`
- 取到最小值的端口 = **支配端口**，其绑定边是**阻塞边**（唯一释放条件）；同节点其余
  `event` 端口建**非阻塞边**（帧照样送到下游共享槽，但不参与"等齐"）
- 抽稀倍数 `N=2` 表示：支配节点每产出 2 帧，本节点触发 1 次
- `T` 不整除超周期 HP 时**不拒绝**，而是把 HP 拓宽到能整除全部周期的倍数（倍数 > 100 才拒）

## 4. 端口与连线

- **同名直连**：消费者输入端口名 = 生产者输出端口名 → 自动建立绑定边，无需显式 edges 列表
- **单写者约束**：同名输出端口至多一个生产者（多写者拒，否则绑定边读哪条不确定）
- **孤立输入**：每个输入端口必须有生产者（含 `hist` 指向的字段；悬空窗口拒）
- 顶点实例绑定式：`pk = ((k+1)·Np − 1) / Nc`（快→慢绑末帧、慢→快共享帧）
- `hist` 端口是**窗口读**（无绑定边），不能同时声明 `event`（同一端口语义互斥）

## 5. `deadline` 与排序

- 缺省 `0` = **未声明**（框架不替用户预设）
- `update_abs_deadline` 滚动计算 `ddl = 滚动起点 + (k+1)·deadline` → `deadline=0` 时 ddl 最小
- EDF（`prio_edf`）按 `ddl − now` 升序 → **0 即"最紧急"**
- ⚠ 不写 `deadline` 的节点 ddl 全等于滚动起点 → 彼此同权（EDF 退化为就绪序）。要靠 EDF 区分
  先后就得在 JSON 里显式写 `deadline`
- ⚠ `DENSITY` 策略用 `wcet / max(1e-9, deadline)`，`deadline=0` 时分母退到 `1e-9` → 优先级爆表

## 6. 校验规则清单

**第一级（`NodeInfo` 构造器，逐节点格式）**

| # | 规则 |
|---|---|
| 1 | `id` / `name` / `version` / `type` 必填 string；`type ∈ {timer, event}` |
| 2 | `configs` 每项须为单键对象，键 = `c{i}_{j}`（后缀必须等于下标） |
| 3 | `inputs` / `outputs` 为 string 数组 |
| 4 | `wcet` / `deadline` / `period` / `cap` 为 number；`cap` 为正整数 |
| 5 | `hist` / `event` 为 `[{端口名: 正整数}, ...]`，同端口不得重复声明 |
| 6 | `parameters` → **拒绝**并提示更名为 `configs` |

**第二级（`check_topology`，跨节点）**

| # | 规则 |
|---|---|
| ① | 单写者：同名输出端口至多一个生产者 |
| ② | 源节点（无输入）须 `type="timer"` 且 `period > 0` |
| ③ | 孤立输入：每个输入端口须有生产者 |
| ④ | `hist`：键 ∈ `inputs`、N > 2、节点须为 timer 或 event、不得与 `event` 键重叠 |
| ⑤ | 事件前序边无环（`event` 端口构成的 producer→consumer 子图） |
| ⑥ | `event`：键 ∈ `inputs`、N ≥ 1 |
| ⑦ | `type` 与触发字段一致（timer ⟺ `period>0`；event ⟺ `event` 非空；不得都有/都无） |
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
| `trig` / `T_ms` | 从不属于本 schema；运行时由 `build_dominance` 现算（`_load.inspect_file` 曾有坏引用，已修） |

## 8. 完整示例

```json
[
  {"id": "n0", "name": "usr_src", "version": "1.0.0", "type": "timer",
   "configs": [{"c0_0": "n0"}, {"c0_1": 1005}], "wcet": 1.005,
   "outputs": ["p0_0"], "period": 100.0},

  {"id": "n1", "name": "usr_acc", "version": "1.0.0", "type": "event",
   "configs": [{"c1_0": "n1"}, {"c1_1": 3979}], "wcet": 3.979,
   "inputs": ["p0_0", "p3_0"], "outputs": ["p1_0"],
   "hist": [{"p3_0": 3}], "event": [{"p0_0": 1}]}
]
```

`n0`：时间触发，周期 100ms，超周期 = 100ms。
`n1`：事件触发，支配端口 `p0_0`（抽稀 1 → 虚拟周期 = n0 的 100ms），`p3_0` 读最近 3 帧窗口。

## 9. 相关

- 解析/校验实现：`core/g_state.hpp`（`NodeInfo` 构造器、`check_topology`）
- 生成器：`tool/_load.py`（`_make_pipeline` 产出、`inspect_file` 打印、`generate_all` 批量）
- 可视化：`tool/viewer.html`（`inputKind` / `triggerDesc` 按 `type` 分派）
- 调度语义：`docs/precedence_graph_design.md`；WCET 自整定：`docs/wcet_estimation.md`
