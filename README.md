# FINS — 实时数据流(Precedence-Graph)多核调度实验床

> IWIN-FINS Lab, Shanghai Jiao Tong University · C++20

FINS 是一个**可观测的实时调度实验床**:把"传感器 → 感知/计算 → 执行"这类**数据流流水线**建成带
时间语义的 **precedence graph(DAG)**,在真实多核(可独占)上以可插拔的算法插件跑起来,用**利用率
驱动的合成负载**做大规模实验,落地并评估面向这类负载的**实时调度方法**(WCET 自整定、makespan/
响应时间上界、动态优先级),最终用统一的 trace 观测对照"设计利用率 u ↔ 实测利用率"。

一句话:**我们不造理论,而是在真实内核上把任务图调度理论的每个环节做成能编译、能跑、能量化、
能画图的最小闭环**,供方法对比与论文复现。

---

## 1. 我们在做什么(研究动机)

现代无人系统/机器人的感知管线是一条多路数据流:多个传感器源周期采样,中间计算(目标检测、特征、
滤波、配准)以不同速率在前序关系下并发执行,结果汇聚后驱动执行机构。它们共享一个多核:

- 节点同时含 **周期任务**(显式 `period` → 定时释放)与**事件任务**(由前驱数据到达触发,无显式
  周期、继承前级周期);
- 周期不齐 → 需要 **超周期(H = 各周期 LCM)内对齐**,任务 j 在超周期内的释放次数 $n_j = H/T_j$;
- 计算还有 **反馈(loop)**:自回边读取最近 N 帧(历史数据槽 `hist`),不是简单 DAG;
- 一旦超载,需要 **过载检测**与可量化的调度依据(wcet / makespan 上界 / 截止期)。

研究主线(每项都在本仓库落地):
| 问题 | 方法 | 位置 |
|---|---|---|
| 单顶点执行时间不自知 | 基于执行历史自整定 wcet(HWM / p 分位数 + 裕度) | `schedule/wcet_updater.hpp` |
| DAG 整体 makespan / 超周期过载检查 | Graham's bound(1979)→ Multi-Path Bound(He et al. 2023) | `schedule/makespan_updater.hpp` |
| 就绪队列该先跑谁 | 静态 FIFO/RM/DM/SJF/density + 图结构 depth/height + 动态 EDF/LLF | `schedule/priority_updater.hpp` |
| 怎么造可控的验证负载 | UniFast 按目标利用率 u 摊算忙等 | `tool/uload.ipynb` |
| 怎么知道真的达标 | 线程级生命周期 trace → 实测 u / 开销 / 绑核 | `include/utils/tracing.hpp` + `tool/plot.ipynb` |

调度器槽位(`wcet/makespan/priority_updater`)都是"一行装配的 `std::function`",宏或枚举即切策略,
方便横向对比。

---

## 2. 仓库结构

```
include/          核心运行时(SDK,头文件即库)
  g_state.hpp        全局状态 + PrecedenceGraph(job 级 DAG) + 无锁原语
  mesg/mesg.hpp      实时消息:类型擦除负载 + 端口/序列/生产者元数据
  algo/              算法执行接口 AlgoBase / AlgoFunc(configs-first 参数契约)
  xmacro.hpp         插件 C 工厂导出宏(X-macro)
  plugin_loader.hpp  插件目录扫描(增/改/删事件),dlopen 装载
  thread_pool.hpp    绑核自调度线程池
  RPC_listener.hpp   HTTP 传输层(httplib),收配置 JSON
  hardware_monitor.hpp 每核 CPU/内存观测
  utils/             tracing(事件流) · tag_ABI · form(DAG) · time · fs · logger
schedule/           三件套:wcet / makespan / priority 自包含策略头
example/            client(主程序 agent) · server(发 cfg) · plugins.cpp(usr_* 插件)
tool/               uload.ipynb(生成) · test.ipynb(采集) · plot.ipynb(分析)
                    viewer.html(设计/运行图可视化) · agent.sh(独占核脚本)
docs/               4 篇调度/DAG 语义设计
third_party/        单头依赖 nlohmann/json.hpp · cpp-httplib.h
bin/ lib/ tool/temp/     构建产物(client/server · plugin.so · tool/temp 导出的 trace)gitignore 不入库
```

**数据流闭环**:

```
    tool/uload.ipynb               server(发 JSON)
    ──────────────                 ──────────────
  UniFast 按 u/m 合成 cfg   ────►  POST /update ──►  client(agent)
                                                        │ cache 缓冲 + pending
                                                        ▼
                                           主线程调度循环(图静止时)
                                     commit → parse_pipeline → check_topology
                                                        │
                                                        ▼ expand_hp(锁外)
                                     job 级 precedence graph(含周期/事件/loop/hist)
                                          │
                   计时线程(tp 定时释放)   │  worker 池(事件/前序就绪 → 执行)
                       ◄──────────────────┘  线程绑核;wcet/makespan/priority 槽在
                                             rollover_hp / grab 决策点注入
                                          │
                                          ▼ 退出
                           tracing → tool/temp/tracing.csv(+tool/temp/dag.json)
                                          │
                tool/plot.ipynb(画) · viewer.html(看 DAG/时间线) · 对比 u
```

---

## 3. 核心语义(设计要点)

- **配置 = 节点数组**(也可 `{nodes:[...]}`)。每节点:`id` / `name`(= 插件算法键)/ `version`
  必填,`inputs`/`outputs`(端口名数组)与 `period`/`wcet`/`deadline`/`cap`/`loop` 可选。例:

  ```json
  [
    {"id":"n0","name":"usr_src","version":"1.0.0","outputs":["p0_0"],
     "parameters":[{"value":98756}], "period":1000, "wcet":98.756},
    {"id":"n1","name":"usr_fork3","version":"1.0.0","outputs":["p1_0","p1_1","p1_2"],
     "inputs":["p0_0"], "parameters":[{"value":146498}], "wcet":146.498}
  ]
  ```

  对 `usr_*` 参考插件,`parameters[0].value` = **每拍忙等 µs**(真实占核烧算力,负载生成器令其
  约等于 `wcet×1000`)。两级解析:先逐节点格式审查(`parse_pipeline`),再查跨节点图结构
  (`check_topology`):无输入源必填显式 `period`;**非 loop 输入端口须有生产者(拒绝孤立输入)**,
  保证任何合法事件节点都锚定在一条以周期源为根的链上。

- **任务模型**:
  - 周期任务 `period>0` → 由计时线程按 tp(时间点)定时释放;**周期任务走 hist 帧(长 1)语义**;
  - 事件任务无显式 `period` → 继承前级周期(多前级取最短),由前序完成事件逐级释放;
  - 超周期 H = 显式周期 LCM;`rollover_hp` 回绕仍由“本超周期完工”触发(干完即翻页,清 done/ready、
    tp 游标归零,loop 数据槽跨周期保留),但**超周期起点推进到绝对网格上的下一未来边界**(按 H 整拍
    对齐 expand 起点),与释放解耦:
    - **周期任务只在真实周期锚点(起点+offset)释放**——早排空自然空等到下一边界、不提前放下一拍;
    - 排空晚于边界(过载)时**跳过已错过的整拍并对齐未来边界**(告警、不累积漂移);
    - **非周期(事件)任务不受影响**:前序完成即就绪即跑(实测单源 period=100 释放间隔=100ms;
      事件跟随的滞后 = 前级忙等)。
  - `loop:{port:N}` 声明反馈窗口 = 回溯最近 N 帧(历史数据槽聚合),供 acc 类节点自反馈。

- **运行时形态**:全局单份 `PrecedenceGraph`,主线程调度循环在"图静止"时才 commit+重建
  (无锁原语 `expand_hp/grab_*/trigger_*/rollover_hp`),worker 各自绑核执行就绪 job。

- **插件 ABI**:每个算法 `.so` 导出 5 个 C 工厂符号,loader dlopen 解析后按键 `[name:version]`
  定位并 `create_algo` 实例化;跨 .so 载荷一致性由编译期 `ABITag` 守护(如 `cv::Mat` 按 OpenCV
  版本打标)。元数据(算法/参数 role=cfg|in|out、说明)刻意不进 `.so`,另维护 sidecar
  `plugin.meta.json` 供可视化/编辑。

---

## 4. 算法插件

仓库当前在库的**参考插件族**是 `usr_*`(`example/plugins.cpp` → `lib/plugin.so`,**33 个算法**),
职责 = "提供端口形状 + 烧指定 µs",作为负载/拓扑实验的算力载体:

| 形状 | 算法 | 说明 |
|---|---|---|
| 基础 | `usr_src`(0→1) / `usr_sink`(1→0) / `usr_relay`(1→1) | 源/汇/转发 |
| 扇出 | `usr_fork`(=2)、`usr_fork3..9`、`usr_fork10` | 1→k 多速率 fork |
| 扇入 | `usr_join`(=2)、`usr_join3..9`、`usr_join10` | k→1 多速率 join |
| 反馈 | `usr_acc`(=1)..`usr_acc10` | 1 feed + k loop(输入为 `std::vector<int>`) |
| 工具 | `usr_burn`(自旋源)/ `usr_nop`(零自旋汇) | — |

> CMake 另预留了**按领域拆 .so 的模块插件钩子**(OpenCV 各模块 / PCL 各库,载荷 `cv::Mat` /
> `pcl::PointCloud<...>`):往 `example/` 放入 `plugins_opencv_*.cpp` / `plugins_pcl_*.cpp`
> 即自动编出 `lib/plugins_opencv_*.so` 等;当前工作区未含演示源,故不产出。

**加新算法**:按形状族仿写一个函数 + 注册(参考 `example/plugins.cpp` 头注释),`cfg` 侧加
`parameters` 即插即用;配一份 `plugin.meta.json` 可被 viewer 展示端口/参数。

---

## 5. 负载生成(利用率驱动)

`tool/uload.ipynb` 是自包含生成器(五个独立 API,详见其 docstring):

- `gen_multihop / gen_fork / gen_join / gen_feedback / gen_mixed` 五类拓扑;每类图内**混合周期
  (timed)与事件(event)任务**;
- 利用率按"周期在超周期内的比例"构造:释放次数 $n_j=H/T_j$、忙等 $C_j=b_j·T_j·1000$ µs,UniFast
  摊 $\sum b_j = u·m$ ⇒ **每核利用率精确 = u**;
- 结构参数(路径/深度/扇出/深度/尾/分段/loopN)默认由 **seed 随机** → 不同 seed 数据流结构与
  timed/event 划分都变;想固定就显式传参;
- 输出 `cfg_<kind>_u<u>_m<m>_s<seed>.json`,如 `tool/uload/cfg_fork_u20_m6_s10011.json`。

---

## 6. 构建与运行

依赖:CMake ≥3.14、C++20 编译器、TBB、fmt、Threads(可选 OpenCV/PCL 模块插件另需对应库)。
第三方 `json.hpp`/`httplib.h` 已随仓库 vendored。

```bash
cmake -S . -B cmake-build-debug && cmake --build cmake-build-debug -j
# 产物: bin/client · bin/server · lib/plugin.so(usr_* 33 算法)

# 冒烟(无独占):开 client,另开终端发一份配置,跑一会儿 Ctrl-C
./bin/client 18080 ./lib
./bin/server tool/uload/cfg_fork_u20_m6_s10011.json 18080   # 发一份 → client 重建运行图

# 正式实验(独占核 + RT,需 root):见 tool/agent.sh
sudo tool/agent.sh -g        # 一次性授 RT(写 limits.d,重登生效)
sudo tool/agent.sh 1-6 6     # 独占核 1-6 + 6 worker 起 bin/client
```

产物约定(均 gitignore):client(agent) 退出写 `tool/temp/tracing.csv`(表头 `tid,seq,kind,t_us,cpu,tag`,
目录自建)与结构重建时的 `tool/temp/dag.json`;插件 .so 在 `lib/`。

**实验工作流**(每个 notebook 自包含):

1. **生成** `tool/uload.ipynb` → 产出 `cfg_*.json`;
2. **采集** `tool/test.ipynb` — 扫 cfg → 每份起 client+server 跑 `dur_s` → 搬原始 trace 到
   `tool/test/<镜像目录>/trace_<...>.csv`(默认纯搬运不算指标);`test(..., cores="1-6")` 走
   `sudo tool/agent.sh` 独占核;
3. **分析** `tool/plot.ipynb` — 画调度时间线 / 每周期开销 / pack·algo·post / rollover 开销;
   `tool/viewer.html` — 拖入 meta/cfg/dag 可视化设计图(pipeline,带端口锚点)与运行图(dag,
   按释放 tp 分列、每算法实例一色、hist 隐式依赖虚线、自环↻仅设计图)。

---

## 7. 设计文档

| 文档 | 内容 | 状态 |
|---|---|---|
| `docs/precedence_graph_design.md` | 节点级帧槽 → job 实例级 precedence graph 的演进与现行语义(周期/事件、hist、loop、超周期) | 定稿 |
| `docs/wcet_estimation.md` | 基于执行历史统计的 wcet 自整定 | 已落地 |
| `docs/makespan_multipath_bound.md` | Graham → Multi-Path Bound 的 makespan/过载上界 | 已落地 |
| `docs/priority_scheduling.md` | 静态 + 动态多策略优先级(默认 EDF) | 已落地 |

各 `.hpp` 顶部注释是最新实现级文档(与设计文档冲突时以代码注释/文档"现行语义"节为准)。

---

## 8. 约定与说明

- 编译期开关集中在 `client.cpp`/`g_state.hpp` 顶部宏:`FINS_TIMING`(耗时统计)、`FINS_CAL_WCET`、
  `FINS_MAKESPAN_METHOD`、`FINS_PRIORITY_POLICY`、`FINS_WCET_METHOD` 等,一行切换策略。
- 本仓库是一个**实验床而非产品**:example 是装配好的参考实现,`include/` 头文件即核心运行库;
  `memory/` 等目录为本地工作痕迹,非工程组成部分。
