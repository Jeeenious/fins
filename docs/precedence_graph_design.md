# Precedence Graph 展开设计（FineVision job 级数据流图）

> 状态：设计定稿。记录 expand_hp 从"节点级帧槽模型"演进为"job 实例级 precedence graph"的完整设计。
> 图例：`A → B` 表示 precedence/数据绑定边。

## 1. 背景与目标

当前 `graph_g`（`g_state.hpp`）是**节点级**模型：每个算法节点 1 个 Vertex（1 个 job 闭包 + 多维权值），边 = stream 的 Message 帧槽，job 运行时从各入边 stream 取"最新帧"（latest-value 隐式语义）。每轮调度对每个就绪节点投递 1 次 job。

目标：演进为 **job 实例级** precedence graph——每个数据帧对应一组确定的 job 实例，实例间有确定的绑定关系，`expand_hp` 从源逐步展开、按一跳邻居关系决定展开方式，对齐 FineVision 论文的连接模式语义。

## 2. 数据流连接模式

| 模式 | 拓扑 | 绑定语义 |
|------|------|----------|
| **Multi-hop** | 1 producer → 1 consumer | 线性流水线；consumer job 与 producer job 一一绑定，每帧恰好处理一次 |
| **Fork** | 1 producer → N consumer | 一帧广播给全部下游；producer job 是每个 consumer job 的直接前驱（共享帧） |
| **Join** | N producer → 1 consumer | latest-value 语义；**最短周期**的上游决定 release pattern 与 job 总数；每个 consumer job 绑定触发 producer job + 其余 stream 最近的 producer job |
| **Loop** | 反馈（下游 → 上游） | 显式端口补充定义（config 顶层 `"loop"`：`{端口: N}`，扁平，N = 回溯迭代次数）；job k 从滑动窗口历史槽聚合最近 N 帧真实历史（恒长 N、未满头部 0 值占位、回绕不清、跨多超周期依赖可表达）。loop 输入参数在插件侧声明 `std::vector<int>`；运行时（bind_job）把最近 N 帧聚合为 typed `std::vector<int>` 直出（恒长 N、头部 0 补位），AlgoFunc/插件按普通 sub 接收、不做转换 |

前三种与 Verucchi（RTAS 2020）"Latency-Aware Generation of Single-Rate DAGs from Multi-Rate Task Sets"的 Replication + 数据边绑定一致（前提全周期）。第四种 Loop 是扩充。

## 3. 图元素模型

单图（`graph_g.dag`，expand_hp 一次构建、配置变化时全量重建）。顶点两类 + 绑定边：

```
① 正常 job 顶点    {id}:{k}            有 Job 执行体 + Attrs + 四态状态
                                     （Pending/Ready/Running/Finished；闭包执行 Running→Finished）
                                     参与调度执行；k 为超周期内实例序号（0..HP/T-1）
② 虚拟源顶点      hist:{stream}:{k}    历史数据源；job 空（不可投递、恒 Finished）
                                     按 job 实例粒度（k = 超周期内 job 序号 0..Nx-1），
                                     每顶点保留该 job 最近 R 轮的输出帧（滚动覆盖最旧）
③ 绑定边          from → to           数据绑定 + precedence：正常 job → job；虚拟源 → job（历史输入）
```

- **顶点 = job 实例**，每个有唯一 id（`{id}:{k}`），不存在"节点含多 job"。
- **边 = 绑定边**，无 latest-value——consumer job 读的是展开时静态绑定的 producer job 输出，不是"从边取最新帧"。
- 数据以 `Message`（`shared_ptr<void>` 帧）承载，只读共享、零拷贝。
- **状态标记状态而非类型**：顶点类别由 `job` 是否非空区分（空 job = 虚拟源），不设类型枚举；`Ready` 由前序推导（前序全部 `Finished` → Ready，唯一前序条件，无释放时刻，调度器契约）。
- **延迟**：① 显式数据路径延迟由用户插入"延迟算法节点"（AlgoBase 派生，函数体内做纯延迟，其 `outputs` 接被延迟 job 的 `inputs`，即前序边）；框架 `offset` 保留为 DelayAlgo 的 `sleep_for` 时长（阻塞真延迟，占 worker），不再参与图/调度；② **周期性任务自动补 delay 假节点**（2026-08-29 新增）——显式配置 period 的节点在其所有相邻 job 实例间插真延迟假节点 `{id}:{k}` → `delay:{id}:{k}` → `{id}:{k+1}`（offset=period，替换原 seq 边），实现"每 period 释放一个 job"的周期性时间语义，见 6.4/7。

## 4. 展开算法（expand_hp 单函数一步建图）

构建围绕图结构：`expand_hp(nodes, so_ctx)`（图域唯一入口，**输入全部显式入参**——nodes = NodeInfo
解析态列表（调用方传 pipeline_g.nodes）、so_ctx = 算法定位表（调用方传 so_contexts_g_），不隐式
读任何全局）单函数一步建图，中间量（端口名索引、实例数、算法实例）全为局部变量、随函数生命周期，
不存成员（避免 expand_hp 外残留状态 + 开头逐个 clear）、无 BuildCtx 结构。

```
expand_hp(nodes, so_ctx) = 清空 + 顺序执行（①结构分析→②超周期→③拓扑序→④实例化→⑤支配周期/实例数→
            ⑥建顶点→⑦建边→⑧填 job 闭包）：
  ① 结构分析    端口名生产者/消费者索引 + 一跳邻居拓扑（局部；loop 端口跳过）
  ② 超周期      HP = lcm(全部显式配置 period)（无 → 0 不回绕）
  ③ 拓扑序      BFS 源优先（环未覆盖补末尾）
  ④ 实例化      内置 delay/ringbuf 或 so C 工厂，局部 by_id 持有（按 config_cache 顺序 configure）
  ⑤ 支配周期    未配置 period 后级继承最短周期前级 → period_final + node_count
  ⑥ 建顶点      dag.add_node({id}:{k}, Vertex{attrs}：period/deadline/wcet/abs_deadline
               = 超周期起点 + (k+1)·deadline)，job 留空
  ⑦ 建边        seq 连续边 {x}:{k}→{x}:{k+1} + 同名端口绑定边（整数式 ((k+1)·Np-1)/Nc，
               loop 端口跳过）
  ⑧ 填 job 闭包 AlgoBase × Message 打包执行体（Running→读绑定边/loop 槽→execute→写下游+槽→Finished）
```

### 阶段一：结构分析 + 超周期 + 拓扑 + 实例化 + 支配周期 + 建顶点

赋予执行周期 → 计算超周期 → 全部顶点建好、attrs 赋好，job 空着：

```
① 归一化 + 结构校验 + 字段抽取已由 Pipeline::parse 完成（array / {nodes} / 单对象；
   null → 空表幂等）；图侧只收 pipeline_g.algos（BuildCtx.nodes_ 引用，AlgoBase 壳），
   无 JSON 类型判断
② 结构分析：producers_ / consumers_ 端口名索引（同名端口直连，连接键 = 端口名）+
   一跳邻居拓扑（局部）；loop 端口（algo->loop）跳过索引
③ period 已由 parse 抽进 algo->period（显式配置；0 = 未配置 → 走继承）
④ 超周期：hyper_period = lcm(全部显式配置的 period)（无 → 0 不回绕）
⑤ 拓扑序（BFS 源优先）
⑥ 实例化：先查内置工具算法（delay/ringbuf，algo_utility.hpp），
   再遍历 so_contexts_g_ 定位 [name:version] → C 工厂；逐字段搬运壳→具体实例
   （id/name/version/loop/period/wcet/deadline + update_*_ports 注入端口名 +
   configure 逐个 parameters + config=壳 raw）并替换 pipeline_g.algos[i]
⑦ 支配周期 + Replication → period_final / node_count_：显式周期节点 Nx = HP/T；
   未配置 period 的后级继承最短周期前级的最终周期（支配原则，多前级取最短），
   Nx = HP/最终周期；孤立未配置 → 1（loop 端口不参与继承）
⑧ 建全部顶点 dag.add_node(vtx, Vertex)：attrs.period（= 支配周期）/wcet/deadline/abs_deadline
   （= hyper_start_ms + (k+1)·deadline 按序排）赋好；v.job 留空
   （顶点 vtx 现拼 id+":"+k，无 JobInst、无 release）
```

### 阶段二：建边（seq 连续边 + 绑定边）

连边逻辑按论文选段（Verucchi，见 §5）：

```
① 同节点连续 job precedence 边：{x}:{k} → {x}:{k+1}（stream="seq:{id}"，串行、共享 AlgoBase）
② 跨节点绑定边：连接键 = 端口名（同名直连，边名 = 端口名）——consumer job k 读其时段
   [k·T_c,(k+1)·T_c) 内 producer 的最新已完成帧，整数式 pk = ((k+1)·Np-1)/Nc（向下取整，
   天然在 [0,Np-1]，无 clamp）——同速率一一对应；快 producer→慢 consumer 绑最后帧（慢
   consumer 在超周期末尾执行、读最终帧）；慢 producer→快 consumer 共享帧；fork 广播全绑
   job 0。恒有边（无"绑不到空输入"回退）。单写者约束：同一输出端口至多一个生产者，
   多写者在 Pipeline::check_topology 抛异常拒绝。loop 端口（algo->loop）不建自环边
③ 跨周期数据滞留经暂存算法顶点中转：需要历史数据的 consumer 由用户显式配置 ringbuf 等
   内置算法节点；producer 输出端口 → ringbuf 输入端口、ringbuf 输出端口 → consumer 输入
   端口，同名端口三段绑定自然形成（ringbuf 内部 deque 滚动保留 depth 帧）
```

### 阶段三：填 job 执行体闭包

对每个顶点填 job 执行体闭包（AlgoBase × Message 打包）。**job 闭包只执行，不碰状态**
（Running 由 grab_ready_workload 拉取时置、Finished 由装配回调回锁直做完成事件置，
见"运行时调度"节）：

```
① 按 algo->get_input_ports()（闭包捕获具体算法实例 shared_ptr，本身即解析态：端口名 +
   loop 定义）逐端口读绑定边（edges_to(vtx, pn) 唯一 → 该 producer job 帧；连接键 = 端口名）；
   loop 端口从滑动窗口历史槽 hist_[pn] 聚合最近 N 帧（config 迭代步）为 typed std::vector<int>
   （恒长 N、未满头部补 0，见第 4 节 Loop）；运行时直出该 vector<int> 进对应输入帧交算法
② algo->execute(inputs, outputs)
③ 按 algo->get_output_ports() 逐端口写全部下游绑定边（edges_from(vtx, pn)，共享帧）+
   维护 loop 历史槽
```

### 输出

job 实例级 precedence DAG（正常 job 顶点 + 绑定边，job 闭包携带执行体），供调度器使用。

## 5. 绑定规则

- **Multi-hop / Fork / Join（时段内最新已完成帧）**：consumer job `{c}:{k}` 读其时段
  `[k·T_c,(k+1)·T_c)` 内 producer 的最新已完成帧，整数式
  `pk = ((k+1)·Np-1)/Nc`（向下取整，天然在 `[0,Np-1]`）——恒有边；Np<Nc 时多 consumer job
  共享同一 producer job 帧；Np>Nc（快 producer→慢 consumer）时慢 consumer 绑 producer 最后帧
  （超周期末尾执行、读最终帧）；无"绑不到空输入"回退。连接键 = 端口名（同名直连，边名 =
  端口名）。单写者约束：同一输出端口至多一个生产者，多写者在 `Pipeline::check_topology` 抛
  `std::invalid_argument` 拒绝（否则 build_edges 对每个 producer 都建边、pack_jobs 只读
  `es[0]`，行为取决于遍历顺序）。
- **Join（触发者 = 最短周期）**：consumer 的 job 实例数由最短周期 producer 决定（其实例数最多），
  其余 stream 按上述时段映射就近绑定。
- **同节点连续 job**：`{x}:{k} → {x}:{k+1}` 保证同一算法的多实例串行（Verucchi Replication）。
- **Loop**：显式端口补充定义——`"loop": {端口: N}`，N = 回溯迭代次数（扁平；不再有
  step/timer 双层与时间→帧数换算）。反馈不建图边（无自环），走滑动窗口数据槽 `message_hist_`
  （最近 N 帧真实历史，容量 = producer 节点 `NodeInfo.cap`（config 顶层 "cap"，默认 10），
  回绕不清满丢最旧、跨重建保留；未满头部补 0）。运行时（bind_job）直接把最近 N 帧聚合为
  typed `std::vector<int>` 直出（当前元素固定 int；loop 入参声明 `std::vector<int>`，恒长 N、
  头部 0 补位），AlgoFunc 与插件按普通 sub 接收、不做任何转换。

## 6. 数据生命周期

- 数据以 `Message`（`shared_ptr<void>`）存在，传递后只读共享，零拷贝。
- **单图复用**：跨超周期数据靠 shared_ptr 引用计数存活——超周期回绕（`update()`）只重置
  job 顶点状态（finished/running、abs_deadline），**不清边、不清数据**。
- 跨周期数据以**虚拟源顶点**形式进入下一超周期的图：consumer job 从绑定它的虚拟源读历史帧。
- 虚拟源按 job 实例粒度：`hist:{stream}:{k}`（k = 一轮内 job 序号，覆盖一轮内全部帧）；
  每顶点内部保留最近 R 轮（该 job 的 R 帧，滚动覆盖最旧）。数据随 expand_hp 全量重建（新配置）一并释放。
- 代价（已确认接受）：仅当前超周期内的数据天然可达；跨超周期数据需显式经虚拟源顶点传递。

## 7. 调度语义

调度器（`scheduler_g` 回调 + `start()` 循环）遍历图顶点读权值 + 拓扑前置依赖算就绪排序：

- **正常 job 顶点**：执行体；完成后置 `finished`，解锁后继 job。
- **虚拟源顶点**：恒就绪（数据源，不执行、不占 WCET）。
- **纯延迟顶点**：参与调度，占用 `offset` 时长的"执行"窗口，约束后继 job 的 ready 时刻。
- 投递顺序 = 调度回调按策略排好的就绪排序，仍经 `ready_set_g` 交线程池 worker 执行。

## 8. 与当前实现的差距与落地步骤

当前（节点级）→ 目标（job 实例级）：

| 维度 | 当前 | 目标 |
|------|------|------|
| 顶点 | 每节点 1 Vertex | 每 job 实例 1 顶点 + 虚拟源 + 延迟顶点 |
| 边 | stream 帧槽（全连接 p×c） | 绑定边（绑最近 / 最短周期触发） |
| job 闭包 | 读该 stream 第一条边（latest-value） | 读绑定 producer job 的输出 |
| 数据 | Message 帧槽 | Message shared_ptr + 虚拟源历史 |
| offset | Attrs 数值 | 延迟顶点时间约束（Attrs 仍保留） |
| 回绕 | update 重置顶点 | update 重置顶点 + 滚动历史，不清数据 |

落地步骤（每步后构建 + 测试验证）：
1. **结构分析改造**：expand_hp 增加一跳邻居拓扑、全周期判定、HP 计算
2. **Replication**：逐节点实例化 job 顶点（{id}:{k} + offset/deadline + 连续 job 边）
3. **绑定**：Multi-hop/Fork 绑最近、Join 最短周期触发
4. **虚拟源顶点**：历史数据 + 多轮滚动
5. **延迟顶点**：offset 时间约束 + 调度参与
6. **调度适配**：scheduler_g/start 处理四类顶点
7. **测试更新**：test_pipeline / test_assembly 断言迁移到 job 实例级

## 落地状态（2026-08-25，g_state.hpp 三步私有成员建图架构）

已完成：
- **三步私有成员建图架构（围绕图结构）**：`PrecedenceGraph::expand_hp` 拆成三个职责清晰
  的私有成员函数，中间态（nodes_/producers_/consumers_/algos_/node_jobs_）存私有成员，
  expand_hp 只做清空 + 顺序调用：
  ① `build_vertices(config)`——归一化节点 + stream 索引 + period + 超周期 HP=lcm +
     拓扑序 + 实例化 algos_（先查内置工具算法 delay/ringbuf，再遍历
     so_contexts_g_ 定位 [name:version] → C 工厂）+ 支配周期 + Replication（显式周期节点
     Nx=HP/T；未配置 period 的后级继承最短周期前级的最终周期）→ 建全部顶点 + attrs
     （period = 支配周期，abs_deadline = hyper_start_ms + (k+1)·deadline），**job 留空**；
  ② `wire_edges()`——同节点连续 job 边 {x}:{k}→{x}:{k+1}（stream="seq:{id}"）+ 跨节点
     绑定边（按裸 release 绑最近：release ≤ 自身最大者；绑不到 → 不建边空输入）；
  ③ `pack_jobs()`——对每个顶点填 job 执行体闭包（置 Running → 按 config["inputs"]
     读绑定边 → algo->execute → 按 config["outputs"] 写全部下游绑定边共享帧 → 置 Finished）。
- **无相位重构（2026-08-25 用户拍板）**：删 JobInst/node_jobs_/release/offset 相位体系，
  改存 `node_count_`（节点→实例数，顶点 vtx 现拼 id+":"+k）；就绪条件 = 前序边来源全部
  Finished（无 now ≥ release，唯一前序条件）；abs_deadline 按序排 = hyper_start_ms + (k+1)·deadline；
  绑定 = 时段内最新已完成帧整数式 ((k+1)·Np-1)/Nc 恒有边（快 producer→慢 consumer 绑最后帧、
  慢 producer→快 consumer 共享帧、无空输入回退；同 stream 多 producer 单写者约束拒绝
  （check_topology 抛）；
  offset 保留为 DelayAlgo 的 sleep_for 时长（阻塞真延迟，占 worker），不再参与图/调度；
  **非 JSON 配置**——由后续超周期展开/调度阶段分析注入（2026-08-26 定稿，见下）。
- **虚拟源顶点整体删除（按用户决策）**：hist:{stream}:{k}/history/hist_writers/
  history_rounds 全部移除；跨周期数据滞留改由用户显式配置**暂存算法顶点**中转——
  producer 输出顶点 → ringbuf（内置算法，内部 deque 滚动保留 depth 帧）→ consumer 输入，
  三段绑定按 stream 匹配自然形成。
- **延迟**：① 用户显式 delay 算法节点（函数体内做纯延迟），其 outputs 接被延迟 job 的
  inputs（即前序边）；delay 的 offset 由后续超周期展开/调度阶段分析注入（非 JSON）；②
  **周期性任务自动补 delay 假节点**（2026-08-29）——显式配置 period 的节点，展开的所有
  相邻 job 实例间插入 delay 假节点（真延迟，offset=period，占 worker，无数据边），seq 边
  拆两段 `{id}:{k}` → `delay:{id}:{k}` → `{id}:{k+1}`，实现"每 period 释放一个 job"时间
  语义；继承周期节点不插（跟随 producer 节奏）。
- **Attrs 定稿**：period/deadline/wcet/abs_deadline（priority 已移除；offset 不入 Attrs，
  由超周期展开分析注入 DelayAlgo 配置，非 JSON）。`rollover_hp()` 回绕时**起点更新为当前
  真实时钟**（now_ms，2026-08-28 修正：旧版机械累加 hyper_period 与实际时间脱钩，执行快慢
  不定），abs_deadline 由主线程每轮 `update_abs_deadline()` 按真实时钟滚动校正（起点从
  hyper_start_ms 滚到 now 所在时窗 + (k+1)·deadline，执行快慢不定始终对齐真实时间轴），数据/边不清
  （跨周期存活靠共享帧 + 暂存算法顶点）。
- **测试迁移**：test_pipeline 断言迁移到 job 实例级（cam:0/disp:0），TEST 4a 覆盖四态
  状态机 + offset 相位（无虚拟源回退、负断言无入边）；TEST 4b 三内置算法端到端
  （delay 透传 / ringbuf 取时间段）；test_assembly 不变
  （仅断言 dag.size()==2）。构建全部通过。
- **支配周期（2026-08-25 用户拍板，推翻"非周期镜像实例数"）**：后级执行周期默认与前级
  **严格一致**——未显式配置 period 的节点继承前级最终周期（多前级继承**最短周期**前级，
  topo 序保证前级已定），`attrs.period` 为真实周期；只有显式配置了 period 才与前级不同
  （multi-hop 速率变化）。HP 仍只统计显式配置周期（继承值必为某显式周期，整除 HP）。
  test_expand TEST 1 的 mon 未配置 → 继承 disp 的 50。
- **Loop 落地（2026-08-25，显式端口补充定义；2026-08-26 hist 容量改 NodeInfo.cap；
  2026-09-02 扁平化 + 运行时 typed std::vector<int>）**：
  节点 config 顶层 `"loop"` = `{端口: N}`（扁平，N = 回溯迭代次数；**timer 时间窗口模式已删**，
  不再有时长→帧数换算；不推导、必须显式定义）。loop 端口不参与拓扑依赖、支配周期继承与
  绑定边（无自环边）；反馈走滑动窗口数据槽 `message_hist_[端口]`（最近 N 帧真实历史，容量 =
  producer 节点 `NodeInfo.cap`（config 顶层 "cap"，默认 10，2026-08-26 起从 JSON 解析）；
  回绕不清、满丢最旧、跨重建保留，跨多超周期依赖可表达）。job 闭包对 loop 端口把最近 N 帧
  聚合为 typed `std::vector<int>` 直出进 `inputs[端口]`（恒长 N：未满头部补 0、尾部 = 最近真实帧
  逐帧 sub<int>；当前元素固定 int，AlgoFunc/插件声明 `std::vector<int>` 按普通 sub 接收、零转换）。
  校验放 pipeline 侧：N 须正整数（NodeInfo 格式级）、loop 端口须同时在本节点 inputs/outputs 中
  声明（check_topology 结构级 ③）。
- **JSON 解析归 Pipeline + 解析态定型 Pipeline::NodeInfo（2026-08-26）**：图侧/运行时彻底剥离 JSON
  类型判断——Pipeline **两函数职责分离**（parse / check_topology 两层检查）：
  `Pipeline::parse(config)`（无返回）**拆封 script**（null/array/{nodes:[...]}/单节点）+ 逐个
  触发 **NodeInfo 构造器自解析**（每节点 1 个 **`Pipeline::NodeInfo`** 纯数据解析态：id/name/
  version、input_ports/output_ports（端口名数组，顺序 = AlgoFunc 参数顺序）、config_cache（位置式
  值表 = config 的 parameters 元素 value，名字丢弃）、loop 拆单个 map（端口 → 迭代步 N；扁平，
  timer 模式已删）、period/wcet/deadline 数值——
  **无 config 原始 JSON 拷贝**（NodeInfo 纯结构化）），逐节点格式违反抛异常——同时充当
  **第一级 json 格式审查**（收包前调）；
  `check_topology()` = **第二级图结构审查**（无参读 nodes：① 单写者约束——同名输出端口多 producer
  抛 `std::invalid_argument`；② 源周期——无输入节点必填 period），装配点 main 重建段 parse 后
  expand_hp 前显式调用。**（validate 已删除（2026-08-26 用户拍板）：原为 parse 薄包装，其格式审查
  职责并入 parse，`parse_pipeline` 亦改名 `parse`——Pipeline 仅剩 parse + check_topology。）****AlgoBase 瘦身为纯执行接口**（initial/execute/configure，无字段、无端口接口、
  不感知 NodeInfo）——execute 按"端口序 array"定位（inputs[i]↔输入端口序[i]），AlgoFunc
  **configs-first 布局**（配置段前置 + 输入段 + 输出段）靠运行时边界拆段，configure **位置式注入**
  （按 configs_.size() 定位，JSON→typed 解码一次进 configs_，execute 零解析；布局契约见
  algo_func.hpp 头注释）。inputs/outputs 为**端口名数组** + **同名端口直接连接**（连接键 = 端口名，
  边名 = 端口名，删 stream）。`expand_hp(nodes, so_ctx)`（输入显式入参：nodes = 调用方传
  pipeline_g.nodes、so_ctx = 调用方传 so_contexts_g_，不隐式读全局）；**单函数一步建图**
  （①结构分析→②超周期→③拓扑序→④实例化→⑤支配周期/实例数→⑥建顶点→⑦建边→⑧填 job 闭包），
  中间量全为局部（无 BuildCtx）：④ 实例化具体算法存局部 by_id（内置 delay/ringbuf 或 so C 工厂；
  按 config_cache 顺序逐个 configure），⑧ 闭包捕获 shared_ptr<const Pipeline::NodeInfo> + 具体实例。
  **delay 的 offset 非 JSON
  配置**——由后续超周期展开/调度阶段分析注入 `DelayAlgo::configure("offset")`（本阶段无注入 →
  恒 0 纯透传）。测试调用点 parse → check_topology → expand_hp 三步（test_pipeline/test_expand/
  test_assembly）。test_expand 加 test8 验证单写者拒绝在 check_topology 抛（断言不变）。
- **expand_hp 合并单函数 + 弃 GraphSpec 方案（2026-08-26 用户拍板）**：曾提出把"图描述"型结构
  `GraphSpec`（form 层，与 `DirectedAcyclicGraph` 双向互转 to_dag/to_spec）作为建图中间层，
  用户评估后认为 GraphSpec 数据与 pipeline 解析态（NodeInfo）基本一致、引入重复，拍板**不拆**：
  直接一个 `expand_hp()` 函数完成建图。落地：form.hpp 删除 GraphSpec/to_dag/to_spec/for_each_edge
  （DAG 恢复原状）；g_state.hpp 把 build_vertices/build_edges/pack_jobs 三个私有函数全内联合并进
  expand_hp()（①结构分析→②超周期→③拓扑序→④实例化→⑤支配周期/实例数→⑥建顶点→⑦建边→
  ⑧填 job 闭包），删 BuildCtx struct 与私有函数声明——中间量全为 expand_hp 局部变量
  （producers/consumers/in_producers/out_consumers/topo/by_id/by_info/period_final/node_count/
  producer_of/loop_w）。建图语义不变（顶点/边/绑定/loop 全部与拆分版一致），
  test_expand 结构断言（64/64）全过。

未实现（设计步骤 6，见第 9 节）：
- 调度适配（scheduler_g 回调注入 + start 投递段处理就绪判定：入边未完成不投、Ready 由
  前序推导）——start() 投递段仍注释，本阶段明确不做调度器；Ready 推导规则写入注释作契约。
- Loop feed 聚合帧数的数值验证（恒长 w、未满 0 值占位、跨周期真实历史）——test_expand 以
  结构断言 + Mermaid 反馈弧验证展开，帧数验证留调度器阶段专门测试。

## 9. 待实现/待确认项

- 调度适配：scheduler_g 回调注入 + start() 就绪排序/投递（入边未完成不投；就绪判定：
  有 job 的 Normal 顶点按"前序全 Finished"推导 Ready，唯一前序条件，无释放时刻；无 job 顶点
  （暂存算法中间体不参与投递，恒 Finished））
- Loop feed 聚合的数值时序验证（恒长 w 帧数、未满 0 值占位、跨周期真实历史，待调度器落地后验证）
- 暂存算法顶点的多帧滚动完整时序验证（ringbuf depth>1 的跨周期精确时序待调度器落地后验证）
- `Attrs` 与顶点权值的最终分工（priority 已移除、offset 不入 Attrs 仅 DelayAlgo 配置，是否有后续调整待定）
