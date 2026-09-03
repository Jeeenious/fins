# FINS — 实时数据流调度实验床

C++20 实时数据流框架 + 插件算法族 + 调度(trace)观测与利用率驱动负载生成。

## 算法插件(共 129,loader 合并所有 .so 的 key)

| 插件族 | .so / 源 | 数量 | 载荷 | 说明 |
|---|---|---|---|---|
| usr 用户插件 | `plugins/plugin.so` · `example/plugins.cpp` | **33** | `int`(反馈为 `std::vector<int>`) | cfg = 每拍忙等 µs;扇出/扇入/反馈均 1..10 |
| OpenCV 模块插件 | `plugins/plugins_opencv_*.so` · `example/plugins_opencv_*.cpp` | **75** | `cv::Mat` | 11 模块,dlopen 时 `cv::setNumThreads(0)` |
| PCL 模块插件 | `plugins/plugins_pcl_*.so` · `example/plugins_pcl_*.cpp` | **21** | `pcl::PointCloud<PointXYZ/XYZRGB/Normal>` | 5 库,无 VTK 依赖 |

usr 形状族(cfg=忙等 µs,见 `example/plugins.cpp` 头注释):
- 基础:`usr_src / usr_sink / usr_relay`
- 扇出 1→k、扇入 k→1:`usr_fork(=2)…usr_fork10`、`usr_join(=2)…usr_join10`
- 多路反馈 1 feed + k loop:`usr_acc(=1)…usr_acc10`(cfg 节点用 `"loop":{h1:N1,…}` 声明窗口)
- 工具:`usr_burn`(源)/ `usr_nop`(零自旋汇)

## 文档(docs/)

- `docs/opencv_plugins.md` / `docs/pcl_plugins.md` — OpenCV / PCL 插件算法清单与封装说明
- `docs/precedence_graph_design.md` · `makespan_multipath_bound.md` · `priority_scheduling.md` · `wcet_estimation.md` — 调度/DAG 语义设计
- `analysis/plot_stats.ipynb` — 读 `tmp/traing.csv` 的 trace,画时间线 / 每周期开销 / pack·algo·post

## 工具(tool/)

- `tool/uload.ipynb` + `tool/uload.py` — **UniFast 利用率驱动拓扑负载生成**:按每 worker 目标
  u × m 生成 pipeline json(u 构造逻辑、wave/harm 发布、拓扑模板、批量 manifest、加速比统计均
  在 notebook 内讲解),喂给 client 跑采集。
- `tool/viewer.html` — 设计/运行图可视化(拖入 meta/cfg/dag)。

## 构建与跑

```bash
cmake -S . -B cmake-build-debug && cmake --build cmake-build-debug -j   # 生成 client/server + plugins/*.so
./client 18080 ./plugins                 # 装载 plugins/ 下全部 .so
./server pipeline/cfg_usr_chain2.json 18080   # 应用一份 pipeline → “pipeline applied: N vertices”
```

trace:`client` 退出时写 `tmp/traing.csv`(`tid,seq,kind,t_us,cpu,tag`),供 notebook 分析。
`pipeline/*.json` 与 `plugins/*.so` 均为本地产物(`*.json`/`*.so` 在 .gitignore),不入版本管理。
