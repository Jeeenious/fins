# PCL 模块插件（plugins_pcl_*.so）

按 PCL 库把常用点云算法封装成 FINS 数据流算法，每库一个独立 `.so`，产出到
`plugins/plugins_pcl_<库>.so`，随插件目录由 client 装载（loader 合并各 `.so` 的算法 key）。

- 首批库：`common filters segmentation features registration`
- 源码：`example/plugins_pcl_<库>.cpp`（**单文件自包含**，无共享私有头）
- 构建：`cmake --build cmake-build-debug -j`（目标 `fins_pcl_<库>`）
- 算法总数：21（实测符号表）

> 与 OpenCV 插件一致：只封装“**单帧点云入 + 标量/字符串配置 + 点云出**”这类可流式化的子集。

---

## 1. 载荷与签名

- 载荷 = `pcl::PointCloud<PointT>`，`PointT` 首批展开多点类型：`PointXYZ` / `PointXYZRGB` /
  `pcl::Normal`。每个算法名带点类型后缀（`_xyz` / `_xyzrgb`），跨边类型必须一致（运行时
  typeid+ABI 校验）。
- 跨 `.so` 一致：各插件同链系统 `libpcl_*`，`util::ABITag<pcl::PointCloud<PointT>>` = PCL
  版本号（`tag_ABI.hpp` 已适配）。**要求插件源先 include PCL 头再 include `xmacro.hpp`**，
  让 `FINS_HAS_PCL` 生效、`p_mutable/p_shared` 两端算出的 ABI 标签一致。
- 签名三段（configs-first）：配置段（`int/double/string` JSON 标量）→ 输入段
  （`const pcl::PointCloud<...>&` 只读共享帧）→ 输出段（`pcl::PointCloud<...>&`）。

## 2. 构建说明（绕开 PCLConfig 的 VTK/MPI 陷阱）

本机 `find_package(PCL)` 会沿 PCLConfig 拉入整条 VTK 链，而 VTK 目标引用 `MPI::MPI_C`
等导入目标，在本机 CMake 下配置即报错。故 CMakeLists 中**不调 `find_package(PCL)`**，改为：

- include 目录取自 `pkg-config --cflags pcl_common`（pcl-1.14 / eigen3 路径）；
- 链接用手工 `find_library` 解析的核心非 VTK pcl 库 + `flann`/`flann_cpp`。

这样 `.so` 不依赖 VTK/Qt，纯点云库即可 dlopen。

## 3. 算法清单

### common —— `plugins_pcl_common.so`（7，源/汇）
| 算法 | 配置 | 说明 |
|---|---|---|
| `pcl_src_cube_xyz(n, half)` | 均匀立方体 | 源：[-half,half]³ XYZ |
| `pcl_src_plane_xyz(n, sigma, ox,oy,oz)` | 平面+噪声+平移 | 源：z≈0 平面，供分割/配准 |
| `pcl_src_blobs_xyz(per, sigma, gap)` | 3 高斯团 | 源：供欧式聚类 |
| `pcl_src_cube_xyzrgb(n, half)` | 彩色立方体 | 源：XYZRGB（逐点随机色） |
| `pcl_sink_xyz` / `pcl_sink_xyzrgb` / `pcl_sink_normal` | — | 汇：丢弃一帧（占位） |

### filters —— `plugins_pcl_filters.so`（8，各点类型 `_xyz`/`_xyzrgb`）
`pcl_passthrough(axis,min,max)`、`pcl_voxel(lx,ly,lz)`、`pcl_stat_outlier(mean_k,stddev)`、
`pcl_radius_outlier(radius,min_neighbors)`——统一 `点云→点云` 单帧无状态。

### segmentation —— `plugins_pcl_segmentation.so`（4）
| 算法 | 输出类型 | 说明 |
|---|---|---|
| `pcl_plane_keep_xyz(dist)` | XYZ | RANSAC 平面 → 保留平面点 |
| `pcl_plane_remove_xyz(dist)` | XYZ | RANSAC 平面 → 去平面留其余 |
| `pcl_cluster_largest_xyz(tol,min,max)` | XYZ | 欧式聚类 → 只留最大团 |
| `pcl_cluster_color_xyzrgb(tol,min,max)` | **XYZRGB** | 欧式聚类 → 每团一色（跨类型边示例） |

### features —— `plugins_pcl_features.so`（1）
`pcl_normal_xyz(radius)`：半径搜索法线估计，输出 `PointCloud<pcl::Normal>`（含 curvature）。

### registration —— `plugins_pcl_registration.so`（1）
`pcl_icp_align_xyz(max_corr,max_iter,eps)`：**两输入**（source, target）→ ICP 把 source 对齐到
target，输出对齐后的 XYZ。

## 4. 运行

```bash
cmake -S . -B cmake-build-debug && cmake --build cmake-build-debug -j
./client 18080 ./plugins            # 装载 plugin.so + plugins_opencv_*.so + plugins_pcl_*.so
./server pipeline/cfg_pcl_filters.json 18080   # → “pipeline applied: N vertices”
```

每模块演示 cfg 见 `pipeline/cfg_pcl_{common,filters,segmentation,features,registration}.json`，
全部内置合成点云源（cube/plane/blobs），无外部 `.pcd` 资产即可跑通。判据：`pipeline applied`
且持续调度无 `Type Mismatch`/异常。

## 5. 文件清单

| 文件 | 说明 |
|---|---|
| `example/plugins_pcl_{common,filters,segmentation,features,registration}.cpp` | 5 个模块插件源（单文件自包含） |
| `pipeline/cfg_pcl_{common,filters,segmentation,features,registration}.json` | 每模块演示 cfg（*.json 为 gitignore，本地） |
| `CMakeLists.txt` | `fins_pcl_*` 目标（绕开 PCLConfig VTK 链） |

> 注：PCL 内部多线程走 OpenMP（构建期决定），未做类似 OpenCV 的运行时禁用；如需确定性可另加
> `omp_set_num_threads(1)`（需链接 OpenMP）。首批只做 XYZ/XYZRGB/Normal 三个点类型，可再扩。
