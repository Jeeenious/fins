# OpenCV 模块插件（plugins_opencv_*.so）

把 OpenCV 常用函数按**模块**封装成 FINS 数据流算法，每模块一个独立 `.so`，全部产出到
`plugins/plugins_opencv_<模块>.so`，由 client 启动时随插件目录一起装载（loader 把各
`.so` 的算法 key 合并，cfg 里的 `[name:version]` 在整个进程内解析）。

- 覆盖模块：`core imgproc photo features2d imgcodecs videoio calib3d objdetect video ml dnn`
- 源码：`example/plugins_opencv_<模块>.cpp`（**单文件自包含**，无共享私有头；每 `.so` 所需
  的“禁用 OpenCV 并行”与状态缓存等辅助就近内联在各源文件内）
- 构建：`cmake --build cmake-build-debug -j`（目标 `fins_opencv_<模块>`）

> 设计取舍：OpenCV 的“公共函数数”远大于“可作为数据流算法”的数——真正干净可用的是
> **单帧输入 + 标量/文件路径配置 + 图到图（cv::Mat→cv::Mat）输出**这一类。以下每模块只
> 封装这批可用子集；需要跨帧点集、相机矩阵、外部模型/设备的长流程不在单帧函数签名内，
> 详见各模块说明与“状态型能力”一节。

---

## 1. 签名约定（configs-first，与 plugins.cpp 一致）

每个算法 = 一个无状态 C 函数，参数**按位置**分三段：

1. **配置段**（cfg `parameters[].value`，顺序即签名顺序）：`int/double/bool/std::string`
   JSON 标量（字符串用于文件路径、扩展名、模型路径等）。
2. **输入段**：`const cv::Mat &` 等只读共享帧（也可为 `const std::vector<uchar>&` 字节流）。
3. **输出段**：`cv::Mat &` 等，算法就地写（AlgoFunc 输出段 pub 新建载荷）。

载荷类型几乎都是 `cv::Mat`（跨 `.so` 一致：typeid/ABI 标签均来自共享 `libopencv_core`，
`util::ABITag<cv::Mat>` = OpenCV 版本号，见 `include/utils/tag_ABI.hpp`）；仅
`imgcodecs` 的 `imencode/imdecode` 用 `std::vector<uchar>`（字节流）。

算法注册与导出见 `include/xmacro.hpp`：每个源文件定义 `FINS_ALGO_LIST(F)` 后调一次
`FINS_ALGO_EXPORT("1.0.0")`。cfg 内算法名 = 函数名，如
`{"name": "ocv_gaussian_blur", "version": "1.0.0", ...}`。

---

## 2. 禁用 OpenCV 后台线程池

每个插件源文件在 `.so` **dlopen 时**自执行一次（匿名命名空间静态对象构造）：

```cpp
struct DisableOcvThreads_ { DisableOcvThreads_() { cv::setNumThreads(0); } };
static DisableOcvThreads_ g_disable_ocv_threads_;
```

按 OpenCV 4.6 文档该调用使 OpenCV *“disable threading optimizations and run all its
functions sequentially”*：每个算法在调用它的 FINS 工作线程内**同步执行**，不再在每次
调用时向 OpenCV 自己的并行后端派发/拉起工作线程——消除每帧小操作的调度开销与非确定性。

要点：

- 作用域是**进程全局**（所有插件共享同一 OpenCV），各 `.so` 重复调用幂等；必须先于任何
  OpenCV 并行区（dlopen 时机满足）。
- 插件文件**单文件自包含**：上面 3 行内联进每个 `plugins_opencv_*.cpp`，插件之间无共享
  私有头；复制任一个源文件即可独立构建该 `.so`（只需 SDK include/ + OpenCV）。
- 本机 OpenCV 以 **TBB** 为并行后端；验证方式：跑 cv 重负载 pipeline 时观测
  `/proc/<pid>/status` 的 `Threads` 数，持续运行不随算法调用增长（实测恒定）。
- 不改 FINS 侧调度线程：禁用的是 cv 内部并行，FINS 的 worker/监听线程不受影响。

---

## 3. 状态型能力（有状态 OpenCV API 的无状态化）

`videoio`（VideoCapture/VideoWriter）、`video`（光流需上一帧、背景减除需历史模型）、
`ml`/`dnn`（需加载模型文件）、`objdetect`（级联模型）等 API 天然跨调用保持对象，无法用
纯函数签名表达。各模块源文件内就地定义**进程内按键持久对象缓存**（约 10 行，单文件
自包含）：

```cpp
template <typename T> std::shared_ptr<T> cache_get(const std::string &key);
```

key 由 cfg 参数（设备号/文件路径 + 参数字符串）拼成；对象首访创建、之后每次调用复用
（互斥保护）。因此这些算法对外仍是“configs-first、可复用”的无状态签名，对象生命周期
随插件 `.so`。

| 模块 | 缓存内容 | key |
|---|---|---|
| videoio | `VideoCapture`（相机/文件）、`VideoWriter`（尺寸取首帧） | `cam:<dev>` / `file:<path>` / `w:<path>\|fourcc\|fps` |
| video | 光流“上一帧”、`BackgroundSubtractorMOG2/KNN` | 固定/参数字符串 |
| ml | `Ptr<SVM>/<KNearest>/<ANN_MLP>`（模型文件） | `svm:\|knn:\|ann:` + 路径 |
| dnn | `cv::dnn::Net` | `model\|config\|framework` |

外部资产缺失（模型文件/相机/视频不存在）时**输出空帧而非抛异常**，保证图能持续运行；
cascade 的 xml 缺失等价空检测、不出框。

---

## 4. 算法清单

cfg 用法示例见 `pipeline/cfg_ocv_<模块>.json`。下表按 **配置段顺序** 列出参数（图→图，
输入恒为前一节点输出的 `cv::Mat`，输出经节点 `outputs` 端口送下一节点）。

### 4.1 core —— `plugins_opencv_core.so`（16）
`ocv_src`(源:梯度图 w×h)、`ocv_sink`(汇:丢弃)、算术/逻辑二元
`ocv_add/subtract/multiply/divide/absdiff/bitwise_and/bitwise_or/bitwise_xor/max/min`、
`ocv_add_weighted(alpha,beta,gamma)`、`ocv_convert_scale_abs(alpha,beta)`、
`ocv_sqrt`、`ocv_log`。

### 4.2 imgproc —— `plugins_opencv_imgproc.so`（20）
颜色/通道：`ocv_rgb2gray/gray2rgb/rgb2hsv/hsv2rgb`、`ocv_split_channel(idx)`、
`ocv_equalize_hist`；
几何：`ocv_resize(w,h,inter)`、`ocv_flip(code)`；
平滑：`ocv_gaussian_blur(ksize,sigma)`、`ocv_median_blur(ksize)`、`ocv_box_blur(ksize)`、
`ocv_bilateral(d,sigma_color,sigma_space)`；
形态：`ocv_erode/dilate(ksize,shape)`、`ocv_morph(op,ksize,shape)`；
阈值/边缘：`ocv_threshold(type,thresh,maxval)`、`ocv_canny(t1,t2)`、
`ocv_sobel(dx,dy,ksize)`、`ocv_laplacian(ksize)`、`ocv_adaptive_threshold(method,type,block,c)`。

### 4.3 photo —— `plugins_opencv_photo.so`（8）
`ocv_photo_denoise(h,tw,sw)`、`ocv_photo_denoise_gray(h,tw,sw)`、`ocv_photo_detail(s_s,s_r)`、
`ocv_photo_edge_preserve(s_s,s_r)`、`ocv_photo_stylize(s_s,s_r)`、
`ocv_photo_inpaint(radius; 第二输入=mask)`、`ocv_photo_illumination_change(alpha,beta; +mask)`、
`ocv_photo_texture_flatten(low,high,kernel)`。

### 4.4 features2d —— `plugins_opencv_features2d.so`（7）
关键点检测画图：`ocv_f2d_orb(nfeatures)`、`ocv_f2d_sift(nfeatures)`、
`ocv_f2d_fast(thresh)`、`ocv_f2d_akaze`、`ocv_f2d_good`；
描述子输出：`ocv_f2d_orb_desc(nfeatures)`、`ocv_f2d_sift_desc(nfeatures)`（输出为描述子
`cv::Mat`，供匹配/后续节点消费）。

### 4.5 imgcodecs —— `plugins_opencv_imgcodecs.so`（4）
`ocv_codec_imread(path)`(源) / `ocv_codec_imwrite(path)`(汇) 为文件 IO；
`ocv_codec_imencode(ext)`（Mat→字节流 `std::vector<uchar>`）与
`ocv_codec_imdecode`（字节流→Mat）为纯内存变换，cfg 示例即
`ocv_src → imencode → imdecode → ocv_sink` 闭环。

### 4.6 videoio —— `plugins_opencv_videoio.so`（3，状态缓存）
`ocv_vio_camera(device)`(源) / `ocv_vio_read(path)`(源,读到尾自动回卷) /
`ocv_vio_write(path,fourcc,fps)`(汇,尺寸取首帧)。外部资产要求：写 demo 先有视频文件，
或用 `ocv_vio_write` 先录一段（MJPG: `fourcc=0x47504A4D`）。

### 4.7 calib3d —— `plugins_opencv_calib3d.so`（5）
`ocv_calib_src_chessboard(cols,rows,square)`(源,棋盘格)；
`ocv_calib_undistort(fx,fy,cx,cy, k1,k2,p1,p2,k3)`（K 由标量就地构造）;
`ocv_calib_fisheye_undistort(fx,fy,cx,cy, k1..k4)`；
`ocv_calib_find_chessboard(cols,rows)`（角点画原图）;
`ocv_calib_chessboard_pose(cols,rows,square, fx,fy,cx,cy)`（solvePnP 画坐标轴）。
其余多视图几何需点集/相机矩阵等数据端口，未纳入单帧子集。

### 4.8 objdetect —— `plugins_opencv_objdetect.so`（3）
`ocv_detect_cascade(xml,scale,min_neighbors)`（Haar/LBP，xml 例
`/usr/share/opencv4/haarcascades/haarcascade_frontalface_default.xml`，按键缓存）;
`ocv_detect_hog`（内置默认行人检测器，无外部资产）；
`ocv_detect_qr`（QRCodeDetector 画四边形）。结果均为“叠加框画到原图”。

### 4.9 video —— `plugins_opencv_video.so`（4，状态缓存）
`ocv_video_farneback(pyr_scale,levels,winsize,itrs,poly_n,poly_sigma)`（稠密光流 HSV 可视化）;
`ocv_video_pyr_lk(max_corners,quality,min_dist)`（稀疏光流轨迹）；
`ocv_video_bgsub_mog2(history,var_th,shadows)` / `ocv_video_bgsub_knn(history,dist2,shadows)`
（前景掩码 8UC1）。均缓存“上一帧/历史模型”，静态图输出空/零，图可持续运行。

### 4.10 ml —— `plugins_opencv_ml.so`（4，状态缓存）
`ocv_ml_src_feat(n)`(源:确定性 1×n 特征行 CV_32F)；
`ocv_ml_svm_predict(path; feat→1×1 标签)`、`ocv_ml_knn_predict(path,k; …)`、
`ocv_ml_ann_predict(path; feat→输出向量)`。模型文件缺失 → 输出 0/空，不抛异常；提供真实
模型路径后即为在线推理节点。

### 4.11 dnn —— `plugins_opencv_dnn.so`（1，状态缓存）
`ocv_dnn_net_forward(model,config,framework, w,h)`：`readNet` 载入一次，每帧
`blobFromImage(w×h)` → `forward()`，输出张量展为二维 `cv::Mat`（分类 N×C / 分割 H×3W）
交下游。任务相关后处理（argmax/画框）由后续数据流节点承担。

---

## 5. 运行

```bash
# 1) 构建（重新配置后）
cmake -S . -B cmake-build-debug && cmake --build cmake-build-debug -j

# 2) 启动 agent（装载 plugins/ 下 plugin.so + 全部 plugins_opencv_*.so）
./client 18080 ./plugins

# 3) 另一终端应用某模块 demo cfg
./server pipeline/cfg_ocv_calib3d.json 18080     # → “pipeline applied: 5 vertices”
```

成功判据：client 日志出现 `pipeline applied: N vertices`，随后按 period 持续调度无异常。
替换 `ocv_src`/`ocv_sink`（core 模块）即可与其他模块 cfg 复用——loader 的算法 key 是进程
级合并的，跨 `.so` 引用同一名字合法。

---

## 6. 文件清单

| 文件 | 说明 |
|---|---|
| `example/plugins_opencv_{core,imgproc,photo,features2d,imgcodecs,videoio,calib3d,objdetect,video,ml,dnn}.cpp` | 11 个模块插件源（**单文件自包含**：内联 dlopen 线程禁用；有状态模块内联 `cache_get<T>` 与 to_gray/to_bgr 辅助） |
| `pipeline/cfg_ocv_{core,imgproc,features,photo,imgcodecs,videoio,calib3d,objdetect,video,ml,dnn}.json` | 每模块演示 cfg |
| `pipeline/plugins_opencv_<模块>.meta.json` | 每插件的 **sidecar meta**（算法名→参数 cfg/in/out·type·desc），与既有 `plugin.meta.json` 同 schema，供 viewer 端口锚点/hover/编辑（详见 §7） |
| `include/xmacro.hpp` | X-macro 导出宏 + sidecar meta JSON 格式（算法/参数元数据不进 .so） |

> 注：`*.json` 与 `*.so` 在仓库 `.gitignore` 中（`*.json`/`*.so`/`/plugins/`），cfg/meta/so 均为本地产物不纳入版本管理。

---

## 7. Sidecar meta（每插件一个 `.meta.json`）

`pipeline/` 下为每个插件 `.so` 各配一份 `<so 基名>.meta.json`（如 `plugins_opencv_core.so` →
`plugins_opencv_core.meta.json`），schema 与既有 `plugin.meta.json`（usr 插件）完全一致：

```json
{
  "ocv_gaussian_blur": {
    "desc": "高斯模糊",
    "params": [
      { "name": "ksize", "role": "cfg", "type": "int",    "desc": "核边长(偶数自动+1)" },
      { "name": "sigma", "role": "cfg", "type": "double", "desc": "σ(两轴同)" },
      { "name": "in",    "role": "in",  "type": "cv::Mat", "desc": "输入图" },
      { "name": "out",   "role": "out", "type": "cv::Mat&", "desc": "模糊图" }
    ]
  }
}
```

- 顶层 key = 算法名（= 函数名，cfg 的 `name` 即用它定位）；`params` 顺序 = 函数签名
  配置段→输入段→输出段，与 cfg `parameters[].value` 及端口锚点一一对应。
- 供 viewer 一次拖入全部 meta：加载后设计图节点按 cfg `inputs/outputs` 出端口锚点，
  hover/detail 展示每个端口的名字·类型·说明。
- 各模块算法前缀互不相同（`ocv_`/`ocv_photo_`/`ocv_f2d_`/`ocv_codec_`/`ocv_vio_`/
  `ocv_calib_`/`ocv_detect_`/`ocv_video_`/`ocv_ml_`/`ocv_dnn_`），多份 meta 同时载入
  不发生 key 覆盖。
- meta 仍刻意**不进 `.so`**：插件 `.so` 保持精简，元数据由作者侧另行维护、随 UI 分发。
