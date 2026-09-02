/*******************************************************************************
 * plugins_opencv_dnn.cpp — OpenCV dnn 模块插件（configs-first）
 * 深度网络通用前向推理：readNet(model[, config, framework]) 载入一次（按键缓存），
 * 每帧 blobFromImage 预处理 → forward()，输出张量展成二维 Mat（如分类 N×C / 分割
 * H×(3W)）交给下游。模型/配置文件缺失 → 输出空帧，不抛异常。
 * 具体任务的输出后处理（argmax/阈值/画框）由后续数据流节点承担。
 ******************************************************************************/
#include <opencv2/opencv.hpp>

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "xmacro.hpp"

namespace {

// dlopen 时禁用 OpenCV 内部并行：cv::setNumThreads(0) → 函数顺序执行、不派发后台线程池
struct DisableOcvThreads_ {
  DisableOcvThreads_() { cv::setNumThreads(0); }
};
static DisableOcvThreads_ g_disable_ocv_threads_;

/// 按键持久对象缓存（Net 跨调用保持；key = model|config|framework）
template <typename T>
std::shared_ptr<T> cache_get(const std::string &key) {
  static std::mutex mtx;
  static std::unordered_map<std::string, std::shared_ptr<T>> store;
  std::lock_guard<std::mutex> lk(mtx);
  auto it = store.find(key);
  if (it != store.end()) return it->second;
  auto p = std::make_shared<T>();
  store.emplace(key, p);
  return p;
}

/// 网络缓存（每 [model|config|framework] 一份）
struct DnnNet {
  cv::dnn::Net net;
  bool loaded = false;
};

// 通用 forward：model = 权重文件；config = 结构文件(可选)；framework 空则自动推断；
// width/height = 送入网络的分辨率
void ocv_dnn_net_forward(const std::string &model, const std::string &config,
                         const std::string &framework, int width, int height,
                         const cv::Mat &in, cv::Mat &out) {
  const std::string key = model + "|" + config + "|" + framework;
  auto st = cache_get<DnnNet>(key);
  if (!st->loaded) {
    st->loaded = true;
    try { st->net = cv::dnn::readNet(model, config, framework); } catch (...) {}
  }
  out.release();
  if (st->net.empty() || in.empty() || width <= 0 || height <= 0) return;
  try {
    const cv::Mat blob =
        cv::dnn::blobFromImage(in, 1.0, cv::Size(width, height), cv::Scalar(),
                               false /*swapRB*/, false /*crop*/, CV_32F);
    st->net.setInput(blob);
    cv::Mat o = st->net.forward();   // 常见形状：(1,1,N,C) 分类 / (1,3,H,W) 分割
    if (o.empty()) return;
    if (o.dims > 2) o = o.reshape(1, o.size[o.dims - 2]);   // 展成 N×C 二维
    out = o.clone();
  } catch (...) {}
}

}  // namespace

#define FINS_ALGO_LIST(F) \
  F(ocv_dnn_net_forward)

FINS_ALGO_EXPORT("1.0.0")
