/*******************************************************************************
 * plugins_opencv_objdetect.cpp — OpenCV objdetect 模块插件（configs-first）
 * 检测结果统一“叠加绘制到原图输出”（BGR Mat 载荷）：
 *   · cascade 需外置级联 xml（如 /usr/share/opencv4/haarcascades/*.xml），按键缓存；
 *   · HOG 用内置默认行人检测器（无外部资产）；
 *   · QR 用内置 QRCodeDetector。
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

// 灰度化 / 上色（单通道直用，否则 BGR↔Gray）
cv::Mat to_gray(const cv::Mat &m) {
  if (m.empty() || m.channels() == 1) return m;
  cv::Mat g;
  cv::cvtColor(m, g, cv::COLOR_BGR2GRAY);
  return g;
}
cv::Mat to_bgr(const cv::Mat &m) {
  if (m.empty()) return cv::Mat();
  if (m.channels() == 3) return m;
  cv::Mat c;
  cv::cvtColor(m, c, cv::COLOR_GRAY2BGR);
  return c;
}

/// 按键持久对象缓存（级联分类器等跨调用保持对象；key = 模型路径）
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

/// 级联分类器缓存（每 xml 一份）
struct Cascade { cv::CascadeClassifier cc; };

// 目标检测：Haar/LBP 级联（scale = 尺度步长，min_neighbors = 最小邻域）
void ocv_detect_cascade(const std::string &xml, double scale, int min_neighbors,
                        const cv::Mat &in, cv::Mat &out) {
  auto st = cache_get<Cascade>(xml);
  if (st->cc.empty()) st->cc.load(xml);   // xml 缺失 → empty，不抛、不出框
  out = to_bgr(in).clone();
  if (st->cc.empty() || out.empty()) return;
  std::vector<cv::Rect> objs;
  st->cc.detectMultiScale(to_gray(in), objs, scale, min_neighbors);
  for (const auto &r : objs) cv::rectangle(out, r, cv::Scalar(0, 255, 0), 2);
}

/// HOG 行人检测（内置默认检测器；无外部资产）
void ocv_detect_hog(const cv::Mat &in, cv::Mat &out) {
  struct Hog { cv::HOGDescriptor h; bool init = false; };
  auto st = cache_get<Hog>("people");
  if (!st->init) {
    st->h.setSVMDetector(cv::HOGDescriptor::getDefaultPeopleDetector());
    st->init = true;
  }
  out = to_bgr(in).clone();
  if (st->init && !out.empty()) {
    std::vector<cv::Rect> found;
    st->h.detectMultiScale(to_gray(in), found, 0, cv::Size(8, 8),
                           cv::Size(32, 32), 1.05, 2);
    for (const auto &r : found) cv::rectangle(out, r, cv::Scalar(0, 255, 0), 2);
  }
}

// QR 码检测：找到则把四边形轮廓画到原图
void ocv_detect_qr(const cv::Mat &in, cv::Mat &out) {
  out = to_bgr(in).clone();
  if (out.empty()) return;
  cv::QRCodeDetector qr;
  std::vector<cv::Point> pts;
  const bool ok = qr.detect(in, pts);
  if (ok && pts.size() >= 4) {
    const size_t n = pts.size();
    for (size_t i = 0; i < n; ++i)
      cv::line(out, pts[i], pts[(i + 1) % n], cv::Scalar(0, 255, 0), 2);
  }
}

}  // namespace

#define FINS_ALGO_LIST(F) \
  F(ocv_detect_cascade)   \
  F(ocv_detect_hog)       \
  F(ocv_detect_qr)

FINS_ALGO_EXPORT("1.0.0")
