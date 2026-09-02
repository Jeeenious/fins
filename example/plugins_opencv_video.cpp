/*******************************************************************************
 * plugins_opencv_video.cpp — OpenCV video 模块插件（configs-first）
 * 视频分析里可“逐帧流式”的部分：
 *   · 稠密光流 Farneback / 稀疏光流 PyrLK —— 需要“上一帧”，经 ocv_plug 缓存；
 *   · 背景减除 MOG2/KNN —— 需要历史模型，缓存创建 BackgroundSubtractor。
 * 所有函数输出 cv::Mat 帧（光流输出为 HSV→BGR 可视化图；减背景输出 8UC1 前景掩码）。
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

/// 按键持久对象缓存（上一帧/背景模型等跨调用保持对象；key = 配置参数串）
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

// 稠密光流（Farneback）：与上一帧比，输出 HSV→BGR 方向/幅值可视化图
void ocv_video_farneback(double pyr_scale, int levels, int winsize, int iterations,
                         int poly_n, double poly_sigma, const cv::Mat &in, cv::Mat &out) {
  struct FlowPrev { cv::Mat prev; };
  auto st = cache_get<FlowPrev>("farneback");
  const cv::Mat gray = to_gray(in);
  out = to_bgr(gray);
  if (gray.empty()) { st->prev.release(); return; }
  if (st->prev.empty()) { st->prev = gray.clone(); return; }   // 首帧只缓存

  cv::Mat flow;
  cv::calcOpticalFlowFarneback(st->prev, gray, flow, pyr_scale, levels, winsize,
                               iterations, poly_n, poly_sigma, 0);
  st->prev = gray.clone();
  if (flow.empty()) return;

  std::vector<cv::Mat> fxy;
  cv::split(flow, fxy);                    // fxy[0]=x, fxy[1]=y (CV_32F)
  cv::Mat mag, ang;
  cv::cartToPolar(fxy[0], fxy[1], mag, ang, true);   // ang ∈ [0,360)

  cv::Mat hsv(gray.size(), CV_8UC3, cv::Scalar::all(0));
  cv::Mat ch[3];
  cv::split(hsv, ch);
  double mx = 0;
  cv::minMaxLoc(mag, nullptr, &mx);
  if (mx <= 1e-6) ch[2] = cv::Mat::zeros(gray.size(), CV_8UC1);   // 静止：全黑
  else mag.convertTo(ch[2], CV_8U, 255.0 / mx);
  ang.convertTo(ch[0], CV_8U, 0.5);                              // hue：0..360→0..180
  ch[1] = cv::Mat::ones(gray.size(), CV_8UC1) * 255;             // 饱和度 255
  cv::merge(ch, 3, hsv);
  cv::cvtColor(hsv, out, cv::COLOR_HSV2BGR);
}

// 稀疏光流（PyrLK）：跟踪上一帧角点，画运动轨迹
void ocv_video_pyr_lk(int max_corners, double quality, double min_dist,
                      const cv::Mat &in, cv::Mat &out) {
  struct LkState { cv::Mat prev; std::vector<cv::Point2f> pts; };
  auto st = cache_get<LkState>("lk");
  const cv::Mat gray = to_gray(in);
  out = to_bgr(gray);
  if (gray.empty()) { st->prev.release(); st->pts.clear(); return; }

  const int nc = max_corners > 0 ? max_corners : 100;
  const double q = quality > 0 ? quality : 0.01;
  const double md = min_dist > 0 ? min_dist : 10;
  if (st->prev.empty() || st->pts.empty()) {                    // 首帧：取角点
    cv::goodFeaturesToTrack(gray, st->pts, nc, q, md);
    st->prev = gray.clone();
    return;
  }

  std::vector<cv::Point2f> nxt;
  std::vector<uchar> status;
  std::vector<float> err;
  cv::calcOpticalFlowPyrLK(st->prev, gray, st->pts, nxt, status, err);
  std::vector<cv::Point2f> good;
  for (size_t i = 0; i < st->pts.size(); ++i) {
    if (status[i]) {
      cv::line(out, st->pts[i], nxt[i], cv::Scalar(0, 255, 0), 1);
      good.push_back(nxt[i]);
    }
  }
  if (good.size() < 10) cv::goodFeaturesToTrack(gray, good, nc, q, md);  // 丢太多则重取
  st->prev = gray.clone();
  st->pts = std::move(good);
}

// 背景减除 MOG2：输出前景掩码 8UC1
void ocv_video_bgsub_mog2(int history, double var_th, int shadows, const cv::Mat &in, cv::Mat &out) {
  const std::string key = "mog2|h" + std::to_string(history) + "|t" +
                          std::to_string((long long)var_th) + "|s" + std::to_string(shadows);
  auto sub = cache_get<cv::Ptr<cv::BackgroundSubtractorMOG2>>(key);
  if (sub->empty()) *sub = cv::createBackgroundSubtractorMOG2(history, var_th, shadows != 0);
  const cv::Mat gray = to_gray(in);
  if (gray.empty()) { out.release(); return; }
  (*sub)->apply(gray, out);
}

// 背景减除 KNN：输出前景掩码 8UC1
void ocv_video_bgsub_knn(int history, double dist2, int shadows, const cv::Mat &in, cv::Mat &out) {
  const std::string key = "knn|h" + std::to_string(history) + "|d" +
                          std::to_string((long long)dist2) + "|s" + std::to_string(shadows);
  auto sub = cache_get<cv::Ptr<cv::BackgroundSubtractorKNN>>(key);
  if (sub->empty()) *sub = cv::createBackgroundSubtractorKNN(history, dist2, shadows != 0);
  const cv::Mat gray = to_gray(in);
  if (gray.empty()) { out.release(); return; }
  (*sub)->apply(gray, out);
}

}  // namespace

#define FINS_ALGO_LIST(F) \
  F(ocv_video_farneback)  \
  F(ocv_video_pyr_lk)     \
  F(ocv_video_bgsub_mog2) \
  F(ocv_video_bgsub_knn)

FINS_ALGO_EXPORT("1.0.0")
