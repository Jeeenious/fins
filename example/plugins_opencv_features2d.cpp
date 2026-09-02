/*******************************************************************************
 * opencv_features2d.cpp — OpenCV features2d 模块插件（configs-first，cv::Mat 载荷）
 * 关键点结果以“绘制图”输出（便于可视化）；另提供描述子输出（cv::Mat 载荷）。
 ******************************************************************************/
#include <opencv2/opencv.hpp>

#include "xmacro.hpp"

namespace {

// dlopen 时禁用 OpenCV 内部并行：cv::setNumThreads(0) → 函数顺序执行、不派发后台线程池
struct DisableOcvThreads_ {
  DisableOcvThreads_() { cv::setNumThreads(0); }
};
static DisableOcvThreads_ g_disable_ocv_threads_;

cv::Mat to_gray(const cv::Mat &m) {
  if (m.channels() == 1) return m;
  cv::Mat g; cv::cvtColor(m, g, cv::COLOR_BGR2GRAY); return g;
}

// —— 关键点绘制（检测结果可视化为 Mat）——
void ocv_f2d_orb(int nfeatures, const cv::Mat &in, cv::Mat &out) {
  std::vector<cv::KeyPoint> kp;
  auto d = cv::ORB::create(nfeatures > 0 ? nfeatures : 500);
  d->detect(to_gray(in), kp);
  cv::drawKeypoints(in, kp, out);
}
void ocv_f2d_sift(int nfeatures, const cv::Mat &in, cv::Mat &out) {
  std::vector<cv::KeyPoint> kp;
  auto d = cv::SIFT::create(nfeatures > 0 ? nfeatures : 0);
  d->detect(to_gray(in), kp);
  cv::drawKeypoints(in, kp, out);
}
void ocv_f2d_fast(int thresh, const cv::Mat &in, cv::Mat &out) {
  std::vector<cv::KeyPoint> kp;
  auto d = cv::FastFeatureDetector::create(thresh > 0 ? thresh : 10);
  d->detect(to_gray(in), kp);
  cv::drawKeypoints(in, kp, out);
}
void ocv_f2d_akaze(const cv::Mat &in, cv::Mat &out) {
  std::vector<cv::KeyPoint> kp;
  auto d = cv::AKAZE::create();
  d->detect(to_gray(in), kp);
  cv::drawKeypoints(in, kp, out);
}
void ocv_f2d_good(const cv::Mat &in, cv::Mat &out) {
  std::vector<cv::Point2f> corners;
  cv::goodFeaturesToTrack(to_gray(in), corners, 50, 0.01, 10);
  out = in.clone();
  for (const auto &p : corners) cv::circle(out, p, 3, cv::Scalar(0, 0, 255), -1);
}

// —— 描述子输出（供后续匹配步骤消费）——
void ocv_f2d_orb_desc(int nfeatures, const cv::Mat &in, cv::Mat &out) {
  std::vector<cv::KeyPoint> kp;
  auto d = cv::ORB::create(nfeatures > 0 ? nfeatures : 500);
  d->detectAndCompute(to_gray(in), cv::noArray(), kp, out);
}
void ocv_f2d_sift_desc(int nfeatures, const cv::Mat &in, cv::Mat &out) {
  std::vector<cv::KeyPoint> kp;
  auto d = cv::SIFT::create(nfeatures > 0 ? nfeatures : 0);
  d->detectAndCompute(to_gray(in), cv::noArray(), kp, out);
}

} // namespace

#define FINS_ALGO_LIST(F) \
  F(ocv_f2d_orb)      \
  F(ocv_f2d_sift)     \
  F(ocv_f2d_fast)     \
  F(ocv_f2d_akaze)    \
  F(ocv_f2d_good)     \
  F(ocv_f2d_orb_desc) \
  F(ocv_f2d_sift_desc)

FINS_ALGO_EXPORT("1.0.0")
