/*******************************************************************************
 * opencv_imgproc.cpp — OpenCV imgproc 模块插件（configs-first，cv::Mat 载荷）
 * 命名即 opencv 语义；cfg parameters 顺序 = 配置段顺序（本文件头注释在每个函数旁标注）。
 ******************************************************************************/
#include <opencv2/opencv.hpp>

#include "xmacro.hpp"

namespace {

// dlopen 时禁用 OpenCV 内部并行：cv::setNumThreads(0) → 函数顺序执行、不派发后台线程池
struct DisableOcvThreads_ {
  DisableOcvThreads_() { cv::setNumThreads(0); }
};
static DisableOcvThreads_ g_disable_ocv_threads_;

// ── 颜色 / 通道 ──
void ocv_rgb2gray(const cv::Mat &in, cv::Mat &out)        { cv::cvtColor(in, out, cv::COLOR_RGB2GRAY); }
void ocv_gray2rgb(const cv::Mat &in, cv::Mat &out)        { cv::cvtColor(in, out, cv::COLOR_GRAY2RGB); }
void ocv_rgb2hsv(const cv::Mat &in, cv::Mat &out)         { cv::cvtColor(in, out, cv::COLOR_RGB2HSV); }
void ocv_hsv2rgb(const cv::Mat &in, cv::Mat &out)         { cv::cvtColor(in, out, cv::COLOR_HSV2RGB); }
void ocv_split_channel(int idx, const cv::Mat &in, cv::Mat &out) { std::vector<cv::Mat> ch; cv::split(in, ch); out = (idx >= 0 && idx < (int)ch.size()) ? ch[idx] : in.clone(); }
void ocv_equalize_hist(const cv::Mat &in, cv::Mat &out)   { cv::equalizeHist(in, out); }

// ── 几何变换 ──
void ocv_resize(int width, int height, int inter, const cv::Mat &in, cv::Mat &out) {
  cv::resize(in, out, cv::Size(width, height), 0, 0, inter);
}
void ocv_flip(int code, const cv::Mat &in, cv::Mat &out)  { cv::flip(in, out, code); }

// ── 平滑 / 滤波 ──
void ocv_gaussian_blur(int ksize, double sigma, const cv::Mat &in, cv::Mat &out) {
  if (ksize % 2 == 0) ksize += 1;
  cv::GaussianBlur(in, out, cv::Size(ksize, ksize), sigma, sigma);
}
void ocv_median_blur(int ksize, const cv::Mat &in, cv::Mat &out) {
  if (ksize % 2 == 0) ksize += 1;
  cv::medianBlur(in, out, ksize);
}
void ocv_box_blur(int ksize, const cv::Mat &in, cv::Mat &out) {
  cv::blur(in, out, cv::Size(ksize, ksize));
}
void ocv_bilateral(int d, double sigma_color, double sigma_space, const cv::Mat &in, cv::Mat &out) {
  cv::bilateralFilter(in, out, d, sigma_color, sigma_space);
}

// ── 形态学 ──
void ocv_erode(int ksize, int shape, const cv::Mat &in, cv::Mat &out) {
  cv::erode(in, out, cv::getStructuringElement(shape, cv::Size(ksize, ksize)));
}
void ocv_dilate(int ksize, int shape, const cv::Mat &in, cv::Mat &out) {
  cv::dilate(in, out, cv::getStructuringElement(shape, cv::Size(ksize, ksize)));
}
void ocv_morph(int op, int ksize, int shape, const cv::Mat &in, cv::Mat &out) {
  cv::morphologyEx(in, out, op, cv::getStructuringElement(shape, cv::Size(ksize, ksize)));
}

// ── 阈值 / 边缘 ──
void ocv_threshold(int type, double thresh, double maxval, const cv::Mat &in, cv::Mat &out) {
  cv::threshold(in, out, thresh, maxval, type);
}
void ocv_canny(double t1, double t2, const cv::Mat &in, cv::Mat &out) {
  cv::Canny(in, out, t1, t2);
}
void ocv_sobel(int dx, int dy, int ksize, const cv::Mat &in, cv::Mat &out) {
  cv::Sobel(in, out, CV_8U, dx, dy, ksize);
}
void ocv_laplacian(int ksize, const cv::Mat &in, cv::Mat &out) {
  cv::Laplacian(in, out, CV_8U, ksize);
}

// ── 自适应阈值（imgproc 特有；convertScaleAbs 已在 core 模块） ──
void ocv_adaptive_threshold(int adaptive_method, int threshold_type, int block_size, double c,
                            const cv::Mat &in, cv::Mat &out) {
  if (block_size % 2 == 0) block_size += 1;
  cv::adaptiveThreshold(in, out, 255, adaptive_method, threshold_type, block_size, c);
}

} // namespace

#define FINS_ALGO_LIST(F) \
  F(ocv_rgb2gray)         \
  F(ocv_gray2rgb)         \
  F(ocv_rgb2hsv)          \
  F(ocv_hsv2rgb)          \
  F(ocv_split_channel)    \
  F(ocv_equalize_hist)    \
  F(ocv_resize)           \
  F(ocv_flip)             \
  F(ocv_gaussian_blur)    \
  F(ocv_median_blur)      \
  F(ocv_box_blur)         \
  F(ocv_bilateral)        \
  F(ocv_erode)            \
  F(ocv_dilate)           \
  F(ocv_morph)            \
  F(ocv_threshold)        \
  F(ocv_canny)            \
  F(ocv_sobel)            \
  F(ocv_laplacian)        \
  F(ocv_adaptive_threshold)

FINS_ALGO_EXPORT("1.0.0")
