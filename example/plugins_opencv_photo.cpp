/*******************************************************************************
 * opencv_photo.cpp — OpenCV photo 模块插件（configs-first，cv::Mat 载荷）
 ******************************************************************************/
#include <opencv2/opencv.hpp>

#include "xmacro.hpp"

namespace {

// dlopen 时禁用 OpenCV 内部并行：cv::setNumThreads(0) → 函数顺序执行、不派发后台线程池
struct DisableOcvThreads_ {
  DisableOcvThreads_() { cv::setNumThreads(0); }
};
static DisableOcvThreads_ g_disable_ocv_threads_;

void ocv_photo_denoise(int h, int tw, int sw, const cv::Mat &in, cv::Mat &out) {
  cv::fastNlMeansDenoisingColored(in, out, (float)h, (float)h, tw, sw);
}
void ocv_photo_denoise_gray(int h, int tw, int sw, const cv::Mat &in, cv::Mat &out) {
  cv::fastNlMeansDenoising(in, out, (float)h, tw, sw);
}
void ocv_photo_detail(double sigma_s, double sigma_r, const cv::Mat &in, cv::Mat &out) {
  cv::detailEnhance(in, out, sigma_s, sigma_r);
}
void ocv_photo_edge_preserve(double sigma_s, double sigma_r, const cv::Mat &in, cv::Mat &out) {
  cv::edgePreservingFilter(in, out, cv::RECURS_FILTER, sigma_s, sigma_r);
}
void ocv_photo_stylize(double sigma_s, double sigma_r, const cv::Mat &in, cv::Mat &out) {
  cv::stylization(in, out, sigma_s, sigma_r);
}
void ocv_photo_inpaint(double radius, const cv::Mat &in, const cv::Mat &mask, cv::Mat &out) {
  cv::inpaint(in, mask, out, radius, cv::INPAINT_TELEA);
}
void ocv_photo_illumination_change(double alpha, double beta, const cv::Mat &in, cv::Mat &mask, cv::Mat &out) {
  cv::illuminationChange(in, mask, out, alpha, beta);
}
void ocv_photo_texture_flatten(double low_th, double high_th, int kernel, const cv::Mat &in, cv::Mat &out) {
  cv::textureFlattening(in, cv::Mat::zeros(in.size(), CV_8UC1), out, low_th, high_th, kernel);
}

} // namespace

#define FINS_ALGO_LIST(F) \
  F(ocv_photo_denoise)        \
  F(ocv_photo_denoise_gray)   \
  F(ocv_photo_detail)         \
  F(ocv_photo_edge_preserve)  \
  F(ocv_photo_stylize)        \
  F(ocv_photo_inpaint)        \
  F(ocv_photo_illumination_change) \
  F(ocv_photo_texture_flatten)

FINS_ALGO_EXPORT("1.0.0")
