/*******************************************************************************
 * opencv_core.cpp — OpenCV core 模块插件（算术/转换，configs-first，cv::Mat 载荷）
 *
 * 签名约定（configs-first）：
 *   配置段参数在前（pipeline 的 parameters[].value 顺序）→ 输入段(cv::Mat) → 输出段(cv::Mat&)。
 * 说明：
 *   · 输出参数用 cv::Mat&（AlgoFunc 输出段 pub 新建载荷，算法写 out）；
 *   · 输入用 const cv::Mat&（共享 producer 帧，零拷贝；算法不得改输入）；
 *   · 无配置的算法可省配置段（如 ocv_add）。
 * 本文件 = 一个 .so（plugins/opencv_core.so），内所有算法共享同一 cv::Mat typeid。
 ******************************************************************************/
#include <opencv2/opencv.hpp>

#include "xmacro.hpp"

namespace {

// dlopen 时禁用 OpenCV 内部并行：cv::setNumThreads(0) → 函数顺序执行、不派发后台线程池
struct DisableOcvThreads_ {
  DisableOcvThreads_() { cv::setNumThreads(0); }
};
static DisableOcvThreads_ g_disable_ocv_threads_;

// 源：生成 w×h 的 8UC1 水平梯度（用于构造可观测输入，验证 cv::Mat 数据流）
void ocv_src(int w, int h, cv::Mat &out) {
  const int W = w > 1 ? w - 1 : 1;
  out = cv::Mat(h, w, CV_8UC1);
  for (int r = 0; r < h; ++r)
    for (int c = 0; c < w; ++c) out.at<uchar>(r, c) = (uchar)((c * 255) / W);
}

// 终端：消费一帧（空实现；占位验证用）
void ocv_sink(const cv::Mat &in) { (void)in; }

// —— 二元算术 / 逻辑 ——
void ocv_add(const cv::Mat &a, const cv::Mat &b, cv::Mat &out)        { cv::add(a, b, out); }
void ocv_subtract(const cv::Mat &a, const cv::Mat &b, cv::Mat &out)   { cv::subtract(a, b, out); }
void ocv_multiply(const cv::Mat &a, const cv::Mat &b, cv::Mat &out)   { cv::multiply(a, b, out); }
void ocv_divide(const cv::Mat &a, const cv::Mat &b, cv::Mat &out)     { cv::divide(a, b, out); }
void ocv_absdiff(const cv::Mat &a, const cv::Mat &b, cv::Mat &out)    { cv::absdiff(a, b, out); }
void ocv_bitwise_and(const cv::Mat &a, const cv::Mat &b, cv::Mat &out){ cv::bitwise_and(a, b, out); }
void ocv_bitwise_or(const cv::Mat &a, const cv::Mat &b, cv::Mat &out) { cv::bitwise_or(a, b, out); }
void ocv_bitwise_xor(const cv::Mat &a, const cv::Mat &b, cv::Mat &out){ cv::bitwise_xor(a, b, out); }
void ocv_max(const cv::Mat &a, const cv::Mat &b, cv::Mat &out)        { cv::max(a, b, out); }
void ocv_min(const cv::Mat &a, const cv::Mat &b, cv::Mat &out)        { cv::min(a, b, out); }

// —— 加权 / 线性变换 ——
void ocv_add_weighted(double alpha, double beta, double gamma,
                      const cv::Mat &a, const cv::Mat &b, cv::Mat &out) {
  cv::addWeighted(a, alpha, b, beta, gamma, out);
}
void ocv_convert_scale_abs(double alpha, double beta, const cv::Mat &in, cv::Mat &out) {
  cv::convertScaleAbs(in, out, alpha, beta);
}
void ocv_sqrt(const cv::Mat &in, cv::Mat &out) { cv::sqrt(in, out); }
void ocv_log(const cv::Mat &in, cv::Mat &out)  { cv::log(in, out); }

} // namespace

// 算法注册表（每函数名 = 一个算法；cfg 用 [name:1.0.0] 定位）
#define FINS_ALGO_LIST(F) \
  F(ocv_src)              \
  F(ocv_sink)             \
  F(ocv_add)              \
  F(ocv_subtract)         \
  F(ocv_multiply)         \
  F(ocv_divide)           \
  F(ocv_absdiff)          \
  F(ocv_bitwise_and)      \
  F(ocv_bitwise_or)       \
  F(ocv_bitwise_xor)      \
  F(ocv_max)              \
  F(ocv_min)              \
  F(ocv_add_weighted)     \
  F(ocv_convert_scale_abs)\
  F(ocv_sqrt)             \
  F(ocv_log)

FINS_ALGO_EXPORT("1.0.0")
