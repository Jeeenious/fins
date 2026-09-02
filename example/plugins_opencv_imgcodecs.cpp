/*******************************************************************************
 * plugins_opencv_imgcodecs.cpp — OpenCV imgcodecs 模块插件（configs-first）
 * 图像文件的读/写 + 内存编解码。载荷约定：
 *   cv::Mat            — 图像帧（BGR）
 *   std::vector<uchar> — 编码字节流（imencode 输出 / imdecode 输入）
 * imread/imwrite 属“源/汇”类算法（每次调用重读/写文件）；
 * imencode/imdecode 为纯内存变换，可直接串成数据流。
 ******************************************************************************/
#include <opencv2/opencv.hpp>

#include "xmacro.hpp"

namespace {

// dlopen 时禁用 OpenCV 内部并行：cv::setNumThreads(0) → 函数顺序执行、不派发后台线程池
struct DisableOcvThreads_ {
  DisableOcvThreads_() { cv::setNumThreads(0); }
};
static DisableOcvThreads_ g_disable_ocv_threads_;

// 源：从文件读入一帧（IMREAD_COLOR；文件不存在→空帧）
void ocv_codec_imread(const std::string &path, cv::Mat &out) {
  out = cv::imread(path, cv::IMREAD_COLOR);
}

// 汇：把一帧写入图像文件（按扩展名编码）
void ocv_codec_imwrite(const std::string &path, const cv::Mat &in) {
  cv::imwrite(path, in);
}

// Mat → 内存字节流（ext 形如 ".png" / ".jpg"，须带点）
void ocv_codec_imencode(const std::string &ext, const cv::Mat &in, std::vector<uchar> &out) {
  cv::imencode(ext, in, out);
}

// 内存字节流 → Mat（IMREAD_COLOR；空流→空帧）
void ocv_codec_imdecode(const std::vector<uchar> &buf, cv::Mat &out) {
  if (buf.empty()) {
    out.release();
    return;
  }
  out = cv::imdecode(buf, cv::IMREAD_COLOR);
}

}  // namespace

#define FINS_ALGO_LIST(F) \
  F(ocv_codec_imread)     \
  F(ocv_codec_imwrite)    \
  F(ocv_codec_imencode)   \
  F(ocv_codec_imdecode)

FINS_ALGO_EXPORT("1.0.0")
