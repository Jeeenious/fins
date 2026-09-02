/*******************************************************************************
 * plugins_opencv_videoio.cpp — OpenCV videoio 模块插件（configs-first）
 * 视频/相机采集源与视频写出汇。VideoCapture / VideoWriter 必须跨调用保持对象，
 * 故经 ocv_plug 按键缓存：key = 设备号/文件路径 + 参数串，进程内首访创建、复用。
 * 帧空（设备打不开/文件读到尾已回卷）时输出空帧，不抛异常。
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

/// 按键持久对象缓存（VideoCapture/VideoWriter 跨调用保持对象；key = 配置参数串）
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

/// 采集缓存对象（每 key 一个 VideoCapture）
struct VioCapture { cv::VideoCapture cap; };

// 源：USB/相机采集（device = 设备号，如 0）
void ocv_vio_camera(int device, cv::Mat &out) {
  try {
    auto st = cache_get<VioCapture>("cam:" + std::to_string(device));
    if (!st->cap.isOpened()) st->cap.open(device);
    if (!st->cap.isOpened() || !st->cap.read(out)) out.release();
  } catch (...) {
    out.release();
  }
}

// 源：读视频文件；读到文件尾自动回卷第 0 帧（循环播放）
void ocv_vio_read(const std::string &path, cv::Mat &out) {
  try {
    auto st = cache_get<VioCapture>("file:" + path);
    if (!st->cap.isOpened()) st->cap.open(path);
    if (!st->cap.isOpened()) { out.release(); return; }
    if (!st->cap.read(out)) {
      st->cap.set(cv::CAP_PROP_POS_FRAMES, 0);
      if (!st->cap.read(out)) out.release();
    }
  } catch (...) {
    out.release();
  }
}

/// 写出缓存对象（每 key 一个 VideoWriter；尺寸取首帧）
struct VioWriter {
  cv::VideoWriter w;
  cv::Size size{0, 0};
};

// 汇：把帧追加写入视频文件（fourcc = VideoWriter::fourcc 数值，如 MJPG=0x47504A4D；
// fps = 目标帧率；路径含 .avi/.mp4）
void ocv_vio_write(const std::string &path, int fourcc, double fps, const cv::Mat &in) {
  try {
    if (in.empty()) return;
    const std::string key = "w:" + path + "|" + std::to_string(fourcc) + "|" +
                            std::to_string((long long)fps);
    auto st = cache_get<VioWriter>(key);
    if (!st->w.isOpened() || st->size != in.size()) {
      if (st->w.isOpened()) st->w.release();
      st->w.open(path, fourcc, fps, in.size(), in.channels() > 1);
      st->size = in.size();
    }
    if (st->w.isOpened()) st->w.write(in);
  } catch (...) {}
}

}  // namespace

#define FINS_ALGO_LIST(F) \
  F(ocv_vio_camera)       \
  F(ocv_vio_read)         \
  F(ocv_vio_write)

FINS_ALGO_EXPORT("1.0.0")
