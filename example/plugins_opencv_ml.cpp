/*******************************************************************************
 * plugins_opencv_ml.cpp — OpenCV ml 模块插件（configs-first）
 * 监督模型“离线训练 + 在线推理”里的在线部分：读训练好的模型文件做预测。
 * 模型经 ocv_plug 按键缓存（key = 模型路径），首访 load、之后复用；
 * feat 为特征行（CV_32F，1×N 或 N×1），out 为预测结果（SVM/KNN → 1×1 标签；
 * ANN → 输出向量）。模型文件缺失 → 预测 0 / 空输出，不抛异常。
 ******************************************************************************/
#include <cmath>
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

/// 按键持久对象缓存（模型 Ptr 跨调用保持；key = 模型路径）
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

// 源：生成确定性的 1×n 特征行 CV_32F（sin 采样），供无外部模型文件的预测 demo 自洽运行
void ocv_ml_src_feat(int n, cv::Mat &out) {
  const int N = n > 0 ? n : 8;
  out = cv::Mat(1, N, CV_32F);
  for (int j = 0; j < N; ++j) out.at<float>(j) = (float)std::sin(0.35 * j);
}

// SVM 预测（模型路径由 cfg 指定；分类/回归按训练时类型返回标签/数值）
void ocv_ml_svm_predict(const std::string &path, const cv::Mat &feat, cv::Mat &out) {
  auto model = cache_get<cv::Ptr<cv::ml::SVM>>("svm:" + path);
  if (model->empty()) *model = cv::ml::SVM::load(path);
  out = cv::Mat(1, 1, CV_32F, cv::Scalar(0.f));
  if (model->empty() || feat.empty()) return;
  try { out.at<float>(0) = (*model)->predict(feat); } catch (...) {}
}

// KNN 预测（k = 近邻数；输出最近邻类别）
void ocv_ml_knn_predict(const std::string &path, int k, const cv::Mat &feat, cv::Mat &out) {
  auto model = cache_get<cv::Ptr<cv::ml::KNearest>>("knn:" + path);
  if (model->empty()) *model = cv::ml::KNearest::load(path);
  out = cv::Mat(1, 1, CV_32F, cv::Scalar(0.f));
  if (model->empty() || feat.empty()) return;
  try {
    cv::Mat res;
    const float r = (*model)->findNearest(feat, k > 0 ? k : 1, res);
    out.at<float>(0) = r;
  } catch (...) {}
}

// ANN 多层感知机预测（输出 = 网络输出向量，如各类别得分）
void ocv_ml_ann_predict(const std::string &path, const cv::Mat &feat, cv::Mat &out) {
  auto model = cache_get<cv::Ptr<cv::ml::ANN_MLP>>("ann:" + path);
  if (model->empty()) *model = cv::ml::ANN_MLP::load(path);
  out = cv::Mat(1, 1, CV_32F, cv::Scalar(0.f));
  if (model->empty() || feat.empty()) return;
  try {
    cv::Mat res;
    (*model)->predict(feat, res);
    if (!res.empty()) out = res.clone();
  } catch (...) {}
}

}  // namespace

#define FINS_ALGO_LIST(F) \
  F(ocv_ml_src_feat)      \
  F(ocv_ml_svm_predict)   \
  F(ocv_ml_knn_predict)   \
  F(ocv_ml_ann_predict)

FINS_ALGO_EXPORT("1.0.0")
