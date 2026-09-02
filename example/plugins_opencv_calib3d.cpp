/*******************************************************************************
 * plugins_opencv_calib3d.cpp — OpenCV calib3d 模块插件（configs-first）
 * 几何/标定里可“单帧 + 标量配置”表达的部分：相机内参 K 由标量 fx/fy/cx/cy
 * 就地构造，畸变系数 k/p 为标量配置——无需跨帧点集/矩阵数据端口。
 * 棋盘格源 + 找格点 + 位姿估计自成闭环，便于无外部资产直接验证。
 ******************************************************************************/
#include <opencv2/opencv.hpp>

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

// 源：生成 cols×rows 黑白棋盘格 CV_8UC1（square = 格边长像素），供 find_chessboard 等
void ocv_calib_src_chessboard(int cols, int rows, int square, cv::Mat &out) {
  const int w = cols * square, h = rows * square;
  out = cv::Mat(h, w, CV_8UC1, cv::Scalar::all(255));
  for (int r = 0; r < rows; ++r)
    for (int c = 0; c < cols; ++c)
      if ((r + c) % 2 == 0)
        out(cv::Rect(c * square, r * square, square, square)).setTo(0);
}

// 针孔畸变矫正（K 由 fx/fy/cx/cy 构造；dist = k1 k2 p1 p2 k3）
void ocv_calib_undistort(double fx, double fy, double cx, double cy,
                         double k1, double k2, double p1, double p2, double k3,
                         const cv::Mat &in, cv::Mat &out) {
  if (in.empty()) { out.release(); return; }
  const cv::Mat K = (cv::Mat_<double>(3, 3) << fx, 0, cx, 0, fy, cy, 0, 0, 1);
  const cv::Mat D = (cv::Mat_<double>(1, 5) << k1, k2, p1, p2, k3);
  cv::undistort(in, out, K, D);
}

// 鱼眼畸变矫正（fisheye 模型 4 系数）
void ocv_calib_fisheye_undistort(double fx, double fy, double cx, double cy,
                                 double k1, double k2, double k3, double k4,
                                 const cv::Mat &in, cv::Mat &out) {
  if (in.empty()) { out.release(); return; }
  const cv::Mat K = (cv::Mat_<double>(3, 3) << fx, 0, cx, 0, fy, cy, 0, 0, 1);
  const cv::Mat D = (cv::Mat_<double>(1, 4) << k1, k2, k3, k4);
  cv::fisheye::undistortImage(in, out, K, D);
}

// 找棋盘格角点并画在原图上（out = 叠加绘制的 BGR 图）
void ocv_calib_find_chessboard(int cols, int rows, const cv::Mat &in, cv::Mat &out) {
  out = to_bgr(in).clone();
  if (out.empty()) return;
  std::vector<cv::Point2f> corners;
  const bool ok = cv::findChessboardCorners(
      to_gray(in), cv::Size(cols, rows), corners,
      cv::CALIB_CB_ADAPTIVE_THRESH | cv::CALIB_CB_NORMALIZE_IMAGE);
  if (ok) cv::drawChessboardCorners(out, cv::Size(cols, rows), corners, true);
}

// 棋盘格位姿估计（solvePnP）：以左上角为原点画 3 条坐标轴到原图（红X 绿Y 蓝Z，长度 1 格）
void ocv_calib_chessboard_pose(int cols, int rows, double square, double fx, double fy,
                               double cx, double cy, const cv::Mat &in, cv::Mat &out) {
  out = to_bgr(in).clone();
  if (out.empty()) return;
  const cv::Mat gray = to_gray(in);
  std::vector<cv::Point2f> corners;
  if (!cv::findChessboardCorners(gray, cv::Size(cols, rows), corners,
                                 cv::CALIB_CB_ADAPTIVE_THRESH | cv::CALIB_CB_NORMALIZE_IMAGE))
    return;

  std::vector<cv::Point3f> obj;   // 棋盘格平面 z=0
  for (int r = 0; r < rows; ++r)
    for (int c = 0; c < cols; ++c)
      obj.emplace_back((float)(c * square), (float)(r * square), 0.f);

  const cv::Mat K = (cv::Mat_<double>(3, 3) << fx, 0, cx, 0, fy, cy, 0, 0, 1);
  const cv::Mat D = cv::Mat::zeros(1, 5, CV_64F);
  cv::Mat rvec, tvec;
  if (!cv::solvePnP(obj, corners, K, D, rvec, tvec)) return;

  const std::vector<cv::Point3f> axes{{0, 0, 0},
                                      {(float)square, 0, 0},
                                      {0, (float)square, 0},
                                      {0, 0, (float)square}};
  std::vector<cv::Point2f> ip;
  cv::projectPoints(axes, rvec, tvec, K, D, ip);
  if (ip.size() != 4) return;
  cv::line(out, ip[0], ip[1], cv::Scalar(0, 0, 255), 2);   // X 红
  cv::line(out, ip[0], ip[2], cv::Scalar(0, 255, 0), 2);   // Y 绿
  cv::line(out, ip[0], ip[3], cv::Scalar(255, 0, 0), 2);   // Z 蓝
}

}  // namespace

#define FINS_ALGO_LIST(F) \
  F(ocv_calib_src_chessboard) \
  F(ocv_calib_undistort)      \
  F(ocv_calib_fisheye_undistort) \
  F(ocv_calib_find_chessboard) \
  F(ocv_calib_chessboard_pose)

FINS_ALGO_EXPORT("1.0.0")
