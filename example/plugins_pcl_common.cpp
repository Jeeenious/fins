/*******************************************************************************
 * plugins_pcl_common.cpp — PCL 公共源/汇（configs-first，pcl::PointCloud 载荷）
 * 载荷 = pcl::PointCloud<PointT>，PointT 目前展开：PointXYZ / PointXYZRGB / Normal。
 * 源：合成点云（无需外部 .pcd 资产即可跑通 demo）：
 *   pcl_src_cube_plane/blob → 供 filters/segmentation/registration 演示；
 * 汇：丢弃点云（占位验证用），按点类型各一。
 ******************************************************************************/
#include <pcl/pcl_config.h>
#include <pcl/point_cloud.h>
#include <pcl/point_types.h>

#include <random>

#include "xmacro.hpp"

namespace {

// 源：均匀立方体 XYZ（默认 half=1：[-1,1]^3），供 passthrough/voxel/去噪等演示
void pcl_src_cube_xyz(int n, double half, pcl::PointCloud<pcl::PointXYZ> &out) {
  const int N = n > 0 ? n : 200;
  const double h = std::abs(half);
  out.width = N; out.height = 1; out.is_dense = true;
  out.points.resize(N);
  std::mt19937 g(20260903u);
  std::uniform_real_distribution<double> u(-h, h);
  for (auto &p : out.points) { p.x = u(g); p.y = u(g); p.z = u(g); }
}

// 源：平面 z≈0（法向噪声 sigma）+ 整体平移 (ox,oy,oz)，供 plane 分割 / ICP 演示
void pcl_src_plane_xyz(int n, double sigma, double ox, double oy, double oz,
                       pcl::PointCloud<pcl::PointXYZ> &out) {
  const int N = n > 0 ? n : 500;
  const double s = sigma > 0 ? sigma : 0.01;
  out.width = N; out.height = 1; out.is_dense = true;
  out.points.resize(N);
  std::mt19937 g(7u);
  std::uniform_real_distribution<double> u(-1.0, 1.0);
  std::normal_distribution<double> nd(0.0, s);
  for (auto &p : out.points) {
    p.x = u(g) + ox; p.y = u(g) + oy; p.z = nd(g) + oz;
  }
}

// 源：3 个高斯点团（x 向 -gap/0/+gap），供欧式聚类(着色/最大团)演示
void pcl_src_blobs_xyz(int per, double sigma, double gap,
                       pcl::PointCloud<pcl::PointXYZ> &out) {
  const int P = per > 0 ? per : 60;
  const double s = sigma > 0 ? sigma : 0.03;
  const double g = gap > 0 ? gap : 0.4;
  out.width = 3 * P; out.height = 1; out.is_dense = true;
  out.points.resize(3 * P);
  std::mt19937 rng(11u);
  std::normal_distribution<double> nd(0.0, s);
  const double cx[3] = {-g, 0.0, g};
  size_t k = 0;
  for (int b = 0; b < 3; ++b)
    for (int i = 0; i < P; ++i, ++k) {
      out.points[k].x = cx[b] + nd(rng);
      out.points[k].y = nd(rng);
      out.points[k].z = nd(rng);
    }
}

// 源：均匀立方体 XYZRGB（逐点随机颜色），供 xyzrgb 链路演示
void pcl_src_cube_xyzrgb(int n, double half, pcl::PointCloud<pcl::PointXYZRGB> &out) {
  const int N = n > 0 ? n : 200;
  const double h = std::abs(half);
  out.width = N; out.height = 1; out.is_dense = true;
  out.points.resize(N);
  std::mt19937 g(20260903u);
  std::uniform_real_distribution<double> u(-h, h);
  std::uniform_int_distribution<int> uc(0, 255);
  for (auto &p : out.points) {
    p.x = u(g); p.y = u(g); p.z = u(g);
    p.r = (std::uint8_t)uc(g); p.g = (std::uint8_t)uc(g); p.b = (std::uint8_t)uc(g);
  }
}

// 汇：丢弃一帧 XYZ（占位验证）
void pcl_sink_xyz(const pcl::PointCloud<pcl::PointXYZ> &in) { (void)in; }
// 汇：丢弃一帧 XYZRGB
void pcl_sink_xyzrgb(const pcl::PointCloud<pcl::PointXYZRGB> &in) { (void)in; }
// 汇：丢弃一帧 Normal
void pcl_sink_normal(const pcl::PointCloud<pcl::Normal> &in) { (void)in; }

}  // namespace

#define FINS_ALGO_LIST(F) \
  F(pcl_src_cube_xyz)     \
  F(pcl_src_plane_xyz)    \
  F(pcl_src_blobs_xyz)    \
  F(pcl_src_cube_xyzrgb)  \
  F(pcl_sink_xyz)         \
  F(pcl_sink_xyzrgb)      \
  F(pcl_sink_normal)

FINS_ALGO_EXPORT("1.0.0")
